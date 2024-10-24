use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::schema_resolver::SchemaResolver;
use arroyo_rpc::{grpc::rpc::StopMode, ControlMessage, ControlResp};

use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_types::*;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use governor::{Quota, RateLimiter as GovernorRateLimiter};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message as KMessage, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

#[cfg(test)]
mod test;

pub struct KafkaSourceFunc {
    pub topic: String,
    pub bootstrap_servers: String,
    pub group_id: Option<String>,
    pub group_id_prefix: Option<String>,
    pub offset_mode: super::SourceOffset,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub schema_resolver: Option<Arc<dyn SchemaResolver + Sync>>,
    pub client_configs: HashMap<String, String>,
    pub messages_per_second: NonZeroU32,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct KafkaMetadata {
    pub offset_id: i64,
    pub partition: i32,
    pub topic_name: String,
}

impl KafkaMetadata {
    pub fn get(&self, key: &str) -> Option<serde_json::Value> {
        match key {
            "offset_id" => Some(serde_json::Value::Number(serde_json::Number::from(
                self.offset_id,
            ))),
            "partition" => Some(serde_json::Value::Number(serde_json::Number::from(
                self.partition,
            ))),
            "topic" => Some(serde_json::Value::String(self.topic_name.clone())),
            _ => None,
        }
    }
}

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct KafkaState {
    partition: i32,
    offset: i64,
}

impl KafkaSourceFunc {
    async fn get_consumer(&mut self, ctx: &mut ArrowContext) -> anyhow::Result<StreamConsumer> {
        info!("Creating kafka consumer for {}", self.bootstrap_servers);
        let mut client_config = ClientConfig::new();

        let group_id = match (&self.group_id, &self.group_id_prefix) {
            (Some(group_id), prefix) => {
                if prefix.is_some() {
                    warn!("both group_id and group_id_prefix are set for Kafka source; using group_id");
                }
                group_id.clone()
            }
            (None, Some(prefix)) => {
                format!(
                    "{}-arroyo-{}-{}",
                    prefix, ctx.task_info.job_id, ctx.task_info.operator_id
                )
            }
            (None, None) => {
                format!(
                    "arroyo-{}-{}-consumer",
                    ctx.task_info.job_id, ctx.task_info.operator_id
                )
            }
        };

        for (key, value) in &self.client_configs {
            client_config.set(key, value);
        }
        let consumer: StreamConsumer = client_config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "false")
            .set("group.id", group_id)
            .create()?;

        let state: Vec<_> = ctx
            .table_manager
            .get_global_keyed_state::<i32, KafkaState>("k")
            .await?
            .get_all()
            .values()
            .collect();

        // did we restore any partitions?
        let has_state = !state.is_empty();

        let state: HashMap<i32, KafkaState> = state.iter().map(|s| (s.partition, **s)).collect();
        let metadata = consumer.fetch_metadata(Some(&self.topic), Duration::from_secs(30))?;

        info!("Fetched metadata for topic {}", self.topic);

        let our_partitions: HashMap<_, _> = {
            let partitions = metadata.topics()[0].partitions();
            partitions
                .iter()
                .enumerate()
                .filter(|(i, _)| i % ctx.task_info.parallelism == ctx.task_info.task_index)
                .map(|(_, p)| {
                    let offset = state
                        .get(&p.id())
                        .map(|s| Offset::Offset(s.offset))
                        .unwrap_or_else(|| {
                            if has_state {
                                // if we've restored partitions and we don't know about this one, that means it's
                                // new, and we want to start from the beginning so we don't drop data
                                Offset::Beginning
                            } else {
                                self.offset_mode.get_offset()
                            }
                        });

                    ((self.topic.clone(), p.id()), offset)
                })
                .collect()
        };

        info!(
            "partition map for {}-{}: {:?}",
            self.topic, ctx.task_info.task_index, our_partitions
        );

        let topic_partitions = TopicPartitionList::from_topic_map(&our_partitions)?;

        consumer.assign(&topic_partitions)?;

        Ok(consumer)
    }

    async fn run_int(&mut self, ctx: &mut ArrowContext) -> Result<SourceFinishType, UserError> {
        let consumer = self
            .get_consumer(ctx)
            .await
            .map_err(|e| UserError::new("Could not create Kafka consumer", format!("{:?}", e)))?;

        let rate_limiter = GovernorRateLimiter::direct(Quota::per_second(self.messages_per_second));
        let mut offsets = HashMap::new();

        if consumer.assignment().unwrap().count() == 0 {
            warn!("Kafka Consumer {}-{} is subscribed to no partitions, as there are more subtasks than partitions... setting idle",
                ctx.task_info.operator_id, ctx.task_info.task_index);
            ctx.broadcast(ArrowMessage::Signal(SignalMessage::Watermark(
                Watermark::Idle,
            )))
            .await;
        }

        if let Some(schema_resolver) = &self.schema_resolver {
            ctx.initialize_deserializer_with_resolver(
                self.format.clone(),
                self.framing.clone(),
                self.bad_data.clone(),
                schema_resolver.clone(),
            );
        } else {
            ctx.initialize_deserializer(
                self.format.clone(),
                self.framing.clone(),
                self.bad_data.clone(),
            );
        }

        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            select! {
                message = consumer.recv() => {
                    match message {
                        Ok(msg) => {
                            if let Some(v) = msg.payload() {
                                let timestamp = msg.timestamp().to_millis()
                                    .ok_or_else(|| UserError::new("Failed to read timestamp from Kafka record",
                                        "The message read from Kafka did not contain a message timestamp"))?;
                                let metadata = KafkaMetadata {
                                    offset_id: msg.offset(),
                                    partition: msg.partition(),
                                    topic_name: self.topic.clone(),
                                };

                                let mut deser = serde_json::from_slice::<serde_json::Value>(&v)
                                .map_err(|e| UserError::new("Failed to deserialize JSON", e.to_string()))?;

                                if let Some(schema) = &ctx.out_schema {
                                    for field in schema.schema.fields() {
                                        if let Some(metadata_key) = field.metadata().get("metadata_argument_") {
                                            if let Some(value) = metadata.get(metadata_key) {
                                                if let serde_json::Value::Object(ref mut map) = deser {
                                                    map.insert(field.name().to_string(), value.clone());
                                                }
                                            } else {
                                                error!("Unsupported metadata field: {}", metadata_key);
                                            }
                                        }
                                    }
                                }
                                let updated_json = serde_json::to_vec(&deser)
                                .map_err(|e| UserError::new("Failed to serialize updated JSON", e.to_string()))?;

                                ctx.deserialize_slice(&updated_json, from_millis(timestamp as u64)).await?;

                                if ctx.should_flush() {
                                    ctx.flush_buffer().await?;
                                }

                                offsets.insert(msg.partition(), msg.offset());
                                rate_limiter.until_ready().await;
                            }
                        },
                        Err(err) => {
                            error!("encountered error {}", err)
                        }
                    }
                }
                _ = flush_ticker.tick() => {
                    if ctx.should_flush() {
                        ctx.flush_buffer().await?;
                    }
                }
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("starting checkpointing {}", ctx.task_info.task_index);
                            let mut topic_partitions = TopicPartitionList::new();
                            let s = ctx.table_manager.get_global_keyed_state("k").await
                                .map_err(|err| UserError::new("failed to get global key value", err.to_string()))?;
                            for (partition, offset) in &offsets {
                                s.insert(*partition, KafkaState {
                                    partition: *partition,
                                    offset: *offset + 1,
                                }).await;
                                topic_partitions.add_partition_offset(
                                    &self.topic, *partition, Offset::Offset(*offset)).unwrap();
                            }

                            if let Err(e) = consumer.commit(&topic_partitions, CommitMode::Async) {
                                // This is just used for progress tracking for metrics, so it's not a fatal error if it
                                // fails. The actual offset is stored in state.
                                warn!("Failed to commit offset to Kafka {:?}", e);
                            }
                            if self.start_checkpoint(c, ctx).await {
                                return Ok(SourceFinishType::Immediate);
                            }
                        },
                        Some(ControlMessage::Stop { mode }) => {
                            info!("Stopping kafka source: {:?}", mode);

                            match mode {
                                StopMode::Graceful => {
                                    return Ok(SourceFinishType::Graceful);
                                }
                                StopMode::Immediate => {
                                    return Ok(SourceFinishType::Immediate);
                                }
                            }
                        }
                        Some(ControlMessage::Commit { .. }) => {
                            unreachable!("sources shouldn't receive commit messages");
                        }
                        Some(ControlMessage::LoadCompacted {compacted}) => {
                            ctx.load_compacted(compacted).await;
                        }
                        Some(ControlMessage::NoOp) => {}
                        None => {

                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl SourceOperator for KafkaSourceFunc {
    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType {
        match self.run_int(ctx).await {
            Ok(r) => r,
            Err(e) => {
                ctx.control_tx
                    .send(ControlResp::Error {
                        operator_id: ctx.task_info.operator_id.clone(),
                        task_index: ctx.task_info.task_index,
                        message: e.name.clone(),
                        details: e.details.clone(),
                    })
                    .await
                    .unwrap();

                panic!("{}: {}", e.name, e.details);
            }
        }
    }

    fn name(&self) -> String {
        format!("kafka-{}", self.topic)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("k", "kafka offsets")
    }
}
