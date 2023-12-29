use crate::engine::ArrowContext;
use crate::operator::{ArrowOperatorConstructor, BaseOperator};
use crate::SourceFinishType;
use arroyo_formats::ArrowDeserializer;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::api::ConnectorOp;
use arroyo_rpc::grpc::{api, TableDescriptor};
use arroyo_rpc::schema_resolver::{ConfluentSchemaRegistry, FailingSchemaResolver, SchemaResolver};
use arroyo_rpc::OperatorConfig;
use arroyo_rpc::{grpc::StopMode, ControlMessage, ControlResp};
use arroyo_state::tables::global_keyed_map::GlobalKeyedState;
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
use tokio::sync::mpsc::Receiver;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

use super::{client_configs, KafkaConfig, KafkaTable, ReadMode, SchemaRegistry, TableType};

#[cfg(test)]
mod test;

pub struct KafkaSourceFunc {
    topic: String,
    bootstrap_servers: String,
    group_id: Option<String>,
    offset_mode: super::SourceOffset,
    format: Format,
    framing: Option<Framing>,
    bad_data: Option<BadData>,
    schema_resolver: Arc<dyn SchemaResolver + Sync>,
    client_configs: HashMap<String, String>,
    messages_per_second: NonZeroU32,
}

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct KafkaState {
    partition: i32,
    offset: i64,
}

pub fn tables() -> Vec<TableDescriptor> {
    vec![arroyo_state::global_table("k", "kafka source state")]
}

impl KafkaSourceFunc {
    pub fn new(
        servers: &str,
        topic: &str,
        group: Option<String>,
        offset_mode: super::SourceOffset,
        format: Format,
        schema_resolver: Arc<dyn SchemaResolver + Sync>,
        bad_data: Option<BadData>,
        framing: Option<Framing>,
        messages_per_second: u32,
        client_configs: Vec<(&str, &str)>,
    ) -> Self {
        Self {
            topic: topic.to_string(),
            bootstrap_servers: servers.to_string(),
            group_id: group,
            offset_mode,
            format,
            framing,
            schema_resolver,
            bad_data,
            client_configs: client_configs
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            messages_per_second: NonZeroU32::new(messages_per_second).unwrap(),
        }
    }

    async fn get_consumer(&mut self, ctx: &mut ArrowContext) -> anyhow::Result<StreamConsumer> {
        info!("Creating kafka consumer for {}", self.bootstrap_servers);
        let mut client_config = ClientConfig::new();

        for (key, value) in &self.client_configs {
            client_config.set(key, value);
        }
        let consumer: StreamConsumer = client_config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "false")
            .set(
                "group.id",
                self.group_id.clone().unwrap_or_else(|| {
                    format!(
                        "arroyo-{}-{}-consumer",
                        ctx.task_info.job_id, ctx.task_info.operator_id
                    )
                }),
            )
            .create()?;

        let mut s: GlobalKeyedState<i32, KafkaState, _> =
            ctx.state.get_global_keyed_state('k').await;
        let state: Vec<&KafkaState> = s.get_all();

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

        let mut deserializer = ArrowDeserializer::with_schema_resolver(
            self.format.clone(),
            self.framing.clone(),
            ctx.out_schema
                .as_ref()
                .expect("kafka source must have an out schema")
                .clone(),
            self.schema_resolver.clone(),
        );

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

                                let errors = deserializer.deserialize_slice(ctx.buffer(), &v, from_millis(timestamp as u64)).await;
                                ctx.collect_source_errors(errors, &self.bad_data).await?;

                                if ctx.should_flush() {
                                    ctx.flush_buffer().await;
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
                        ctx.flush_buffer().await;
                    }
                }
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("starting checkpointing {}", ctx.task_info.task_index);
                            let mut topic_partitions = TopicPartitionList::new();
                            let mut s = ctx.state.get_global_keyed_state('k').await;
                            for (partition, offset) in &offsets {
                                let partition2 = partition;
                                s.insert(*partition, KafkaState {
                                    partition: *partition2,
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
                            if self.checkpoint(c, ctx).await {
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

impl ArrowOperatorConstructor<api::ConnectorOp, Self> for KafkaSourceFunc {
    fn from_config(config: ConnectorOp) -> anyhow::Result<Self> {
        let config: OperatorConfig =
            serde_json::from_str(&config.config).expect("Invalid config for KafkaSource");
        let connection: KafkaConfig = serde_json::from_value(config.connection)
            .expect("Invalid connection config for KafkaSource");
        let table: KafkaTable =
            serde_json::from_value(config.table).expect("Invalid table config for KafkaSource");
        let TableType::Source {
            offset,
            read_mode,
            group_id,
        } = &table.type_
        else {
            panic!("found non-source kafka config in source operator");
        };
        let mut client_configs = client_configs(&connection, &table);
        if let Some(ReadMode::ReadCommitted) = read_mode {
            client_configs.insert("isolation.level".to_string(), "read_committed".to_string());
        }

        let schema_resolver: Arc<dyn SchemaResolver + Sync> =
            if let Some(SchemaRegistry::ConfluentSchemaRegistry {
                endpoint,
                api_key,
                api_secret,
            }) = &connection.schema_registry_enum
            {
                Arc::new(
                    ConfluentSchemaRegistry::new(
                        &endpoint,
                        &table.topic,
                        api_key.clone(),
                        api_secret.clone(),
                    )
                    .expect("failed to construct confluent schema resolver"),
                )
            } else {
                Arc::new(FailingSchemaResolver::new())
            };

        Ok(Self {
            topic: table.topic,
            bootstrap_servers: connection.bootstrap_servers.to_string(),
            group_id: group_id.clone(),
            offset_mode: *offset,
            format: config.format.expect("Format must be set for Kafka source"),
            framing: config.framing,
            schema_resolver,
            bad_data: config.bad_data,
            client_configs,
            messages_per_second: NonZeroU32::new(
                config
                    .rate_limit
                    .map(|l| l.messages_per_second)
                    .unwrap_or(u32::MAX),
            )
            .unwrap(),
        })
    }
}

#[async_trait]
impl BaseOperator for KafkaSourceFunc {
    async fn run_behavior(
        mut self: Box<Self>,
        ctx: &mut ArrowContext,
        _: Vec<Receiver<ArrowMessage>>,
    ) -> Option<ArrowMessage> {
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
        .into()
    }

    fn name(&self) -> String {
        format!("kafka-{}", self.topic)
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        tables()
    }
}
