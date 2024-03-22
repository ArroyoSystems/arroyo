use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::TableConfig;
use arroyo_rpc::schema_resolver::SchemaResolver;
use arroyo_rpc::ControlResp;

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
    pub consumer: Option<StreamConsumer>,
    pub offset_mode: super::SourceOffset,
    pub offsets: HashMap<i32, i64>,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub schema_resolver: Arc<dyn SchemaResolver + Sync>,
    pub client_configs: HashMap<String, String>,
    pub messages_per_second: NonZeroU32,
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
                        .map(|s| {
                            self.offsets.insert(s.partition, s.offset);
                            Offset::Offset(s.offset)
                        })
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
        self.consumer =
            Some(self.get_consumer(ctx).await.map_err(|e| {
                UserError::new("Could not create Kafka consumer", format!("{:?}", e))
            })?);

        let rate_limiter = GovernorRateLimiter::direct(Quota::per_second(self.messages_per_second));

        if self
            .consumer
            .as_ref()
            .unwrap()
            .assignment()
            .unwrap()
            .count()
            == 0
        {
            warn!("Kafka Consumer {}-{} is subscribed to no partitions, as there are more subtasks than partitions... setting idle",
                ctx.task_info.operator_id, ctx.task_info.task_index);
            ctx.broadcast(ArrowMessage::Signal(SignalMessage::Watermark(
                Watermark::Idle,
            )))
            .await;
        }

        ctx.initialize_deserializer_with_resolver(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            self.schema_resolver.clone(),
        );

        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            select! {
                message = self.consumer.as_ref().unwrap().recv() => {
                    match message {
                        Ok(msg) => {
                            if let Some(v) = msg.payload() {
                                let timestamp = msg.timestamp().to_millis()
                                    .ok_or_else(|| UserError::new("Failed to read timestamp from Kafka record",
                                        "The message read from Kafka did not contain a message timestamp"))?;

                                ctx.deserialize_slice(&v, from_millis(timestamp as u64)).await?;

                                if ctx.should_flush() {
                                    ctx.flush_buffer().await?;
                                }

                                self.offsets.insert(msg.partition(), msg.offset());
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
                    if let Some(control_message) = control_message {
                        if let Some(stop_mode) = self.handle_control_message(ctx, control_message).await {
                            return Ok(stop_mode);
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

    async fn flush_before_checkpoint(&mut self, _cp: CheckpointBarrier, ctx: &mut ArrowContext) {
        debug!("starting checkpointing {}", ctx.task_info.task_index);
        let mut topic_partitions = TopicPartitionList::new();
        // TODO: return this as a UserError rather than panicking.
        let s = ctx
            .table_manager
            .get_global_keyed_state("k")
            .await
            .expect("should be able to get kafka state");
        for (partition, offset) in &self.offsets {
            s.insert(
                *partition,
                KafkaState {
                    partition: *partition,
                    offset: *offset + 1,
                },
            )
            .await;
            topic_partitions
                .add_partition_offset(&self.topic, *partition, Offset::Offset(*offset))
                .unwrap();
        }

        if let Err(e) = self
            .consumer
            .as_ref()
            .unwrap()
            .commit(&topic_partitions, CommitMode::Async)
        {
            // This is just used for progress tracking for metrics, so it's not a fatal error if it
            // fails. The actual offset is stored in state.
            warn!("Failed to commit offset to Kafka {:?}", e);
        }
    }
}
