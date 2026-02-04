use anyhow::{Context as _, bail};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use futures::FutureExt;
use governor::{Quota, RateLimiter as GovernorRateLimiter};
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::{ClientConfig, Message as KMessage, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

use arroyo_formats::de::FieldValueType;
use arroyo_operator::SourceFinishType;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::schema_resolver::SchemaResolver;
use arroyo_rpc::{ControlMessage, MetadataField, connector_err, grpc::rpc::StopMode};
use arroyo_types::*;

use super::{Context, SourceOffset, StreamConsumer};

#[cfg(test)]
mod test;

pub struct KafkaSourceFunc {
    pub topic: Option<String>,
    pub topic_pattern: Option<String>,
    pub bootstrap_servers: String,
    pub group_id: Option<String>,
    pub group_id_prefix: Option<String>,
    pub offset_mode: SourceOffset,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub schema_resolver: Option<Arc<dyn SchemaResolver + Sync>>,
    pub client_configs: HashMap<String, String>,
    pub context: Context,
    pub messages_per_second: NonZeroU32,
    pub metadata_fields: Vec<MetadataField>,
}

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct KafkaState {
    topic: String,
    partition: i32,
    offset: i64,
}

/// Key for storing Kafka state: (topic, partition)
#[derive(Clone, Debug, Encode, Decode, PartialEq, Eq, Hash)]
pub struct KafkaStateKey {
    topic: String,
    partition: i32,
}

impl KafkaSourceFunc {
    /// Returns the topic name for display purposes (either the exact topic or the pattern)
    fn topic_display(&self) -> String {
        self.topic.clone().unwrap_or_else(|| {
            self.topic_pattern
                .clone()
                .map(|p| format!("pattern:{}", p))
                .unwrap_or_else(|| "unknown".to_string())
        })
    }

    async fn get_consumer(&mut self, ctx: &mut SourceContext) -> anyhow::Result<StreamConsumer> {
        info!("Creating kafka consumer for {}", self.bootstrap_servers);
        let mut client_config = ClientConfig::new();

        let group_id = match (&self.group_id, &self.group_id_prefix) {
            (Some(group_id), prefix) => {
                if prefix.is_some() {
                    warn!(
                        "both group_id and group_id_prefix are set for Kafka source; using group_id"
                    );
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
            .set("group.id", &group_id)
            .create_with_context(self.context.clone())?;

        // NOTE: this is required to trigger an oauth token refresh (when using
        // OAUTHBEARER auth).
        if consumer.recv().now_or_never().is_some() {
            bail!("unexpected recv before assignments");
        }

        // Handle topic pattern subscription (uses Kafka's group coordination)
        if let Some(pattern) = &self.topic_pattern {
            info!(
                "Subscribing to topic pattern '{}' with group_id '{}'",
                pattern, group_id
            );

            // rdkafka expects patterns to start with '^' for regex matching
            let pattern_str = if pattern.starts_with('^') {
                pattern.clone()
            } else {
                format!("^{}", pattern)
            };

            consumer
                .subscribe(&[&pattern_str])
                .context("Failed to subscribe to topic pattern")?;

            info!(
                "Successfully subscribed to pattern '{}' - Kafka will handle partition assignment",
                pattern_str
            );

            return Ok(consumer);
        }

        // Handle single topic (existing behavior with manual partition assignment)
        let topic = self.topic.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Either 'topic' or 'topic_pattern' must be specified"))?;

        let state: Vec<_> = ctx
            .table_manager
            .get_global_keyed_state::<KafkaStateKey, KafkaState>("k")
            .await?
            .get_all()
            .values()
            .collect();

        // did we restore any partitions?
        let has_state = !state.is_empty();

        let state: HashMap<(String, i32), KafkaState> = state
            .iter()
            .map(|s| ((s.topic.clone(), s.partition), (*s).clone()))
            .collect();
        let metadata = consumer.fetch_metadata(Some(topic), Duration::from_secs(30))?;

        info!("Fetched metadata for topic {}", topic);

        let our_partitions: HashMap<_, _> = {
            let partitions = metadata.topics()[0].partitions();
            partitions
                .iter()
                .enumerate()
                .filter(|(i, _)| {
                    i % ctx.task_info.parallelism as usize == ctx.task_info.task_index as usize
                })
                .map(|(_, p)| {
                    let offset = state
                        .get(&(topic.clone(), p.id()))
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

                    ((topic.clone(), p.id()), offset)
                })
                .collect()
        };

        info!(
            "partition map for {}-{}: {:?}",
            topic, ctx.task_info.task_index, our_partitions
        );

        let topic_partitions = TopicPartitionList::from_topic_map(&our_partitions)?;

        consumer.assign(&topic_partitions)?;

        Ok(consumer)
    }

    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType> {
        let consumer = self
            .get_consumer(ctx)
            .await
            .context("creating kafka consumer")?;

        let rate_limiter = GovernorRateLimiter::direct(Quota::per_second(self.messages_per_second));
        // Track offsets by (topic, partition) to support multi-topic subscriptions
        let mut offsets: HashMap<(String, i32), i64> = HashMap::new();

        // For pattern subscriptions, we skip the initial assignment check since
        // Kafka handles partition assignment dynamically via the consumer group protocol
        if self.topic_pattern.is_none() && consumer.assignment().unwrap().count() == 0 {
            warn!(
                "Kafka Consumer {}-{} is subscribed to no partitions, as there are more subtasks than partitions... setting idle",
                ctx.task_info.operator_id, ctx.task_info.task_index
            );
            collector
                .broadcast(SignalMessage::Watermark(Watermark::Idle))
                .await;
        }

        if let Some(schema_resolver) = &self.schema_resolver {
            collector.initialize_deserializer_with_resolver(
                self.format.clone(),
                self.framing.clone(),
                self.bad_data.clone(),
                &self.metadata_fields,
                schema_resolver.clone(),
            );
        } else {
            collector.initialize_deserializer(
                self.format.clone(),
                self.framing.clone(),
                self.bad_data.clone(),
                &self.metadata_fields,
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
                                    .ok_or_else(|| connector_err!(External, NoRetry, "Failed to read timestamp from Kafka record: The message read from Kafka did not contain a message timestamp"))?;

                                let topic = msg.topic();

                                let connector_metadata = if !self.metadata_fields.is_empty() {
                                    let mut connector_metadata = HashMap::new();
                                    for f in &self.metadata_fields {
                                        connector_metadata.insert(f.field_name.as_str(), match f.key.as_str() {
                                            "key" => FieldValueType::Bytes(msg.key()),
                                            "offset_id" => FieldValueType::Int64(Some(msg.offset())),
                                            "partition" => FieldValueType::Int32(Some(msg.partition())),
                                            "topic" => FieldValueType::String(Some(topic)),
                                            "timestamp" => FieldValueType::Int64(Some(timestamp)),
                                            k => unreachable!("Invalid metadata key '{}'", k),
                                        });
                                    }
                                    Some(connector_metadata)
                                } else {
                                    None
                                };

                                collector.deserialize_slice(v, from_millis(timestamp.max(0) as u64), connector_metadata.as_ref()).await?;

                                if collector.should_flush() {
                                    collector.flush_buffer().await?;
                                }

                                // Store offset with (topic, partition) key for multi-topic support
                                offsets.insert((topic.to_string(), msg.partition()), msg.offset());
                                rate_limiter.until_ready().await;
                            }
                        },
                        Err(err) => {
                            error!("encountered error {}", err)
                        }
                    }
                }
                _ = flush_ticker.tick() => {
                    if collector.should_flush() {
                        collector.flush_buffer().await?;
                    }
                }
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("starting checkpointing {}", ctx.task_info.task_index);
                            let mut topic_partitions = TopicPartitionList::new();
                            let s = ctx.table_manager.get_global_keyed_state("k").await?;
                            for ((topic, partition), offset) in &offsets {
                                s.insert(
                                    KafkaStateKey {
                                        topic: topic.clone(),
                                        partition: *partition,
                                    },
                                    KafkaState {
                                        topic: topic.clone(),
                                        partition: *partition,
                                        offset: *offset + 1,
                                    }
                                ).await;
                                topic_partitions.add_partition_offset(
                                    topic, *partition, Offset::Offset(*offset)).unwrap();
                            }

                            if let Err(e) = consumer.commit(&topic_partitions, CommitMode::Async) {
                                // This is just used for progress tracking for metrics, so it's not a fatal error if it
                                // fails. The actual offset is stored in state.
                                warn!("Failed to commit offset to Kafka {:?}", e);
                            }
                            if self.start_checkpoint(c, ctx, collector).await {
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
    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> DataflowResult<SourceFinishType> {
        self.run_int(ctx, collector).await
    }

    fn name(&self) -> String {
        format!("kafka-{}", self.topic_display())
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("k", "kafka offsets")
    }
}
