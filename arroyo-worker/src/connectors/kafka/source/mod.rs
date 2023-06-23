use crate::connectors::{OperatorConfig, OperatorConfigSerializationMode};
use crate::engine::{Context, StreamNode};
use crate::SourceFinishType;
use arroyo_macro::source_fn;
use arroyo_rpc::grpc::TableDescriptor;
use arroyo_rpc::{grpc::StopMode, ControlMessage, ControlResp};
use arroyo_state::tables::GlobalKeyedState;
use arroyo_types::*;
use bincode::{Decode, Encode};
use governor::{Quota, RateLimiter};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message as KMessage, Offset, TopicPartitionList};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::time::Duration;
use tokio::select;
use tracing::{debug, error, info, warn};

use crate::operators::{SerializationMode, UserError};

use super::{KafkaConfig, KafkaTable, TableType};

#[cfg(test)]
mod test;

#[derive(StreamNode, Clone)]
pub struct KafkaSourceFunc<K, T>
where
    K: DeserializeOwned + Data,
    T: DeserializeOwned + Data,
{
    topic: String,
    bootstrap_servers: String,
    offset_mode: super::SourceOffset,
    serialization_mode: SerializationMode,
    client_configs: HashMap<String, String>,
    messages_per_second: NonZeroU32,
    _t: PhantomData<(K, T)>,
}

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct KafkaState {
    partition: i32,
    offset: i64,
}

pub fn tables() -> Vec<TableDescriptor> {
    vec![arroyo_state::global_table("k", "kafka source state")]
}

#[source_fn(out_k = (), out_t = T)]
impl<K, T> KafkaSourceFunc<K, T>
where
    K: DeserializeOwned + Data,
    T: DeserializeOwned + Data,
{
    pub fn new(
        servers: &str,
        topic: &str,
        offset_mode: super::SourceOffset,
        serialization_mode: SerializationMode,
        messages_per_second: u32,
        client_configs: Vec<(&str, &str)>,
    ) -> Self {
        Self {
            topic: topic.to_string(),
            bootstrap_servers: servers.to_string(),
            offset_mode,
            serialization_mode,
            client_configs: client_configs
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            messages_per_second: NonZeroU32::new(messages_per_second).unwrap(),
            _t: PhantomData,
        }
    }

    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for KafkaSource");
        let connection: KafkaConfig = serde_json::from_value(config.connection)
            .expect("Invalid connection config for KafkaSource");
        let table: KafkaTable =
            serde_json::from_value(config.table).expect("Invalid table config for KafkaSource");
        let TableType::Source{ offset, .. } = &table.type_ else {
            panic!("found non-source kafka config in source operator");
        };


        let mut client_configs: HashMap<String, String> = HashMap::new();

        match connection.authentication {
            None | Some(super::KafkaConfigAuthentication::None {  }) => {},
            Some(super::KafkaConfigAuthentication::Sasl {
                mechanism,
                password,
                protocol,
                username }) =>
                {
                    client_configs.insert("sasl.mechanism".to_string(), mechanism);
                    client_configs.insert("security.protocol".to_string(), protocol);
                    client_configs.insert("sasl.username".to_string(), username);
                    client_configs.insert("sasl.password".to_string(), password);
                }
        };

        Self {
            topic: table.topic,
            bootstrap_servers: connection.bootstrap_servers.to_string(),
            offset_mode: *offset,
            serialization_mode: match config.serialization_mode {
                OperatorConfigSerializationMode::Json => SerializationMode::Json,
                OperatorConfigSerializationMode::JsonSchemaRegistry => {
                    SerializationMode::JsonSchemaRegistry
                }
                OperatorConfigSerializationMode::RawJson => SerializationMode::RawJson,
                OperatorConfigSerializationMode::DebeziumJson => todo!(),
            },
            client_configs,
            messages_per_second: NonZeroU32::new(100000).unwrap(),
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        format!("kafka-{}", self.topic)
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        tables()
    }

    async fn get_consumer(&mut self, ctx: &mut Context<(), T>) -> anyhow::Result<StreamConsumer> {
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
                format!(
                    "arroyo-{}-{}-consumer",
                    ctx.task_info.job_id, ctx.task_info.operator_id
                ),
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

        let topic_partitions = TopicPartitionList::from_topic_map(&our_partitions)?;

        consumer.assign(&topic_partitions)?;

        Ok(consumer)
    }

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
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

    async fn run_int(&mut self, ctx: &mut Context<(), T>) -> Result<SourceFinishType, UserError> {
        let consumer = self
            .get_consumer(ctx)
            .await
            .map_err(|e| UserError::new("Could not create Kafka consumer", format!("{:?}", e)))?;

        let rate_limiter = RateLimiter::direct(Quota::per_second(self.messages_per_second));
        let mut offsets = HashMap::new();
        loop {
            select! {
                message = consumer.recv() => {
                    match message {
                        Ok(msg) => {
                            if let Some(v) = msg.payload() {
                                let timestamp = msg.timestamp().to_millis()
                                    .ok_or_else(|| UserError::new("Failed to read timestamp from Kafka record",
                                        "The message read from Kafka did not contain a message timestamp"))?;

                                ctx.collector.collect(Record {
                                    timestamp: from_millis(timestamp as u64),
                                    key: None,
                                    value: self.serialization_mode.deserialize_slice(v)?,
                                }).await;
                                offsets.insert(msg.partition(), msg.offset());
                                rate_limiter.until_ready().await;
                            }
                        },
                        Err(err) => {
                            error!("encountered error {}", err)
                        }
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
                        None => {

                        }
                    }

                }
            }
        }
    }
}
