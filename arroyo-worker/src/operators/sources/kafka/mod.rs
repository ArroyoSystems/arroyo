use crate::engine::{Context, StreamNode};
use crate::SourceFinishType;
use arroyo_macro::source_fn;
use arroyo_rpc::grpc::TableDescriptor;
use arroyo_rpc::{grpc::StopMode, ControlMessage};
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
use tracing::{debug, error, info};

#[cfg(test)]
mod test;

#[derive(Copy, Clone, Debug)]
pub enum OffsetMode {
    Earliest,
    Latest,
}

impl OffsetMode {
    fn get_offset(&self) -> Offset {
        match self {
            OffsetMode::Earliest => Offset::Beginning,
            OffsetMode::Latest => Offset::End,
        }
    }
}

#[derive(Clone, Copy)]
pub enum SerializationMode {
    Json,
    // https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
    JsonSchemaRegistry,
}

#[derive(StreamNode, Clone)]
pub struct KafkaSourceFunc<T>
where
    T: DeserializeOwned + Data,
{
    topic: String,
    bootstrap_servers: String,
    offset_mode: OffsetMode,
    serialization_mode: SerializationMode,
    client_configs: HashMap<String, String>,
    messages_per_second: NonZeroU32,
    _t: PhantomData<T>,
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
impl<T> KafkaSourceFunc<T>
where
    T: DeserializeOwned + Data,
{
    pub fn new(
        servers: &str,
        topic: &str,
        offset_mode: OffsetMode,
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

    fn name(&self) -> String {
        format!("kafka-{}", self.topic)
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        tables()
    }

    async fn get_consumer(&mut self, ctx: &mut Context<(), T>) -> Result<StreamConsumer, ()> {
        info!("Creating kafka consumer for {}", self.bootstrap_servers);
        let mut client_config = ClientConfig::new();

        for (key, value) in &self.client_configs {
            client_config.set(key, value);
        }
        let consumer: StreamConsumer = client_config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("enable.auto.commit", "false")
            .set(
                "group.id",
                format!(
                    "{}-{}-consumer",
                    ctx.task_info.job_id, ctx.task_info.operator_id
                ),
            )
            .create()
            .expect("Consumer creation failed");

        let mut s: GlobalKeyedState<i32, KafkaState, _> =
            ctx.state.get_global_keyed_state('k').await;
        let state: Vec<&KafkaState> = s.get_all();
        let state: HashMap<i32, KafkaState> = state.iter().map(|s| (s.partition, **s)).collect();
        let metadata = consumer
            .fetch_metadata(Some(&self.topic), Duration::from_secs(30))
            .expect("failed to fetch kafka metadata");

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
                        .unwrap_or_else(|| self.offset_mode.get_offset());

                    ((self.topic.clone(), p.id()), offset)
                })
                .collect()
        };

        let topic_partitions = TopicPartitionList::from_topic_map(&our_partitions).unwrap();

        consumer.assign(&topic_partitions).unwrap();

        Ok(consumer)
    }

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
        let consumer = self.get_consumer(ctx).await.unwrap();

        let rate_limiter = RateLimiter::direct(Quota::per_second(self.messages_per_second));
        let mut offsets = HashMap::new();
        loop {
            select! {
                message = consumer.recv() => {
                    match message {
                        Ok(msg) => {
                            if let Some(v) = msg.payload() {
                                ctx.collector.collect(Record {
                                    timestamp: from_millis(msg.timestamp().to_millis().unwrap() as u64),
                                    key: None,
                                    value: match self.serialization_mode {
                                        SerializationMode::Json => serde_json::from_slice(v)
                                            .unwrap_or_else(|_| panic!("Failed to deserialize message: {}", String::from_utf8_lossy(v))),
                                        SerializationMode::JsonSchemaRegistry => serde_json::from_slice(&v[5..]).unwrap(),
                                    }
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

                            consumer.commit(&topic_partitions, CommitMode::Async).unwrap();
                            if self.checkpoint(c, ctx).await {
                                return SourceFinishType::Immediate;
                            }
                        },
                        Some(ControlMessage::Stop { mode }) => {
                            info!("Stopping kafka source: {:?}", mode);

                            match mode {
                                StopMode::Graceful => {
                                    return SourceFinishType::Graceful;
                                }
                                StopMode::Immediate => {
                                    return SourceFinishType::Immediate;
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
