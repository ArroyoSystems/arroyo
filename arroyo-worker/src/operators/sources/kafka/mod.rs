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
use tracing::{debug, info, warn};

use crate::operators::SerializationMode;

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
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "false")
            .set(
                "group.id",
                format!(
                    "arroyo-{}-{}-consumer",
                    ctx.task_info.job_id, ctx.task_info.operator_id
                ),
            )
            .create()
            .expect("Consumer creation failed");

        let mut s: GlobalKeyedState<i32, KafkaState, _> =
            ctx.state.get_global_keyed_state('k').await;
        let state: Vec<&KafkaState> = s.get_all();

        // did we restore any partitions?
        let has_state = !state.is_empty();

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

        let topic_partitions = TopicPartitionList::from_topic_map(&our_partitions).unwrap();

        consumer.assign(&topic_partitions).unwrap();

        Ok(consumer)
    }

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
        let consumer = self.get_consumer(ctx).await;
        match consumer {
            Ok(consumer) => {
                let rate_limiter = RateLimiter::direct(Quota::per_second(self.messages_per_second));
                let mut offsets = HashMap::new();
                loop {
                    select! {
                        message = consumer.recv() => {
                            match message {
                                Ok(msg) => {
                                    if let Some(v) = msg.payload() {
                                        let deserialized = self.serialization_mode.deserialize_slice(&v);
                                        match deserialized {
                                            Ok(deserialized) => {
                                                ctx.collector.collect(Record {
                                                    timestamp: from_millis(msg.timestamp().to_millis().unwrap() as u64),
                                                    key: None,
                                                    value: deserialized,
                                                }).await;
                                            },
                                            Err(err) => {
                                                 ctx.control_tx.send(
                                                    ControlResp::Error {
                                                        operator_id: ctx.task_info.operator_id.clone(),
                                                        task_index: ctx.task_info.task_index.clone(),
                                                        message: "Error while deserializing Kafka message".to_string(),
                                                        details: format!("{:?}", err)}
                                                ).await.unwrap();
                                                panic!("Error while deserializing Kafka message")
                                            }
                                        }
                                        offsets.insert(msg.partition(), msg.offset());
                                        rate_limiter.until_ready().await;
                                    }
                                },
                                Err(err) => {
                                    ctx.control_tx.send(
                                        ControlResp::Error {
                                            operator_id: ctx.task_info.operator_id.clone(),
                                            task_index: ctx.task_info.task_index.clone(),
                                            message: "Error while reading from Kafka".to_string(),
                                            details: format!("{:?}", err)}
                                    ).await.unwrap();
                                    panic!("Error while reading from Kafka {}", err)
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
            Err(err) => {
                ctx.control_tx
                    .send(ControlResp::Error {
                        operator_id: ctx.task_info.operator_id.clone(),
                        task_index: ctx.task_info.task_index.clone(),
                        message: "Error while getting kafka consumer".to_string(),
                        details: format!("{:?}", err),
                    })
                    .await
                    .unwrap();
                panic!("Error while getting kafka consumer")
            }
        }
    }
}
