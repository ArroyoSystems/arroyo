use crate::engine::{Context, StreamNode};
use crate::formats::DataDeserializer;
use crate::{SchemaData, SourceFinishType};
use anyhow::anyhow;
use arroyo_macro::source_fn;
use arroyo_rpc::formats::{Format, Framing};
use arroyo_rpc::grpc::TableDescriptor;
use arroyo_rpc::OperatorConfig;
use arroyo_rpc::{grpc::StopMode, ControlMessage};
use arroyo_state::tables::global_keyed_map::GlobalKeyedState;
use arroyo_types::*;
use bincode::{Decode, Encode};
use fluvio::dataplane::link::ErrorCode;
use fluvio::metadata::objects::Metadata;
use fluvio::metadata::topic::TopicSpec;
use fluvio::{consumer::Record as ConsumerRecord, Fluvio, FluvioConfig, Offset};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::marker::PhantomData;
use tokio::select;
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::{debug, error, info, warn};

use super::{FluvioTable, SourceOffset, TableType};

#[derive(StreamNode)]
pub struct FluvioSourceFunc<K, T>
where
    K: DeserializeOwned + Data,
    T: SchemaData,
{
    topic: String,
    endpoint: Option<String>,
    offset_mode: SourceOffset,
    deserializer: DataDeserializer<T>,
    _t: PhantomData<K>,
}

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct FluvioState {
    partition: u32,
    offset: i64,
}

pub fn tables() -> Vec<TableDescriptor> {
    vec![arroyo_state::global_table("f", "Fluvio source state")]
}

#[source_fn(out_k = (), out_t = T)]
impl<K, T> FluvioSourceFunc<K, T>
where
    K: DeserializeOwned + Data,
    T: SchemaData,
{
    pub fn new(
        endpoint: Option<&str>,
        topic: &str,
        offset_mode: SourceOffset,
        format: Format,
        framing: Option<Framing>,
    ) -> Self {
        Self {
            topic: topic.to_string(),
            endpoint: endpoint.map(|e| e.to_string()),
            offset_mode,
            deserializer: DataDeserializer::new(format, framing),
            _t: PhantomData,
        }
    }

    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for FluvioSource");
        let table: FluvioTable =
            serde_json::from_value(config.table).expect("Invalid table config for FluvioSource");
        let TableType::Source { offset, .. } = &table.type_ else {
            panic!("found non-source Fluvio config in source operator");
        };

        Self {
            topic: table.topic,
            endpoint: table.endpoint.clone(),
            offset_mode: *offset,
            deserializer: DataDeserializer::new(
                config.format.expect("Format must be specified for fluvio"),
                config.framing,
            ),
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        format!("fluvio-{}", self.topic)
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        tables()
    }

    async fn get_consumer(
        &mut self,
        ctx: &mut Context<(), T>,
    ) -> anyhow::Result<StreamMap<u32, impl Stream<Item = Result<ConsumerRecord, ErrorCode>>>> {
        // anyhow::Result<Vec<impl Stream<Item = >>> {
        info!("Creating Fluvio consumer for {:?}", self.endpoint);

        let config: Option<FluvioConfig> = self
            .endpoint
            .as_ref()
            .map(|endpoint| FluvioConfig::new(endpoint));

        let client = if let Some(config) = &config {
            Fluvio::connect_with_config(config).await?
        } else {
            Fluvio::connect().await?
        };

        let admin = client.admin().await;
        let metadata: Metadata<TopicSpec> = admin
            .list(vec![self.topic.clone()])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                anyhow!(
                    "Could not fetch metadata for topic {}; may not exist",
                    self.topic
                )
            })?;

        let partitions = metadata.spec.partitions() as usize;
        info!("Fetched metadata for topic {}", self.topic);

        let mut s: GlobalKeyedState<u32, FluvioState, _> =
            ctx.state.get_global_keyed_state('f').await;
        let state: Vec<&FluvioState> = s.get_all();

        // did we restore any partitions?
        let has_state = !state.is_empty();

        let state: HashMap<u32, FluvioState> = state.iter().map(|s| (s.partition, **s)).collect();

        let parts: Vec<_> = (0..partitions)
            .filter(|i| *i % ctx.task_info.parallelism == ctx.task_info.task_index)
            .map(|i| {
                let offset = state
                    .get(&(i as u32))
                    .map(|s| Offset::absolute(s.offset).unwrap())
                    .unwrap_or_else(|| {
                        if has_state {
                            // if we've restored partitions and we don't know about this one, that means it's
                            // new, and we want to start from the beginning so we don't drop data
                            Offset::beginning()
                        } else {
                            self.offset_mode.offset()
                        }
                    });

                (i as u32, offset)
            })
            .collect();

        let mut streams = StreamMap::new();
        for (p, offset) in parts {
            let c = client.partition_consumer(self.topic.clone(), p).await?;
            info!("Starting partition {} at offset {:?}", p, offset);
            streams.insert(p, c.stream(offset).await?);
        }

        Ok(streams)
    }

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
        match self.run_int(ctx).await {
            Ok(r) => r,
            Err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;

                panic!("{}: {}", e.name, e.details);
            }
        }
    }

    async fn run_int(&mut self, ctx: &mut Context<(), T>) -> Result<SourceFinishType, UserError> {
        let mut streams = self
            .get_consumer(ctx)
            .await
            .map_err(|e| UserError::new("Could not create Fluvio consumer", format!("{:?}", e)))?;

        if streams.is_empty() {
            warn!("Fluvio Consumer {}-{} is subscribed to no partitions, as there are more subtasks than partitions... setting idle",
                ctx.task_info.operator_id, ctx.task_info.task_index);
            ctx.broadcast(Message::Watermark(Watermark::Idle)).await;
        }

        let mut offsets = HashMap::new();
        loop {
            select! {
                message = streams.next() => {
                    match message {
                        Some((_, Ok(msg))) => {
                            let timestamp = from_millis(msg.timestamp().max(0) as u64);
                            let iter = self.deserializer.deserialize_slice(msg.value()).await;
                            for value in iter {
                                ctx.collector.collect(Record {
                                    timestamp,
                                    key: None,
                                    value: value?,
                                }).await;
                            }
                            offsets.insert(msg.partition(), msg.offset());
                        },
                        Some((p, Err(e))) => {
                            error!("encountered error {:?} while reading partition {}", e, p);
                        }
                        None => {
                            panic!("Stream closed");
                        }
                    }
                }
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("starting checkpointing {}", ctx.task_info.task_index);
                            let mut s = ctx.state.get_global_keyed_state('f').await;
                            for (partition, offset) in &offsets {
                                let partition2 = partition;
                                s.insert(*partition, FluvioState {
                                    partition: *partition2,
                                    offset: *offset + 1,
                                }).await;
                            }

                            if self.checkpoint(c, ctx).await {
                                return Ok(SourceFinishType::Immediate);
                            }
                        },
                        Some(ControlMessage::Stop { mode }) => {
                            info!("Stopping Fluvio source: {:?}", mode);

                            match mode {
                                StopMode::Graceful => {
                                    return Ok(SourceFinishType::Graceful);
                                }
                                StopMode::Immediate => {
                                    return Ok(SourceFinishType::Immediate);
                                }
                            }
                        }
                        Some(ControlMessage::Commit{..}) => {
                            return Err(UserError::new("Fluvio source does not support committing", ""));
                        }
                        Some(ControlMessage::LoadCompacted {compacted}) => {
                            ctx.load_compacted(compacted).await;
                        }
                        Some(ControlMessage::NoOp ) => {}
                        None => {

                        }
                    }

                }
            }
        }
    }
}
