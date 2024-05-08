use anyhow::anyhow;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::TableConfig;
use arroyo_rpc::{grpc::StopMode, ControlMessage};
use arroyo_state::global_table_config;
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::*;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use fluvio::dataplane::link::ErrorCode;
use fluvio::metadata::objects::Metadata;
use fluvio::metadata::topic::TopicSpec;
use fluvio::{consumer::Record as ConsumerRecord, Fluvio, FluvioConfig, Offset};
use std::collections::HashMap;
use std::time::Duration;
use tokio::select;
use tokio::time::MissedTickBehavior;
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::{debug, error, info, warn};

use super::SourceOffset;

pub struct FluvioSourceFunc {
    pub topic: String,
    pub endpoint: Option<String>,
    pub offset_mode: SourceOffset,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
}

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct FluvioState {
    partition: u32,
    offset: i64,
}

#[async_trait]
impl SourceOperator for FluvioSourceFunc {
    fn name(&self) -> String {
        format!("fluvio-{}", self.topic)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("f", "fluvio source state")
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        ctx.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
        );
    }

    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType {
        match self.run_int(ctx).await {
            Ok(r) => r,
            Err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;

                panic!("{}: {}", e.name, e.details);
            }
        }
    }
}

impl FluvioSourceFunc {
    async fn get_consumer(
        &mut self,
        ctx: &mut ArrowContext,
    ) -> anyhow::Result<StreamMap<u32, impl Stream<Item = Result<ConsumerRecord, ErrorCode>>>> {
        info!("Creating Fluvio consumer for {:?}", self.endpoint);

        let config: Option<FluvioConfig> = self.endpoint.as_ref().map(FluvioConfig::new);

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

        let s: &mut GlobalKeyedView<u32, FluvioState> = ctx
            .table_manager
            .get_global_keyed_state("f")
            .await
            .expect("should be able to get fluvio state");
        let state: HashMap<u32, FluvioState> = s.get_all().clone();

        // did we restore any partitions?
        let has_state = !state.is_empty();

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
            #[allow(deprecated)]
            let c = client.partition_consumer(self.topic.clone(), p).await?;
            info!("Starting partition {} at offset {:?}", p, offset);
            #[allow(deprecated)]
            streams.insert(p, c.stream(offset).await?);
        }

        Ok(streams)
    }

    async fn run_int(&mut self, ctx: &mut ArrowContext) -> Result<SourceFinishType, UserError> {
        let mut streams = self
            .get_consumer(ctx)
            .await
            .map_err(|e| UserError::new("Could not create Fluvio consumer", format!("{:?}", e)))?;

        if streams.is_empty() {
            warn!("Fluvio Consumer {}-{} is subscribed to no partitions, as there are more subtasks than partitions... setting idle",
                ctx.task_info.operator_id, ctx.task_info.task_index);
            ctx.broadcast(ArrowMessage::Signal(SignalMessage::Watermark(
                Watermark::Idle,
            )))
            .await;
        }

        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut offsets = HashMap::new();
        loop {
            select! {
                message = streams.next() => {
                    match message {
                        Some((_, Ok(msg))) => {
                            let timestamp = from_millis(msg.timestamp().max(0) as u64);
                            ctx.deserialize_slice(msg.value(), timestamp).await?;

                            if ctx.should_flush() {
                                ctx.flush_buffer().await?;
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
                _ = flush_ticker.tick() => {
                    if ctx.should_flush() {
                        ctx.flush_buffer().await?;
                    }
                }
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("starting checkpointing {}", ctx.task_info.task_index);
                            let s = ctx.table_manager.get_global_keyed_state("f")
                               .await
                               .expect("should be able to get fluvio state");
                            for (partition, offset) in &offsets {
                                let partition2 = partition;
                                s.insert(*partition, FluvioState {
                                    partition: *partition2,
                                    offset: *offset + 1,
                                }).await;
                            }

                            if self.start_checkpoint(c, ctx).await {
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
