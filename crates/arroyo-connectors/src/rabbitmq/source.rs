use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use super::{RabbitmqStreamConfig, SourceOffset};
use arroyo_operator::context::SourceContext;
use arroyo_operator::{context::SourceCollector, operator::SourceOperator, SourceFinishType};
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::{grpc::rpc::StopMode, ControlMessage};
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_rpc::errors::UserError;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use rabbitmq_stream_client::types::OffsetSpecification;
use rabbitmq_stream_client::Consumer;
use tokio::{select, time::MissedTickBehavior};
use tokio_stream::StreamExt;
use tracing::{debug, error, info};

pub struct RabbitmqStreamSourceFunc {
    pub config: RabbitmqStreamConfig,
    pub stream: String,
    pub offset_mode: SourceOffset,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
}

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct RabbitmqStreamState {
    offset: u64,
}

#[async_trait]
impl SourceOperator for RabbitmqStreamSourceFunc {
    fn name(&self) -> String {
        format!("rabbitmq-stream-{}", self.stream)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("s", "rabbitmq stream source state")
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &[],
        );

        match self.run_int(ctx, collector).await {
            Ok(r) => r,
            Err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;

                panic!("{}: {}", e.name, e.details);
            }
        }
    }
}

impl RabbitmqStreamSourceFunc {
    async fn get_consumer(&mut self, ctx: &mut SourceContext) -> anyhow::Result<Consumer> {
        info!(
            "Creating rabbitmq stream consumer for {}",
            self.config.host.clone().unwrap_or("localhost".to_string())
        );
        let environment = self.config.get_environment().await?;

        let s: &mut GlobalKeyedView<String, RabbitmqStreamState> = ctx
            .table_manager
            .get_global_keyed_state("s")
            .await
            .expect("should be able to get rabbitmq stream state");
        let state: HashMap<String, RabbitmqStreamState> = s.get_all().clone();

        let offset = state
            .get(&self.stream)
            .map(|s| OffsetSpecification::Offset(s.offset))
            .unwrap_or_else(|| self.offset_mode.offset());

        let consumer = environment
            .consumer()
            .offset(offset)
            .client_provided_name(&ctx.task_info.operator_name)
            .build(&self.stream)
            .await?;

        Ok(consumer)
    }

    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, UserError> {
        let mut consumer = self.get_consumer(ctx).await.map_err(|e| {
            UserError::new(
                "Could not create RabbitMQ Stream consumer",
                format!("{e:?}"),
            )
        })?;

        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut offset: u64 = 0;

        loop {
            select! {
                delivery = consumer.next() => {
                    match delivery {
                        Some(Ok(delivery)) => {
                            let message = delivery.message();

                            if let Some(data) = message.data() {
                                let timestamp = SystemTime::now();
                                collector.deserialize_slice(data, timestamp, None).await?;
                            }

                            if collector.should_flush() {
                                collector.flush_buffer().await?;
                            }

                            offset = delivery.offset();
                        },
                        Some(Err(e)) => {
                            error!("encountered error {:?} while reading stream {}", e, self.stream);
                        },
                        None => {
                            panic!("Stream closed");
                        }
                    }

                },
                _ = flush_ticker.tick() => {
                    if collector.should_flush() {
                        collector.flush_buffer().await?;
                    }
                },
                control_message = ctx.control_rx.recv() => {
                    info!("control_message {:?}", control_message);
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("starting checkpointing {}", ctx.task_info.task_index);

                            let s = ctx.table_manager.get_global_keyed_state("s")
                               .await
                               .expect("should be able to get rabbitmq stream state");
                            s.insert(self.stream.clone(), RabbitmqStreamState {
                                offset
                            }).await;

                            if self.start_checkpoint(c, ctx, collector).await {
                                return Ok(SourceFinishType::Immediate);
                            }
                        },
                        Some(ControlMessage::Stop { mode }) => {
                            info!("Stopping RabbitMQ Stream source: {:?}", mode);
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
                            return Err(UserError::new("RabbitMQ Stream source does not support committing", ""));
                        }
                        Some(ControlMessage::LoadCompacted {compacted}) => {
                            ctx.load_compacted(compacted).await;
                        }
                        Some(ControlMessage::NoOp ) => {}
                        None => {}
                    }
                }
            }
        }
    }
}
