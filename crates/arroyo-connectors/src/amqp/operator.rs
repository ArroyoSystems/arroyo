use anyhow::{Context, Result};
use arroyo_formats::de::FieldValueType;
use arroyo_operator::{
    context::{SourceCollector, SourceContext},
    operator::SourceOperator,
    SourceFinishType,
};
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::{grpc::rpc::StopMode, ControlMessage, MetadataField};
use arroyo_state::global_table_config;
use arroyo_types::*;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use futures::StreamExt;
use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use std::collections::HashMap;
use std::time::Duration;
use tokio::select;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info};

#[derive(Debug)]
pub struct AmqpSourceFunc {
    pub address: String,
    pub topic: String,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub metadata_fields: Vec<MetadataField>,
}

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct AmqpState {
    pub delivery_tag: u64,
}

impl AmqpSourceFunc {
    /// Manages the main loop for consuming messages from the AMQP stream
    /// It first creates a connection and handles any errors that occur during this process.
    /// It sets up a ticker to periodically flush the message collector's buffer.
    /// Inside the loop, it uses the select! macro to handle different asynchronous events: receiving a message from the consumer, ticking the flush ticker, or receiving control messages from the context.
    /// Depending on the event, it processes the message, flushes the buffer if needed, handles errors, or processes control messages such as checkpoints, stopping, committing, or loading compacted data.
    /// The function ensures that the stream is read and processed continuously until a stop condition is met.
    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, UserError> {
        let mut consumer = self
            .get_consumer(ctx)
            .await
            .map_err(|e| UserError::new("Failed to get a consumer", e.to_string()))?;

        // todo might add governor if rate limiting bevcomes necessary https://crates.io/crates/governor
        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut deliveries_cache = HashMap::new();

        loop {
            select! {
                delivery_result = consumer.next() => {
                    match delivery_result {
                        Some(Ok(delivery)) => {
                            // Extract message payload
                            let data: Vec<u8> = delivery.data.clone();

                            // Extract timestamp (if exists)
                            let timestamp: u64 = delivery.properties.timestamp()
                                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as u64); // Default to current time

                            // Extract metadata fields (equivalent to Kafka's metadata fields)
                            let mut connector_metadata = HashMap::new();
                            connector_metadata.insert("exchange", FieldValueType::String(delivery.exchange.as_str().into()));
                            connector_metadata.insert("routing_key", FieldValueType::String(delivery.routing_key.as_str().into()));
                            connector_metadata.insert("timestamp", FieldValueType::Int64(Some(timestamp.try_into().unwrap())));

                            // Deserialize and process the message
                            collector.deserialize_slice(&data, from_millis(timestamp), Some(&connector_metadata)).await?;

                            if collector.should_flush() {
                                collector.flush_buffer().await?;
                            }

                            // Store last processed offset (RabbitMQ uses delivery_tag instead of offset)
                            // todo instead insert into delivery tags list?
                            deliveries_cache.insert(delivery.delivery_tag, delivery.delivery_tag);

                            // Acknowledge the message
                            delivery.ack(BasicAckOptions::default()).await.map_err(|e| UserError::new("failed to acknowledge the message", e.to_string()))?;
                        },
                        Some(Err(err)) => {
                            error!("Encountered AMQP error: {:?}", err);
                        },
                        None => {
                            tokio::time::sleep(Duration::from_millis(500)).await;
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
                            debug!("Starting checkpoint {}", ctx.task_info.task_index);

                            let s = ctx.table_manager.get_global_keyed_state("q").await.map_err(|e| UserError::new("Could not get global key value", format!("{:?}", e)))?;

                            // todo makes no sense needs fix
                            for (_index, &delivery_tag) in &deliveries_cache {
                                s.insert(delivery_tag,  AmqpState{
                                    delivery_tag,
                                }).await;
                            }

                            if self.start_checkpoint(c, ctx, collector).await {
                                return Ok(SourceFinishType::Immediate);
                            }
                        },

                        Some(ControlMessage::Stop { mode }) => {
                            info!("Stopping RabbitMQ source: {:?}", mode);
                            return Ok(match mode {
                                StopMode::Graceful => SourceFinishType::Graceful,
                                StopMode::Immediate => SourceFinishType::Immediate,
                            });
                        }

                        Some(ControlMessage::LoadCompacted { compacted }) => {
                            ctx.load_compacted(compacted).await;
                        }

                        Some(ControlMessage::Commit { .. }) => {
                            unreachable!("sources shouldn't receive commit messages");
                        },

                        Some(ControlMessage::NoOp) | None => {}
                    }
                }
            }
        }
    }

    async fn get_consumer(&mut self, ctx: &mut SourceContext) -> Result<lapin::Consumer> {
        let conn = Connection::connect(&self.address, ConnectionProperties::default())
            .await
            .context("failed to connect to rabbitmq.")?;

        let channel = conn.create_channel().await.context("create_channel")?;
        let queue_name = format!(
            "amqp-arroyo-{}-{}",
            ctx.task_info.job_id, ctx.task_info.operator_id
        );
        let consumer_name = format!(
            "arroyo-{}-{}-consumer",
            ctx.task_info.job_id, ctx.task_info.operator_id
        );
        let consumer = channel
            .basic_consume(
                &queue_name,
                &consumer_name,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("Failed to create consumer")?;

        Ok(consumer)
    }
}

#[async_trait]
impl SourceOperator for AmqpSourceFunc {
    fn name(&self) -> String {
        format!("amqp-lapin-{}", self.topic)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("q", "AMQP table")
    }

    async fn run(
        &mut self,
        ctx: &mut arroyo_operator::context::SourceContext,
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
                ctx.report_error(&e.name, "failed to configure the AMQP source")
                    .await;
                SourceFinishType::Graceful
            }
        }
    }
}
