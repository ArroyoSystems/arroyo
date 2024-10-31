use arroyo_formats::de::FieldValueType;
use async_trait::async_trait;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::{grpc::rpc::StopMode, ControlMessage, ControlResp, MetadataField};
use arroyo_types::{ArrowMessage, SignalMessage, UserError, Watermark};
use governor::{Quota, RateLimiter as GovernorRateLimiter};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{ConnectionError, Event as MqttEvent, Incoming};
use rumqttc::Outgoing;

use crate::mqtt::{create_connection, MqttConfig};
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::grpc::rpc::TableConfig;
use tokio::select;
use tokio::time::MissedTickBehavior;

#[cfg(test)]
mod test;

pub struct MqttSourceFunc {
    pub config: MqttConfig,
    pub topic: String,
    pub qos: QoS,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub messages_per_second: NonZeroU32,
    pub subscribed: Arc<AtomicBool>,
    pub metadata_fields: Vec<MetadataField>,
}

#[async_trait]
impl SourceOperator for MqttSourceFunc {
    fn name(&self) -> String {
        format!("mqtt-{}", self.topic)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("m", "mqtt source state")
    }

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
}

#[allow(clippy::too_many_arguments)]
impl MqttSourceFunc {
    pub fn new(
        config: MqttConfig,
        topic: String,
        qos: QoS,
        format: Format,
        framing: Option<Framing>,
        bad_data: Option<BadData>,
        messages_per_second: u32,
        metadata_fields: Vec<MetadataField>,
    ) -> Self {
        Self {
            config,
            topic,
            qos,
            format,
            framing,
            bad_data,
            messages_per_second: NonZeroU32::new(messages_per_second).unwrap(),
            subscribed: Arc::new(AtomicBool::new(false)),
            metadata_fields,
        }
    }

    pub fn subscribed(&self) -> Arc<AtomicBool> {
        self.subscribed.clone()
    }

    async fn run_int(&mut self, ctx: &mut ArrowContext) -> Result<SourceFinishType, UserError> {
        ctx.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
        );

        if ctx.task_info.task_index > 0 {
            tracing::warn!(
                "Mqtt Consumer {}-{} can only be executed on a single worker... setting idle",
                ctx.task_info.operator_id,
                ctx.task_info.task_index
            );
            ctx.broadcast(ArrowMessage::Signal(SignalMessage::Watermark(
                Watermark::Idle,
            )))
            .await;
        }

        let (client, mut eventloop) =
            match create_connection(&self.config, ctx.task_info.task_index) {
                Ok(c) => c,
                Err(e) => {
                    return Err(UserError {
                        name: "MqttSourceError".to_string(),
                        details: format!("Failed to create connection: {}", e),
                    });
                }
            };

        match client.subscribe(self.topic.clone(), self.qos).await {
            Ok(_) => (),
            Err(e) => {
                return Err(UserError {
                    name: "MqttSourceError".to_string(),
                    details: format!("Failed to subscribe to topic: {}", e),
                });
            }
        }

        let rate_limiter = GovernorRateLimiter::direct(Quota::per_second(self.messages_per_second));

        let topic = self.topic.clone();
        let qos = self.qos;
        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            select! {
                event = eventloop.poll() => {
                    match event {
                        Ok(MqttEvent::Incoming(Incoming::Publish(p))) => {
                            let topic = String::from_utf8_lossy(&p.topic).to_string();

                            let connector_metadata = if !self.metadata_fields.is_empty() {
                                let mut connector_metadata = HashMap::new();
                                for mf in &self.metadata_fields {
                                    connector_metadata.insert(&mf.field_name, match mf.key.as_str() {
                                        "topic" => FieldValueType::String(&topic),
                                        k => unreachable!("invalid metadata key '{}' for mqtt", k)
                                    });
                                }
                                Some(connector_metadata)
                            } else {
                                None
                            };

                            ctx.deserialize_slice(&p.payload, SystemTime::now(), connector_metadata.as_ref()).await?;
                            rate_limiter.until_ready().await;
                        }
                        Ok(MqttEvent::Outgoing(Outgoing::Subscribe(_))) => {
                            self.subscribed.store(true, Ordering::Relaxed);
                        }
                        Ok(_) => (),
                        Err(err) => {
                            if let ConnectionError::Timeout(_) = err {
                                continue;
                            }
                            tracing::error!("Failed to poll mqtt eventloop: {}", err);
                            if let Err(err) = client
                                .subscribe(
                                    topic.clone(),
                                    qos,
                                )
                                .await {
                                    return Err(UserError {
                                        name: "MqttSourceError".to_string(),
                                        details: format!("Error while subscribing to mqtt topic {}: {:?}", topic, err),
                                    });
                                }
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
                            tracing::debug!("starting checkpointing {}", ctx.task_info.task_index);
                            if self.start_checkpoint(c, ctx).await {
                                return Ok(SourceFinishType::Immediate);
                            }
                        },
                        Some(ControlMessage::Stop { mode }) => {
                            tracing::info!("Stopping Mqtt source: {:?}", mode);

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
