use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use crate::connectors::mqtt::{MqttConfig, MqttTable, QualityOfService};
use crate::engine::{Context, StreamNode};
use crate::{RateLimiter, SourceFinishType};

use arroyo_formats::{DataDeserializer, SchemaData};
use arroyo_macro::source_fn;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::TableDescriptor;
use arroyo_rpc::OperatorConfig;
use arroyo_rpc::{grpc::StopMode, ControlMessage, ControlResp};
use arroyo_types::{Data, Message, UserError, Watermark};
use governor::{Quota, RateLimiter as GovernorRateLimiter};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{Event as MqttEvent, Incoming};
use rumqttc::Outgoing;

use serde::de::DeserializeOwned;
use tokio::select;

#[cfg(test)]
mod test;

#[derive(StreamNode)]
pub struct MqttSourceFunc<K, T>
where
    K: DeserializeOwned + Data,
    T: SchemaData + Data,
{
    config: MqttConfig,
    topic: String,
    qos: QoS,
    deserializer: DataDeserializer<T>,
    bad_data: Option<BadData>,
    rate_limiter: RateLimiter,
    messages_per_second: NonZeroU32,
    subscribed: Arc<AtomicBool>,
    _t: PhantomData<K>,
}

fn tables() -> Vec<TableDescriptor> {
    vec![arroyo_state::global_table("m", "mqtt source state")]
}

#[source_fn(out_k = (), out_t = T)]
impl<K, T> MqttSourceFunc<K, T>
where
    K: DeserializeOwned + Data,
    T: SchemaData + Data,
{
    pub fn new(
        config: MqttConfig,
        topic: String,
        qos: QoS,
        format: Format,
        bad_data: Option<BadData>,
        rate_limiter: RateLimiter,
        framing: Option<Framing>,
        messages_per_second: u32,
    ) -> Self {
        Self {
            config,
            topic,
            qos,
            bad_data,
            rate_limiter,
            deserializer: DataDeserializer::new(format, framing),
            messages_per_second: NonZeroU32::new(messages_per_second).unwrap(),
            subscribed: Arc::new(AtomicBool::new(false)),
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        format!("mqtt-{}", self.topic)
    }

    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for MqttSource");
        let connection: MqttConfig = serde_json::from_value(config.connection)
            .expect("Invalid connection config for MqttSource");
        let table: MqttTable =
            serde_json::from_value(config.table).expect("Invalid table config for MqttSource");

        Self {
            config: connection,
            topic: table.topic,
            qos: table
                .qos
                .and_then(|qos| match qos {
                    QualityOfService::AtMostOnce => Some(QoS::AtMostOnce),
                    QualityOfService::AtLeastOnce => Some(QoS::AtLeastOnce),
                    QualityOfService::ExactlyOnce => Some(QoS::ExactlyOnce),
                })
                .unwrap_or(QoS::AtMostOnce),
            deserializer: DataDeserializer::new(
                config.format.expect("Format must be set for Mqtt source"),
                config.framing,
            ),
            bad_data: config.bad_data,
            rate_limiter: RateLimiter::new(),
            messages_per_second: NonZeroU32::new(
                config
                    .rate_limit
                    .map(|l| l.messages_per_second)
                    .unwrap_or(u32::MAX),
            )
            .unwrap(),
            subscribed: Arc::new(AtomicBool::new(false)),
            _t: PhantomData,
        }
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        tables()
    }

    pub fn subscribed(&self) -> Arc<AtomicBool> {
        self.subscribed.clone()
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
        if ctx.task_info.task_index > 0 {
            tracing::warn!(
                "Mqtt Consumer {}-{} can only be executed on a single worker... setting idle",
                ctx.task_info.operator_id,
                ctx.task_info.task_index
            );
            ctx.broadcast(Message::Watermark(Watermark::Idle)).await;
        }

        let (client, mut eventloop) =
            match super::create_connection(self.config.clone(), ctx.task_info.task_index) {
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

        loop {
            select! {
                event = eventloop.poll() => {
                    match event {
                        Ok(MqttEvent::Incoming(Incoming::Publish(p))) => {
                            let iter = self.deserializer.deserialize_slice(&p.payload).await;

                            for v in iter {
                                ctx.collect_source_record(
                                    SystemTime::now(),
                                    v,
                                    &self.bad_data,
                                    &mut self.rate_limiter,
                                )
                                .await?;
                            }
                            rate_limiter.until_ready().await;
                        }
                        Ok(MqttEvent::Outgoing(Outgoing::Subscribe(_))) => {
                            self.subscribed.store(true, Ordering::Relaxed);
                        }
                        Ok(_) => (),
                        Err(e) => {
                            return Err(UserError {
                                name: "MqttSourceError".to_string(),
                                details: format!("Error while reading from Mqtt: {:?}", e),
                            });
                        }
                    }
                }
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            tracing::debug!("starting checkpointing {}", ctx.task_info.task_index);
                            if self.checkpoint(c, ctx).await {
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
