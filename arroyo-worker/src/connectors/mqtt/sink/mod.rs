use std::marker::PhantomData;
use std::time::Duration;

use arroyo_formats::{DataSerializer, SchemaData};
use arroyo_macro::process_fn;
use arroyo_rpc::formats::Format;
use arroyo_rpc::ControlResp;
use arroyo_rpc::OperatorConfig;
use arroyo_types::{Key, Record};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::AsyncClient;
use serde::Serialize;

use crate::connectors::mqtt::{MqttConfig, MqttTable, QualityOfService, Retain, TableType};
use crate::engine::{Context, StreamNode};

#[cfg(test)]
mod test;

#[derive(StreamNode)]
pub struct MqttSinkFunc<K: Key + Serialize, T: SchemaData> {
    config: MqttConfig,
    qos: QoS,
    topic: String,
    retain: bool,
    serializer: DataSerializer<T>,
    client: Option<AsyncClient>,
    _t: PhantomData<K>,
}

#[process_fn(in_k = K, in_t = T)]
impl<K: Key + Serialize, T: SchemaData + Serialize> MqttSinkFunc<K, T> {
    pub fn new(config: MqttConfig, qos: QoS, topic: String, retain: bool, format: Format) -> Self {
        Self {
            config,
            qos,
            topic,
            retain,
            serializer: DataSerializer::new(format),
            client: None,
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        format!("mqtt-producer-{}", self.topic)
    }

    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for KafkaSink");
        let connection: MqttConfig = serde_json::from_value(config.connection)
            .expect("Invalid connection config for KafkaSink");
        let table: MqttTable =
            serde_json::from_value(config.table).expect("Invalid table config for KafkaSource");
        let TableType::Sink { retain } = table.type_ else {
            panic!("found non-sink mqtt config in sink operator");
        };

        Self {
            config: connection,
            qos: table
                .qos
                .and_then(|qos| match qos {
                    QualityOfService::AtMostOnce => Some(QoS::AtMostOnce),
                    QualityOfService::AtLeastOnce => Some(QoS::AtLeastOnce),
                    QualityOfService::ExactlyOnce => Some(QoS::ExactlyOnce),
                })
                .unwrap_or(QoS::AtMostOnce),
            topic: table.topic,
            retain: retain
                .and_then(|r| match r {
                    Retain::Yes => Some(true),
                    Retain::No => Some(false),
                })
                .unwrap_or_default(),
            serializer: DataSerializer::new(
                config.format.expect("Format must be defined for KafkaSink"),
            ),
            client: None,
            _t: PhantomData,
        }
    }

    async fn on_start(&mut self, ctx: &mut Context<(), ()>) {
        let mut attempts = 0;
        while attempts < 20 {
            match super::create_connection(self.config.clone(), ctx.task_info.task_index) {
                Ok((client, mut eventloop)) => {
                    self.client = Some(client);
                    tokio::spawn(async move {
                        loop {
                            let event = eventloop.poll().await;
                            if let Err(err) = event {
                                tracing::error!("Error in mqtt event loop: {:?}", err);
                                panic!("Error in mqtt event loop: {:?}", err);
                            }
                        }
                    });
                    return;
                }
                Err(e) => {
                    ctx.report_error("Failed to connect", e.to_string()).await;
                }
            };

            tokio::time::sleep(Duration::from_millis((50 * (1 << attempts)).min(5_000))).await;
            attempts -= 1;
        }

        panic!("Failed to establish connection to mqtt after 20 retries");
    }

    async fn process_element(&mut self, record: &Record<K, T>, ctx: &mut Context<(), ()>) {
        let v = self.serializer.to_vec(&record.value);

        if let Some(v) = v {
            match self
                .client
                .as_mut()
                .unwrap()
                .publish(self.topic.clone(), self.qos, self.retain, v)
                .await
            {
                Ok(_) => (),
                Err(e) => {
                    ctx.control_tx
                        .send(ControlResp::Error {
                            operator_id: ctx.task_info.operator_id.clone(),
                            task_index: ctx.task_info.task_index,
                            message: "Could not write to mqtt".to_string(),
                            details: format!("{:?}", e),
                        })
                        .await
                        .unwrap();

                    panic!("Could not write to mqtt: {:?}", e);
                }
            }
        }
    }
}
