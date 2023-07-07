use crate::connectors::OperatorConfig;
use crate::engine::{Context, StreamNode};
use arroyo_macro::process_fn;
use arroyo_types::*;
use std::collections::HashMap;
use std::marker::PhantomData;

use tracing::info;

use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use rdkafka::ClientConfig;

use arroyo_types::CheckpointBarrier;
use rdkafka::error::KafkaError;
use rdkafka_sys::RDKafkaErrorCode;
use serde::Serialize;
use std::time::Duration;

use super::{client_configs, KafkaConfig, KafkaTable, TableType};

#[cfg(test)]
mod test;

#[derive(StreamNode)]
pub struct KafkaSinkFunc<K: Key + Serialize, T: Data + Serialize> {
    topic: String,
    bootstrap_servers: String,
    producer: Option<FutureProducer>,
    write_futures: Vec<DeliveryFuture>,
    client_config: HashMap<String, String>,
    _t: PhantomData<(K, T)>,
}

impl<K: Key + Serialize, T: Data + Serialize> KafkaSinkFunc<K, T> {
    pub fn new(servers: &str, topic: &str, client_config: Vec<(&str, &str)>) -> Self {
        KafkaSinkFunc {
            topic: topic.to_string(),
            bootstrap_servers: servers.to_string(),
            producer: None,
            write_futures: vec![],
            client_config: client_config
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            _t: PhantomData,
        }
    }

    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for KafkaSink");
        let connection: KafkaConfig = serde_json::from_value(config.connection)
            .expect("Invalid connection config for KafkaSink");
        let table: KafkaTable =
            serde_json::from_value(config.table).expect("Invalid table config for KafkaSource");
        let TableType::Sink{ .. } = &table.type_ else {
            panic!("found non-sink kafka config in sink operator");
        };

        Self {
            topic: table.topic,
            bootstrap_servers: connection.bootstrap_servers.to_string(),
            producer: None,
            write_futures: vec![],
            client_config: client_configs(&connection),
            _t: PhantomData,
        }
    }
}

#[process_fn(in_k = K, in_t = T)]
impl<K: Key + Serialize, T: Data + Serialize> KafkaSinkFunc<K, T> {
    fn name(&self) -> String {
        format!("kafka-producer-{}", self.topic)
    }

    async fn on_start(&mut self, _ctx: &mut Context<(), ()>) {
        info!("Creating kafka producer for {}", self.bootstrap_servers);
        let mut client_config = ClientConfig::new();

        client_config.set("bootstrap.servers", &self.bootstrap_servers);

        for (key, value) in &self.client_config {
            client_config.set(key, value);
        }

        self.producer = Some(client_config.create().expect("Producer creation failed"));
    }

    async fn handle_checkpoint(&mut self, _: &CheckpointBarrier, _: &mut Context<(), ()>) {
        self.flush().await;
    }

    async fn flush(&mut self) {
        self.producer
            .as_ref()
            .unwrap()
            // FutureProducer has a thread polling every 100ms,
            // but better to send a signal immediately
            // Duration 0 timeouts are non-blocking,
            .poll(Timeout::After(Duration::ZERO));

        // ensure all messages were delivered before finishing the checkpoint
        for future in self.write_futures.drain(..) {
            match future.await.expect("Kafka producer shut down") {
                Ok(_) => {}
                Err((e, _)) => {
                    panic!("Unhandled kafka error: {:?}", e);
                }
            }
        }
    }

    async fn publish(&mut self, k: Option<String>, v: String) {
        let mut rec = {
            if let Some(k) = k.as_ref() {
                FutureRecord::to(&self.topic).key(k).payload(&v)
            } else {
                FutureRecord::to(&self.topic).payload(&v)
            }
        };

        loop {
            match self.producer.as_mut().unwrap().send_result(rec) {
                Ok(future) => {
                    self.write_futures.push(future);
                    return;
                }
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), f)) => {
                    rec = f;
                }
                Err((e, _)) => {
                    panic!("Unhandled kafka error: {:?}", e);
                }
            }

            // back off and retry
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn process_element(&mut self, record: &Record<K, T>, _ctx: &mut Context<(), ()>) {
        let k = record
            .key
            .as_ref()
            .map(|k| serde_json::to_string(k).unwrap());
        let v = serde_json::to_string(&record.value).unwrap();

        self.publish(k, v).await;
    }
}
