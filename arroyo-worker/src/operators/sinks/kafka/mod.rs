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

#[cfg(test)]
mod test;

#[derive(StreamNode)]
pub struct KafkaSinkFunc<K: Key + Serialize, T: Data + Serialize> {
    topic: String,
    bootstrap_servers: String,
    producer: Option<FutureProducer>,
    last_write: Option<DeliveryFuture>,
    client_config: HashMap<String, String>,
    _t: PhantomData<(K, T)>,
}

impl<K: Key + Serialize, T: Data + Serialize> KafkaSinkFunc<K, T> {
    pub fn new(servers: &str, topic: &str, client_config: Vec<(&str, &str)>) -> Self {
        KafkaSinkFunc {
            topic: topic.to_string(),
            bootstrap_servers: servers.to_string(),
            producer: None,
            last_write: None,
            client_config: client_config
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            _t: PhantomData,
        }
    }
}

#[process_fn(in_k = K, in_t = T)]
impl<K: Key + Serialize, T: Data + Serialize> KafkaSinkFunc<K, T> {
    fn name(&self) -> String {
        format!("kafka-producer-{}", self.topic)
    }

    fn get_producer(&mut self) -> FutureProducer {
        info!("Creating kafka producer for {}", self.bootstrap_servers);
        let mut client_config = ClientConfig::new();

        client_config.set("bootstrap.servers", &self.bootstrap_servers);

        for (key, value) in &self.client_config {
            client_config.set(key, value);
        }
        client_config.create().expect("Producer creation failed")
    }

    async fn on_start(&mut self, _ctx: &mut Context<(), ()>) {
        self.producer = Some(self.get_producer());
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
        if let Some(delivery_future) = self.last_write.as_mut() {
            // block on confirmation the last message before the checkpoint was delivered.
            delivery_future.await.unwrap().unwrap();
        }
    }

    async fn publish<'a>(
        producer: &mut FutureProducer,
        mut rec: FutureRecord<'a, String, String>,
    ) -> DeliveryFuture {
        loop {
            match producer.send_result(rec) {
                Ok(future) => {
                    return future;
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

        let future_record = {
            if let Some(k) = k.as_ref() {
                FutureRecord::to(&self.topic).key(k).payload(&v)
            } else {
                FutureRecord::to(&self.topic).payload(&v)
            }
        };

        self.last_write = Some(Self::publish(self.producer.as_mut().unwrap(), future_record).await);
    }
}
