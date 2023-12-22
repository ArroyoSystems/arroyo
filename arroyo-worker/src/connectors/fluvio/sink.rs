use crate::engine::StreamNode;
use arroyo_macro::process_fn;
use arroyo_rpc::OperatorConfig;
use arroyo_types::*;
use fluvio::{Fluvio, FluvioConfig, TopicProducer};
use std::marker::PhantomData;

use tracing::info;

use arroyo_types::CheckpointBarrier;
use serde::Serialize;
use crate::old::Context;

use super::{FluvioTable, TableType};

#[derive(StreamNode)]
pub struct FluvioSinkFunc<K: Key + Serialize, T: Data + Serialize> {
    topic: String,
    endpoint: Option<String>,
    producer: Option<TopicProducer>,
    _t: PhantomData<(K, T)>,
}

impl<K: Key + Serialize, T: Data + Serialize> FluvioSinkFunc<K, T> {
    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for FluvioSink");
        let table: FluvioTable =
            serde_json::from_value(config.table).expect("Invalid table config for FluvioSource");
        let TableType::Sink { .. } = &table.type_ else {
            panic!("found non-sink fluvio config in sink operator");
        };

        Self {
            topic: table.topic,
            endpoint: table.endpoint,
            producer: None,
            _t: PhantomData,
        }
    }
}

#[process_fn(in_k = K, in_t = T)]
impl<K: Key + Serialize, T: Data + Serialize> FluvioSinkFunc<K, T> {
    fn name(&self) -> String {
        format!("fluvio-sink-{}", self.topic)
    }

    async fn get_producer(&mut self) -> anyhow::Result<TopicProducer> {
        info!("Creating fluvio producer for {:?}", self.endpoint);

        let config: Option<FluvioConfig> = self
            .endpoint
            .as_ref()
            .map(|endpoint| FluvioConfig::new(endpoint));

        let client = if let Some(config) = &config {
            Fluvio::connect_with_config(config).await?
        } else {
            Fluvio::connect().await?
        };

        Ok(client.topic_producer(&self.topic).await?)
    }

    async fn on_start(&mut self, ctx: &mut Context<(), ()>) {
        match self.get_producer().await {
            Ok(producer) => {
                self.producer = Some(producer);
            }
            Err(e) => {
                ctx.report_error(
                    "Failed to construct Fluvio producer".to_string(),
                    e.to_string(),
                )
                .await;
                panic!("Failed to construct Fluvio producer: {:?}", e);
            }
        }
    }

    async fn handle_checkpoint(&mut self, _: &CheckpointBarrier, _: &mut Context<(), ()>) {
        self.producer.as_mut().unwrap().flush().await.unwrap();
    }

    async fn process_element(&mut self, record: &Record<K, T>, _ctx: &mut Context<(), ()>) {
        let k = record
            .key
            .as_ref()
            .map(|k| serde_json::to_string(k).unwrap());
        let v = serde_json::to_string(&record.value).unwrap();

        self.producer
            .as_mut()
            .unwrap()
            .send(k.unwrap_or_default(), v)
            .await
            .unwrap();
    }
}
