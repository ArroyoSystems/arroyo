use arrow::array::RecordBatch;
use async_trait::async_trait;
use fluvio::{Fluvio, FluvioConfig, TopicProducerPool};
use std::fmt::Debug;

use arroyo_formats::ser::ArrowSerializer;
use tracing::info;

use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::ArrowOperator;
use arroyo_types::CheckpointBarrier;

pub struct FluvioSinkFunc {
    pub topic: String,
    pub endpoint: Option<String>,
    pub producer: Option<TopicProducerPool>,
    pub serializer: ArrowSerializer,
}

impl Debug for FluvioSinkFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FluvioSinkFunc")
            .field("topic", &self.topic)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

#[async_trait]
impl ArrowOperator for FluvioSinkFunc {
    fn name(&self) -> String {
        format!("fluvio-sink-{}", self.topic)
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
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

    async fn process_batch(&mut self, batch: RecordBatch, _: &mut ArrowContext) {
        let values = self.serializer.serialize(&batch);
        for v in values {
            self.producer
                .as_mut()
                .unwrap()
                .send(Vec::new(), v)
                .await
                .unwrap();
        }
    }

    async fn handle_checkpoint(&mut self, _: CheckpointBarrier, _: &mut ArrowContext) {
        self.producer.as_mut().unwrap().flush().await.unwrap();
    }
}

impl FluvioSinkFunc {
    async fn get_producer(&mut self) -> anyhow::Result<TopicProducerPool> {
        info!("Creating fluvio producer for {:?}", self.endpoint);

        let config: Option<FluvioConfig> = self.endpoint.as_ref().map(FluvioConfig::new);

        let client = if let Some(config) = &config {
            Fluvio::connect_with_config(config).await?
        } else {
            Fluvio::connect().await?
        };

        client.topic_producer(&self.topic).await
    }
}
