use arrow::array::RecordBatch;
use async_trait::async_trait;
use fluvio::{Fluvio, FluvioConfig, TopicProducerPool};
use std::fmt::Debug;

use arroyo_formats::ser::ArrowSerializer;
use tracing::info;

use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::errors::{DataflowError, DataflowResult};
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

    async fn on_start(&mut self, _: &mut OperatorContext) -> DataflowResult<()> {
        self.producer = Some(self.get_producer().await.map_err(|e| {
            DataflowError::ArgumentError(format!("Failed to construct Fluvio producer: {e:?}"))
        })?);
        
        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let values = self.serializer.serialize(&batch);
        for v in values {
            self.producer
                .as_mut()
                .unwrap()
                .send(Vec::new(), v)
                .await
                .map_err(|e| DataflowError::ExternalError(e.to_string()))?;
        }
        
        Ok(())
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        _: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.producer.as_mut().unwrap().flush().await
            .map_err(|e| DataflowError::ExternalError(e.to_string()))
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
