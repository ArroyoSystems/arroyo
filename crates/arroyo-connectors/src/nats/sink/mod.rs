use super::NatsConfig;
use super::NatsTable;
use super::{get_nats_client, SinkType};
use arrow::array::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_types::*;
use async_trait::async_trait;
use std::collections::HashMap;

pub struct NatsSinkFunc {
    pub sink_type: SinkType,
    pub servers: String,
    pub connection: NatsConfig,
    pub table: NatsTable,
    pub publisher: Option<async_nats::Client>,
    pub serializer: ArrowSerializer,
}

#[async_trait]
impl ArrowOperator for NatsSinkFunc {
    fn name(&self) -> String {
        format!(
            "nats-publisher-{}",
            match &self.sink_type {
                SinkType::Subject(s) => {
                    s
                }
            }
        )
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::new()
    }

    async fn on_start(&mut self, _ctx: &mut OperatorContext) {
        match get_nats_client(&self.connection).await {
            Ok(client) => {
                self.publisher = Some(client);
            }
            Err(e) => {
                panic!("Failed to construct NATS publisher: {:?}", e);
            }
        }
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        _ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        // TODO: Implement checkpointing of in-progress data to avoid depending on
        // the downstream NATS availability to flush and checkpoint.
        let publisher = self
            .publisher
            .as_mut()
            .expect("Something went wrong while instantiating the publisher.");

        match publisher.flush().await {
            Ok(_) => {}
            Err(e) => {
                panic!("Failed to flush NATS publisher: {:?}", e);
            }
        }
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        let SinkType::Subject(s) = &self.sink_type;
        let nats_subject = async_nats::Subject::from(s.clone());
        for msg in self.serializer.serialize(&batch) {
            let publisher = self
                .publisher
                .as_mut()
                .expect("Something went wrong while instantiating the publisher.");

            match publisher.publish(nats_subject.clone(), msg.into()).await {
                Ok(_) => {}
                Err(e) => {
                    ctx.report_error(e.to_string(), format!("{:?}", e)).await;
                    panic!("Panicked while processing element: {}", e);
                }
            }
        }
    }
}
