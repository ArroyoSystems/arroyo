use super::get_nats_client;
use super::NatsConfig;
use super::NatsTable;
use arrow::array::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::grpc::TableConfig;
use arroyo_rpc::ControlMessage;
use arroyo_rpc::ControlResp;
use arroyo_types::*;
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::warn;

pub struct NatsSinkFunc {
    pub subject: String,
    pub servers: String,
    pub connection: NatsConfig,
    pub table: NatsTable,
    pub publisher: Option<async_nats::Client>,
    pub client_config: HashMap<String, String>,
    pub serializer: ArrowSerializer,
}

#[async_trait]
impl ArrowOperator for NatsSinkFunc {
    fn name(&self) -> String {
        format!("nats-publisher-{}", self.subject)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        HashMap::new()
    }

    async fn on_start(&mut self, _ctx: &mut ArrowContext) {
        match get_nats_client(&self.connection, &self.table).await {
            Ok(client) => {
                self.publisher = Some(client);
            }
            Err(e) => {
                panic!("Failed to construct NATS publisher: {:?}", e);
            }
        }
    }

    async fn on_close(&mut self, _: &Option<SignalMessage>, ctx: &mut ArrowContext) {
        if let Some(ControlMessage::Commit { epoch, commit_data }) = ctx.control_rx.recv().await {
            self.handle_commit(epoch, &commit_data, ctx).await;
        } else {
            warn!("No commit message received, not committing")
        }
    }

    async fn handle_checkpoint(&mut self, _: CheckpointBarrier, _ctx: &mut ArrowContext) {
        // TODO: Is it necessary to insert any kind of checklpoint in the state for NATS sink?
        // let sequence_number = 1;
        // ctx.table_manager
        //     .get_global_keyed_state("n")
        //     .await
        //     .as_mut()
        //     .unwrap()
        //     .insert(ctx.task_info.task_index, sequence_number)
        //     .await;

        // TODO: Is flushing sufficient here to ensure messages are neither
        // sent twice nor lost before being published successfully?
        self.publisher.as_mut().unwrap().flush().await.unwrap();
    }

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext) {
        let nats_subject = async_nats::Subject::from(self.subject.clone());
        for msg in self.serializer.serialize(&batch) {
            match self
                .publisher
                .as_mut()
                .unwrap()
                .publish(nats_subject.clone(), msg.into())
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    ctx.control_tx
                        .send(ControlResp::Error {
                            operator_id: ctx.task_info.operator_id.clone(),
                            task_index: ctx.task_info.task_index,
                            message: e.to_string(),
                            details: e.to_string(),
                        })
                        .await
                        .unwrap();
                    panic!("Panicked while processing element: {}", e.to_string());
                }
            }
        }
    }
}
