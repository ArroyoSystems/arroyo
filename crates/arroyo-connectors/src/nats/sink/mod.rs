use super::get_client_config;
use super::ConnectorType;
use super::NatsConfig;
use super::NatsTable;
use anyhow::Result;
use arrow::array::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::grpc::TableConfig;
use arroyo_rpc::ControlMessage;
use arroyo_rpc::{ControlResp, OperatorConfig};
use arroyo_types::*;
use async_nats::ServerAddr;
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::info;
use tracing::warn;

pub struct NatsSinkFunc {
    pub publisher: Option<async_nats::Client>,
    pub servers: String,
    pub subject: String,
    pub client_config: HashMap<String, String>,
    pub serializer: ArrowSerializer,
}

impl NatsSinkFunc {
    async fn get_nats_client(&mut self) -> Result<async_nats::Client> {
        info!("Creating NATS publisher for {:?}", self.subject);
        let servers_vec: Vec<ServerAddr> = self
            .servers
            .split(',')
            .map(|s| s.parse::<ServerAddr>().unwrap())
            .collect();
        let nats_client: async_nats::Client = async_nats::ConnectOptions::new()
            .user_and_password(
                self.client_config.get("nats.username").unwrap().to_string(),
                self.client_config.get("nats.password").unwrap().to_string(),
            )
            .connect(servers_vec)
            .await
            .unwrap();
        Ok(nats_client)
    }

    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for NatSinkFunc");
        let table: NatsTable =
            serde_json::from_value(config.table).expect("Invalid table config for NatsSinkFunc");
        let format = config
            .format
            .expect("NATS source must have a format configured");
        let connection: NatsConfig = serde_json::from_value(config.connection)
            .expect("Invalid connection config for NatsSinkFunc");
        let subject = match &table.connector_type {
            ConnectorType::Source { .. } => panic!("NATS sink cannot be created from a source"),
            ConnectorType::Sink { subject, .. } => subject.clone(),
        };
        Self {
            publisher: None,
            servers: connection.servers.clone(),
            subject: subject.unwrap().clone(),
            client_config: get_client_config(&connection, &table),
            serializer: ArrowSerializer::new(format),
        }
    }
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
        // TODO: Get the NATS state sequence_number, i.e. what's the last
        // message that was succesfully published to the NATS server
        match self.get_nats_client().await {
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
        // TODO: In order to be generic, we shoudln't reuse the sequence number from the NATS source
        // but rather a identifier specific to the pipeline. How to increment it?
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
