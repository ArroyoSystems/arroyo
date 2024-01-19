use anyhow::Result;

use arroyo_rpc::formats::Format;
use arroyo_rpc::grpc::{
    api, TableConfig,
};
use arroyo_rpc::{CheckpointEvent, ControlMessage, ControlResp, OperatorConfig};
use arroyo_types::*;
use std::collections::HashMap;

use tracing::{error, warn};

use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

use rdkafka::ClientConfig;

use crate::engine::ArrowContext;
use crate::operator::{ArrowOperator, ArrowOperatorConstructor, OperatorNode};
use arrow_array::RecordBatch;
use arroyo_formats::serialize::ArrowSerializer;
use arroyo_rpc::grpc::api::ConnectorOp;
use arroyo_types::CheckpointBarrier;
use async_trait::async_trait;
use rdkafka::error::KafkaError;
use rdkafka_sys::RDKafkaErrorCode;
use std::time::{Duration, SystemTime};

use super::{client_configs, KafkaConfig, KafkaTable, SinkCommitMode, TableType};

#[cfg(test)]
mod test;

pub struct KafkaSinkFunc {
    topic: String,
    bootstrap_servers: String,
    consistency_mode: ConsistencyMode,
    producer: Option<FutureProducer>,
    write_futures: Vec<DeliveryFuture>,
    client_config: HashMap<String, String>,
    serializer: ArrowSerializer,
}

enum ConsistencyMode {
    AtLeastOnce,
    ExactlyOnce {
        next_transaction_index: usize,
        producer_to_complete: Option<FutureProducer>,
    },
}

impl From<SinkCommitMode> for ConsistencyMode {
    fn from(commit_mode: SinkCommitMode) -> Self {
        match commit_mode {
            SinkCommitMode::AtLeastOnce => ConsistencyMode::AtLeastOnce,
            SinkCommitMode::ExactlyOnce => ConsistencyMode::ExactlyOnce {
                next_transaction_index: 0,
                producer_to_complete: None,
            },
        }
    }
}

impl ArrowOperatorConstructor<api::ConnectorOp> for KafkaSinkFunc {
    fn from_config(config: ConnectorOp) -> Result<OperatorNode> {
        let config: OperatorConfig =
            serde_json::from_str(&config.config).expect("Invalid config for KafkaSink");
        let connection: KafkaConfig = serde_json::from_value(config.connection)
            .expect("Invalid connection config for KafkaSink");
        let table: KafkaTable =
            serde_json::from_value(config.table).expect("Invalid table config for KafkaSource");
        let TableType::Sink { commit_mode } = table.type_ else {
            panic!("found non-sink kafka config in sink operator");
        };

        Ok(OperatorNode::from_operator(Box::new(Self {
            bootstrap_servers: connection.bootstrap_servers.to_string(),
            producer: None,
            consistency_mode: commit_mode.into(),
            write_futures: vec![],
            client_config: client_configs(&connection, &table),
            topic: table.topic,
            serializer: ArrowSerializer::new(
                config.format.expect("Format must be defined for KafkaSink"),
            ),
        })))
    }
}

impl KafkaSinkFunc {
    pub fn new(
        servers: &str,
        topic: &str,
        format: Format,
        client_config: Vec<(&str, &str)>,
    ) -> Self {
        KafkaSinkFunc {
            topic: topic.to_string(),
            bootstrap_servers: servers.to_string(),
            producer: None,
            consistency_mode: ConsistencyMode::AtLeastOnce,
            write_futures: vec![],
            client_config: client_config
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            serializer: ArrowSerializer::new(format),
        }
    }

    fn is_committing(&self) -> bool {
        matches!(self.consistency_mode, ConsistencyMode::ExactlyOnce { .. })
    }

    fn init_producer(&mut self, task_info: &TaskInfo) -> Result<()> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &self.bootstrap_servers);
        for (key, value) in &self.client_config {
            client_config.set(key, value);
        }

        match &mut self.consistency_mode {
            ConsistencyMode::AtLeastOnce => {
                self.producer = Some(client_config.create()?);
            }
            ConsistencyMode::ExactlyOnce {
                next_transaction_index,
                ..
            } => {
                client_config.set("enable.idempotence", "true");
                let transactional_id = format!(
                    "arroyo-id-{}-{}-{}-{}-{}",
                    task_info.job_id,
                    task_info.operator_id,
                    self.topic,
                    task_info.task_index,
                    next_transaction_index
                );
                client_config.set("transactional.id", transactional_id);
                let producer: FutureProducer = client_config.create()?;
                producer.init_transactions(Timeout::After(Duration::from_secs(30)))?;
                producer.begin_transaction()?;
                *next_transaction_index += 1;
                self.producer = Some(producer);
            }
        }
        Ok(())
    }

    async fn flush(&mut self, ctx: &mut ArrowContext) {
        self.producer
            .as_ref()
            .unwrap()
            // FutureProducer has a thread polling every 100ms,
            // but better to send a signal immediately
            // Duration 0 timeouts are non-blocking,
            .poll(Timeout::After(Duration::ZERO));

        // ensure all messages were delivered before finishing the checkpoint
        for future in self.write_futures.drain(..) {
            if let Err((e, _)) = future.await.unwrap() {
                ctx.error_reporter
                    .report_error("Kafka producer shut down", e.to_string())
                    .await;
                panic!("Kafka producer shut down: {:?}", e);
            }
        }
    }

    async fn publish(&mut self, k: Option<Vec<u8>>, v: Vec<u8>, ctx: &mut ArrowContext) {
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
                    ctx.error_reporter
                        .report_error("Could not write to Kafka", format!("{:?}", e))
                        .await;

                    panic!("Failed to write to kafka: {:?}", e);
                }
            }

            // back off and retry
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[async_trait]
impl ArrowOperator for KafkaSinkFunc {
    fn name(&self) -> String {
        format!("kafka-producer-{}", self.topic)
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        self.init_producer(&ctx.task_info)
            .expect("Producer creation failed");
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        if self.is_committing() {
            todo!("implement committing state")
        } else {
            HashMap::new()
        }
    }

    async fn handle_checkpoint(&mut self, _: CheckpointBarrier, ctx: &mut ArrowContext) {
        self.flush(ctx).await;
        if let ConsistencyMode::ExactlyOnce {
            next_transaction_index,
            producer_to_complete,
        } = &mut self.consistency_mode
        {
            *producer_to_complete = self.producer.take();
            ctx.table_manager
                .get_global_keyed_state("i")
                .await
                .as_mut()
                .unwrap()
                .insert(ctx.task_info.task_index, *next_transaction_index)
                .await;
            self.init_producer(&ctx.task_info)
                .expect("creating new producer during checkpointing");
        }
    }

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext) {
        let values = self.serializer.serialize(&batch);

        if !ctx.in_schemas[0].key_indices.is_empty() {
            let k = batch.project(&ctx.in_schemas[0].key_indices).unwrap();

            // TODO: we can probably batch this for better performance
            for (k, v) in self.serializer.serialize(&k).zip(values) {
                self.publish(Some(k), v, ctx).await;
            }
        } else {
            for v in values {
                self.publish(None, v, ctx).await;
            }
        };
    }

    async fn handle_commit(
        &mut self,
        epoch: u32,
        _commit_data: &HashMap<char, HashMap<u32, Vec<u8>>>,
        ctx: &mut ArrowContext,
    ) {
        let ConsistencyMode::ExactlyOnce {
            next_transaction_index: _,
            producer_to_complete,
        } = &mut self.consistency_mode
        else {
            warn!("received commit but consistency mode is not exactly once");
            return;
        };

        let Some(committing_producer) = producer_to_complete.take() else {
            unimplemented!("received a commit message without a producer ready to commit. Restoring from commit phase not yet implemented");
        };
        let mut commits_attempted = 0;
        loop {
            if committing_producer
                .commit_transaction(Timeout::After(Duration::from_secs(10)))
                .is_ok()
            {
                break;
            } else if commits_attempted == 5 {
                panic!("failed to commit 5 times, giving up");
            } else {
                error!("failed to commit {} times, retrying", commits_attempted);
                commits_attempted += 1;
            }
        }
        let checkpoint_event = ControlResp::CheckpointEvent(CheckpointEvent {
            checkpoint_epoch: epoch,
            operator_id: ctx.task_info.operator_id.clone(),
            subtask_index: ctx.task_info.task_index as u32,
            time: SystemTime::now(),
            event_type: arroyo_rpc::grpc::TaskCheckpointEventType::FinishedCommit.into(),
        });
        ctx.control_tx
            .send(checkpoint_event)
            .await
            .expect("sent commit event");
    }

    async fn on_close(&mut self, _: &Option<SignalMessage>, ctx: &mut ArrowContext) {
        if !self.is_committing() {
            return;
        }
        if let Some(ControlMessage::Commit { epoch, commit_data }) = ctx.control_rx.recv().await {
            self.handle_commit(epoch, &commit_data, ctx).await;
        } else {
            warn!("no commit message received, not committing")
        }
    }
}
