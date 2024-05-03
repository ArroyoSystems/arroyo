use anyhow::Result;

use arroyo_rpc::grpc::TableConfig;
use arroyo_rpc::{CheckpointEvent, ControlMessage, ControlResp};
use arroyo_types::*;
use std::collections::HashMap;

use tracing::{error, warn};

use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

use rdkafka::ClientConfig;

use arrow::array::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::ArrowOperator;
use arroyo_types::CheckpointBarrier;
use async_trait::async_trait;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use std::time::{Duration, SystemTime};

use super::SinkCommitMode;

#[cfg(test)]
mod test;

pub struct KafkaSinkFunc {
    pub topic: String,
    pub bootstrap_servers: String,
    pub consistency_mode: ConsistencyMode,
    pub producer: Option<FutureProducer>,
    pub write_futures: Vec<DeliveryFuture>,
    pub client_config: HashMap<String, String>,
    pub serializer: ArrowSerializer,
}

pub enum ConsistencyMode {
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

impl KafkaSinkFunc {
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

    fn tables(&self) -> HashMap<String, TableConfig> {
        if self.is_committing() {
            todo!("implement committing state")
        } else {
            HashMap::new()
        }
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        self.init_producer(&ctx.task_info)
            .expect("Producer creation failed");
    }

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext) {
        let values = self.serializer.serialize(&batch);

        for v in values {
            self.publish(None, v, ctx).await;
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

    async fn handle_commit(
        &mut self,
        epoch: u32,
        _commit_data: &HashMap<String, HashMap<u32, Vec<u8>>>,
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
            event_type: arroyo_rpc::grpc::TaskCheckpointEventType::FinishedCommit,
        });
        ctx.control_tx
            .send(checkpoint_event)
            .await
            .expect("sent commit event");
    }

    async fn on_close(&mut self, _: &Option<SignalMessage>, ctx: &mut ArrowContext) {
        self.flush(ctx).await;
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
