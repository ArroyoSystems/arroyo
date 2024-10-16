use anyhow::Result;
use std::borrow::Cow;

use arroyo_rpc::grpc::rpc::{GlobalKeyedTableConfig, TableConfig, TableEnum};
use arroyo_rpc::{CheckpointEvent, ControlMessage, ControlResp};
use arroyo_types::*;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use tracing::{error, warn};

use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

use rdkafka::ClientConfig;

use super::SinkCommitMode;
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, TimeUnit};
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::{ArrowOperator, AsDisplayable, DisplayableOperator};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_types::CheckpointBarrier;
use async_trait::async_trait;
use prost::Message;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use std::time::{Duration, SystemTime};

#[cfg(test)]
mod test;

pub struct KafkaSinkFunc {
    pub topic: String,
    pub bootstrap_servers: String,
    pub consistency_mode: ConsistencyMode,
    pub timestamp_field: Option<String>,
    pub timestamp_col: Option<usize>,
    pub key_field: Option<String>,
    pub key_col: Option<usize>,
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

impl Display for ConsistencyMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsistencyMode::AtLeastOnce => write!(f, "AtLeastOnce"),
            ConsistencyMode::ExactlyOnce { .. } => write!(f, "ExactlyOnce"),
        }
    }
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

    fn set_timestamp_col(&mut self, schema: &ArroyoSchema) {
        if let Some(f) = &self.timestamp_field {
            if let Ok(f) = schema.schema.field_with_name(f) {
                match f.data_type() {
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        self.timestamp_col = Some(schema.schema.index_of(f.name()).unwrap());
                        return;
                    }
                    _ => {
                        warn!(
                            "Kafka sink configured with timestamp_field '{f}', but it has type \
                        {}, not TIMESTAMP... ignoring",
                            f.data_type()
                        );
                    }
                }
            } else {
                warn!(
                    "Kafka sink configured with timestamp_field '{f}', but that \
                does not appear in the schema... ignoring"
                );
            }
        }

        self.timestamp_col = Some(schema.timestamp_index);
    }

    fn set_key_col(&mut self, schema: &ArroyoSchema) {
        if let Some(f) = &self.key_field {
            if let Ok(f) = schema.schema.field_with_name(f) {
                if matches!(f.data_type(), DataType::Utf8) {
                    self.key_col = Some(schema.schema.index_of(f.name()).unwrap());
                } else {
                    warn!(
                        "Kafka sink configured with key_field '{f}', but it has type \
                {}, not TEXT... ignoring",
                        f.data_type()
                    );
                }
            } else {
                warn!(
                    "Kafka sink configured with key_field '{f}', but that \
                does not appear in the schema... ignoring"
                );
            }
        }
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

    async fn publish(
        &mut self,
        ts: Option<i64>,
        k: Option<Vec<u8>>,
        v: Vec<u8>,
        ctx: &mut ArrowContext,
    ) {
        let mut rec = {
            let mut rec = FutureRecord::<Vec<u8>, Vec<u8>>::to(&self.topic);
            if let Some(ts) = ts {
                rec = rec.timestamp(ts);
            }
            if let Some(k) = k.as_ref() {
                rec = rec.key(k);
            }

            rec.payload(&v)
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

    fn display(&self) -> DisplayableOperator {
        DisplayableOperator {
            name: Cow::Borrowed("KafkaSinkFunc"),
            fields: vec![
                ("topic", self.topic.as_str().into()),
                ("bootstrap_servers", self.bootstrap_servers.as_str().into()),
                (
                    "consistency_mode",
                    AsDisplayable::Display(&self.consistency_mode),
                ),
                (
                    "timestamp_field",
                    AsDisplayable::Debug(&self.timestamp_field),
                ),
                ("key_field", AsDisplayable::Debug(&self.key_field)),
                ("client_config", AsDisplayable::Debug(&self.client_config)),
            ],
        }
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        if self.is_committing() {
            single_item_hash_map(
                "i".to_string(),
                TableConfig {
                    table_type: TableEnum::GlobalKeyValue.into(),
                    config: GlobalKeyedTableConfig {
                        table_name: "i".to_string(),
                        description: "index for transactional ids".to_string(),
                        uses_two_phase_commit: true,
                    }
                    .encode_to_vec(),
                },
            )
        } else {
            HashMap::new()
        }
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        self.set_timestamp_col(&ctx.in_schemas[0]);
        self.set_key_col(&ctx.in_schemas[0]);

        self.init_producer(&ctx.task_info)
            .expect("Producer creation failed");
    }

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext) {
        let values = self.serializer.serialize(&batch);
        let timestamps = batch
            .column(
                self.timestamp_col
                    .expect("timestamp column not initialized!"),
            )
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>();

        let keys = self.key_col.map(|i| batch.column(i).as_string::<i32>());

        for (i, v) in values.enumerate() {
            // kafka timestamp as unix millis
            let timestamp = timestamps.map(|ts| {
                if ts.is_null(i) {
                    0
                } else {
                    ts.value(i) / 1_000_000
                }
            });
            // TODO: this copy should be unnecessary but likely needs a custom trait impl
            let key = keys.map(|k| k.value(i).as_bytes().to_vec());
            self.publish(timestamp, key, v, ctx).await;
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
            event_type: arroyo_rpc::grpc::rpc::TaskCheckpointEventType::FinishedCommit,
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
