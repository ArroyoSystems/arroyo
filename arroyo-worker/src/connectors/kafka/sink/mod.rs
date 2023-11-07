use crate::engine::{Context, StreamNode};
use crate::formats::DataSerializer;
use crate::SchemaData;
use anyhow::Result;
use arroyo_macro::process_fn;
use arroyo_rpc::formats::Format;
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableWriteBehavior};
use arroyo_rpc::{CheckpointEvent, ControlMessage, OperatorConfig};
use arroyo_types::*;
use std::collections::HashMap;
use std::marker::PhantomData;

use tracing::{error, warn};

use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

use rdkafka::ClientConfig;

use arroyo_types::CheckpointBarrier;
use rdkafka::error::KafkaError;
use rdkafka_sys::RDKafkaErrorCode;
use serde::Serialize;
use std::time::{Duration, SystemTime};

use super::{client_configs, KafkaConfig, KafkaTable, SinkCommitMode, TableType};

#[cfg(test)]
mod test;

#[derive(StreamNode)]
pub struct KafkaSinkFunc<K: Key + Serialize, T: SchemaData> {
    topic: String,
    bootstrap_servers: String,
    consistency_mode: ConsistencyMode,
    producer: Option<FutureProducer>,
    write_futures: Vec<DeliveryFuture>,
    client_config: HashMap<String, String>,
    serializer: DataSerializer<T>,
    _t: PhantomData<K>,
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

impl<K: Key + Serialize, T: SchemaData> KafkaSinkFunc<K, T> {
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
            serializer: DataSerializer::new(format),
            _t: PhantomData,
        }
    }

    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for KafkaSink");
        let connection: KafkaConfig = serde_json::from_value(config.connection)
            .expect("Invalid connection config for KafkaSink");
        let table: KafkaTable =
            serde_json::from_value(config.table).expect("Invalid table config for KafkaSource");
        let TableType::Sink { commit_mode } = table.type_ else {
            panic!("found non-sink kafka config in sink operator");
        };

        Self {
            topic: table.topic,
            bootstrap_servers: connection.bootstrap_servers.to_string(),
            producer: None,
            consistency_mode: commit_mode.unwrap_or(SinkCommitMode::AtLeastOnce).into(),
            write_futures: vec![],
            client_config: client_configs(&connection),
            serializer: DataSerializer::new(
                config.format.expect("Format must be defined for KafkaSink"),
            ),
            _t: PhantomData,
        }
    }
}

#[process_fn(in_k = K, in_t = T)]
impl<K: Key + Serialize, T: SchemaData + Serialize> KafkaSinkFunc<K, T> {
    fn name(&self) -> String {
        format!("kafka-producer-{}", self.topic)
    }

    async fn on_start(&mut self, ctx: &mut Context<(), ()>) {
        self.init_producer(&ctx.task_info)
            .expect("Producer creation failed");
    }

    fn is_committing(&self) -> bool {
        matches!(self.consistency_mode, ConsistencyMode::ExactlyOnce { .. })
    }

    fn tables(&self) -> Vec<arroyo_rpc::grpc::TableDescriptor> {
        if self.is_committing() {
            vec![TableDescriptor {
                name: "i".to_string(),
                description: "index for transactional ids".to_string(),
                table_type: arroyo_rpc::grpc::TableType::Global as i32,
                delete_behavior: TableDeleteBehavior::None as i32,
                write_behavior: TableWriteBehavior::CommitWrites as i32,
                retention_micros: 0,
            }]
        } else {
            Vec::new()
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

    async fn handle_checkpoint(&mut self, _: &CheckpointBarrier, ctx: &mut Context<(), ()>) {
        self.flush().await;
        if let ConsistencyMode::ExactlyOnce {
            next_transaction_index,
            producer_to_complete,
        } = &mut self.consistency_mode
        {
            *producer_to_complete = self.producer.take();
            ctx.state
                .get_global_keyed_state('i')
                .await
                .insert(ctx.task_info.task_index, *next_transaction_index)
                .await;
            self.init_producer(&ctx.task_info)
                .expect("creating new producer during checkpointing");
        }
    }

    async fn flush(&mut self) {
        self.producer
            .as_ref()
            .unwrap()
            // FutureProducer has a thread polling every 100ms,
            // but better to send a signal immediately
            // Duration 0 timeouts are non-blocking,
            .poll(Timeout::After(Duration::ZERO));

        // ensure all messages were delivered before finishing the checkpoint
        for future in self.write_futures.drain(..) {
            match future.await.expect("Kafka producer shut down") {
                Ok(_) => {}
                Err((e, _)) => {
                    panic!("Unhandled kafka error: {:?}", e);
                }
            }
        }
    }

    async fn publish(&mut self, k: Option<String>, v: Vec<u8>) {
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
        let v = self.serializer.to_vec(&record.value);

        if let Some(v) = v {
            self.publish(k, v).await;
        }
    }

    async fn handle_commit(
        &mut self,
        epoch: u32,
        _commit_data: HashMap<char, HashMap<u32, Vec<u8>>>,
        ctx: &mut crate::engine::Context<(), ()>,
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
        let checkpoint_event = arroyo_rpc::ControlResp::CheckpointEvent(CheckpointEvent {
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

    async fn on_close(&mut self, ctx: &mut crate::engine::Context<(), ()>) {
        if !self.is_committing() {
            return;
        }
        if let Some(ControlMessage::Commit { epoch, commit_data }) = ctx.control_rx.recv().await {
            self.handle_commit(epoch, commit_data, ctx).await;
        } else {
            warn!("no commit message received, not committing")
        }
    }
}
