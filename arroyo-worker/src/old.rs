use crate::engine::{ErrorReporter, TimerValue, WatermarkHolder, QUEUE_SIZE};
use crate::metrics::{register_queue_gauges, QueueGauges, TaskCounters};
use crate::{RateLimiter, TIMER_TABLE};
use anyhow::bail;
use arrow_array::RecordBatch;
use arroyo_datastream::Operator;
use arroyo_rpc::formats::BadData;
use arroyo_rpc::grpc::{
    CheckpointMetadata, TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior,
};
use arroyo_rpc::{CompactionResult, ControlMessage, ControlResp};
use arroyo_state::tables::time_key_map::TimeKeyMap;
use arroyo_state::{hash_key, BackingStore, StateBackend, StateStore};
use arroyo_types::{
    from_micros, server_for_hash, Data, Key, Message, Record, RecordBatchData, SourceError,
    TaskInfo, UserError, Watermark,
};
use bincode::config;
use rand::Rng;
use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

#[derive(Clone)]
pub struct Collector<K: Key, T: Data> {
    task_info: Arc<TaskInfo>,
    out_qs: Vec<Vec<OutQueue>>,
    _ts: PhantomData<(K, T)>,
    tx_queue_rem_gauges: QueueGauges,
    tx_queue_size_gauges: QueueGauges,
}

impl<K: Key, T: Data> Collector<K, T> {
    pub async fn collect_record_batch(&mut self, record_batch: RecordBatch) {
        let message: Message<K, T> = Message::RecordBatch(RecordBatchData(record_batch));
        self.out_qs[0][0].send(&self.task_info, message).await;
    }

    pub async fn collect(&mut self, record: Record<K, T>) {
        fn out_idx<K: Key>(key: &Option<K>, qs: usize) -> usize {
            let hash = if let Some(key) = &key {
                hash_key(key)
            } else {
                // TODO: do we want this be random or deterministic?
                rand::thread_rng().gen()
            };

            server_for_hash(hash, qs)
        }

        TaskCounters::MessagesSent.for_task(&self.task_info).inc();

        if self.out_qs.len() == 1 {
            let idx = out_idx(&record.key, self.out_qs[0].len());

            self.tx_queue_rem_gauges[0][idx]
                .iter()
                .for_each(|g| g.set(self.out_qs[0][idx].tx.capacity() as i64));

            self.tx_queue_size_gauges[0][idx]
                .iter()
                .for_each(|g| g.set(QUEUE_SIZE as i64));

            self.out_qs[0][idx]
                .send(&self.task_info, Message::Record(record))
                .await;
        } else {
            let key = record.key.clone();
            let message = Message::Record(record);

            for (i, out_node_qs) in self.out_qs.iter().enumerate() {
                let idx = out_idx(&key, out_node_qs.len());
                self.tx_queue_rem_gauges[i][idx]
                    .iter()
                    .for_each(|c| c.set(self.out_qs[i][idx].tx.capacity() as i64));

                self.tx_queue_size_gauges[i][idx]
                    .iter()
                    .for_each(|c| c.set(QUEUE_SIZE as i64));

                out_node_qs[idx]
                    .send(&self.task_info, message.clone())
                    .await;
            }
        }
    }

    pub async fn broadcast(&mut self, message: Message<K, T>) {
        for out_node in &self.out_qs {
            for q in out_node {
                q.send(&self.task_info, message.clone()).await;
            }
        }
    }
}

pub struct Context<K: Key, T: Data, S: BackingStore = StateBackend> {
    pub task_info: Arc<TaskInfo>,
    pub control_rx: Receiver<ControlMessage>,
    pub control_tx: Sender<ControlResp>,
    pub error_reporter: ErrorReporter,
    pub watermarks: WatermarkHolder,
    pub state: StateStore<S>,
    pub collector: Collector<K, T>,
    _ts: PhantomData<(K, T)>,
}

unsafe impl<K: Key, T: Data, S: BackingStore> Sync for Context<K, T, S> {}

impl<K: Key, T: Data> Context<K, T> {
    pub async fn new(
        task_info: TaskInfo,
        restore_from: Option<CheckpointMetadata>,
        control_rx: Receiver<ControlMessage>,
        control_tx: Sender<ControlResp>,
        input_partitions: usize,
        out_qs: Vec<Vec<OutQueue>>,
        mut tables: Vec<TableDescriptor>,
    ) -> Context<K, T> {
        tables.push(TableDescriptor {
            name: TIMER_TABLE.to_string(),
            description: "timer state".to_string(),
            table_type: TableType::TimeKeyMap as i32,
            delete_behavior: TableDeleteBehavior::None as i32,
            write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
            retention_micros: 0,
        });

        let (state, watermark) = if let Some(metadata) = restore_from {
            let watermark = {
                let metadata = StateBackend::load_operator_metadata(
                    &task_info.job_id,
                    &task_info.operator_id,
                    metadata.epoch,
                )
                .await;
                metadata
                    .expect("require metadata")
                    .min_watermark
                    .map(from_micros)
            };
            let state = StateStore::<StateBackend>::from_checkpoint(
                &task_info,
                metadata,
                tables,
                control_tx.clone(),
            )
            .await;

            (state, watermark)
        } else {
            (
                StateStore::<StateBackend>::new(&task_info, tables, control_tx.clone()).await,
                None,
            )
        };

        let (tx_queue_size_gauges, tx_queue_rem_gauges) =
            register_queue_gauges(&task_info, &out_qs);

        let task_info = Arc::new(task_info);
        Context {
            task_info: task_info.clone(),
            control_rx,
            control_tx: control_tx.clone(),
            watermarks: WatermarkHolder::new(vec![
                watermark.map(Watermark::EventTime);
                input_partitions
            ]),
            collector: Collector::<K, T> {
                task_info: task_info.clone(),
                out_qs,
                tx_queue_rem_gauges,
                tx_queue_size_gauges,
                _ts: PhantomData,
            },
            error_reporter: ErrorReporter {
                tx: control_tx,
                task_info,
            },
            state,
            _ts: PhantomData,
        }
    }

    pub fn new_for_test() -> (Self, Receiver<QueueItem>) {
        let (_, control_rx) = channel(128);
        let (command_tx, _) = channel(128);
        let (data_tx, data_rx) = channel(128);

        let out_queue = OutQueue::new(data_tx, false);

        let task_info = TaskInfo {
            job_id: "instance-1".to_string(),
            operator_name: "test-operator".to_string(),
            operator_id: "test-operator-1".to_string(),
            task_index: 0,
            parallelism: 1,
            key_range: 0..=0,
        };

        let ctx = futures::executor::block_on(Context::new(
            task_info,
            None,
            control_rx,
            command_tx,
            1,
            vec![vec![out_queue]],
            vec![],
        ));

        (ctx, data_rx)
    }

    pub fn watermark(&self) -> Option<Watermark> {
        self.watermarks.watermark()
    }

    pub fn last_present_watermark(&self) -> Option<SystemTime> {
        self.watermarks.last_present_watermark()
    }

    pub async fn schedule_timer<D: Data + PartialEq + Eq>(
        &mut self,
        key: &mut K,
        event_time: SystemTime,
        data: D,
    ) {
        if let Some(watermark) = self.last_present_watermark() {
            assert!(watermark < event_time, "Timer scheduled for past");
        };

        let mut timer_state: TimeKeyMap<K, TimerValue<K, D>, _> =
            self.state.get_time_key_map(TIMER_TABLE, None).await;
        let value = TimerValue {
            time: event_time,
            key: key.clone(),
            data,
        };

        debug!(
            "[{}] scheduling timer for [{}, {:?}]",
            self.task_info.task_index,
            hash_key(key),
            event_time
        );

        timer_state.insert(event_time, key.clone(), value);
    }

    pub async fn cancel_timer<D: Data + PartialEq + Eq>(
        &mut self,
        key: &mut K,
        event_time: SystemTime,
    ) -> Option<D> {
        let mut timer_state: TimeKeyMap<K, TimerValue<K, D>, _> =
            self.state.get_time_key_map(TIMER_TABLE, None).await;

        timer_state.remove(event_time, key).await.map(|v| v.data)
    }

    pub async fn flush_timers<D: Data + PartialEq + Eq>(&mut self) {
        let mut timer_state: TimeKeyMap<K, TimerValue<K, D>, _> =
            self.state.get_time_key_map(TIMER_TABLE, None).await;
        timer_state.flush().await;
    }

    pub async fn collect(&mut self, record: Record<K, T>) {
        self.collector.collect(record).await;
    }

    pub async fn collect_record_batch(&mut self, record: RecordBatch) {
        self.collector.collect_record_batch(record).await;
    }

    pub async fn broadcast(&mut self, message: Message<K, T>) {
        self.collector.broadcast(message).await;
    }

    pub async fn report_error(&mut self, message: impl Into<String>, details: impl Into<String>) {
        self.error_reporter.report_error(message, details).await;
    }

    pub async fn report_user_error(&mut self, error: UserError) {
        self.control_tx
            .send(ControlResp::Error {
                operator_id: self.task_info.operator_id.clone(),
                task_index: self.task_info.task_index,
                message: error.name,
                details: error.details,
            })
            .await
            .unwrap();
    }

    pub async fn load_compacted(&mut self, compaction: CompactionResult) {
        self.state.load_compacted(compaction).await;
    }

    /// Collects a source record, handling errors and rate limiting.
    /// Considers the `bad_data` option to determine whether to drop or fail on bad data.
    pub async fn collect_source_record(
        &mut self,
        timestamp: SystemTime,
        value: anyhow::Result<T, SourceError>,
        bad_data: &Option<BadData>,
        rate_limiter: &mut RateLimiter,
    ) -> anyhow::Result<(), UserError> {
        match value {
            Ok(value) => Ok(self
                .collector
                .collect(Record {
                    timestamp,
                    key: None,
                    value,
                })
                .await),
            Err(SourceError::BadData { details }) => match bad_data {
                Some(BadData::Drop {}) => {
                    rate_limiter
                        .rate_limit(|| async {
                            warn!("Dropping invalid data: {}", details.clone());
                            self.report_user_error(UserError::new(
                                "Dropping invalid data",
                                details,
                            ))
                            .await;
                        })
                        .await;
                    TaskCounters::DeserializationErrors
                        .for_task(&self.task_info)
                        .inc();
                    return Ok(());
                }
                Some(BadData::Fail {}) | None => {
                    Err(UserError::new("Deserialization error", details))
                }
            },
            Err(SourceError::Other { name, details }) => Err(UserError::new(name, details)),
        }
    }
}

#[derive(Clone)]
pub struct OutQueue {
    tx: Sender<QueueItem>,
    serialize: bool,
}

impl OutQueue {
    pub fn new(tx: Sender<QueueItem>, serialize: bool) -> Self {
        Self { tx, serialize }
    }

    pub async fn send(&self, task_info: &TaskInfo, message: Message<impl Key, impl Data>) {
        let is_end = message.is_end();
        let item = if self.serialize {
            let bytes = bincode::encode_to_vec(&message, config::standard()).unwrap();
            TaskCounters::BytesSent
                .for_task(task_info)
                .inc_by(bytes.len() as u64);

            QueueItem::Bytes(bytes)
        } else {
            QueueItem::Data(Box::new(message))
        };

        if self.tx.send(item).await.is_err() && !is_end {
            panic!("Failed to send, queue closed");
        }
    }
}

pub trait StreamNode: Send {
    fn node_name(&self) -> String;
    fn start(
        self: Box<Self>,
        task_info: TaskInfo,
        checkpoint_metadata: Option<CheckpointMetadata>,
        control_rx: Receiver<ControlMessage>,
        control_tx: Sender<ControlResp>,
        in_qs: Vec<Vec<Receiver<QueueItem>>>,
        out_qs: Vec<Vec<OutQueue>>,
    ) -> JoinHandle<()>;
}

impl TryFrom<Operator> for Box<dyn StreamNode> {
    type Error = anyhow::Error;

    fn try_from(operator: Operator) -> anyhow::Result<Self> {
        match operator {
            operator => bail!(
                "{:?} requires code generation, cannot instantiate directly",
                operator
            ),
        }
    }
}

#[derive(Debug)]
pub enum QueueItem {
    Data(Box<dyn Any + Send>),
    Bytes(Vec<u8>),
}

impl<K: Key, T: Data> From<QueueItem> for Message<K, T> {
    fn from(value: QueueItem) -> Self {
        match value {
            QueueItem::Data(datum) => *datum.downcast().unwrap(),
            QueueItem::Bytes(bs) => {
                bincode::decode_from_slice(&bs, config::standard())
                    .unwrap()
                    .0
            }
        }
    }
}
