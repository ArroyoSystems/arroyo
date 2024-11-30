use crate::{server_for_hash_array, RateLimiter};
use arrow::array::{make_builder, Array, ArrayBuilder, PrimitiveArray, RecordBatch};
use arrow::compute::{partition, sort_to_indices, take};
use arrow::datatypes::{SchemaRef, UInt64Type};
use arroyo_formats::de::{ArrowDeserializer, FieldValueType};
use arroyo_formats::should_flush;
use arroyo_metrics::{register_queue_gauge, QueueGauges, TaskCounters};
use arroyo_rpc::config::config;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, OperatorCheckpointMetadata, TableConfig, TaskCheckpointEventType,
};
use arroyo_rpc::schema_resolver::SchemaResolver;
use arroyo_rpc::{get_hasher, CompactionResult, ControlMessage, ControlResp};
use arroyo_state::tables::table_manager::TableManager;
use arroyo_state::{BackingStore, StateBackend};
use arroyo_types::{
    from_micros, ArrowMessage, ChainInfo, CheckpointBarrier, SourceError, TaskInfo, UserError,
    Watermark,
};
use async_trait::async_trait;
use datafusion::common::hash_utils;
use rand::Rng;
use std::collections::HashMap;
use std::mem::size_of_val;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::Notify;
use tracing::warn;

pub type QueueItem = ArrowMessage;

pub struct WatermarkHolder {
    // This is the last watermark with an actual value; this helps us keep track of the watermark we're at even
    // if we're currently idle
    last_present_watermark: Option<SystemTime>,
    cur_watermark: Option<Watermark>,
    watermarks: Vec<Option<Watermark>>,
}

impl WatermarkHolder {
    pub fn new(watermarks: Vec<Option<Watermark>>) -> Self {
        let mut s = Self {
            last_present_watermark: None,
            cur_watermark: None,
            watermarks,
        };
        s.update_watermark();

        s
    }

    pub fn watermark(&self) -> Option<Watermark> {
        self.cur_watermark
    }

    pub fn last_present_watermark(&self) -> Option<SystemTime> {
        self.last_present_watermark
    }

    fn update_watermark(&mut self) {
        self.cur_watermark =
            self.watermarks
                .iter()
                .try_fold(Watermark::Idle, |current, next| match (current, (*next)?) {
                    (Watermark::EventTime(cur), Watermark::EventTime(next)) => {
                        Some(Watermark::EventTime(cur.min(next)))
                    }
                    (Watermark::Idle, Watermark::EventTime(t))
                    | (Watermark::EventTime(t), Watermark::Idle) => Some(Watermark::EventTime(t)),
                    (Watermark::Idle, Watermark::Idle) => Some(Watermark::Idle),
                });

        if let Some(Watermark::EventTime(t)) = self.cur_watermark {
            self.last_present_watermark = Some(t);
        }
    }

    pub fn set(&mut self, idx: usize, watermark: Watermark) -> Option<Option<Watermark>> {
        *(self.watermarks.get_mut(idx)?) = Some(watermark);
        self.update_watermark();
        Some(self.cur_watermark)
    }
}

/// A wrapper for an UnboundedSender<QueueItem> that bounds by the number of rows within
/// a batch rather than the number of batches
#[derive(Clone)]
pub struct BatchSender {
    size: u32,
    tx: UnboundedSender<QueueItem>,
    queued_messages: Arc<AtomicU32>,
    queued_bytes: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

#[inline]
fn message_count(item: &QueueItem, size: u32) -> u32 {
    match item {
        QueueItem::Data(d) => (d.num_rows() as u32).min(size),
        QueueItem::Signal(_) => 1,
    }
}

#[inline]
fn message_bytes(item: &QueueItem) -> u64 {
    match item {
        QueueItem::Data(d) => d.get_array_memory_size() as u64,
        QueueItem::Signal(s) => size_of_val(s) as u64,
    }
}

impl BatchSender {
    pub async fn send(&self, item: QueueItem) -> Result<(), SendError<QueueItem>> {
        // Ensure that every message is sendable, even if it's bigger than our max size
        let count = message_count(&item, self.size);
        loop {
            if self.tx.is_closed() {
                return Err(SendError(item));
            }

            let cur = self.queued_messages.load(Ordering::Acquire);
            if cur as usize + count as usize <= self.size as usize {
                match self.queued_messages.compare_exchange(
                    cur,
                    cur + count,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        self.queued_bytes
                            .fetch_add(message_bytes(&item), Ordering::AcqRel);
                        return self.tx.send(item);
                    }
                    Err(_) => {
                        // try again
                        continue;
                    }
                }
            } else {
                // not enough room in the queue, wait to be notified that the receiver has
                // consumed
                self.notify.notified().await;
            }
        }
    }

    pub fn capacity(&self) -> u32 {
        self.size
            .saturating_sub(self.queued_messages.load(Ordering::Relaxed))
    }

    pub fn queued_bytes(&self) -> u64 {
        self.queued_bytes.load(Ordering::Relaxed)
    }

    pub fn size(&self) -> u32 {
        self.size
    }
}

pub struct BatchReceiver {
    size: u32,
    rx: UnboundedReceiver<QueueItem>,
    queued_messages: Arc<AtomicU32>,
    queued_bytes: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

impl BatchReceiver {
    pub async fn recv(&mut self) -> Option<QueueItem> {
        let item = self.rx.recv().await;
        if let Some(item) = &item {
            let count = message_count(item, self.size);
            self.queued_messages.fetch_sub(count, Ordering::SeqCst);
            self.queued_bytes
                .fetch_sub(message_bytes(item), Ordering::AcqRel);
            self.notify.notify_waiters();
        }
        item
    }
}

pub fn batch_bounded(size: u32) -> (BatchSender, BatchReceiver) {
    let (tx, rx) = unbounded_channel();
    let notify = Arc::new(Notify::new());
    let queued_messages = Arc::new(AtomicU32::new(0));
    let queued_bytes = Arc::new(AtomicU64::new(0));
    (
        BatchSender {
            size,
            tx,
            queued_messages: queued_messages.clone(),
            queued_bytes: queued_bytes.clone(),
            notify: notify.clone(),
        },
        BatchReceiver {
            size,
            rx,
            notify,
            queued_bytes,
            queued_messages,
        },
    )
}

struct ContextBuffer {
    buffer: Vec<Box<dyn ArrayBuilder>>,
    created: Instant,
    schema: SchemaRef,
}

impl ContextBuffer {
    fn new(schema: SchemaRef) -> Self {
        let buffer = schema
            .fields
            .iter()
            .map(|f| make_builder(f.data_type(), 16))
            .collect();

        Self {
            buffer,
            created: Instant::now(),
            schema,
        }
    }

    pub fn size(&self) -> usize {
        self.buffer[0].len()
    }

    pub fn should_flush(&self) -> bool {
        should_flush(self.size(), self.created)
    }

    pub fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            self.schema.clone(),
            self.buffer.iter_mut().map(|a| a.finish()).collect(),
        )
        .unwrap()
    }
}

pub struct SourceContext {
    pub out_schema: ArroyoSchema,
    pub error_reporter: ErrorReporter,
    buffer: ContextBuffer,
    buffered_error: Option<UserError>,
    error_rate_limiter: RateLimiter,
    deserializer: Option<ArrowDeserializer>,
    pub control_tx: Sender<ControlResp>,
    pub control_rx: Receiver<ControlMessage>,
    pub chain_info: Arc<ChainInfo>,
    pub task_info: Arc<TaskInfo>,
    pub table_manager: TableManager,
    pub watermarks: WatermarkHolder,
    pub(crate) collector: ArrowCollector,
}

impl SourceContext {
    pub async fn new(
        out_schema: ArroyoSchema,
        control_tx: Sender<ControlResp>,
        control_rx: Receiver<ControlMessage>,
        chain_info: Arc<ChainInfo>,
        task_info: Arc<TaskInfo>,
        tables: HashMap<String, TableConfig>,
        restore_from: Option<&CheckpointMetadata>,
        out_qs: Vec<Vec<BatchSender>>,
    ) -> Self {
        let (table_manager, watermark) =
            TableManager::load(task_info.clone(), tables, control_tx.clone(), restore_from)
                .await
                .expect("should be able to create TableManager");

        Self {
            out_schema: out_schema.clone(),
            error_reporter: ErrorReporter {
                tx: control_tx.clone(),
                task_info: task_info.clone(),
            },
            collector: ArrowCollector::new(chain_info.clone(), Some(out_schema.clone()), out_qs),
            buffer: ContextBuffer::new(out_schema.schema),
            buffered_error: None,
            error_rate_limiter: RateLimiter::new(),
            deserializer: None,
            control_tx,
            control_rx,
            chain_info,
            task_info,
            table_manager,
            watermarks: WatermarkHolder::new(vec![watermark.map(Watermark::EventTime); 1]),
        }
    }

    pub fn should_flush(&self) -> bool {
        self.buffer.should_flush()
            || self
                .deserializer
                .as_ref()
                .map(|d| d.should_flush())
                .unwrap_or(false)
    }

    pub async fn load_compacted(&mut self, compaction: CompactionResult) {
        //TODO: support compaction in the table manager
        self.table_manager
            .load_compacted(&compaction)
            .await
            .expect("should be able to load compacted");
    }

    pub async fn report_error(&mut self, message: impl Into<String>, details: impl Into<String>) {
        self.error_reporter.report_error(message, details).await;
    }

    pub async fn report_user_error(&mut self, error: UserError) {
        self.control_tx
            .send(ControlResp::Error {
                node_id: self.task_info.node_id,
                task_index: self.task_info.task_index as usize,
                message: error.name,
                details: error.details,
            })
            .await
            .unwrap();
    }

    pub fn initialize_deserializer(
        &mut self,
        format: Format,
        framing: Option<Framing>,
        bad_data: Option<BadData>,
    ) {
        if self.deserializer.is_some() {
            panic!("Deserialize already initialized");
        }

        self.deserializer = Some(ArrowDeserializer::new(
            format,
            self.out_schema.clone(),
            framing,
            bad_data.unwrap_or_default(),
        ));
    }

    pub fn initialize_deserializer_with_resolver(
        &mut self,
        format: Format,
        framing: Option<Framing>,
        bad_data: Option<BadData>,
        schema_resolver: Arc<dyn SchemaResolver + Sync>,
    ) {
        self.deserializer = Some(ArrowDeserializer::with_schema_resolver(
            format,
            framing,
            self.out_schema.clone(),
            bad_data.unwrap_or_default(),
            schema_resolver,
        ));
    }

    pub async fn deserialize_slice(
        &mut self,
        msg: &[u8],
        time: SystemTime,
        additional_fields: Option<&HashMap<&String, FieldValueType<'_>>>,
    ) -> Result<(), UserError> {
        let deserializer = self
            .deserializer
            .as_mut()
            .expect("deserializer not initialized!");

        let errors = deserializer
            .deserialize_slice(&mut self.buffer.buffer, msg, time, additional_fields)
            .await;
        self.collect_source_errors(errors).await?;

        Ok(())
    }

    /// Handling errors and rate limiting error reporting.
    /// Considers the `bad_data` option to determine whether to drop or fail on bad data.
    async fn collect_source_errors(&mut self, errors: Vec<SourceError>) -> Result<(), UserError> {
        let bad_data = self
            .deserializer
            .as_ref()
            .expect("deserializer not initialized")
            .bad_data();
        for error in errors {
            match error {
                SourceError::BadData { details } => match bad_data {
                    BadData::Drop {} => {
                        self.error_rate_limiter
                            .rate_limit(|| async {
                                warn!("Dropping invalid data: {}", details.clone());
                                self.control_tx
                                    .send(ControlResp::Error {
                                        node_id: self.task_info.node_id,
                                        task_index: self.task_info.task_index as usize,
                                        message: "Dropping invalid data".to_string(),
                                        details,
                                    })
                                    .await
                                    .unwrap();
                            })
                            .await;
                        TaskCounters::DeserializationErrors.for_task(&self.chain_info, |c| c.inc())
                    }
                    BadData::Fail {} => {
                        return Err(UserError::new("Deserialization error", details));
                    }
                },
                SourceError::Other { name, details } => {
                    return Err(UserError::new(name, details));
                }
            }
        }

        Ok(())
    }

    pub async fn flush_buffer(&mut self) -> Result<(), UserError> {
        if self.buffer.size() > 0 {
            let batch = self.buffer.finish();
            self.collector.collect(batch).await;
        }

        if let Some(deserializer) = self.deserializer.as_mut() {
            if let Some(buffer) = deserializer.flush_buffer() {
                match buffer {
                    Ok(batch) => {
                        self.collector.collect(batch).await;
                    }
                    Err(e) => {
                        self.collect_source_errors(vec![e]).await?;
                    }
                }
            }
        }

        if let Some(error) = self.buffered_error.take() {
            return Err(error);
        }

        Ok(())
    }

    pub async fn collect(&mut self, record: RecordBatch) {
        self.collector.collect(record).await;
    }

    pub async fn broadcast(&mut self, message: ArrowMessage) {
        if let Err(e) = self.flush_buffer().await {
            self.buffered_error.replace(e);
        }
        self.collector.broadcast(message).await;
    }
}

pub async fn send_checkpoint_event(
    tx: &Sender<ControlResp>,
    chain_info: &ChainInfo,
    barrier: CheckpointBarrier,
    event_type: TaskCheckpointEventType,
) {
    // These messages are received by the engine control thread,
    // which then sends a TaskCheckpointEventReq to the controller.
    tx.send(ControlResp::CheckpointEvent(arroyo_rpc::CheckpointEvent {
        checkpoint_epoch: barrier.epoch,
        node_id: chain_info.node_id,
        subtask_index: chain_info.task_index,
        time: SystemTime::now(),
        event_type,
    }))
    .await
    .unwrap();
}

pub struct OperatorContext {
    pub task_info: Arc<TaskInfo>,
    pub control_tx: Sender<ControlResp>,
    pub watermarks: WatermarkHolder,
    pub in_schemas: Vec<ArroyoSchema>,
    pub out_schema: Option<ArroyoSchema>,
    pub table_manager: TableManager,
}

#[derive(Clone)]
pub struct ErrorReporter {
    pub tx: Sender<ControlResp>,
    pub task_info: Arc<TaskInfo>,
}

impl ErrorReporter {
    pub async fn report_error(&mut self, message: impl Into<String>, details: impl Into<String>) {
        self.tx
            .send(ControlResp::Error {
                node_id: self.task_info.node_id,
                task_index: self.task_info.task_index as usize,
                message: message.into(),
                details: details.into(),
            })
            .await
            .unwrap();
    }
}

#[async_trait]
pub trait Collector: Send {
    async fn collect(&mut self, batch: RecordBatch);
    async fn broadcast(&mut self, message: ArrowMessage);
}

pub struct ChainCollector {
    messages: Vec<RecordBatch>,
}

impl ChainCollector {
    pub fn new() -> Self {
        Self { messages: vec![] }
    }

    pub fn iter(&self) -> impl Iterator<Item = &RecordBatch> {
        self.messages.iter()
    }

    pub fn clear(&mut self) {
        self.messages.clear();
    }
}

#[async_trait]
impl Collector for ChainCollector {
    async fn collect(&mut self, batch: RecordBatch) {
        self.messages.push(batch);
    }

    async fn broadcast(&mut self, message: ArrowMessage) {
        todo!()
    }
}

#[derive(Clone)]
pub struct ArrowCollector {
    pub chain_info: Arc<ChainInfo>,
    out_schema: Option<ArroyoSchema>,
    out_qs: Vec<Vec<BatchSender>>,
    tx_queue_rem_gauges: QueueGauges,
    tx_queue_size_gauges: QueueGauges,
    tx_queue_bytes_gauges: QueueGauges,
}

fn repartition<'a>(
    record: &'a RecordBatch,
    keys: &'a Option<Vec<usize>>,
    qs: usize,
) -> impl Iterator<Item = (usize, RecordBatch)> + 'a {
    let mut buf = vec![0; record.num_rows()];

    if let Some(keys) = keys {
        let keys: Vec<_> = keys.iter().map(|i| record.column(*i).clone()).collect();

        hash_utils::create_hashes(&keys[..], &get_hasher(), &mut buf).unwrap();
        let buf_array = PrimitiveArray::from(buf);

        let servers = server_for_hash_array(&buf_array, qs).unwrap();

        let indices = sort_to_indices(&servers, None, None).unwrap();
        let columns = record
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).unwrap())
            .collect();
        let sorted = RecordBatch::try_new(record.schema(), columns).unwrap();
        let sorted_keys = take(&servers, &indices, None).unwrap();

        let partition: arrow::compute::Partitions =
            partition(vec![sorted_keys.clone()].as_slice()).unwrap();
        let typed_keys: &PrimitiveArray<UInt64Type> = sorted_keys.as_any().downcast_ref().unwrap();
        let result: Vec<_> = partition
            .ranges()
            .into_iter()
            .map(|range| {
                let server_batch = sorted.slice(range.start, range.end - range.start);
                let server_id = typed_keys.value(range.start) as usize;
                (server_id, server_batch)
            })
            .collect();
        result.into_iter()
    } else {
        let range_size = record.num_rows() / qs + 1;
        let rotation = rand::thread_rng().gen_range(0..qs);
        let result: Vec<_> = (0..qs)
            .filter_map(|i| {
                let start = i * range_size;
                let end = (i + 1) * range_size;
                if start >= record.num_rows() {
                    None
                } else {
                    let server_batch = record.slice(start, end.min(record.num_rows()) - start);
                    Some(((i + rotation) % qs, server_batch))
                }
            })
            .collect();
        result.into_iter()
    }
}

#[async_trait]
impl Collector for ArrowCollector {
    async fn collect(&mut self, record: RecordBatch) {
        TaskCounters::MessagesSent
            .for_task(&self.chain_info, |c| c.inc_by(record.num_rows() as u64));
        TaskCounters::BatchesSent.for_task(&self.chain_info, |c| c.inc());
        TaskCounters::BytesSent.for_task(&self.chain_info, |c| {
            c.inc_by(record.get_array_memory_size() as u64)
        });

        let out_schema = self
            .out_schema
            .as_ref()
            .unwrap_or_else(|| panic!("No out-schema in {}!", self.chain_info));

        let record = RecordBatch::try_new(out_schema.schema.clone(), record.columns().to_vec())
            .unwrap_or_else(|e| {
                panic!(
                    "Data does not match expected schema for {}: {:?}. expected schema:\n{:#?}\n, actual schema:\n{:#?}",
                    self.chain_info, e, out_schema.schema, record.schema()
                );
            });

        for (i, out_q) in self.out_qs.iter_mut().enumerate() {
            let partitions = repartition(&record, &out_schema.key_indices, out_q.len());

            for (partition, batch) in partitions {
                out_q[partition]
                    .send(ArrowMessage::Data(batch))
                    .await
                    .unwrap();

                self.tx_queue_rem_gauges[i][partition]
                    .iter()
                    .for_each(|g| g.set(out_q[partition].capacity() as i64));

                self.tx_queue_size_gauges[i][partition]
                    .iter()
                    .for_each(|g| g.set(out_q[partition].size() as i64));

                self.tx_queue_bytes_gauges[i][partition]
                    .iter()
                    .for_each(|g| g.set(out_q[partition].queued_bytes() as i64));
            }
        }
    }

    async fn broadcast(&mut self, message: ArrowMessage) {
        for out_node in &self.out_qs {
            for q in out_node {
                q.send(message.clone()).await.unwrap_or_else(|e| {
                    panic!(
                        "failed to broadcast message <{:?}> for operator {}: {}",
                        message, self.chain_info, e
                    )
                });
            }
        }
    }
}

impl ArrowCollector {
    pub fn new(
        chain_info: Arc<ChainInfo>,
        out_schema: Option<ArroyoSchema>,
        out_qs: Vec<Vec<BatchSender>>,
    ) -> Self {
        let tx_queue_size_gauges = register_queue_gauge(
            "arroyo_worker_tx_queue_size",
            "Size of a tx queue",
            &chain_info,
            &out_qs,
            config().worker.queue_size as i64,
        );

        let tx_queue_rem_gauges = register_queue_gauge(
            "arroyo_worker_tx_queue_rem",
            "Remaining space in a tx queue",
            &chain_info,
            &out_qs,
            config().worker.queue_size as i64,
        );

        let tx_queue_bytes_gauges = register_queue_gauge(
            "arroyo_worker_tx_bytes",
            "Number of bytes queued in a tx queue",
            &chain_info,
            &out_qs,
            0,
        );

        // initialize counters so that tasks that never produce data still report 0
        for m in TaskCounters::variants() {
            m.for_task(&chain_info, |_| {});
        }

        Self {
            chain_info,
            out_schema,
            out_qs,
            tx_queue_rem_gauges,
            tx_queue_size_gauges,
            tx_queue_bytes_gauges,
        }
    }
}

impl OperatorContext {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        task_info: Arc<TaskInfo>,
        restore_from: Option<&CheckpointMetadata>,
        control_tx: Sender<ControlResp>,
        input_partitions: usize,
        in_schemas: Vec<ArroyoSchema>,
        out_schema: Option<ArroyoSchema>,
        tables: HashMap<String, TableConfig>,
    ) -> Self {
        let (table_manager, watermark) =
            TableManager::load(task_info.clone(), tables, control_tx.clone(), restore_from)
                .await
                .expect("should be able to create TableManager");

        Self {
            task_info: task_info.clone(),
            control_tx: control_tx.clone(),
            watermarks: WatermarkHolder::new(vec![
                watermark.map(Watermark::EventTime);
                input_partitions
            ]),
            in_schemas,
            out_schema: out_schema.clone(),
            table_manager,
        }
    }

    pub fn watermark(&self) -> Option<Watermark> {
        self.watermarks.watermark()
    }

    pub fn last_present_watermark(&self) -> Option<SystemTime> {
        self.watermarks.last_present_watermark()
    }

    pub async fn load_compacted(&mut self, compaction: &CompactionResult) {
        //TODO: support compaction in the table manager
        self.table_manager
            .load_compacted(compaction)
            .await
            .expect("should be able to load compacted");
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arroyo_types::to_nanos;
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_watermark_holder() {
        let t1 = SystemTime::UNIX_EPOCH;
        let t2 = t1 + Duration::from_secs(1);
        let t3 = t2 + Duration::from_secs(1);

        let mut w = WatermarkHolder::new(vec![None, None, None]);

        assert!(w.watermark().is_none());

        w.set(0, Watermark::EventTime(t1));
        w.set(1, Watermark::EventTime(t2));

        assert!(w.watermark().is_none());

        w.set(2, Watermark::EventTime(t3));

        assert_eq!(w.watermark(), Some(Watermark::EventTime(t1)));

        w.set(0, Watermark::Idle);
        assert_eq!(w.watermark(), Some(Watermark::EventTime(t2)));

        w.set(1, Watermark::Idle);
        w.set(2, Watermark::Idle);
        assert_eq!(w.watermark(), Some(Watermark::Idle));
    }

    #[tokio::test]
    async fn test_shuffles() {
        let timestamp = SystemTime::now();

        let data = vec![0, 101, 0, 101, 0, 101, 0, 0];

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(data.clone())),
            Arc::new(TimestampNanosecondArray::from(
                data.iter()
                    .map(|_| to_nanos(timestamp) as i64)
                    .collect::<Vec<_>>(),
            )),
        ];

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, false),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
        ]));

        let (tx1, mut rx1) = batch_bounded(8);
        let (tx2, mut rx2) = batch_bounded(8);

        let record = RecordBatch::try_new(schema.clone(), columns).unwrap();

        let task_info = Arc::new(TaskInfo {
            job_id: "test-job".to_string(),
            operator_name: "test-operator".to_string(),
            operator_id: "test-operator-1".to_string(),
            task_index: 0,
            parallelism: 1,
            key_range: 0..=1,
        });

        let out_qs = vec![vec![tx1, tx2]];

        let tx_queue_size_gauges = register_queue_gauge(
            "arroyo_worker_tx_queue_size",
            "Size of a tx queue",
            &task_info,
            &out_qs,
            0,
        );

        let tx_queue_rem_gauges = register_queue_gauge(
            "arroyo_worker_tx_queue_rem",
            "Remaining space in a tx queue",
            &task_info,
            &out_qs,
            0,
        );

        let tx_queue_bytes_gauges = register_queue_gauge(
            "arroyo_worker_tx_bytes",
            "Number of bytes queued in a tx queue",
            &task_info,
            &out_qs,
            0,
        );

        let mut collector = ArrowCollector {
            task_info,
            out_schema: Some(ArroyoSchema::new_keyed(schema, 1, vec![0])),
            projection: None,
            out_qs,
            tx_queue_rem_gauges,
            tx_queue_size_gauges,
            tx_queue_bytes_gauges,
        };

        collector.collect(record).await;

        drop(collector);

        // pull all messages out of the two queues
        let mut q1 = vec![];
        while let Some(m) = rx1.recv().await {
            q1.push(m);
        }

        let mut q2 = vec![];
        while let Some(m) = rx2.recv().await {
            q2.push(m);
        }

        let v1 = &q1[0];
        for v in &q1[1..] {
            assert_eq!(v1, v);
        }

        let v2 = &q2[0];
        for v in &q2[1..] {
            assert_eq!(v2, v);
        }
    }

    #[tokio::test]
    async fn test_batch_queues() {
        let (tx, mut rx) = batch_bounded(8);
        let msg = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();

        tx.send(ArrowMessage::Data(msg.clone())).await.unwrap();
        tx.send(ArrowMessage::Data(msg.clone())).await.unwrap();

        assert_eq!(tx.capacity(), 0);

        rx.recv().await.unwrap();
        rx.recv().await.unwrap();

        assert_eq!(tx.capacity(), 8);
    }

    #[tokio::test]
    async fn test_panic_propagation() {
        let (tx, mut rx) = batch_bounded(8);

        let msg = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();

        tokio::task::spawn(async move {
            let _f = rx.recv();
            panic!("at the disco");
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(tx.send(ArrowMessage::Data(msg)).await.is_err());
    }
}
