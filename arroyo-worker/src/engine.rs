use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::{mem, thread};

use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{bail, Error, Result};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_array::builder::UInt64Builder;
use arroyo_datastream::Operator;
use arroyo_datastream::{ArroyoSchema, ConnectorOp};
use arroyo_df::meta::SchemaRefWithMeta;
use arroyo_state::tables::time_key_map::TimeKeyMap;
use bincode::{Decode, Encode};
use datafusion_common::hash_utils;

use tracing::{debug, info, warn};

use crate::arrow::tumbling_aggregating_window::TumblingAggregatingWindowFunc;
use crate::arrow::{GrpcRecordBatchSink, KeyExecutionOperator, ValueExecutionOperator};
use crate::connectors::filesystem::source::FileSystemSourceFunc;
use crate::connectors::impulse::ImpulseSourceFunc;
use crate::metrics::{register_queue_gauges, QueueGauges, TaskCounters};
use crate::network_manager::{NetworkManager, Quad, Senders};
use crate::operator::{server_for_hash, ArrowOperator, ArrowOperatorConstructor, BaseOperator};
use crate::operators::PeriodicWatermarkGenerator;
use crate::{RateLimiter, METRICS_PUSH_INTERVAL, PROMETHEUS_PUSH_GATEWAY, TIMER_TABLE};
use arroyo_datastream::logical::{
    LogicalEdge, LogicalEdgeType, LogicalGraph, LogicalNode, OperatorName,
};
pub use arroyo_macro::StreamNode;
use arroyo_rpc::formats::BadData;
use arroyo_rpc::grpc::{
    api, CheckpointMetadata, TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior,
    TaskAssignment, TaskCheckpointEventType,
};
use arroyo_rpc::{CompactionResult, ControlMessage, ControlResp};
use arroyo_state::{BackingStore, StateBackend, StateStore};
use arroyo_types::{
    from_micros, range_for_server, ArrowMessage, CheckpointBarrier, Data, Key, SourceError,
    TaskInfo, UserError, Watermark, WorkerId,
};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use prometheus::labels;
use rand::{random, Rng};
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub const QUEUE_SIZE: usize = 4 * 1024;

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
        self.cur_watermark = self
            .watermarks
            .iter()
            .fold(Some(Watermark::Idle), |current, next| {
                match (current?, (*next)?) {
                    (Watermark::EventTime(cur), Watermark::EventTime(next)) => {
                        Some(Watermark::EventTime(cur.min(next)))
                    }
                    (Watermark::Idle, Watermark::EventTime(t))
                    | (Watermark::EventTime(t), Watermark::Idle) => Some(Watermark::EventTime(t)),
                    (Watermark::Idle, Watermark::Idle) => Some(Watermark::Idle),
                }
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

pub struct ArrowContext<S: BackingStore = StateBackend> {
    pub task_info: Arc<TaskInfo>,
    pub control_rx: Receiver<ControlMessage>,
    pub control_tx: Sender<ControlResp>,
    pub error_reporter: ErrorReporter,
    pub watermarks: WatermarkHolder,
    pub state: StateStore<S>,
    pub in_schemas: Vec<ArroyoSchema>,
    pub out_schema: Option<ArroyoSchema>,
    pub collector: ArrowCollector,
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
                operator_id: self.task_info.operator_id.clone(),
                task_index: self.task_info.task_index,
                message: message.into(),
                details: details.into(),
            })
            .await
            .unwrap();
    }
}

#[derive(Clone)]
pub struct ArrowCollector {
    task_info: Arc<TaskInfo>,
    out_schema: Option<ArroyoSchema>,
    projection: Option<Vec<usize>>,
    out_qs: Vec<Vec<Sender<ArrowMessage>>>,
    tx_queue_rem_gauges: QueueGauges,
    tx_queue_size_gauges: QueueGauges,
}

impl ArrowCollector {
    pub async fn collect(&mut self, record: RecordBatch) {
        fn repartition<'a>(record: &'a RecordBatch, keys: &'a Vec<usize>, qs: usize) -> impl Iterator<Item=(usize, RecordBatch)> + 'a {
            let mut buf = vec![0; record.num_rows()];

            if !keys.is_empty() {
                let keys: Vec<_> = keys.iter()
                    .map(|i| record.column(*i).clone())
                    .collect();

                hash_utils::create_hashes(&keys[..], &ahash::RandomState::new(), &mut buf)
                    .unwrap();
            } else {
                // TODO: do we want this be random or deterministic?
                buf.iter_mut().for_each(|x| *x = random());
            };

            let mut indices: Vec<_> = (0..qs)
                .map(|_| UInt64Builder::with_capacity(record.num_rows()))
                .collect();

            for (index, hash) in buf.into_iter().enumerate() {
                indices[server_for_hash(hash, qs)].append_value(index as u64);
            }

            indices
                .into_iter()
                .enumerate()
                .filter_map(|(partition, mut indices)| {
                    let indices = indices.finish();
                    (!indices.is_empty()).then_some((partition, indices))
                })
                .map(move |(partition, indices)| {
                    let columns = record
                        .columns()
                        .iter()
                        .map(|c| {
                            arrow::compute::take(c.as_ref(), &indices, None).unwrap()
                        })
                        .collect();

                    let batch =
                        RecordBatch::try_new(record.schema(), columns).unwrap();

                    (partition, batch)
                })
        }

        TaskCounters::MessagesSent.for_task(&self.task_info).inc();

        let out_schema = self.out_schema.as_ref().unwrap();

        let record = if let Some(projection) = &self.projection {
            record.project(&projection).unwrap_or_else(|e| {
                panic!(
                    "failed to project for operator {}: {}",
                    self.task_info.operator_id, e
                )
            })
        } else {
            record
        };

        let record = RecordBatch::try_new(out_schema.schema.clone(), record.columns().to_vec())
            .unwrap_or_else(|e| {
                panic!(
                    "Data does not match expected schema for {}: {:?}",
                    self.task_info.operator_id, e
                );
            });


        for (i, out_q) in self.out_qs.iter_mut().enumerate() {
            let partitions = repartition(&record,
                                         &self.out_schema.as_ref().unwrap().key_cols, out_q.len());

            for (partition, batch) in partitions {
                out_q[partition]
                    .send(ArrowMessage::Record(batch))
                    .await
                    .unwrap();

                self.tx_queue_rem_gauges[i][partition]
                    .iter()
                    .for_each(|g| g.set(out_q[partition].capacity() as i64));

                self.tx_queue_size_gauges[i][partition]
                    .iter()
                    .for_each(|g| g.set(QUEUE_SIZE as i64));
            }
        }
    }

    pub async fn broadcast(&mut self, message: ArrowMessage) {
        for out_node in &self.out_qs {
            for q in out_node {
                q.send(message.clone()).await.unwrap()
            }
        }
    }
}

impl ArrowContext {
    pub async fn new(
        task_info: TaskInfo,
        restore_from: Option<CheckpointMetadata>,
        control_rx: Receiver<ControlMessage>,
        control_tx: Sender<ControlResp>,
        input_partitions: usize,
        in_schemas: Vec<ArroyoSchema>,
        out_schema: Option<ArroyoSchema>,
        projection: Option<Vec<usize>>,
        out_qs: Vec<Vec<Sender<ArrowMessage>>>,
        mut tables: Vec<TableDescriptor>,
        _table_schemas: HashMap<char, SchemaRefWithMeta>,
    ) -> Self {
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

        Self {
            task_info: task_info.clone(),
            control_rx,
            control_tx: control_tx.clone(),
            watermarks: WatermarkHolder::new(vec![
                watermark.map(Watermark::EventTime);
                input_partitions
            ]),
            in_schemas,
            out_schema: out_schema.clone(),
            collector: ArrowCollector {
                task_info: task_info.clone(),
                out_qs,
                tx_queue_rem_gauges,
                tx_queue_size_gauges,
                out_schema,
                projection,
            },
            error_reporter: ErrorReporter {
                tx: control_tx,
                task_info,
            },
            state,
        }
    }

    pub fn new_for_test() -> (Self, Receiver<QueueItem>) {
        todo!()

        // let (_, control_rx) = channel(128);
        // let (command_tx, _) = channel(128);
        // let (data_tx, data_rx) = channel(128);
        //
        // let task_info = TaskInfo {
        //     job_id: "instance-1".to_string(),
        //     operator_name: "test-operator".to_string(),
        //     operator_id: "test-operator-1".to_string(),
        //     task_index: 0,
        //     parallelism: 1,
        //     key_range: 0..=0,
        // };

        // let ctx = futures::executor::block_on(ArrowContext::new(
        //     task_info,
        //     None,
        //     control_rx,
        //     command_tx,
        //     1,
        //     vec![vec![data_tx]],
        //     vec![],
        // ));
        //
        // (ctx, data_rx)
    }

    pub fn watermark(&self) -> Option<Watermark> {
        self.watermarks.watermark()
    }

    pub fn last_present_watermark(&self) -> Option<SystemTime> {
        self.watermarks.last_present_watermark()
    }

    pub async fn schedule_timer<D: Data + PartialEq + Eq, K: Key>(
        &mut self,
        key: &mut K,
        event_time: SystemTime,
        data: D,
    ) {
        todo!("timer");
    }

    pub async fn cancel_timer<D: Data + PartialEq + Eq, K: Key>(
        &mut self,
        key: &mut K,
        event_time: SystemTime,
    ) -> Option<D> {
        todo!("timer")
    }

    pub async fn flush_timers<D: Data + PartialEq + Eq>(&mut self) {
        todo!("timer")
    }

    pub async fn collect(&mut self, record: RecordBatch) {
        self.collector.collect(record).await;
    }

    pub async fn broadcast(&mut self, message: ArrowMessage) {
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

    pub async fn send_checkpoint_event(
        &mut self,
        barrier: CheckpointBarrier,
        event_type: TaskCheckpointEventType,
    ) {
        // These messages are received by the engine control thread,
        // which then sends a TaskCheckpointEventReq to the controller.
        self.control_tx
            .send(ControlResp::CheckpointEvent(arroyo_rpc::CheckpointEvent {
                checkpoint_epoch: barrier.epoch,
                operator_id: self.task_info.operator_id.clone(),
                subtask_index: self.task_info.task_index as u32,
                time: std::time::SystemTime::now(),
                event_type,
            }))
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
        value: Result<RecordBatch, SourceError>,
        bad_data: &Option<BadData>,
        rate_limiter: &mut RateLimiter,
    ) -> Result<(), UserError> {
        todo!("collect source record");
        match value {
            Ok(value) => Ok(self.collector.collect(value).await),
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
                    Ok(())
                }
                Some(BadData::Fail {}) | None => {
                    Err(UserError::new("Deserialization error", details))
                }
            },
            Err(SourceError::Other { name, details }) => Err(UserError::new(name, details)),
        }
    }
}

#[derive(Encode, Decode, Clone, Debug, PartialEq, Eq)]
pub struct TimerValue<K: Key, T: Decode + Encode + Clone + PartialEq + Eq> {
    pub time: SystemTime,
    pub key: K,
    pub data: T,
}

#[derive(Debug)]
pub struct CheckpointCounter {
    inputs: Vec<Option<u32>>,
    counter: Option<usize>,
}

impl CheckpointCounter {
    pub fn new(size: usize) -> CheckpointCounter {
        CheckpointCounter {
            inputs: vec![None; size],
            counter: None,
        }
    }

    pub fn is_blocked(&self, idx: usize) -> bool {
        self.inputs[idx].is_some()
    }

    pub fn all_clear(&self) -> bool {
        self.inputs.iter().all(|x| x.is_none())
    }

    pub fn mark(&mut self, idx: usize, checkpoint: &CheckpointBarrier) -> bool {
        assert!(self.inputs[idx].is_none());

        if self.inputs.len() == 1 {
            return true;
        }

        self.inputs[idx] = Some(checkpoint.epoch);
        self.counter = match self.counter {
            None => Some(self.inputs.len() - 1),
            Some(1) => {
                for v in self.inputs.iter_mut() {
                    *v = None;
                }
                None
            }
            Some(n) => Some(n - 1),
        };

        self.counter.is_none()
    }
}

pub struct SubtaskNode {
    pub id: String,
    pub subtask_idx: usize,
    pub parallelism: usize,
    pub in_schemas: Vec<ArroyoSchema>,
    pub out_schema: Option<ArroyoSchema>,
    pub projection: Option<Vec<usize>>,
    pub node: Box<dyn BaseOperator>,
}

impl Debug for SubtaskNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}-{}", self.node.name(), self.id, self.subtask_idx)
    }
}

pub struct QueueNode {
    task_info: TaskInfo,
    tx: Sender<ControlMessage>,
}

impl Debug for QueueNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            self.task_info.operator_name, self.task_info.operator_id, self.task_info.task_index
        )
    }
}

#[derive(Debug)]
enum SubtaskOrQueueNode {
    SubtaskNode(SubtaskNode),
    QueueNode(QueueNode),
}

struct PhysicalGraphEdge {
    edge_idx: usize,
    in_logical_idx: usize,
    out_logical_idx: usize,
    edge: LogicalEdgeType,
    tx: Option<Sender<QueueItem>>,
    rx: Option<Receiver<QueueItem>>,
}

impl Debug for PhysicalGraphEdge {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} -> {}",
            self.edge_idx, self.in_logical_idx, self.out_logical_idx
        )
    }
}

impl SubtaskOrQueueNode {
    pub fn take_subtask(&mut self, job_id: String) -> (SubtaskNode, Receiver<ControlMessage>) {
        let (mut qn, rx) = match self {
            SubtaskOrQueueNode::SubtaskNode(sn) => {
                let (tx, rx) = channel(16);

                let n = SubtaskOrQueueNode::QueueNode(QueueNode {
                    task_info: TaskInfo {
                        job_id,
                        operator_name: sn.node.name(),
                        operator_id: sn.id.clone(),
                        task_index: sn.subtask_idx,
                        parallelism: sn.parallelism,
                        key_range: range_for_server(sn.subtask_idx, sn.parallelism),
                    },
                    tx,
                });

                (n, rx)
            }
            SubtaskOrQueueNode::QueueNode(_) => panic!("already swapped for queue node"),
        };

        mem::swap(self, &mut qn);

        (qn.unwrap_subtask(), rx)
    }

    pub fn id(&self) -> &str {
        match self {
            SubtaskOrQueueNode::SubtaskNode(n) => &n.id,
            SubtaskOrQueueNode::QueueNode(n) => &n.task_info.operator_id,
        }
    }

    pub fn subtask_idx(&self) -> usize {
        match self {
            SubtaskOrQueueNode::SubtaskNode(n) => n.subtask_idx,
            SubtaskOrQueueNode::QueueNode(n) => n.task_info.task_index,
        }
    }

    fn unwrap_subtask(self) -> SubtaskNode {
        match self {
            SubtaskOrQueueNode::SubtaskNode(n) => n,
            SubtaskOrQueueNode::QueueNode(_) => panic!("not subtask node"),
        }
    }

    fn as_queue(&self) -> &QueueNode {
        match self {
            SubtaskOrQueueNode::SubtaskNode(_) => panic!("not a queue node"),
            SubtaskOrQueueNode::QueueNode(qn) => qn,
        }
    }
}

pub struct Program {
    pub name: String,
    graph: DiGraph<SubtaskOrQueueNode, PhysicalGraphEdge>,
}

impl Program {
    pub fn total_nodes(&self) -> usize {
        self.graph.node_count()
    }

    pub fn local_from_logical(name: String, logical: &DiGraph<LogicalNode, LogicalEdge>) -> Self {
        let assignments = logical
            .node_weights()
            .flat_map(|weight| {
                (0..weight.parallelism).map(|index| TaskAssignment {
                    operator_id: weight.operator_id.clone(),
                    operator_subtask: index as u64,
                    worker_id: 0,
                    worker_addr: "".into(),
                })
            })
            .collect();
        Self::from_logical(name, logical, &assignments)
    }

    pub fn from_logical(
        name: String,
        logical: &LogicalGraph,
        assignments: &Vec<TaskAssignment>,
    ) -> Program {
        let mut physical = DiGraph::new();

        let mut parallelism_map = HashMap::new();
        for task in assignments {
            *(parallelism_map.entry(&task.operator_id).or_insert(0usize)) += 1;
        }

        for idx in logical.node_indices() {
            let in_schemas: Vec<_> = logical
                .edges_directed(idx, Direction::Incoming)
                .map(|edge| edge.weight().schema.clone())
                .collect();

            let out_schema = logical
                .edges_directed(idx, Direction::Outgoing)
                .map(|edge| edge.weight().schema.clone())
                .next();

            let projection = logical
                .edges_directed(idx, Direction::Outgoing)
                .map(|edge| edge.weight().projection.clone())
                .next()
                .unwrap_or_default();

            let node = logical.node_weight(idx).unwrap();
            let parallelism = *parallelism_map.get(&node.operator_id).unwrap_or_else(|| {
                warn!("no assignments for operator {}", node.operator_id);
                &node.parallelism
            });
            for i in 0..parallelism {
                physical.add_node(SubtaskOrQueueNode::SubtaskNode(SubtaskNode {
                    id: node.operator_id.clone(),
                    subtask_idx: i,
                    parallelism,
                    in_schemas: in_schemas.clone(),
                    out_schema: out_schema.clone(),
                    node: construct_operator(node.operator_name, node.operator_config.clone()),
                    projection: projection.clone(),
                }));
            }
        }

        for idx in logical.edge_indices() {
            let edge = logical.edge_weight(idx).unwrap();
            let (logical_in_node_idx, logical_out_node_idx) = logical.edge_endpoints(idx).unwrap();
            let logical_in_node = logical.node_weight(logical_in_node_idx).unwrap();
            let logical_out_node = logical.node_weight(logical_out_node_idx).unwrap();

            let from_nodes: Vec<_> = physical
                .node_indices()
                .filter(|n| physical.node_weight(*n).unwrap().id() == logical_in_node.operator_id)
                .collect();
            assert_ne!(from_nodes.len(), 0, "failed to find from nodes");
            let to_nodes: Vec<_> = physical
                .node_indices()
                .filter(|n| physical.node_weight(*n).unwrap().id() == logical_out_node.operator_id)
                .collect();
            assert_ne!(from_nodes.len(), 0, "failed to find to nodes");

            match edge.edge_type {
                LogicalEdgeType::Forward => {
                    if from_nodes.len() != to_nodes.len() && !from_nodes.is_empty() {
                        panic!("cannot create a forward connection between nodes of different parallelism");
                    }
                    for (f, t) in from_nodes.iter().zip(&to_nodes) {
                        let (tx, rx) = channel(QUEUE_SIZE);
                        let edge = PhysicalGraphEdge {
                            edge_idx: 0,
                            in_logical_idx: logical_in_node_idx.index(),
                            out_logical_idx: logical_out_node_idx.index(),
                            edge: edge.edge_type,
                            tx: Some(tx),
                            rx: Some(rx),
                        };
                        physical.add_edge(*f, *t, edge);
                    }
                }
                LogicalEdgeType::Shuffle
                | LogicalEdgeType::LeftJoin
                | LogicalEdgeType::RightJoin => {
                    for f in &from_nodes {
                        for (idx, t) in to_nodes.iter().enumerate() {
                            let (tx, rx) = channel(QUEUE_SIZE);
                            let edge = PhysicalGraphEdge {
                                edge_idx: idx,
                                in_logical_idx: logical_in_node_idx.index(),
                                out_logical_idx: logical_out_node_idx.index(),
                                edge: edge.edge_type,
                                tx: Some(tx),
                                rx: Some(rx),
                            };
                            physical.add_edge(*f, *t, edge);
                        }
                    }
                }
            }
        }

        Program {
            name,
            graph: physical,
        }
    }

    pub fn tasks_per_operator(&self) -> HashMap<String, usize> {
        let mut tasks_per_operator = HashMap::new();
        for node in self.graph.node_weights() {
            let entry = tasks_per_operator.entry(node.id().to_string()).or_insert(0);
            *entry += 1;
        }
        tasks_per_operator
    }
}

pub struct Engine {
    program: Program,
    worker_id: WorkerId,
    run_id: String,
    job_id: String,
    network_manager: NetworkManager,
    assignments: HashMap<(String, usize), TaskAssignment>,
}

pub struct StreamConfig {
    pub restore_epoch: Option<u32>,
}

pub struct RunningEngine {
    program: Program,
    assignments: HashMap<(String, usize), TaskAssignment>,
    worker_id: WorkerId,
}

impl RunningEngine {
    pub fn source_controls(&self) -> Vec<Sender<ControlMessage>> {
        self.program
            .graph
            .externals(Direction::Incoming)
            .filter(|idx| {
                let w = self.program.graph.node_weight(*idx).unwrap();
                self.assignments
                    .get(&(w.id().to_string(), w.subtask_idx()))
                    .unwrap()
                    .worker_id
                    == self.worker_id.0
            })
            .map(|idx| {
                self.program
                    .graph
                    .node_weight(idx)
                    .unwrap()
                    .as_queue()
                    .tx
                    .clone()
            })
            .collect()
    }

    pub fn sink_controls(&self) -> Vec<Sender<ControlMessage>> {
        self.program
            .graph
            .externals(Direction::Outgoing)
            .filter(|idx| {
                let w = self.program.graph.node_weight(*idx).unwrap();
                self.assignments
                    .get(&(w.id().to_string(), w.subtask_idx()))
                    .unwrap()
                    .worker_id
                    == self.worker_id.0
            })
            .map(|idx| {
                self.program
                    .graph
                    .node_weight(idx)
                    .unwrap()
                    .as_queue()
                    .tx
                    .clone()
            })
            .collect()
    }

    pub fn operator_controls(&self) -> HashMap<String, Vec<Sender<ControlMessage>>> {
        let mut controls = HashMap::new();

        self.program
            .graph
            .node_indices()
            .filter(|idx| {
                let w = self.program.graph.node_weight(*idx).unwrap();
                self.assignments
                    .get(&(w.id().to_string(), w.subtask_idx()))
                    .unwrap()
                    .worker_id
                    == self.worker_id.0
            })
            .for_each(|idx| {
                let w = self.program.graph.node_weight(idx).unwrap();
                let assignment = self
                    .assignments
                    .get(&(w.id().to_string(), w.subtask_idx()))
                    .unwrap();
                let tx = self
                    .program
                    .graph
                    .node_weight(idx)
                    .unwrap()
                    .as_queue()
                    .tx
                    .clone();
                controls
                    .entry(assignment.operator_id.clone())
                    .or_insert(vec![])
                    .push(tx);
            });

        controls
    }
}

impl Engine {
    pub fn new(
        program: Program,
        worker_id: WorkerId,
        job_id: String,
        run_id: String,
        network_manager: NetworkManager,
        assignments: Vec<TaskAssignment>,
    ) -> Self {
        let assignments = assignments
            .into_iter()
            .map(|a| ((a.operator_id.to_string(), a.operator_subtask as usize), a))
            .collect();

        Self {
            program,
            worker_id,
            job_id,
            run_id,
            network_manager,
            assignments,
        }
    }

    pub fn for_local(program: Program, job_id: String) -> Self {
        let worker_id = WorkerId(0);
        let assignments = program
            .graph
            .node_weights()
            .map(|n| {
                (
                    (n.id().to_string(), n.subtask_idx()),
                    TaskAssignment {
                        operator_id: n.id().to_string(),
                        operator_subtask: n.subtask_idx() as u64,
                        worker_id: worker_id.0,
                        worker_addr: "locahost:0".to_string(),
                    },
                )
            })
            .collect();

        Self {
            program,
            worker_id,
            job_id,
            run_id: "0".to_string(),
            network_manager: NetworkManager::new(0),
            assignments,
        }
    }

    pub async fn start(mut self, config: StreamConfig) -> (RunningEngine, Receiver<ControlResp>) {
        info!("Starting job {}", self.job_id);

        let checkpoint_metadata = if let Some(epoch) = config.restore_epoch {
            info!("Restoring checkpoint {} for job {}", epoch, self.job_id);
            Some(
                StateBackend::load_checkpoint_metadata(&self.job_id, epoch)
                    .await
                    .unwrap_or_else(|| {
                        panic!("failed to load checkpoint metadata for epoch {}", epoch)
                    }),
            )
        } else {
            None
        };

        let node_indexes: Vec<_> = self.program.graph.node_indices().collect();

        let (control_tx, control_rx) = channel(128);
        let mut senders = Senders::new();
        let worker_id = self.worker_id;

        for idx in node_indexes {
            self.schedule_node(&checkpoint_metadata, &control_tx, &mut senders, idx)
                .await;
        }

        self.network_manager.start(senders).await;

        // clear all of the TXs in the graph so that we don't leave dangling senders
        for n in self.program.graph.edge_weights_mut() {
            n.tx = None;
        }

        self.spawn_metrics_thread();

        (
            RunningEngine {
                program: self.program,
                assignments: self.assignments,
                worker_id,
            },
            control_rx,
        )
    }

    async fn schedule_node(
        &mut self,
        checkpoint_metadata: &Option<CheckpointMetadata>,
        control_tx: &Sender<ControlResp>,
        senders: &mut Senders,
        idx: NodeIndex,
    ) {
        let (node, control_rx) = self
            .program
            .graph
            .node_weight_mut(idx)
            .unwrap()
            .take_subtask(self.job_id.clone());

        let assignment = &self
            .assignments
            .get(&(node.id.clone().to_string(), node.subtask_idx))
            .cloned()
            .unwrap_or_else(|| {
                panic!(
                    "Could not find assignment for node {}-{}",
                    node.id.clone(),
                    node.subtask_idx
                )
            });

        if assignment.worker_id == self.worker_id.0 {
            self.run_locally(checkpoint_metadata, control_tx, idx, node, control_rx)
                .await;
        } else {
            self.connect_to_remote_task(
                senders,
                idx,
                node.id.clone(),
                node.subtask_idx,
                assignment,
            )
            .await;
        }
    }

    async fn connect_to_remote_task(
        &mut self,
        senders: &mut Senders,
        idx: NodeIndex,
        node_id: String,
        node_subtask_idx: usize,
        assignment: &TaskAssignment,
    ) {
        info!(
            "Connecting to remote task {}-{} running on {}",
            node_id, node_subtask_idx, assignment.worker_addr
        );

        for edge in self.program.graph.edges_directed(idx, Direction::Outgoing) {
            let target = self.program.graph.node_weight(edge.target()).unwrap();

            let quad = Quad {
                src_id: edge.weight().in_logical_idx,
                src_idx: node_subtask_idx,
                dst_id: edge.weight().out_logical_idx,
                dst_idx: target.subtask_idx(),
            };

            senders.add(quad, edge.weight().tx.as_ref().unwrap().clone());
        }

        let mut connects = vec![];

        for edge in self.program.graph.edges_directed(idx, Direction::Incoming) {
            let source = self.program.graph.node_weight(edge.source()).unwrap();

            let quad = Quad {
                src_id: edge.weight().in_logical_idx,
                src_idx: source.subtask_idx(),
                dst_id: edge.weight().out_logical_idx,
                dst_idx: node_subtask_idx,
            };

            connects.push((edge.id(), quad));
        }

        for (id, quad) in connects {
            let edge = self.program.graph.edge_weight_mut(id).unwrap();

            self.network_manager
                .connect(
                    assignment.worker_addr.clone(),
                    quad,
                    edge.rx.take().unwrap(),
                )
                .await;
        }
    }

    async fn run_locally(
        &mut self,
        checkpoint_metadata: &Option<CheckpointMetadata>,
        control_tx: &Sender<ControlResp>,
        idx: NodeIndex,
        node: SubtaskNode,
        control_rx: Receiver<ControlMessage>,
    ) {
        info!(
            "[{:?}] Scheduling {}-{}-{} ({}/{})",
            self.worker_id,
            node.node.name(),
            node.id,
            node.subtask_idx,
            node.subtask_idx + 1,
            node.parallelism
        );

        let mut in_qs_map: BTreeMap<(LogicalEdgeType, usize), Vec<Receiver<QueueItem>>> =
            BTreeMap::new();

        for edge in self.program.graph.edge_indices() {
            if self.program.graph.edge_endpoints(edge).unwrap().1 == idx {
                let weight = self.program.graph.edge_weight_mut(edge).unwrap();
                in_qs_map
                    .entry((weight.edge.clone(), weight.in_logical_idx))
                    .or_default()
                    .push(weight.rx.take().unwrap());
            }
        }

        let mut out_qs_map: BTreeMap<usize, BTreeMap<usize, Sender<ArrowMessage>>> =
            BTreeMap::new();

        for edge in self.program.graph.edges_directed(idx, Direction::Outgoing) {
            // is the target of this edge local or remote?
            let local = {
                let target = self.program.graph.node_weight(edge.target()).unwrap();
                self.assignments
                    .get(&(target.id().to_string(), target.subtask_idx()))
                    .unwrap()
                    .worker_id
                    == self.worker_id.0
            };

            let tx = edge.weight().tx.as_ref().unwrap().clone();
            out_qs_map
                .entry(edge.weight().out_logical_idx)
                .or_default()
                .insert(edge.weight().edge_idx, tx);
        }

        let task_info = self
            .program
            .graph
            .node_weight(idx)
            .unwrap()
            .as_queue()
            .task_info
            .clone();

        let operator_id = task_info.operator_id.clone();
        let task_index = task_info.task_index;
        let join_task = node.node.start(
            task_info,
            checkpoint_metadata.clone(),
            control_rx,
            control_tx.clone(),
            node.in_schemas,
            node.out_schema,
            node.projection,
            in_qs_map.into_values().collect(),
            out_qs_map
                .into_values()
                .map(|v| v.into_values().collect())
                .collect(),
        );

        let send_copy = control_tx.clone();
        tokio::spawn(async move {
            send_copy
                .send(ControlResp::TaskStarted {
                    operator_id: operator_id.clone(),
                    task_index,
                    start_time: SystemTime::now(),
                })
                .await
                .unwrap();
            if let Err(error) = join_task.await {
                send_copy
                    .send(ControlResp::TaskFailed {
                        operator_id,
                        task_index,
                        error: error.to_string(),
                    })
                    .await
                    .ok();
            };
        });
    }

    fn spawn_metrics_thread(&mut self) {
        let labels = labels! {
            "worker_id".to_string() => format!("{}", self.worker_id.0),
            "job_name".to_string() => self.program.name.clone(),
            "job_id".to_string() => self.job_id.clone(),
            "run_id".to_string() => self.run_id.to_string(),
        };
        let job_id = self.job_id.clone();

        thread::spawn(move || {
            #[cfg(not(target_os = "freebsd"))]
            let _agent = arroyo_server_common::try_profile_start(
                "node",
                [("job_id", job_id.as_str())].to_vec(),
            );
            // push to metrics gateway
            loop {
                let metrics = prometheus::gather();
                if let Err(e) = prometheus::push_metrics(
                    "arroyo-worker",
                    labels.clone(),
                    PROMETHEUS_PUSH_GATEWAY,
                    metrics,
                    None,
                ) {
                    debug!(
                        "Failed to push metrics to {}: {}",
                        PROMETHEUS_PUSH_GATEWAY, e
                    );
                }
                thread::sleep(METRICS_PUSH_INTERVAL);
            }
        });
    }
}

pub fn construct_operator(operator: OperatorName, config: Vec<u8>) -> Box<dyn BaseOperator> {
    let mut buf = config.as_slice();
    match operator {
        OperatorName::Watermark => Box::new(
            PeriodicWatermarkGenerator::from_config(prost::Message::decode(&mut buf).unwrap())
                .unwrap(),
        ),
        OperatorName::ArrowValue => Box::new(
            ValueExecutionOperator::from_config(prost::Message::decode(&mut buf).unwrap()).unwrap(),
        ),
        OperatorName::ArrowKey => Box::new(
            KeyExecutionOperator::from_config(prost::Message::decode(&mut buf).unwrap()).unwrap(),
        ),
        OperatorName::ArrowAggregate => Box::new(
            TumblingAggregatingWindowFunc::from_config(prost::Message::decode(&mut buf).unwrap())
                .unwrap(),
        ),
        OperatorName::ConnectorSource | OperatorName::ConnectorSink => {
            let op: api::ConnectorOp = prost::Message::decode(&mut buf).unwrap();
            match op.operator.as_str() {
                "connectors::impulse::ImpulseSourceFunc" => {
                    Box::new(ImpulseSourceFunc::from_config(op).unwrap())
                }
                "connectors::filesystem::source::FileSystemSourceFunc" => {
                    Box::new(FileSystemSourceFunc::from_config(op).unwrap())
                }
                "GrpcSink" => Box::new(GrpcRecordBatchSink::from_config(op).unwrap()),
                c => panic!("unknown operator {}", c),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use arrow_array::{TimestampNanosecondArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use arroyo_types::to_nanos;

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

        let data = vec![0,1,0,1,0,1,0,0];

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(data.clone())),
            Arc::new(TimestampNanosecondArray::from(
                data.iter().map(|_| to_nanos(timestamp) as i64).collect::<Vec<_>>(),
            )),
        ];

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, false),
            Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        ]));

        let (tx1, mut rx1) = channel(8);
        let (tx2, mut rx2) = channel(8);

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

        let (tx_queue_size_gauges, tx_queue_rem_gauges) =
            register_queue_gauges(&*task_info, &out_qs);


        let mut collector = ArrowCollector {
            task_info,
            out_schema: Some(ArroyoSchema {
                schema,
                timestamp_col: 1,
                key_cols: vec![0],
            }),
            projection: None,
            out_qs,
            tx_queue_rem_gauges,
            tx_queue_size_gauges,
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

        println!("{:?}\n{:?}", q1, q2);

    }
}
