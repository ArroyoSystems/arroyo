use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::{mem, thread};

use std::sync::Arc;
use std::time::SystemTime;

use arroyo_state::tables::time_key_map::TimeKeyMap;
use bincode::{config, Decode, Encode};

use tracing::{debug, info, warn};

pub use arroyo_macro::StreamNode;
use arroyo_rpc::grpc::{
    CheckpointMetadata, TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior,
    TaskAssignment,
};
use arroyo_rpc::{CompactionResult, ControlMessage, ControlResp};
use arroyo_types::{
    from_micros, range_for_server, server_for_hash, CheckpointBarrier, Data, Key, Message, Record,
    TaskInfo, UserError, Watermark, WorkerId,
};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use prometheus::labels;
use rand::Rng;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;

use crate::metrics::{register_queue_gauges, QueueGauges, TaskCounters};
use crate::network_manager::{NetworkManager, Quad, Senders};
use crate::TIMER_TABLE;
use crate::{LogicalEdge, LogicalNode, METRICS_PUSH_INTERVAL, PROMETHEUS_PUSH_GATEWAY};
use arroyo_state::{hash_key, BackingStore, StateBackend, StateStore};

const QUEUE_SIZE: usize = 4 * 1024;

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

#[derive(Clone)]
pub struct ErrorReporter {
    tx: Sender<ControlResp>,
    task_info: Arc<TaskInfo>,
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
pub struct Collector<K: Key, T: Data> {
    task_info: Arc<TaskInfo>,
    out_qs: Vec<Vec<OutQueue>>,
    _ts: PhantomData<(K, T)>,
    tx_queue_rem_gauges: QueueGauges,
    tx_queue_size_gauges: QueueGauges,
}

impl<K: Key, T: Data> Collector<K, T> {
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
    pub node: Box<dyn StreamNode>,
}

impl Debug for SubtaskNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            self.node.node_name(),
            self.id,
            self.subtask_idx
        )
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
    edge: LogicalEdge,
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
                        operator_name: sn.node.node_name(),
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
                (0..weight.initial_parallelism).map(|index| TaskAssignment {
                    operator_id: weight.id.clone(),
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
        logical: &DiGraph<LogicalNode, LogicalEdge>,
        assignments: &Vec<TaskAssignment>,
    ) -> Program {
        let mut physical = DiGraph::new();

        let mut parallelism_map = HashMap::new();
        for task in assignments {
            *(parallelism_map.entry(&task.operator_id).or_insert(0usize)) += 1;
        }

        for idx in logical.node_indices() {
            let node = logical.node_weight(idx).unwrap();
            let parallelism = *parallelism_map.get(&node.id).unwrap_or_else(|| {
                warn!("no assignments for operator {}", node.id);
                &node.initial_parallelism
            });
            for i in 0..parallelism {
                physical.add_node(SubtaskOrQueueNode::SubtaskNode((*node.create_fn)(
                    i,
                    parallelism,
                )));
            }
        }

        for idx in logical.edge_indices() {
            let edge = logical.edge_weight(idx).unwrap();
            let (logical_in_node_idx, logical_out_node_idx) = logical.edge_endpoints(idx).unwrap();
            let logical_in_node = logical.node_weight(logical_in_node_idx).unwrap();
            let logical_out_node = logical.node_weight(logical_out_node_idx).unwrap();

            let from_nodes: Vec<_> = physical
                .node_indices()
                .filter(|n| physical.node_weight(*n).unwrap().id() == logical_in_node.id)
                .collect();
            assert_ne!(from_nodes.len(), 0, "failed to find from nodes");
            let to_nodes: Vec<_> = physical
                .node_indices()
                .filter(|n| physical.node_weight(*n).unwrap().id() == logical_out_node.id)
                .collect();
            assert_ne!(from_nodes.len(), 0, "failed to find to nodes");

            match edge {
                LogicalEdge::Forward => {
                    if from_nodes.len() != to_nodes.len() && !from_nodes.is_empty() {
                        panic!("cannot create a forward connection between nodes of different parallelism");
                    }
                    for (f, t) in from_nodes.iter().zip(&to_nodes) {
                        let (tx, rx) = channel(QUEUE_SIZE);
                        let edge = PhysicalGraphEdge {
                            edge_idx: 0,
                            in_logical_idx: logical_in_node_idx.index(),
                            out_logical_idx: logical_out_node_idx.index(),
                            edge: edge.clone(),
                            tx: Some(tx),
                            rx: Some(rx),
                        };
                        physical.add_edge(*f, *t, edge);
                    }
                }
                LogicalEdge::Shuffle | LogicalEdge::ShuffleJoin(_) => {
                    for f in &from_nodes {
                        for (idx, t) in to_nodes.iter().enumerate() {
                            let (tx, rx) = channel(QUEUE_SIZE);
                            let edge = PhysicalGraphEdge {
                                edge_idx: idx,
                                in_logical_idx: logical_in_node_idx.index(),
                                out_logical_idx: logical_out_node_idx.index(),
                                edge: edge.clone(),
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
            node.node.node_name(),
            node.id,
            node.subtask_idx,
            node.subtask_idx + 1,
            node.parallelism
        );

        let mut in_qs_map: BTreeMap<(LogicalEdge, usize), Vec<Receiver<QueueItem>>> =
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

        let mut out_qs_map: BTreeMap<usize, BTreeMap<usize, OutQueue>> = BTreeMap::new();

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
            let sender = OutQueue::new(tx, !local);
            out_qs_map
                .entry(edge.weight().out_logical_idx)
                .or_default()
                .insert(edge.weight().edge_idx, sender);
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

#[cfg(test)]
mod tests {
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
}
