use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;

use std::collections::{BTreeMap, HashMap};
use std::ops::RangeInclusive;

use std::any::Any;
use std::process::exit;
use std::{mem, thread};

use std::time::{Duration, SystemTime};

use arroyo_metrics::{counter_for_task, gauge_for_task};
use arroyo_state::tables::TimeKeyMap;
use bincode::{config, Decode, Encode};

use tracing::{debug, error, info, warn};

pub use arroyo_macro::StreamNode;
use arroyo_rpc::grpc::controller_grpc_client::ControllerGrpcClient;
use arroyo_rpc::grpc::{
    CheckpointMetadata, HeartbeatReq, TableDeleteBehavior, TableDescriptor, TableType,
    TableWriteBehavior, TaskAssignment, TaskCheckpointCompletedReq, TaskCheckpointEventReq,
    TaskFailedReq, TaskFinishedReq, TaskStartedReq,
};
use arroyo_rpc::{ControlMessage, ControlResp};
use arroyo_types::{
    from_micros, to_micros, CheckpointBarrier, Data, Key, Message, Record, TaskInfo, WorkerId,
    BYTES_RECV, BYTES_SENT, MESSAGES_RECV, MESSAGES_SENT,
};
use petgraph::graph::DiGraph;
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use prometheus::{labels, IntCounter, IntGauge};
use rand::Rng;
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tonic::Request;

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
            crate::engine::QueueItem::Data(datum) => *datum.downcast().unwrap(),
            crate::engine::QueueItem::Bytes(bs) => {
                bincode::decode_from_slice(&bs, config::standard())
                    .unwrap()
                    .0
            }
        }
    }
}
fn range_for_server(i: usize, n: usize) -> RangeInclusive<u64> {
    let range_size = u64::MAX / (n as u64);
    let start = range_size * (i as u64);
    let end = if i + 1 == n {
        u64::MAX
    } else {
        start + range_size - 1
    };
    start..=end
}

fn server_for_hash(x: u64, n: usize) -> usize {
    let range_size = u64::MAX / (n as u64);
    (n - 1).min((x / range_size) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_for_server() {
        let n = 6;

        for i in 0..(n - 1) {
            let range1 = range_for_server(i, n);
            let range2 = range_for_server(i + 1, n);

            assert_eq!(*range1.end() + 1, *range2.start(), "Ranges not adjacent");
        }

        let last_range = range_for_server(n - 1, n);
        assert_eq!(
            *last_range.end(),
            u64::MAX,
            "Last range does not contain u64::MAX"
        );
    }

    #[test]
    fn test_server_for_hash() {
        let n = 2;
        let x = u64::MAX;

        let server_index = server_for_hash(x, n);
        let server_range = range_for_server(server_index, n);

        assert!(
            server_range.contains(&x),
            "u64::MAX is not in the correct range"
        );
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

pub struct Context<K: Key, T: Data, S: BackingStore = StateBackend> {
    pub task_info: TaskInfo,
    pub control_rx: Receiver<ControlMessage>,
    pub control_tx: Sender<ControlResp>,
    pub watermarks: Vec<Option<SystemTime>>,
    pub state: StateStore<S>,
    pub collector: Collector<K, T>,
    pub counters: HashMap<&'static str, IntCounter>,
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

    pub async fn send(
        &self,
        message: Message<impl Key, impl Data>,
        sent_bytes: &Option<IntCounter>,
    ) {
        let is_end = message.is_end();
        let item = if self.serialize {
            let bytes = bincode::encode_to_vec(&message, config::standard()).unwrap();
            sent_bytes.iter().for_each(|c| c.inc_by(bytes.len() as u64));

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
pub struct Collector<K: Key, T: Data> {
    out_qs: Vec<Vec<OutQueue>>,
    _ts: PhantomData<(K, T)>,
    sent_bytes: Option<IntCounter>,
    sent_messages: Option<IntCounter>,
    tx_queue_rem_gauges: Vec<Vec<Option<IntGauge>>>,
    tx_queue_size_gauges: Vec<Vec<Option<IntGauge>>>,
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

        self.sent_messages.iter().for_each(|c| c.inc());

        if self.out_qs.len() == 1 {
            let idx = out_idx(&record.key, self.out_qs[0].len());

            self.tx_queue_rem_gauges[0][idx]
                .iter()
                .for_each(|g| g.set(self.out_qs[0][idx].tx.capacity() as i64));

            self.tx_queue_size_gauges[0][idx]
                .iter()
                .for_each(|g| g.set(QUEUE_SIZE as i64));

            self.out_qs[0][idx]
                .send(Message::Record(record), &self.sent_bytes)
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
                    .send(message.clone(), &self.sent_bytes)
                    .await;
            }
        }
    }

    pub async fn broadcast(&mut self, message: Message<K, T>) {
        for out_node in &self.out_qs {
            for q in out_node {
                q.send(message.clone(), &self.sent_bytes).await;
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

        let mut counters = HashMap::new();

        if let Some(c) = counter_for_task(
            &task_info,
            MESSAGES_RECV,
            "Count of messages received by this subtask",
            HashMap::new(),
        ) {
            counters.insert(MESSAGES_RECV, c);
        }

        if let Some(c) = counter_for_task(
            &task_info,
            MESSAGES_SENT,
            "Count of messages sent by this subtask",
            HashMap::new(),
        ) {
            counters.insert(MESSAGES_SENT, c);
        }

        if let Some(c) = counter_for_task(
            &task_info,
            BYTES_RECV,
            "Count of bytes received by this subtask",
            HashMap::new(),
        ) {
            counters.insert(BYTES_RECV, c);
        }

        if let Some(c) = counter_for_task(
            &task_info,
            BYTES_SENT,
            "Count of bytes sent by this subtask",
            HashMap::new(),
        ) {
            counters.insert(BYTES_SENT, c);
        }

        let tx_queue_size_gauges = out_qs
            .iter()
            .enumerate()
            .map(|(i, qs)| {
                qs.iter()
                    .enumerate()
                    .map(|(j, _)| {
                        gauge_for_task(
                            &task_info,
                            "arroyo_worker_tx_queue_size",
                            "Size of a tx queue",
                            labels! {
                                "next_node".to_string() => format!("{}", i),
                                "next_node_idx".to_string() => format!("{}", j)
                            },
                        )
                    })
                    .collect()
            })
            .collect();

        let tx_queue_rem_gauges = out_qs
            .iter()
            .enumerate()
            .map(|(i, qs)| {
                qs.iter()
                    .enumerate()
                    .map(|(j, _)| {
                        gauge_for_task(
                            &task_info,
                            "arroyo_worker_tx_queue_rem",
                            "Remaining space in a tx queue",
                            labels! {
                                "next_node".to_string() => format!("{}", i),
                                "next_node_idx".to_string() => format!("{}", j)
                            },
                        )
                    })
                    .collect()
            })
            .collect();

        Context {
            task_info,
            control_rx,
            control_tx,
            watermarks: vec![watermark; input_partitions],
            collector: Collector::<K, T> {
                out_qs,
                sent_messages: counters.remove(MESSAGES_SENT),
                sent_bytes: counters.remove(BYTES_SENT),
                tx_queue_rem_gauges,
                tx_queue_size_gauges,
                _ts: PhantomData,
            },
            state,
            counters,
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

    pub fn watermark(&self) -> Option<SystemTime> {
        self.watermarks
            .iter()
            .copied()
            .reduce(|current, next| match next {
                Some(next) => current.map(|current| current.min(next)),
                None => None,
            })
            .flatten()
    }

    pub async fn schedule_timer<D: Data + PartialEq + Eq>(
        &mut self,
        key: &mut K,
        event_time: SystemTime,
        data: D,
    ) {
        let Some(watermark) = self.watermark() else {
            return;
         };
        let mut timer_state: TimeKeyMap<K, TimerValue<K, D>, _> =
            self.state.get_time_key_map(TIMER_TABLE, None).await;
        let value = TimerValue {
            time: event_time,
            key: key.clone(),
            data,
        };

        assert!(watermark < event_time, "Timer scheduled for past");

        debug!(
            "[{}] scheduling timer for [{}, {:?}]",
            self.task_info.task_index,
            hash_key(key),
            event_time
        );

        timer_state.insert(event_time, key.clone(), value);
    }

    pub async fn collect(&mut self, record: Record<K, T>) {
        self.collector.collect(record).await;
    }

    pub async fn broadcast(&mut self, message: Message<K, T>) {
        self.collector.broadcast(message).await;
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
}

pub struct Engine {
    program: Program,
    worker_id: WorkerId,
    run_id: String,
    job_id: String,
    controller_addr: Option<String>,
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
    shutdown_tx: tokio::sync::broadcast::Sender<bool>,
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

    pub fn stop(&mut self) {
        self.shutdown_tx.send(true).unwrap();
    }
}

impl Engine {
    pub fn new(
        program: Program,
        worker_id: WorkerId,
        job_id: String,
        run_id: String,
        controller_addr: String,
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
            controller_addr: Some(controller_addr),
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
            controller_addr: None,
            network_manager: NetworkManager::new(0),
            assignments,
        }
    }

    pub async fn start(mut self, config: StreamConfig) -> RunningEngine {
        //console_subscriber::init();
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

        info!("Starting job {}", self.job_id);

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

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);

        let (control_tx, mut control_rx) = channel(128);

        let indices: Vec<_> = self.program.graph.node_indices().collect();

        let mut senders = Senders::new();

        let worker_id = self.worker_id;
        let job_id = self.job_id.clone();
        let mut controller = if let Some(addr) = self.controller_addr.clone() {
            Some(ControllerGrpcClient::connect(addr).await.unwrap())
        } else {
            None
        };

        for idx in indices {
            let (node, control_rx) = self
                .program
                .graph
                .node_weight_mut(idx)
                .unwrap()
                .take_subtask(self.job_id.clone());

            let assignment = self
                .assignments
                .get(&(node.id.to_string(), node.subtask_idx))
                .unwrap_or_else(|| {
                    panic!(
                        "Could not find assignment for node {}-{}",
                        node.id, node.subtask_idx
                    )
                });

            if assignment.worker_id == self.worker_id.0 {
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

                // TODO: make async
                if let Some(c) = controller.as_mut() {
                    c.task_started(TaskStartedReq {
                        worker_id: worker_id.0,
                        time: to_micros(SystemTime::now()),
                        job_id: job_id.clone(),
                        operator_id: node.id.clone(),
                        operator_subtask: node.subtask_idx as u64,
                    })
                    .await
                    .unwrap();
                }
            } else {
                info!(
                    "Connecting to remote task {}-{} running on {}",
                    node.id, node.subtask_idx, assignment.worker_addr
                );

                for edge in self.program.graph.edges_directed(idx, Direction::Outgoing) {
                    let target = self.program.graph.node_weight(edge.target()).unwrap();

                    let quad = Quad {
                        src_id: edge.weight().in_logical_idx,
                        src_idx: node.subtask_idx,
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
                        dst_idx: node.subtask_idx,
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
        }

        self.network_manager.start(senders).await;

        // clear all of the TXs in the graph so that we don't leave dangling senders
        for n in self.program.graph.edge_weights_mut() {
            n.tx = None;
        }

        tokio::spawn(async move {
            loop {
                select! {
                    msg = control_rx.recv() => {
                        let err = match msg {
                            Some(ControlResp::CheckpointEvent(c)) => {
                                if let Some(controller) = controller.as_mut() {
                                    controller.task_checkpoint_event(Request::new(
                                        TaskCheckpointEventReq {
                                            worker_id: worker_id.0,
                                            time: to_micros(c.time),
                                            job_id: job_id.clone(),
                                            operator_id: c.operator_id,
                                            subtask_index: c.subtask_index,
                                            epoch: c.checkpoint_epoch,
                                            event_type: c.event_type as i32,
                                        }
                                    )).await.err()
                                } else {
                                    None
                                }
                            }
                            Some(ControlResp::CheckpointCompleted(c)) => {
                                if let Some(controller) = controller.as_mut() {
                                    controller.task_checkpoint_completed(Request::new(
                                        TaskCheckpointCompletedReq {
                                            worker_id: worker_id.0,
                                            time: c.subtask_metadata.finish_time,
                                            job_id: job_id.clone(),
                                            operator_id: c.operator_id,
                                            epoch: c.checkpoint_epoch,
                                            metadata: Some(c.subtask_metadata),
                                        }
                                    )).await.err()
                                } else {
                                    None
                                }
                            }
                            Some(ControlResp::TaskFinished { operator_id, task_index }) => {
                                info!(message = "Task finished", operator_id, task_index);
                                if let Some(controller) = controller.as_mut() {
                                    controller.task_finished(Request::new(
                                        TaskFinishedReq {
                                            worker_id: worker_id.0,
                                            job_id: job_id.clone(),
                                            time: to_micros(SystemTime::now()),
                                            operator_id: operator_id.to_string(),
                                            operator_subtask: task_index as u64,
                                        }
                                    )).await.err()
                                } else {
                                    None
                                }
                            }
                            Some(ControlResp::TaskFailed { operator_id, task_index, error }) => {
                                info!(message = "Task failed", operator_id, task_index, error);
                                if let Some(controller) = controller.as_mut() {
                                    controller.task_failed(Request::new(
                                        TaskFailedReq {
                                            worker_id: worker_id.0,
                                            job_id: job_id.clone(),
                                            time: to_micros(SystemTime::now()),
                                            operator_id: operator_id.to_string(),
                                            operator_subtask: task_index as u64,
                                            error,
                                        }
                                    )).await.err()
                                } else {
                                    None
                                }
                            }
                            None => {
                                // TODO: remove the control queue from the select at this point
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                None
                            }
                        };
                        if let Some(err) = err {
                            error!("encountered control message failure {}", err);
                            exit(1);
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        if let Some(controller) = controller.as_mut() {
                            let result = controller.heartbeat(Request::new(HeartbeatReq {
                                job_id: job_id.clone(),
                                time: to_micros(SystemTime::now()),
                                worker_id: worker_id.0,
                            })).await;
                            if let Err(err) = result {
                                error!("heartbeat failed {:?}", err);
                                exit(1);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });

        RunningEngine {
            program: self.program,
            shutdown_tx,
            assignments: self.assignments,
            worker_id,
        }
    }
}
