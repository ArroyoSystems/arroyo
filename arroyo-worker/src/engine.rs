use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::{mem, thread};

use std::time::SystemTime;

use arroyo_connectors::connectors;
use arroyo_rpc::df::ArroyoSchema;
use bincode::{Decode, Encode};
use tracing::{debug, info, warn};

use crate::arrow::join_with_expiration::JoinWithExpirationConstructor;
use crate::arrow::session_aggregating_window::SessionAggregatingWindowConstructor;
use crate::arrow::sliding_aggregating_window::SlidingAggregatingWindowConstructor;
use crate::arrow::tumbling_aggregating_window::TumblingAggregateWindowConstructor;
use crate::arrow::{KeyExecutionConstructor, ValueExecutionConstructor};
use crate::network_manager::{NetworkManager, Quad, Senders};
use crate::operators::watermark_generator::WatermarkGeneratorConstructor;
use crate::{METRICS_PUSH_INTERVAL, PROMETHEUS_PUSH_GATEWAY};
use arroyo_datastream::logical::{
    LogicalEdge, LogicalEdgeType, LogicalGraph, LogicalNode, OperatorName,
};
pub use arroyo_macro::StreamNode;
use arroyo_operator::context::{ArrowContext, QueueItem};
use arroyo_operator::operator::OperatorNode;
use arroyo_operator::ErasedConstructor;
use arroyo_rpc::grpc::{api, CheckpointMetadata, TaskAssignment};
use arroyo_rpc::{ControlMessage, ControlResp};
use arroyo_state::{BackingStore, StateBackend};
use arroyo_types::{range_for_server, ArrowMessage, Key, TaskInfo, WorkerId, QUEUE_SIZE};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use prometheus::labels;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[derive(Encode, Decode, Clone, Debug, PartialEq, Eq)]
pub struct TimerValue<K: Key, T: Decode + Encode + Clone + PartialEq + Eq> {
    pub time: SystemTime,
    pub key: K,
    pub data: T,
}

pub struct SubtaskNode {
    pub id: String,
    pub subtask_idx: usize,
    pub parallelism: usize,
    pub in_schemas: Vec<ArroyoSchema>,
    pub out_schema: Option<ArroyoSchema>,
    pub projection: Option<Vec<usize>>,
    pub node: OperatorNode,
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
    schema: ArroyoSchema,
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
                    node: construct_operator(node.operator_name, node.operator_config.clone())
                        .into(),
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
                            schema: edge.schema.clone(),
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
                                schema: edge.schema.clone(),
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
                    .expect(&format!(
                        "failed to load checkpoint metadata for epoch {}",
                        epoch
                    )),
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

            senders.add(
                quad,
                edge.weight().schema.schema.clone(),
                edge.weight().tx.as_ref().unwrap().clone(),
            );
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

    pub async fn run_locally(
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
            let _local = {
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

        let tables = node.node.tables();
        let in_qs: Vec<_> = in_qs_map.into_values().flatten().collect();

        let ctx = ArrowContext::new(
            task_info,
            checkpoint_metadata.clone(),
            control_rx,
            control_tx.clone(),
            in_qs.len(),
            node.in_schemas,
            node.out_schema,
            node.projection,
            out_qs_map
                .into_values()
                .map(|v| v.into_values().collect())
                .collect(),
            tables,
        )
        .await;

        let operator = Box::new(node.node);
        let join_task = tokio::spawn(async move {
            operator.start(ctx, in_qs).await;
        });

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

pub fn construct_operator(operator: OperatorName, config: Vec<u8>) -> OperatorNode {
    let ctor: Box<dyn ErasedConstructor> = match operator {
        OperatorName::ArrowValue => Box::new(ValueExecutionConstructor),
        OperatorName::ArrowKey => Box::new(KeyExecutionConstructor),
        OperatorName::ArrowAggregate => {
            // TODO: this should not be in a specific window.
            Box::new(TumblingAggregateWindowConstructor)
        }
        OperatorName::TumblingWindowAggregate => Box::new(TumblingAggregateWindowConstructor),
        OperatorName::SlidingWindowAggregate => Box::new(SlidingAggregatingWindowConstructor),
        OperatorName::SessionWindowAggregate => Box::new(SessionAggregatingWindowConstructor),
        OperatorName::ExpressionWatermark => Box::new(WatermarkGeneratorConstructor),
        OperatorName::Join => Box::new(JoinWithExpirationConstructor),
        OperatorName::ConnectorSource | OperatorName::ConnectorSink => {
            let op: api::ConnectorOp = prost::Message::decode(&mut config.as_slice()).unwrap();
            return connectors()
                .get(op.connector.as_str())
                .unwrap_or_else(|| panic!("No connector with name '{}'", op.connector))
                .make_operator(
                    serde_json::from_str(&op.config)
                        .unwrap_or_else(|e| panic!("invalid operator config: {:?}", e)),
                )
                .unwrap_or_else(|e| {
                    panic!("Failed to construct connector {}: {:?}", op.connector, e)
                });
        }
    };

    ctor.with_config(config)
        .expect(&format!("Failed to construct operator {:?}", operator))
}
