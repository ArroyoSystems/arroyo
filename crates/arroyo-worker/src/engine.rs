use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::mem;
use std::sync::{Arc, RwLock};

use std::time::SystemTime;

use arroyo_connectors::connectors;
use arroyo_rpc::df::ArroyoSchema;
use bincode::{Decode, Encode};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tracing::{info, warn};

use crate::arrow::async_udf::AsyncUdfConstructor;
use crate::arrow::instant_join::InstantJoinConstructor;
use crate::arrow::join_with_expiration::JoinWithExpirationConstructor;
use crate::arrow::session_aggregating_window::SessionAggregatingWindowConstructor;
use crate::arrow::sliding_aggregating_window::SlidingAggregatingWindowConstructor;
use crate::arrow::tumbling_aggregating_window::TumblingAggregateWindowConstructor;
use crate::arrow::updating_aggregator::UpdatingAggregatingConstructor;
use crate::arrow::watermark_generator::WatermarkGeneratorConstructor;
use crate::arrow::window_fn::WindowFunctionConstructor;
use crate::arrow::{KeyExecutionConstructor, ValueExecutionConstructor};
use crate::network_manager::{NetworkManager, Quad, Senders};
use arroyo_datastream::logical::{
    LogicalEdge, LogicalEdgeType, LogicalGraph, LogicalNode, OperatorName,
};
use arroyo_df::physical::new_registry;
use arroyo_operator::context::{batch_bounded, ArrowContext, BatchReceiver, BatchSender};
use arroyo_operator::operator::OperatorNode;
use arroyo_operator::operator::Registry;
use arroyo_operator::ErasedConstructor;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::{
    api,
    rpc::{CheckpointMetadata, TaskAssignment},
};
use arroyo_rpc::{ControlMessage, ControlResp};
use arroyo_state::{BackingStore, StateBackend};
use arroyo_types::{range_for_server, Key, TaskInfo, WorkerId};
use arroyo_udf_host::LocalUdf;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Barrier;

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
pub enum SubtaskOrQueueNode {
    SubtaskNode(SubtaskNode),
    QueueNode(QueueNode),
}

pub struct PhysicalGraphEdge {
    edge_idx: usize,
    in_logical_idx: usize,
    out_logical_idx: usize,
    schema: ArroyoSchema,
    edge: LogicalEdgeType,
    tx: Option<BatchSender>,
    rx: Option<BatchReceiver>,
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
    pub graph: Arc<RwLock<DiGraph<SubtaskOrQueueNode, PhysicalGraphEdge>>>,
}

impl Program {
    pub fn total_nodes(&self) -> usize {
        self.graph.read().unwrap().node_count()
    }

    pub fn local_from_logical(
        name: String,
        logical: &DiGraph<LogicalNode, LogicalEdge>,
        udfs: &[LocalUdf],
    ) -> Self {
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

        let mut registry = new_registry();
        for udf in udfs {
            registry.add_local_udf(udf);
        }
        Self::from_logical(name, logical, &assignments, registry)
    }

    pub fn from_logical(
        name: String,
        logical: &LogicalGraph,
        assignments: &Vec<TaskAssignment>,
        registry: Registry,
    ) -> Program {
        let mut physical = DiGraph::new();

        let registry = Arc::new(registry);

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
                    node: construct_operator(
                        node.operator_name,
                        node.operator_config.clone(),
                        registry.clone(),
                    ),
                    projection: projection.clone(),
                }));
            }
        }

        let queue_size = config().worker.queue_size;

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
                        let (tx, rx) = batch_bounded(queue_size);
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
                            let (tx, rx) = batch_bounded(queue_size);
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
            graph: Arc::new(RwLock::new(physical)),
        }
    }

    pub fn tasks_per_operator(&self) -> HashMap<String, usize> {
        let mut tasks_per_operator = HashMap::new();
        for node in self.graph.read().unwrap().node_weights() {
            let entry = tasks_per_operator.entry(node.id().to_string()).or_insert(0);
            *entry += 1;
        }
        tasks_per_operator
    }
}

pub struct Engine {
    program: Program,
    worker_id: WorkerId,
    #[allow(unused)]
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
        let graph = self.program.graph.read().unwrap();
        graph
            .externals(Direction::Incoming)
            .filter(|idx| {
                let w = graph.node_weight(*idx).unwrap();
                self.assignments
                    .get(&(w.id().to_string(), w.subtask_idx()))
                    .unwrap()
                    .worker_id
                    == self.worker_id.0
            })
            .map(|idx| graph.node_weight(idx).unwrap().as_queue().tx.clone())
            .collect()
    }

    pub fn sink_controls(&self) -> Vec<Sender<ControlMessage>> {
        let graph = self.program.graph.read().unwrap();
        graph
            .externals(Direction::Outgoing)
            .filter(|idx| {
                let w = graph.node_weight(*idx).unwrap();
                self.assignments
                    .get(&(w.id().to_string(), w.subtask_idx()))
                    .unwrap()
                    .worker_id
                    == self.worker_id.0
            })
            .map(|idx| graph.node_weight(idx).unwrap().as_queue().tx.clone())
            .collect()
    }

    pub fn operator_controls(&self) -> HashMap<String, Vec<Sender<ControlMessage>>> {
        let mut controls = HashMap::new();
        let graph = self.program.graph.read().unwrap();

        graph
            .node_indices()
            .filter(|idx| {
                let w = graph.node_weight(*idx).unwrap();
                self.assignments
                    .get(&(w.id().to_string(), w.subtask_idx()))
                    .unwrap()
                    .worker_id
                    == self.worker_id.0
            })
            .for_each(|idx| {
                let w = graph.node_weight(idx).unwrap();
                let assignment = self
                    .assignments
                    .get(&(w.id().to_string(), w.subtask_idx()))
                    .unwrap();
                let tx = graph.node_weight(idx).unwrap().as_queue().tx.clone();
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

    fn local_task_count(&self) -> usize {
        self.assignments
            .iter()
            .filter(|(_, a)| a.worker_id == self.worker_id.0)
            .count()
    }

    pub fn for_local(program: Program, job_id: String) -> Self {
        let worker_id = WorkerId(0);
        let assignments = program
            .graph
            .read()
            .unwrap()
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
                    .unwrap_or_else(|_| {
                        panic!("failed to load checkpoint metadata for epoch {}", epoch)
                    }),
            )
        } else {
            None
        };

        let node_indexes: Vec<_> = self.program.graph.read().unwrap().node_indices().collect();

        let (control_tx, control_rx) = channel(128);
        let worker_id = self.worker_id;

        let mut senders = Senders::new();

        let ready = Arc::new(Barrier::new(self.local_task_count()));
        {
            let mut futures = FuturesUnordered::new();

            for idx in node_indexes {
                futures.push(self.schedule_node(
                    &checkpoint_metadata,
                    &control_tx,
                    idx,
                    ready.clone(),
                ));
            }

            while let Some(result) = futures.next().await {
                senders.merge(result);
            }
        }

        self.network_manager.start(senders).await;

        // clear all of the TXs in the graph so that we don't leave dangling senders
        for n in self.program.graph.write().unwrap().edge_weights_mut() {
            n.tx = None;
        }

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
        &self,
        checkpoint_metadata: &Option<CheckpointMetadata>,
        control_tx: &Sender<ControlResp>,
        idx: NodeIndex,
        ready: Arc<Barrier>,
    ) -> Senders {
        let (node, control_rx) = self
            .program
            .graph
            .write()
            .unwrap()
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

        let mut senders = Senders::new();

        if assignment.worker_id == self.worker_id.0 {
            self.run_locally(
                checkpoint_metadata,
                control_tx,
                idx,
                node,
                control_rx,
                ready,
            )
            .await;
        } else {
            self.connect_to_remote_task(
                &mut senders,
                idx,
                node.id.clone(),
                node.subtask_idx,
                assignment,
            )
            .await;
        }

        senders
    }

    async fn connect_to_remote_task(
        &self,
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

        let mut connects = vec![];
        {
            let graph = self.program.graph.read().unwrap();

            for edge in graph.edges_directed(idx, Direction::Outgoing) {
                let target = graph.node_weight(edge.target()).unwrap();

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

            for edge in graph.edges_directed(idx, Direction::Incoming) {
                let source = graph.node_weight(edge.source()).unwrap();

                let quad = Quad {
                    src_id: edge.weight().in_logical_idx,
                    src_idx: source.subtask_idx(),
                    dst_id: edge.weight().out_logical_idx,
                    dst_idx: node_subtask_idx,
                };

                connects.push((edge.id(), quad));
            }
        }

        for (id, quad) in connects {
            let rx = {
                let mut graph = self.program.graph.write().unwrap();
                let edge = graph.edge_weight_mut(id).unwrap();
                edge.rx.take().unwrap()
            };

            self.network_manager
                .connect(assignment.worker_addr.clone(), quad, rx)
                .await;
        }

        info!(
            "Connected to remote task {}-{} running on {}",
            node_id, node_subtask_idx, assignment.worker_addr
        );
    }

    pub async fn run_locally(
        &self,
        checkpoint_metadata: &Option<CheckpointMetadata>,
        control_tx: &Sender<ControlResp>,
        idx: NodeIndex,
        node: SubtaskNode,
        control_rx: Receiver<ControlMessage>,
        ready: Arc<Barrier>,
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

        let mut in_qs_map: BTreeMap<(LogicalEdgeType, usize), Vec<BatchReceiver>> = BTreeMap::new();
        let mut out_qs_map: BTreeMap<usize, BTreeMap<usize, BatchSender>> = BTreeMap::new();
        let task_info = {
            let mut graph = self.program.graph.write().unwrap();
            for edge in graph.edge_indices() {
                if graph.edge_endpoints(edge).unwrap().1 == idx {
                    let weight = graph.edge_weight_mut(edge).unwrap();
                    in_qs_map
                        .entry((weight.edge, weight.in_logical_idx))
                        .or_default()
                        .push(weight.rx.take().unwrap());
                }
            }

            for edge in graph.edges_directed(idx, Direction::Outgoing) {
                // is the target of this edge local or remote?
                let _local = {
                    let target = graph.node_weight(edge.target()).unwrap();
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

            graph.node_weight(idx).unwrap().as_queue().task_info.clone()
        };

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
            operator.start(ctx, in_qs, ready).await;
        });

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
    }
}

pub fn construct_operator(
    operator: OperatorName,
    config: Vec<u8>,
    registry: Arc<Registry>,
) -> OperatorNode {
    let ctor: Box<dyn ErasedConstructor> = match operator {
        OperatorName::ArrowValue => Box::new(ValueExecutionConstructor),
        OperatorName::ArrowKey => Box::new(KeyExecutionConstructor),
        OperatorName::AsyncUdf => Box::new(AsyncUdfConstructor),
        OperatorName::TumblingWindowAggregate => Box::new(TumblingAggregateWindowConstructor),
        OperatorName::SlidingWindowAggregate => Box::new(SlidingAggregatingWindowConstructor),
        OperatorName::SessionWindowAggregate => Box::new(SessionAggregatingWindowConstructor),
        OperatorName::UpdatingAggregate => Box::new(UpdatingAggregatingConstructor),
        OperatorName::ExpressionWatermark => Box::new(WatermarkGeneratorConstructor),
        OperatorName::Join => Box::new(JoinWithExpirationConstructor),
        OperatorName::InstantJoin => Box::new(InstantJoinConstructor),
        OperatorName::WindowFunction => Box::new(WindowFunctionConstructor),
        OperatorName::ConnectorSource | OperatorName::ConnectorSink => {
            let op: api::ConnectorOp = prost::Message::decode(&mut config.as_slice()).unwrap();
            return connectors()
                .get(op.connector.as_str())
                .unwrap_or_else(|| panic!("No connector with name '{}'", op.connector))
                .make_operator(
                    serde_json::from_str(&op.config)
                        .unwrap_or_else(|e| panic!("invalid operator config: {:?}, {:?}", op, e)),
                )
                .unwrap_or_else(|e| {
                    panic!("Failed to construct connector {}: {:?}", op.connector, e)
                });
        }
    };

    ctor.with_config(config, registry).unwrap_or_else(|e| {
        panic!(
            "Failed to construct operator {:?}, with error:\n{:?}",
            operator, e
        )
    })
}
