use crate::arrow::async_udf::AsyncUdfConstructor;
use crate::arrow::incremental_aggregator::IncrementalAggregatingConstructor;
use crate::arrow::instant_join::InstantJoinConstructor;
use crate::arrow::join_with_expiration::JoinWithExpirationConstructor;
use crate::arrow::lookup_join::LookupJoinConstructor;
use crate::arrow::session_aggregating_window::SessionAggregatingWindowConstructor;
use crate::arrow::sliding_aggregating_window::SlidingAggregatingWindowConstructor;
use crate::arrow::tumbling_aggregating_window::TumblingAggregateWindowConstructor;
use crate::arrow::watermark_generator::WatermarkGeneratorConstructor;
use crate::arrow::window_fn::WindowFunctionConstructor;
use crate::arrow::{KeyExecutionConstructor, ValueExecutionConstructor};
use crate::network_manager::{NetworkManager, Quad, Senders};
use arroyo_connectors::connectors;
use arroyo_datastream::logical::{
    LogicalEdge, LogicalEdgeType, LogicalGraph, LogicalNode, OperatorChain, OperatorName,
};
use arroyo_operator::context::{batch_bounded, BatchReceiver, BatchSender, OperatorContext};
use arroyo_operator::operator::Registry;
use arroyo_operator::operator::{ChainedOperator, ConstructedOperator, OperatorNode, SourceNode};
use arroyo_operator::ErasedConstructor;
use arroyo_planner::physical::new_registry;
use arroyo_rpc::config::config;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::{
    api,
    rpc::{CheckpointMetadata, TaskAssignment},
};
use arroyo_rpc::{ControlMessage, ControlResp};
use arroyo_state::{BackingStore, StateBackend};
use arroyo_types::{range_for_server, TaskInfo, WorkerId};
use arroyo_udf_host::LocalUdf;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::{dot, Direction};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::mem;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Barrier;
use tracing::{debug, info, warn};

pub struct SubtaskNode {
    pub node_id: u32,
    pub subtask_idx: usize,
    pub parallelism: usize,
    pub in_schemas: Vec<Arc<ArroyoSchema>>,
    pub out_schema: Option<Arc<ArroyoSchema>>,
    pub node: OperatorNode,
}

impl Debug for SubtaskNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            self.node.name(),
            self.node_id,
            self.subtask_idx
        )
    }
}

pub struct QueueNode {
    task_info: Arc<TaskInfo>,
    operator_ids: Vec<String>,
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
    SubtaskNode(Box<SubtaskNode>),
    QueueNode(QueueNode),
}

pub struct PhysicalGraphEdge {
    edge_idx: usize,
    in_logical_idx: usize,
    out_logical_idx: usize,
    schema: Arc<ArroyoSchema>,
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
    pub fn take_subtask(&mut self) -> (SubtaskNode, Receiver<ControlMessage>) {
        let (mut qn, rx) = match self {
            SubtaskOrQueueNode::SubtaskNode(sn) => {
                let (tx, rx) = channel(16);

                let n = SubtaskOrQueueNode::QueueNode(QueueNode {
                    task_info: sn.node.task_info().clone(),
                    operator_ids: sn.node.operator_ids(),
                    tx,
                });

                (n, rx)
            }
            SubtaskOrQueueNode::QueueNode(_) => panic!("already swapped for queue node"),
        };

        mem::swap(self, &mut qn);

        (qn.unwrap_subtask(), rx)
    }

    pub fn id(&self) -> u32 {
        match self {
            SubtaskOrQueueNode::SubtaskNode(n) => n.node_id,
            SubtaskOrQueueNode::QueueNode(n) => n.task_info.node_id,
        }
    }

    pub fn subtask_idx(&self) -> usize {
        match self {
            SubtaskOrQueueNode::SubtaskNode(n) => n.subtask_idx,
            SubtaskOrQueueNode::QueueNode(n) => n.task_info.task_index as usize,
        }
    }

    fn unwrap_subtask(self) -> SubtaskNode {
        match self {
            SubtaskOrQueueNode::SubtaskNode(n) => *n,
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
    pub control_tx: Option<Sender<ControlResp>>,
}

impl Program {
    pub fn total_nodes(&self) -> usize {
        self.graph.read().unwrap().node_count()
    }

    pub async fn local_from_logical(
        job_id: String,
        logical: &DiGraph<LogicalNode, LogicalEdge>,
        udfs: &[LocalUdf],
        restore_epoch: Option<u32>,
        control_tx: Sender<ControlResp>,
    ) -> Self {
        let assignments = logical
            .node_weights()
            .flat_map(|weight| {
                (0..weight.parallelism).map(|index| TaskAssignment {
                    node_id: weight.node_id,
                    subtask_idx: index as u32,
                    worker_id: 0,
                    worker_addr: "".into(),
                })
            })
            .collect();

        let mut registry = new_registry();
        for udf in udfs {
            registry.add_local_udf(udf);
        }
        Self::from_logical(
            "local".to_string(),
            &job_id,
            logical,
            &assignments,
            registry,
            restore_epoch,
            control_tx,
        )
        .await
    }

    pub async fn from_logical(
        name: String,
        job_id: &str,
        logical: &LogicalGraph,
        assignments: &Vec<TaskAssignment>,
        registry: Registry,
        restore_epoch: Option<u32>,
        control_tx: Sender<ControlResp>,
    ) -> Program {
        let mut physical = DiGraph::new();

        let checkpoint_metadata = if let Some(epoch) = restore_epoch {
            info!("Restoring checkpoint {} for job {}", epoch, job_id);
            Some(
                StateBackend::load_checkpoint_metadata(job_id, epoch)
                    .await
                    .unwrap_or_else(|_| {
                        panic!("failed to load checkpoint metadata for epoch {}", epoch)
                    }),
            )
        } else {
            None
        };

        debug!("Logical graph\n{:?}", dot::Dot::new(logical));

        let registry = Arc::new(registry);

        let mut parallelism_map = HashMap::new();
        for task in assignments {
            *(parallelism_map.entry(&task.node_id).or_insert(0usize)) += 1;
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

            let in_queue_count = logical
                .edges_directed(idx, Direction::Incoming)
                .map(|edge| match edge.weight().edge_type {
                    LogicalEdgeType::Forward => 1,
                    LogicalEdgeType::Shuffle
                    | LogicalEdgeType::LeftJoin
                    | LogicalEdgeType::RightJoin => {
                        logical.node_weight(edge.source()).unwrap().parallelism as u32
                    }
                })
                .sum();

            let node = logical.node_weight(idx).unwrap();
            let parallelism = *parallelism_map.get(&node.node_id).unwrap_or_else(|| {
                warn!("no assignments for node {}", node.node_id);
                &node.parallelism
            });
            for i in 0..parallelism {
                physical.add_node(SubtaskOrQueueNode::SubtaskNode(Box::new(SubtaskNode {
                    node_id: node.node_id,
                    subtask_idx: i,
                    parallelism,
                    in_schemas: in_schemas.clone(),
                    out_schema: out_schema.clone(),
                    node: construct_node(
                        node.operator_chain.clone(),
                        job_id,
                        node.node_id,
                        i as u32,
                        parallelism as u32,
                        in_queue_count,
                        in_schemas.clone(),
                        out_schema.clone(),
                        checkpoint_metadata.as_ref(),
                        control_tx.clone(),
                        registry.clone(),
                    )
                    .await,
                })));
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
                .filter(|n| physical.node_weight(*n).unwrap().id() == logical_in_node.node_id)
                .collect();
            assert_ne!(from_nodes.len(), 0, "failed to find from nodes");
            let to_nodes: Vec<_> = physical
                .node_indices()
                .filter(|n| physical.node_weight(*n).unwrap().id() == logical_out_node.node_id)
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
            control_tx: Some(control_tx),
        }
    }
}

pub struct Engine {
    program: Program,
    worker_id: WorkerId,
    #[allow(unused)]
    run_id: String,
    job_id: String,
    network_manager: NetworkManager,
    assignments: HashMap<(u32, usize), TaskAssignment>,
}

pub struct StreamConfig {
    pub restore_epoch: Option<u32>,
}

pub struct RunningEngine {
    program: Program,
    assignments: HashMap<(u32, usize), TaskAssignment>,
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
                    .get(&(w.id(), w.subtask_idx()))
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
                    .get(&(w.id(), w.subtask_idx()))
                    .unwrap()
                    .worker_id
                    == self.worker_id.0
            })
            .map(|idx| graph.node_weight(idx).unwrap().as_queue().tx.clone())
            .collect()
    }

    pub fn operator_controls(&self) -> HashMap<u32, Vec<Sender<ControlMessage>>> {
        let mut controls = HashMap::new();
        let graph = self.program.graph.read().unwrap();

        graph
            .node_indices()
            .filter(|idx| {
                let w = graph.node_weight(*idx).unwrap();
                self.assignments
                    .get(&(w.id(), w.subtask_idx()))
                    .unwrap()
                    .worker_id
                    == self.worker_id.0
            })
            .for_each(|idx| {
                let w = graph.node_weight(idx).unwrap();
                let assignment = self.assignments.get(&(w.id(), w.subtask_idx())).unwrap();
                let tx = graph.node_weight(idx).unwrap().as_queue().tx.clone();
                controls
                    .entry(assignment.node_id)
                    .or_insert(vec![])
                    .push(tx);
            });

        controls
    }

    pub fn operator_to_node(&self) -> HashMap<String, u32> {
        let program = self.program.graph.read().unwrap();
        let mut result = HashMap::new();
        for n in program.node_weights() {
            for id in &n.as_queue().operator_ids {
                result.insert(id.clone(), n.as_queue().task_info.node_id);
            }
        }
        result
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
            .map(|a| ((a.node_id, a.subtask_idx as usize), a))
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
                    (n.id(), n.subtask_idx()),
                    TaskAssignment {
                        node_id: n.id(),
                        subtask_idx: n.subtask_idx() as u32,
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

    pub async fn start(mut self) -> RunningEngine {
        info!("Starting job {}", self.job_id);

        let node_indexes: Vec<_> = self.program.graph.read().unwrap().node_indices().collect();

        let worker_id = self.worker_id;

        let mut senders = Senders::new();

        let ready = Arc::new(Barrier::new(self.local_task_count()));
        {
            let mut futures = FuturesUnordered::new();

            for idx in node_indexes {
                futures.push(self.schedule_node(
                    self.program.control_tx.as_ref().unwrap(),
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

        self.program.control_tx = None;

        RunningEngine {
            program: self.program,
            assignments: self.assignments,
            worker_id,
        }
    }

    async fn schedule_node(
        &self,
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
            .take_subtask();

        let assignment = &self
            .assignments
            .get(&(node.node_id, node.subtask_idx))
            .cloned()
            .unwrap_or_else(|| {
                panic!(
                    "Could not find assignment for node {}-{}",
                    node.node_id, node.subtask_idx
                )
            });

        let mut senders = Senders::new();

        if assignment.worker_id == self.worker_id.0 {
            self.run_locally(control_tx, idx, node, control_rx, ready)
                .await;
        } else {
            self.connect_to_remote_task(
                &mut senders,
                idx,
                node.node_id,
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
        node_id: u32,
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

            let addr = assignment.worker_addr.parse().unwrap_or_else(|e| {
                panic!("invalid worker address {}: {}", assignment.worker_addr, e)
            });

            self.network_manager.connect(addr, quad, rx).await;
        }

        info!(
            "Connected to remote task {}-{} running on {}",
            node_id, node_subtask_idx, assignment.worker_addr
        );
    }

    pub async fn run_locally(
        &self,
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
            node.node_id,
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
                        .get(&(target.id(), target.subtask_idx()))
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

        let task_index = task_info.task_index;

        let in_qs: Vec<_> = in_qs_map.into_values().flatten().collect();

        let out_qs = out_qs_map
            .into_values()
            .map(|v| v.into_values().collect())
            .collect();

        let operator = Box::new(node.node);
        let join_task = {
            let control_tx = control_tx.clone();
            tokio::spawn(async move {
                operator
                    .start(
                        control_tx.clone(),
                        control_rx,
                        in_qs,
                        out_qs,
                        node.out_schema,
                        ready,
                    )
                    .await;
            })
        };

        let send_copy = control_tx.clone();
        tokio::spawn(async move {
            if let Err(error) = join_task.await {
                send_copy
                    .send(ControlResp::TaskFailed {
                        node_id: node.node_id,
                        task_index: task_index as usize,
                        error: error.to_string(),
                    })
                    .await
                    .ok();
            };
        });
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn construct_node(
    chain: OperatorChain,
    job_id: &str,
    node_id: u32,
    subtask_idx: u32,
    parallelism: u32,
    input_partitions: u32,
    in_schemas: Vec<Arc<ArroyoSchema>>,
    out_schema: Option<Arc<ArroyoSchema>>,
    restore_from: Option<&CheckpointMetadata>,
    control_tx: Sender<ControlResp>,
    registry: Arc<Registry>,
) -> OperatorNode {
    if chain.is_source() {
        let (head, _) = chain.iter().next().unwrap();
        let ConstructedOperator::Source(operator) =
            construct_operator(head.operator_name, &head.operator_config, registry)
        else {
            unreachable!();
        };

        let task_info = Arc::new(TaskInfo {
            job_id: job_id.to_string(),
            node_id,
            operator_name: head.operator_name.to_string(),
            operator_id: head.operator_id.clone(),
            task_index: subtask_idx,
            parallelism,
            key_range: range_for_server(subtask_idx as usize, parallelism as usize),
        });

        OperatorNode::Source(SourceNode {
            context: OperatorContext::new(
                task_info,
                restore_from,
                control_tx,
                1,
                vec![],
                out_schema,
                operator.tables(),
            )
            .await,
            operator,
        })
    } else {
        let mut head = None;
        let mut cur: Option<&mut ChainedOperator> = None;
        let mut input_partitions = input_partitions as usize;
        for (node, edge) in chain.iter() {
            let ConstructedOperator::Operator(op) =
                construct_operator(node.operator_name, &node.operator_config, registry.clone())
            else {
                unreachable!("sources must be the first node in a chain");
            };

            let ctx = OperatorContext::new(
                Arc::new(TaskInfo {
                    job_id: job_id.to_string(),
                    node_id,
                    operator_name: node.operator_name.to_string(),
                    operator_id: node.operator_id.clone(),
                    task_index: subtask_idx,
                    parallelism,
                    key_range: range_for_server(subtask_idx as usize, parallelism as usize),
                }),
                restore_from,
                control_tx.clone(),
                input_partitions,
                if let Some(cur) = &mut cur {
                    vec![cur.context.out_schema.clone().unwrap()]
                } else {
                    in_schemas.clone()
                },
                edge.cloned().or(out_schema.clone()),
                op.tables(),
            )
            .await;

            if cur.is_none() {
                head = Some(ChainedOperator::new(op, ctx));
                cur = head.as_mut();
                input_partitions = 1;
            } else {
                cur.as_mut().unwrap().next = Some(Box::new(ChainedOperator::new(op, ctx)));
                cur = Some(cur.unwrap().next.as_mut().unwrap().as_mut());
            }
        }

        OperatorNode::Chained(head.unwrap())
    }
}

pub fn construct_operator(
    operator: OperatorName,
    config: &[u8],
    registry: Arc<Registry>,
) -> ConstructedOperator {
    let ctor: Box<dyn ErasedConstructor> = match operator {
        OperatorName::ArrowValue => Box::new(ValueExecutionConstructor),
        OperatorName::ArrowKey => Box::new(KeyExecutionConstructor),
        OperatorName::AsyncUdf => Box::new(AsyncUdfConstructor),
        OperatorName::TumblingWindowAggregate => Box::new(TumblingAggregateWindowConstructor),
        OperatorName::SlidingWindowAggregate => Box::new(SlidingAggregatingWindowConstructor),
        OperatorName::SessionWindowAggregate => Box::new(SessionAggregatingWindowConstructor),
        OperatorName::UpdatingAggregate => Box::new(IncrementalAggregatingConstructor),
        OperatorName::ExpressionWatermark => Box::new(WatermarkGeneratorConstructor),
        OperatorName::Join => Box::new(JoinWithExpirationConstructor),
        OperatorName::InstantJoin => Box::new(InstantJoinConstructor),
        OperatorName::LookupJoin => Box::new(LookupJoinConstructor),
        OperatorName::WindowFunction => Box::new(WindowFunctionConstructor),
        OperatorName::ConnectorSource | OperatorName::ConnectorSink => {
            let op: api::ConnectorOp = prost::Message::decode(config).unwrap();
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
