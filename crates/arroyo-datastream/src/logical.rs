use datafusion_proto::protobuf::ArrowType;
use itertools::Itertools;

use crate::optimizers::Optimizer;
use anyhow::anyhow;
use arrow_schema::DataType;
use arroyo_rpc::api_types::pipelines::{PipelineEdge, PipelineGraph, PipelineNode};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::api;
use arroyo_rpc::grpc::api::{ArrowProgram, ArrowProgramConfig, ConnectorOp, EdgeType};
use petgraph::Direction;
use petgraph::dot::Dot;
use petgraph::graph::DiGraph;
use petgraph::prelude::EdgeRef;
use prost::Message;
use rand::distr::Alphanumeric;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hasher;
use std::str::FromStr;
use std::sync::Arc;
use strum::{Display, EnumString};

#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumString, Display)]
pub enum OperatorName {
    ExpressionWatermark,
    ArrowValue,
    ArrowKey,
    Projection,
    AsyncUdf,
    Join,
    InstantJoin,
    LookupJoin,
    WindowFunction,
    TumblingWindowAggregate,
    SlidingWindowAggregate,
    SessionWindowAggregate,
    UpdatingAggregate,
    ConnectorSource,
    ConnectorSink,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum LogicalEdgeType {
    Forward,
    Shuffle,
    LeftJoin,
    RightJoin,
}

impl Display for LogicalEdgeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalEdgeType::Forward => write!(f, "→"),
            LogicalEdgeType::Shuffle => write!(f, "⤨"),
            LogicalEdgeType::LeftJoin => write!(f, "-[left]⤨"),
            LogicalEdgeType::RightJoin => write!(f, "-[right]⤨"),
        }
    }
}

impl From<arroyo_rpc::grpc::api::EdgeType> for LogicalEdgeType {
    fn from(value: EdgeType) -> Self {
        match value {
            EdgeType::Unused => panic!("invalid edge type"),
            EdgeType::Forward => LogicalEdgeType::Forward,
            EdgeType::Shuffle => LogicalEdgeType::Shuffle,
            EdgeType::LeftJoin => LogicalEdgeType::LeftJoin,
            EdgeType::RightJoin => LogicalEdgeType::RightJoin,
        }
    }
}

impl From<LogicalEdgeType> for api::EdgeType {
    fn from(value: LogicalEdgeType) -> Self {
        match value {
            LogicalEdgeType::Forward => EdgeType::Forward,
            LogicalEdgeType::Shuffle => EdgeType::Shuffle,
            LogicalEdgeType::LeftJoin => EdgeType::LeftJoin,
            LogicalEdgeType::RightJoin => EdgeType::RightJoin,
        }
    }
}

impl TryFrom<LogicalProgram> for PipelineGraph {
    type Error = anyhow::Error;
    fn try_from(value: LogicalProgram) -> anyhow::Result<Self> {
        let nodes: anyhow::Result<Vec<_>> = value
            .graph
            .node_weights()
            .map(|node| Ok(PipelineNode {
                node_id: node.node_id,
                operator: match node.operator_chain.operators.first() {
                    Some(ChainedLogicalOperator { operator_name: OperatorName::ConnectorSource | OperatorName::ConnectorSink, operator_config, .. }) => {
                        ConnectorOp::decode(&operator_config[..])
                            .map_err(|_| anyhow!("invalid graph: could not decode connector configuration for {}", node.node_id))?
                            .connector
                    }
                    Some(op) if node.operator_chain.operators.len() == 1 => {
                        op.operator_id.to_string()
                    }
                    _ => "chained_op".to_string(),
                },
                description: node.description.clone(),
                parallelism: node.parallelism as u32,
            }))
            .collect();

        let edges = value
            .graph
            .edge_references()
            .map(|edge| {
                let src = value.graph.node_weight(edge.source()).unwrap();
                let target = value.graph.node_weight(edge.target()).unwrap();
                PipelineEdge {
                    src_id: src.node_id,
                    dest_id: target.node_id,
                    key_type: "()".to_string(),
                    value_type: "()".to_string(),
                    edge_type: format!("{:?}", edge.weight().edge_type),
                }
            })
            .collect();

        Ok(Self {
            nodes: nodes?,
            edges,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalEdge {
    pub edge_type: LogicalEdgeType,
    pub schema: Arc<ArroyoSchema>,
}

impl LogicalEdge {
    pub fn new(edge_type: LogicalEdgeType, schema: ArroyoSchema) -> Self {
        LogicalEdge {
            edge_type,
            schema: Arc::new(schema),
        }
    }

    pub fn project_all(edge_type: LogicalEdgeType, schema: ArroyoSchema) -> Self {
        LogicalEdge {
            edge_type,
            schema: Arc::new(schema),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChainedLogicalOperator {
    pub operator_id: String,
    pub operator_name: OperatorName,
    pub operator_config: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct OperatorChain {
    pub(crate) operators: Vec<ChainedLogicalOperator>,
    pub(crate) edges: Vec<Arc<ArroyoSchema>>,
}

impl OperatorChain {
    pub fn new(operator: ChainedLogicalOperator) -> Self {
        Self {
            operators: vec![operator],
            edges: vec![],
        }
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&ChainedLogicalOperator, Option<&Arc<ArroyoSchema>>)> {
        self.operators
            .iter()
            .zip_longest(self.edges.iter())
            .map(|e| e.left_and_right())
            .map(|(l, r)| (l.unwrap(), r))
    }

    pub fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = (&mut ChainedLogicalOperator, Option<&Arc<ArroyoSchema>>)> {
        self.operators
            .iter_mut()
            .zip_longest(self.edges.iter())
            .map(|e| e.left_and_right())
            .map(|(l, r)| (l.unwrap(), r))
    }

    pub fn first(&self) -> &ChainedLogicalOperator {
        &self.operators[0]
    }

    pub fn len(&self) -> usize {
        self.operators.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    pub fn is_source(&self) -> bool {
        self.operators[0].operator_name == OperatorName::ConnectorSource
    }

    pub fn is_sink(&self) -> bool {
        self.operators[0].operator_name == OperatorName::ConnectorSink
    }
}

#[derive(Clone)]
pub struct LogicalNode {
    pub node_id: u32,
    pub description: String,
    pub operator_chain: OperatorChain,
    pub parallelism: usize,
}

impl LogicalNode {
    pub fn single(
        id: u32,
        operator_id: String,
        name: OperatorName,
        config: Vec<u8>,
        description: String,
        parallelism: usize,
    ) -> Self {
        Self {
            node_id: id,
            description,
            operator_chain: OperatorChain {
                operators: vec![ChainedLogicalOperator {
                    operator_id,
                    operator_name: name,
                    operator_config: config,
                }],
                edges: vec![],
            },
            parallelism,
        }
    }
}

impl Display for LogicalNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl Debug for LogicalNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}[{}]",
            self.operator_chain
                .operators
                .iter()
                .map(|op| op.operator_id.clone())
                .collect::<Vec<_>>()
                .join(" -> "),
            self.parallelism
        )
    }
}

pub type LogicalGraph = DiGraph<LogicalNode, LogicalEdge>;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct DylibUdfConfig {
    pub dylib_path: String,
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
    pub aggregate: bool,
    pub is_async: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PythonUdfConfig {
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
    pub name: Arc<String>,
    pub definition: Arc<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ProgramConfig {
    pub udf_dylibs: HashMap<String, DylibUdfConfig>,
    pub python_udfs: HashMap<String, PythonUdfConfig>,
}

#[derive(Clone, Debug, Default)]
pub struct LogicalProgram {
    pub graph: LogicalGraph,
    pub program_config: ProgramConfig,
}

impl LogicalProgram {
    pub fn new(graph: LogicalGraph, program_config: ProgramConfig) -> Self {
        Self {
            graph,
            program_config,
        }
    }

    pub fn optimize(&mut self, optimizer: &dyn Optimizer) {
        optimizer.optimize(&mut self.graph);
    }

    pub fn update_parallelism(&mut self, overrides: &HashMap<u32, usize>) {
        for node in self.graph.node_weights_mut() {
            if let Some(p) = overrides.get(&node.node_id) {
                node.parallelism = *p;
            }
        }
    }

    pub fn dot(&self) -> String {
        format!("{:?}", Dot::with_config(&self.graph, &[]))
    }

    pub fn task_count(&self) -> usize {
        // TODO: this can be cached
        self.graph.node_weights().map(|nw| nw.parallelism).sum()
    }

    pub fn sources(&self) -> HashSet<u32> {
        // TODO: this can be memoized
        self.graph
            .externals(Direction::Incoming)
            .map(|t| self.graph.node_weight(t).unwrap().node_id)
            .collect()
    }

    pub fn get_hash(&self) -> String {
        let mut hasher = DefaultHasher::new();
        let bs = api::ArrowProgram::from(self.clone()).encode_to_vec();
        for b in bs {
            hasher.write_u8(b);
        }

        let rng = SmallRng::seed_from_u64(hasher.finish());

        rng.sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .map(|c| c.to_ascii_lowercase())
            .collect()
    }

    pub fn tasks_per_operator(&self) -> HashMap<String, usize> {
        let mut tasks_per_operator = HashMap::new();
        for node in self.graph.node_weights() {
            for op in &node.operator_chain.operators {
                tasks_per_operator.insert(op.operator_id.clone(), node.parallelism);
            }
        }
        tasks_per_operator
    }

    pub fn operator_names_by_id(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        for node in self.graph.node_weights() {
            for op in &node.operator_chain.operators {
                m.insert(
                    op.operator_id.clone(),
                    match op.operator_name {
                        OperatorName::ConnectorSource | OperatorName::ConnectorSink => {
                            let c: api::ConnectorOp =
                                prost::Message::decode(&op.operator_config[..]).unwrap();
                            c.connector
                        }
                        name => name.to_string(),
                    },
                );
            }
        }

        m
    }

    pub fn tasks_per_node(&self) -> HashMap<u32, usize> {
        let mut tasks_per_node = HashMap::new();
        for node in self.graph.node_weights() {
            tasks_per_node.insert(node.node_id, node.parallelism);
        }
        tasks_per_node
    }

    pub fn features(&self) -> HashSet<String> {
        let mut s = HashSet::new();

        for n in self.graph.node_weights() {
            for t in &n.operator_chain.operators {
                let feature = match &t.operator_name {
                    OperatorName::AsyncUdf => "async-udf".to_string(),
                    OperatorName::ExpressionWatermark
                    | OperatorName::ArrowValue
                    | OperatorName::ArrowKey
                    | OperatorName::Projection => continue,
                    OperatorName::Join => "join-with-expiration".to_string(),
                    OperatorName::InstantJoin => "windowed-join".to_string(),
                    OperatorName::WindowFunction => "sql-window-function".to_string(),
                    OperatorName::LookupJoin => "lookup-join".to_string(),
                    OperatorName::TumblingWindowAggregate => {
                        "sql-tumbling-window-aggregate".to_string()
                    }
                    OperatorName::SlidingWindowAggregate => {
                        "sql-sliding-window-aggregate".to_string()
                    }
                    OperatorName::SessionWindowAggregate => {
                        "sql-session-window-aggregate".to_string()
                    }
                    OperatorName::UpdatingAggregate => "sql-updating-aggregate".to_string(),
                    OperatorName::ConnectorSource => {
                        let Ok(connector_op) = ConnectorOp::decode(&t.operator_config[..]) else {
                            continue;
                        };
                        format!("{}-source", connector_op.connector)
                    }
                    OperatorName::ConnectorSink => {
                        let Ok(connector_op) = ConnectorOp::decode(&t.operator_config[..]) else {
                            continue;
                        };
                        format!("{}-sink", connector_op.connector)
                    }
                };
                s.insert(feature);
            }
        }

        s
    }
}

impl TryFrom<ArrowProgram> for LogicalProgram {
    type Error = anyhow::Error;

    fn try_from(value: ArrowProgram) -> anyhow::Result<Self> {
        let mut graph = DiGraph::new();

        let mut id_map = HashMap::new();

        for node in value.nodes {
            id_map.insert(
                node.node_index,
                graph.add_node(LogicalNode {
                    node_id: node.node_id,
                    description: node.description,
                    operator_chain: OperatorChain {
                        operators: node
                            .operators
                            .into_iter()
                            .map(|op| {
                                Ok(ChainedLogicalOperator {
                                    operator_id: op.operator_id,
                                    operator_name: OperatorName::from_str(&op.operator_name)?,
                                    operator_config: op.operator_config,
                                })
                            })
                            .collect::<anyhow::Result<Vec<_>>>()?,
                        edges: node
                            .edges
                            .into_iter()
                            .map(|e| Ok(Arc::new(e.try_into()?)))
                            .collect::<anyhow::Result<Vec<_>>>()?,
                    },
                    parallelism: node.parallelism as usize,
                }),
            );
        }

        for edge in value.edges {
            let source = *id_map.get(&edge.source).unwrap();
            let target = *id_map.get(&edge.target).unwrap();
            let schema = edge.schema.as_ref().unwrap();

            graph.add_edge(
                source,
                target,
                LogicalEdge {
                    edge_type: edge.edge_type().into(),
                    schema: Arc::new(schema.clone().try_into()?),
                },
            );
        }

        let program_config = value
            .program_config
            .unwrap_or_else(|| ArrowProgramConfig {
                udf_dylibs: HashMap::new(),
                python_udfs: HashMap::new(),
            })
            .into();

        Ok(LogicalProgram::new(graph, program_config))
    }
}

impl From<DylibUdfConfig> for api::DylibUdfConfig {
    fn from(from: DylibUdfConfig) -> Self {
        api::DylibUdfConfig {
            dylib_path: from.dylib_path,
            arg_types: from
                .arg_types
                .iter()
                .map(|t| {
                    ArrowType::try_from(t)
                        .expect("unsupported data type")
                        .encode_to_vec()
                })
                .collect(),
            return_type: ArrowType::try_from(&from.return_type)
                .expect("unsupported data type")
                .encode_to_vec(),
            aggregate: from.aggregate,
            is_async: from.is_async,
        }
    }
}

impl From<api::DylibUdfConfig> for DylibUdfConfig {
    fn from(from: api::DylibUdfConfig) -> Self {
        DylibUdfConfig {
            dylib_path: from.dylib_path,
            arg_types: from
                .arg_types
                .iter()
                .map(|t| {
                    DataType::try_from(
                        &ArrowType::decode(&mut t.as_slice()).expect("invalid arrow type"),
                    )
                    .expect("invalid arrow type")
                })
                .collect(),
            return_type: DataType::try_from(
                &ArrowType::decode(&mut from.return_type.as_slice()).unwrap(),
            )
            .expect("invalid arrow type"),
            aggregate: from.aggregate,
            is_async: from.is_async,
        }
    }
}

impl From<api::PythonUdfConfig> for PythonUdfConfig {
    fn from(value: api::PythonUdfConfig) -> Self {
        PythonUdfConfig {
            arg_types: value
                .arg_types
                .iter()
                .map(|t| {
                    DataType::try_from(
                        &ArrowType::decode(&mut t.as_slice()).expect("invalid arrow type"),
                    )
                    .expect("invalid arrow type")
                })
                .collect(),
            return_type: DataType::try_from(
                &ArrowType::decode(&mut value.return_type.as_slice()).unwrap(),
            )
            .expect("invalid arrow type"),
            name: Arc::new(value.name),
            definition: Arc::new(value.definition),
        }
    }
}

impl From<PythonUdfConfig> for api::PythonUdfConfig {
    fn from(from: PythonUdfConfig) -> Self {
        api::PythonUdfConfig {
            arg_types: from
                .arg_types
                .iter()
                .map(|t| {
                    ArrowType::try_from(t)
                        .expect("unsupported data type")
                        .encode_to_vec()
                })
                .collect(),
            return_type: ArrowType::try_from(&from.return_type)
                .expect("unsupported data type")
                .encode_to_vec(),
            name: (*from.name).clone(),
            definition: (*from.definition).clone(),
        }
    }
}

impl From<ProgramConfig> for ArrowProgramConfig {
    fn from(from: ProgramConfig) -> Self {
        ArrowProgramConfig {
            udf_dylibs: from
                .udf_dylibs
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            python_udfs: from
                .python_udfs
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

impl From<ArrowProgramConfig> for ProgramConfig {
    fn from(from: ArrowProgramConfig) -> Self {
        ProgramConfig {
            udf_dylibs: from
                .udf_dylibs
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            python_udfs: from
                .python_udfs
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

impl From<LogicalProgram> for ArrowProgram {
    fn from(value: LogicalProgram) -> Self {
        let graph = value.graph;
        let nodes = graph
            .node_indices()
            .map(|idx| {
                let node = graph.node_weight(idx).unwrap();
                api::ArrowNode {
                    node_index: idx.index() as i32,
                    node_id: node.node_id,
                    parallelism: node.parallelism as u32,
                    description: node.description.clone(),
                    operators: node
                        .operator_chain
                        .operators
                        .iter()
                        .map(|op| api::ChainedOperator {
                            operator_id: op.operator_id.clone(),
                            operator_name: op.operator_name.to_string(),
                            operator_config: op.operator_config.clone(),
                        })
                        .collect(),
                    edges: node
                        .operator_chain
                        .edges
                        .iter()
                        .map(|edge| (**edge).clone().into())
                        .collect(),
                }
            })
            .collect();

        let edges = graph
            .edge_indices()
            .map(|idx| {
                let edge = graph.edge_weight(idx).unwrap();
                let (source, target) = graph.edge_endpoints(idx).unwrap();

                let edge_type: api::EdgeType = edge.edge_type.into();
                api::ArrowEdge {
                    source: source.index() as i32,
                    target: target.index() as i32,
                    schema: Some((*edge.schema).clone().into()),
                    edge_type: edge_type as i32,
                }
            })
            .collect();

        api::ArrowProgram {
            nodes,
            edges,
            program_config: Some(value.program_config.into()),
        }
    }
}
