use arrow_schema::Schema;
use arroyo_rpc::grpc::api::EdgeType;
use petgraph::graph::DiGraph;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArrowSchema {
    pub schema: Arc<Schema>,
    pub timestamp_col: usize,
    pub key_col: Option<usize>,
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
            LogicalEdgeType::LeftJoin => write!(f, "[left]⤨"),
            LogicalEdgeType::RightJoin => write!(f, "[right]⤨"),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalEdge {
    pub edge_type: LogicalEdgeType,
    pub schema: ArrowSchema,
}

#[derive(Clone)]
pub struct LogicalNode {
    pub id: String,
    pub description: String,
    pub operator_name: String,
    pub operator_config: Vec<u8>,
    pub initial_parallelism: usize,
}

impl Display for LogicalNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl Debug for LogicalNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

pub type LogicalGraph = DiGraph<LogicalNode, LogicalEdge>;

pub fn from_proto(proto: arroyo_rpc::grpc::api::ArrowProgram) -> LogicalGraph {
    let mut graph = DiGraph::new();

    let mut id_map = HashMap::new();

    for node in proto.nodes {
        id_map.insert(
            node.node_index,
            graph.add_node(LogicalNode {
                id: node.node_id,
                description: node.description,
                operator_name: node.operator_name,
                operator_config: node.operator_config,
                initial_parallelism: node.parallelism as usize,
            }),
        );
    }

    for edge in proto.edges {
        let source = *id_map.get(&edge.source).unwrap();
        let target = *id_map.get(&edge.target).unwrap();
        let schema = edge.schema.as_ref().unwrap();

        graph.add_edge(
            source,
            target,
            LogicalEdge {
                edge_type: edge.edge_type().into(),
                schema: ArrowSchema {
                    schema: serde_json::from_str(&schema.arrow_schema).unwrap(),
                    timestamp_col: schema.timestamp_col as usize,
                    key_col: schema.key_col.map(|t| t as usize),
                },
            },
        );
    }

    graph
}
