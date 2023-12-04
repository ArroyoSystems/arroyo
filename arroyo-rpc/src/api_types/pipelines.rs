use crate::api_types::udfs::Udf;
use crate::grpc as grpc_proto;
use crate::grpc::api as api_proto;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ValidateQueryPost {
    pub query: String,
    pub udfs: Option<Vec<Udf>>, // needed for query validation but are not themselves validated
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryValidationResult {
    pub graph: Option<PipelineGraph>,
    #[serde(default)]
    pub errors: Vec<String>,
    pub missing_query: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelinePost {
    pub name: String,
    pub query: String,
    pub udfs: Option<Vec<Udf>>,
    pub preview: Option<bool>,
    pub parallelism: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelinePatch {
    pub parallelism: Option<u64>,
    pub checkpoint_interval_micros: Option<u64>,
    pub stop: Option<StopType>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineRestart {
    pub force: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Pipeline {
    pub id: String,
    pub name: String,
    pub query: String,
    pub udfs: Vec<Udf>,
    pub checkpoint_interval_micros: u64,
    pub stop: StopType,
    pub created_at: u64,
    pub action: Option<StopType>,
    pub action_text: String,
    pub action_in_progress: bool,
    pub graph: PipelineGraph,
    pub preview: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineGraph {
    pub nodes: Vec<PipelineNode>,
    pub edges: Vec<PipelineEdge>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineNode {
    pub node_id: String,
    pub operator: String,
    pub parallelism: u32,
}

impl From<api_proto::JobNode> for PipelineNode {
    fn from(value: api_proto::JobNode) -> Self {
        PipelineNode {
            node_id: value.node_id,
            operator: value.operator,
            parallelism: value.parallelism,
        }
    }
}

impl From<api_proto::JobEdge> for PipelineEdge {
    fn from(value: api_proto::JobEdge) -> Self {
        PipelineEdge {
            src_id: value.src_id,
            dest_id: value.dest_id,
            key_type: value.key_type,
            value_type: value.value_type,
            edge_type: value.edge_type,
        }
    }
}

impl From<api_proto::JobGraph> for PipelineGraph {
    fn from(value: api_proto::JobGraph) -> Self {
        PipelineGraph {
            nodes: value.nodes.into_iter().map(|n| n.into()).collect(),
            edges: value.edges.into_iter().map(|n| n.into()).collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineEdge {
    pub src_id: String,
    pub dest_id: String,
    pub key_type: String,
    pub value_type: String,
    pub edge_type: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum StopType {
    None,
    Checkpoint,
    Graceful,
    Immediate,
    Force,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    pub id: String,
    pub running_desired: bool,
    pub state: String,
    pub run_id: u64,
    pub start_time: Option<u64>,
    pub finish_time: Option<u64>,
    pub tasks: Option<u64>,
    pub failure_message: Option<String>,
    pub created_at: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum JobLogLevel {
    Info,
    Warn,
    Error,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct JobLogMessage {
    pub id: String,
    pub created_at: u64,
    pub operator_id: Option<String>,
    pub task_index: Option<u64>,
    pub level: JobLogLevel,
    pub message: String,
    pub details: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OutputData {
    pub operator_id: String,
    pub timestamp: u64,
    pub key: String,
    pub value: Value,
}

impl From<grpc_proto::OutputData> for OutputData {
    fn from(value: grpc_proto::OutputData) -> Self {
        OutputData {
            operator_id: value.operator_id,
            timestamp: value.timestamp,
            key: value.key,
            value: serde_json::from_str(&value.value)
                .expect("Received non-JSON data from web sink"),
        }
    }
}
