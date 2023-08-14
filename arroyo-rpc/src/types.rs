use crate::grpc as grpc_proto;
use crate::grpc::api as api_proto;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ValidatePipelinePost {
    pub query: String,
    pub udfs: Option<Vec<Udf>>,
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

impl Into<api_proto::StopType> for StopType {
    fn into(self) -> api_proto::StopType {
        match self {
            StopType::None => api_proto::StopType::None,
            StopType::Checkpoint => api_proto::StopType::Checkpoint,
            StopType::Graceful => api_proto::StopType::Graceful,
            StopType::Immediate => api_proto::StopType::Immediate,
            StopType::Force => api_proto::StopType::Force,
        }
    }
}

impl From<api_proto::StopType> for StopType {
    fn from(value: api_proto::StopType) -> Self {
        match value {
            api_proto::StopType::None => StopType::None,
            api_proto::StopType::Checkpoint => StopType::Checkpoint,
            api_proto::StopType::Graceful => StopType::Graceful,
            api_proto::StopType::Immediate => StopType::Immediate,
            api_proto::StopType::Force => StopType::Force,
        }
    }
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
pub enum UdfLanguage {
    Rust,
}

impl From<api_proto::UdfLanguage> for UdfLanguage {
    fn from(value: api_proto::UdfLanguage) -> Self {
        match value {
            api_proto::UdfLanguage::Rust => UdfLanguage::Rust,
        }
    }
}

impl From<api_proto::Udf> for Udf {
    fn from(value: api_proto::Udf) -> Self {
        Udf {
            language: value.language().into(),
            definition: value.definition,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Udf {
    pub language: UdfLanguage,
    pub definition: String,
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
    pub created_at: u64,
    pub operator_id: Option<String>,
    pub task_index: Option<u64>,
    pub level: JobLogLevel,
    pub message: String,
    pub details: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    pub epoch: u32,
    pub backend: String,
    pub start_time: u64,
    pub finish_time: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
#[aliases(
    JobCollection = Collection<Job>,
    PipelineCollection = Collection<Pipeline>,
    JobLogMessageCollection = Collection<JobLogMessage>,
    CheckpointCollection = Collection<Checkpoint>,
    OperatorMetricGroupCollection = Collection<OperatorMetricGroup>,
    ConnectorCollection = Collection<Connector>,
)]
pub struct Collection<T> {
    pub data: Vec<T>,
    pub has_more: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OutputData {
    pub operator_id: String,
    pub timestamp: u64,
    pub key: String,
    pub value: String,
}

impl From<grpc_proto::OutputData> for OutputData {
    fn from(value: grpc_proto::OutputData) -> Self {
        OutputData {
            operator_id: value.operator_id,
            timestamp: value.timestamp,
            key: value.key,
            value: value.value,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, Hash, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetricNames {
    BytesRecv,
    BytesSent,
    MessagesRecv,
    MessagesSent,
    Backpressure,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Metric {
    pub time: u64,
    pub value: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubtaskMetrics {
    pub idx: u32,
    pub metrics: Vec<Metric>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetricGroup {
    pub name: MetricNames,
    pub subtasks: Vec<SubtaskMetrics>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperatorMetricGroup {
    pub operator_id: String,
    pub metric_groups: Vec<MetricGroup>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Connector {
    pub id: String,
    pub name: String,
    pub icon: String,
    pub description: String,
    pub table_config: String,
    pub enabled: bool,
    pub source: bool,
    pub sink: bool,
    pub custom_schemas: bool,
    pub testing: bool,
    pub hidden: bool,
    pub connection_config: Option<String>,
}
