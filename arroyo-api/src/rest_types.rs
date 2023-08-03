use crate::types::public::LogLevel;
use crate::types::public::StopMode;
use arroyo_datastream::Program;
use arroyo_rpc::grpc;
use arroyo_rpc::grpc::api;
use petgraph::visit::EdgeRef;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ValidatePipelinePost {
    pub query: String,
    pub udfs: Vec<Udf>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelinePost {
    pub name: String,
    pub query: String,
    pub udfs: Vec<Udf>,
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

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineEdge {
    pub src_id: String,
    pub dest_id: String,
    pub key_type: String,
    pub value_type: String,
    pub edge_type: String,
}

impl From<Program> for PipelineGraph {
    fn from(value: Program) -> Self {
        let nodes = value
            .graph
            .node_weights()
            .map(|node| PipelineNode {
                node_id: node.operator_id.to_string(),
                operator: format!("{:?}", node),
                parallelism: node.parallelism as u32,
            })
            .collect();

        let edges = value
            .graph
            .edge_references()
            .map(|edge| {
                let src = value.graph.node_weight(edge.source()).unwrap();
                let target = value.graph.node_weight(edge.target()).unwrap();
                PipelineEdge {
                    src_id: src.operator_id.to_string(),
                    dest_id: target.operator_id.to_string(),
                    key_type: edge.weight().key.to_string(),
                    value_type: edge.weight().value.to_string(),
                    edge_type: format!("{:?}", edge.weight().typ),
                }
            })
            .collect();

        PipelineGraph { nodes, edges }
    }
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

impl From<StopMode> for StopType {
    fn from(value: StopMode) -> Self {
        match value {
            StopMode::none => StopType::None,
            StopMode::checkpoint => StopType::Checkpoint,
            StopMode::graceful => StopType::Graceful,
            StopMode::immediate => StopType::Immediate,
            StopMode::force => StopType::Force,
        }
    }
}

impl Into<api::StopType> for StopType {
    fn into(self) -> api::StopType {
        match self {
            StopType::None => api::StopType::None,
            StopType::Checkpoint => api::StopType::Checkpoint,
            StopType::Graceful => api::StopType::Graceful,
            StopType::Immediate => api::StopType::Immediate,
            StopType::Force => api::StopType::Force,
        }
    }
}

impl From<api::StopType> for StopType {
    fn from(value: api::StopType) -> Self {
        match value {
            api::StopType::None => StopType::None,
            api::StopType::Checkpoint => StopType::Checkpoint,
            api::StopType::Graceful => StopType::Graceful,
            api::StopType::Immediate => StopType::Immediate,
            api::StopType::Force => StopType::Force,
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

impl From<api::UdfLanguage> for UdfLanguage {
    fn from(value: api::UdfLanguage) -> Self {
        match value {
            api::UdfLanguage::Rust => UdfLanguage::Rust,
        }
    }
}

impl From<api::Udf> for Udf {
    fn from(value: api::Udf) -> Self {
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

impl From<LogLevel> for JobLogLevel {
    fn from(value: LogLevel) -> Self {
        match value {
            LogLevel::info => JobLogLevel::Info,
            LogLevel::warn => JobLogLevel::Warn,
            LogLevel::error => JobLogLevel::Error,
        }
    }
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
    OperatorMetricGroupCollection = Collection<OperatorMetricGroup>
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

impl From<grpc::OutputData> for OutputData {
    fn from(value: grpc::OutputData) -> Self {
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
