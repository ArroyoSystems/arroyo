use crate::grpc as grpc_proto;
use crate::grpc::api as api_proto;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use utoipa::{IntoParams, ToSchema};

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
#[serde(rename_all = "snake_case")]
pub enum CheckpointSpanType {
    Alignment,
    Sync,
    Async,
    Committing,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointEventSpan {
    pub start_time: u64,
    pub finish_time: u64,
    pub span_type: CheckpointSpanType,
    pub description: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubtaskCheckpointGroup {
    pub index: u32,
    pub bytes: u64,
    pub event_spans: Vec<CheckpointEventSpan>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperatorCheckpointGroup {
    pub operator_id: String,
    pub bytes: u64,
    pub subtasks: Vec<SubtaskCheckpointGroup>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
#[aliases(
    JobCollection = PaginatedCollection<Job>,
    PipelineCollection = PaginatedCollection<Pipeline>,
    JobLogMessageCollection = PaginatedCollection<JobLogMessage>,
    CheckpointCollection = PaginatedCollection<Checkpoint>,
    OperatorMetricGroupCollection = PaginatedCollection<OperatorMetricGroup>,
    ConnectorCollection = PaginatedCollection<Connector>,
    ConnectionProfileCollection = PaginatedCollection<ConnectionProfile>,
    ConnectionTableCollection = PaginatedCollection<ConnectionTable>,
)]
pub struct PaginatedCollection<T> {
    pub data: Vec<T>,
    pub has_more: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
#[aliases(
    OperatorCheckpointGroupCollection = NonPaginatedCollection<OperatorCheckpointGroup>,
)]
pub struct NonPaginatedCollection<T> {
    pub data: Vec<T>,
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
    pub index: u32,
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

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionProfile {
    pub id: String,
    pub name: String,
    pub connector: String,
    pub config: serde_json::Value,
    pub description: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionProfilePost {
    pub name: String,
    pub connector: String,
    pub config: serde_json::Value,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionType {
    Source,
    Sink,
}

impl Display for ConnectionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionType::Source => write!(f, "SOURCE"),
            ConnectionType::Sink => write!(f, "SINK"),
        }
    }
}

impl TryFrom<String> for ConnectionType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "source" => Ok(ConnectionType::Source),
            "sink" => Ok(ConnectionType::Sink),
            _ => Err(format!("Invalid connection type: {}", value)),
        }
    }
}

#[derive(
    Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default, Hash, PartialOrd, ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TimestampFormat {
    #[default]
    #[serde(rename = "rfc3339")]
    RFC3339,
    UnixMillis,
}

impl TryFrom<&str> for TimestampFormat {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "RFC3339" => Ok(TimestampFormat::RFC3339),
            "UnixMillis" | "unix_millis" => Ok(TimestampFormat::UnixMillis),
            _ => Err(()),
        }
    }
}

#[derive(
    Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default, Hash, PartialOrd, ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct JsonFormat {
    #[serde(default)]
    pub confluent_schema_registry: bool,

    #[serde(default)]
    pub include_schema: bool,

    #[serde(default)]
    pub debezium: bool,

    #[serde(default)]
    pub unstructured: bool,

    #[serde(default)]
    pub timestamp_format: TimestampFormat,
}

impl JsonFormat {
    fn from_opts(debezium: bool, opts: &mut HashMap<String, String>) -> Result<Self, String> {
        let confluent_schema_registry = opts
            .remove("json.confluent_schema_registry")
            .filter(|t| t == "true")
            .is_some();

        let include_schema = opts
            .remove("json.include_schema")
            .filter(|t| t == "true")
            .is_some();

        let unstructured = opts
            .remove("json.unstructured")
            .filter(|t| t == "true")
            .is_some();

        let timestamp_format: TimestampFormat = opts
            .remove("json.timestamp_format")
            .map(|t| t.as_str().try_into())
            .transpose()
            .map_err(|_| "json.timestamp_format".to_string())?
            .unwrap_or_else(|| {
                if debezium {
                    TimestampFormat::UnixMillis
                } else {
                    TimestampFormat::default()
                }
            });

        Ok(Self {
            confluent_schema_registry,
            include_schema,
            debezium,
            unstructured,
            timestamp_format,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RawStringFormat {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AvroFormat {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ParquetFormat {}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Json(JsonFormat),
    Avro(AvroFormat),
    Parquet(ParquetFormat),
    RawString(RawStringFormat),
}

impl Format {
    pub fn from_opts(opts: &mut HashMap<String, String>) -> Result<Option<Self>, String> {
        let Some(name) = opts.remove("format") else {
            return Ok(None);
        };

        Ok(Some(match name.as_str() {
            "json" => Format::Json(JsonFormat::from_opts(false, opts)?),
            "debezium_json" => Format::Json(JsonFormat::from_opts(true, opts)?),
            "protobuf" => return Err("protobuf is not yet supported".to_string()),
            "avro" => return Err("avro is not yet supported".to_string()),
            "raw_string" => Format::RawString(RawStringFormat {}),
            "parquet" => Format::Parquet(ParquetFormat {}),
            f => return Err(format!("Unknown format '{}'", f)),
        }))
    }

    pub fn is_updating(&self) -> bool {
        match self {
            Format::Json(JsonFormat { debezium: true, .. }) => true,
            Format::Json(_) | Format::Avro(_) | Format::Parquet(_) | Format::RawString(_) => false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PrimitiveType {
    Int32,
    Int64,
    UInt32,
    UInt64,
    F32,
    F64,
    Bool,
    String,
    Bytes,
    UnixMillis,
    UnixMicros,
    UnixNanos,
    DateTime,
    Json,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StructType {
    pub name: Option<String>,
    pub fields: Vec<SourceField>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Primitive(PrimitiveType),
    Struct(StructType),
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceFieldType {
    pub r#type: FieldType,
    pub sql_name: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceField {
    pub field_name: String,
    pub field_type: SourceFieldType,
    pub nullable: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaDefinition {
    JsonSchema(String),
    ProtobufSchema(String),
    AvroSchema(String),
    RawSchema(String),
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionSchema {
    pub format: Option<Format>,
    pub struct_name: Option<String>,
    pub fields: Vec<SourceField>,
    pub definition: Option<SchemaDefinition>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionTable {
    #[serde(skip_serializing)]
    pub id: i64,
    #[serde(rename = "id")]
    pub pub_id: String,
    pub name: String,
    pub created_at: u64,
    pub connector: String,
    pub connection_profile: Option<ConnectionProfile>,
    pub table_type: ConnectionType,
    pub config: serde_json::Value,
    pub schema: ConnectionSchema,
    pub consumers: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionTablePost {
    pub name: String,
    pub connector: String,
    pub connection_profile_id: Option<String>,
    pub config: serde_json::Value,
    pub schema: Option<ConnectionSchema>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TestSourceMessage {
    pub error: bool,
    pub done: bool,
    pub message: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfluentSchema {
    pub schema: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct ConfluentSchemaQueryParams {
    pub endpoint: String,
    pub topic: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "snake_case")]
pub struct PaginationQueryParams {
    pub starting_after: Option<String>,
    pub limit: Option<u32>,
}
