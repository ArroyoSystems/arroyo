use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio_postgres::error::SqlState;
use tracing::warn;
use utoipa::OpenApi;

use crate::connection_profiles::{
    __path_create_connection_profile, __path_get_connection_profiles,
};
use crate::connection_tables::{
    __path_create_connection_table, __path_delete_connection_table, __path_get_confluent_schema,
    __path_get_connection_tables, __path_test_connection_table, __path_test_schema,
};
use crate::connectors::__path_get_connectors;
use crate::jobs::{
    __path_get_checkpoint_details, __path_get_job_checkpoints, __path_get_job_errors,
    __path_get_job_output, __path_get_jobs,
};
use crate::metrics::__path_get_operator_metric_groups;
use crate::pipelines::__path_get_pipelines;
use crate::pipelines::__path_post_pipeline;
use crate::pipelines::{
    __path_delete_pipeline, __path_get_pipeline, __path_get_pipeline_jobs, __path_patch_pipeline,
    __path_validate_pipeline,
};
use crate::rest::__path_ping;
use crate::rest_utils::{bad_request, log_and_map, ErrorResp};
use arroyo_rpc::formats::*;
use arroyo_rpc::types::{
    Checkpoint, CheckpointCollection, CheckpointEventSpan, CheckpointSpanType, ConfluentSchema,
    ConnectionProfile, ConnectionProfileCollection, ConnectionProfilePost, ConnectionSchema,
    ConnectionTable, ConnectionTableCollection, ConnectionTablePost, ConnectionType, Connector,
    ConnectorCollection, FieldType, Job, JobCollection, JobLogLevel, JobLogMessage,
    JobLogMessageCollection, Metric, MetricGroup, MetricNames, OperatorCheckpointGroup,
    OperatorCheckpointGroupCollection, OperatorMetricGroup, OutputData, PaginationQueryParams,
    Pipeline, PipelineCollection, PipelineEdge, PipelineGraph, PipelineNode, PipelinePatch,
    PipelinePost, PrimitiveType, SchemaDefinition, SourceField, SourceFieldType,
    StopType as StopTypeRest, StructType, SubtaskCheckpointGroup, SubtaskMetrics,
    TestSourceMessage, Udf, UdfLanguage, ValidatePipelinePost,
};

mod cloud;
mod connection_profiles;
mod connection_tables;
mod connectors;
mod jobs;
mod metrics;
mod optimizations;
mod pipelines;
pub mod rest;
mod rest_utils;

include!(concat!(env!("OUT_DIR"), "/api-sql.rs"));

fn default_max_nexmark_qps() -> f64 {
    1000.0
}
fn default_max_impulse_qps() -> f64 {
    1000.0
}
fn default_max_parallelism() -> u32 {
    32
}
fn default_max_operators() -> u32 {
    30
}
fn default_max_running_jobs() -> u32 {
    10
}
fn default_kafka_qps() -> u32 {
    10_000
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct OrgMetadata {
    #[serde(default)]
    can_create_programs: bool,

    #[serde(default = "default_max_nexmark_qps")]
    max_nexmark_qps: f64,

    #[serde(default = "default_max_impulse_qps")]
    max_impulse_qps: f64,

    #[serde(default = "default_max_parallelism")]
    max_parallelism: u32,

    #[serde(default = "default_max_operators")]
    max_operators: u32,

    #[serde(default = "default_max_running_jobs")]
    max_running_jobs: u32,

    #[serde(default = "default_kafka_qps")]
    kafka_qps: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AuthData {
    pub user_id: String,
    pub organization_id: String,
    pub role: String,
    pub org_metadata: OrgMetadata,
}

fn handle_db_error(name: &str, err: tokio_postgres::Error) -> ErrorResp {
    if let Some(db) = &err.as_db_error() {
        if *db.code() == SqlState::UNIQUE_VIOLATION {
            // TODO improve error message
            warn!("SQL error: {}", db.message());
            return bad_request(format!("A {} with that name already exists", name));
        }
    }

    log_and_map(err)
}

fn handle_delete(name: &str, users: &str, err: tokio_postgres::Error) -> ErrorResp {
    if let Some(db) = &err.as_db_error() {
        if *db.code() == SqlState::FOREIGN_KEY_VIOLATION {
            return bad_request(format!(
                "Cannot delete {}; it is still being used by {}",
                name, users
            ));
        }
    }

    log_and_map(err)
}

pub(crate) fn to_micros(dt: OffsetDateTime) -> u64 {
    (dt.unix_timestamp_nanos() / 1_000) as u64
}

#[derive(OpenApi)]
#[openapi(
    info(title = "Arroyo REST API", version = "1.0.0"),
    servers((url = "/api/")),
    paths(
        ping,
        validate_pipeline,
        post_pipeline,
        patch_pipeline,
        get_pipeline,
        delete_pipeline,
        get_pipelines,
        get_jobs,
        get_pipeline_jobs,
        get_job_errors,
        get_job_checkpoints,
        get_job_output,
        get_operator_metric_groups,
        get_connectors,
        get_connection_profiles,
        get_connection_tables,
        create_connection_table,
        create_connection_profile,
        delete_connection_table,
        test_connection_table,
        test_schema,
        get_confluent_schema,
        get_checkpoint_details,
    ),
    components(schemas(
        ValidatePipelinePost,
        PipelinePost,
        PipelinePatch,
        Pipeline,
        PipelineGraph,
        PipelineNode,
        PipelineEdge,
        Job,
        StopTypeRest,
        Udf,
        UdfLanguage,
        PipelineCollection,
        JobCollection,
        JobLogMessage,
        JobLogMessageCollection,
        JobLogLevel,
        Checkpoint,
        CheckpointCollection,
        OutputData,
        MetricNames,
        Metric,
        SubtaskMetrics,
        MetricGroup,
        OperatorMetricGroup,
        ConnectorCollection,
        Connector,
        ConnectionProfile,
        ConnectionProfilePost,
        ConnectionProfileCollection,
        ConnectionTable,
        ConnectionTablePost,
        ConnectionTableCollection,
        ConnectionSchema,
        ConnectionType,
        SourceField,
        Format,
        SourceFieldType,
        FieldType,
        StructType,
        PrimitiveType,
        SchemaDefinition,
        TestSourceMessage,
        ConfluentSchema,
        JsonFormat,
        AvroFormat,
        ParquetFormat,
        RawStringFormat,
        TimestampFormat,
        Framing,
        FramingMethod,
        NewlineDelimitedFraming,
        PaginationQueryParams,
        CheckpointEventSpan,
        CheckpointSpanType,
        OperatorCheckpointGroupCollection,
        SubtaskCheckpointGroup,
        OperatorCheckpointGroup,
    )),
    tags(
        (name = "ping", description = "Ping endpoint"),
        (name = "connection_profiles", description = "Connection profiles management endpoints"),
        (name = "connection_tables", description = "Connection tables management endpoints"),
        (name = "pipelines", description = "Pipeline management endpoints"),
        (name = "jobs", description = "Job management endpoints"),
        (name = "connectors", description = "Connector management endpoints"),
    )
)]
pub struct ApiDoc;
