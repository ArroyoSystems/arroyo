// https://github.com/rust-lang/rust-clippy/issues/12908
#![allow(clippy::needless_lifetimes)]

use axum::response::IntoResponse;
use axum::Json;
use cornucopia_async::DatabaseSource;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tonic::transport::Channel;
use tower_http::compression::predicate::NotForContentType;
use tower_http::compression::{CompressionLayer, DefaultPredicate, Predicate};
use tracing::{error, info};
use utoipa::OpenApi;

use crate::connection_profiles::{
    __path_create_connection_profile, __path_delete_connection_profile,
    __path_get_connection_profile_autocomplete, __path_get_connection_profiles,
    __path_test_connection_profile,
};
use crate::connection_tables::{
    __path_create_connection_table, __path_delete_connection_table, __path_get_connection_tables,
    __path_test_connection_table, __path_test_schema,
};
use crate::connectors::__path_get_connectors;
use crate::jobs::{
    __path_get_checkpoint_details, __path_get_job_checkpoints, __path_get_job_errors,
    __path_get_job_output, __path_get_jobs,
};
use crate::metrics::__path_get_operator_metric_groups;
use crate::pipelines::__path_get_pipelines;
use crate::pipelines::{
    __path_create_pipeline, __path_create_preview_pipeline, __path_delete_pipeline,
    __path_get_pipeline, __path_get_pipeline_jobs, __path_patch_pipeline, __path_restart_pipeline,
    __path_validate_query,
};
use crate::rest::__path_ping;
use crate::rest_utils::{service_unavailable, ErrorResp};
use crate::udfs::{__path_create_udf, __path_delete_udf, __path_get_udfs, __path_validate_udf};
use arroyo_rpc::api_types::{checkpoints::*, connections::*, metrics::*, pipelines::*, udfs::*, *};
use arroyo_rpc::config::config;
use arroyo_rpc::formats::*;
use arroyo_rpc::grpc::rpc::compiler_grpc_client::CompilerGrpcClient;
use arroyo_server_common::shutdown::ShutdownGuard;
use arroyo_server_common::wrap_start;

mod cloud;
mod connection_profiles;
mod connection_tables;
mod connectors;
mod jobs;
mod metrics;
mod pipelines;
pub mod rest;
mod rest_utils;
pub mod sql;
mod udfs;

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

pub const DEFAULT_ORG: &str = "org";

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

pub(crate) fn to_micros(dt: OffsetDateTime) -> u64 {
    (dt.unix_timestamp_nanos() / 1_000) as u64
}

pub async fn compiler_service() -> Result<CompilerGrpcClient<Channel>, ErrorResp> {
    // TODO: cache this
    CompilerGrpcClient::connect(config().compiler_endpoint().to_string())
        .await
        .map_err(|e| {
            error!("Failed to connect to compiler service: {}", e);
            service_unavailable("compiler-service")
        })
}

pub async fn start_server(database: DatabaseSource, guard: ShutdownGuard) -> anyhow::Result<u16> {
    let config = config();
    let addr = SocketAddr::new(config.api.bind_address, config.api.http_port);
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    let app = rest::create_rest_app(database, &config.controller_endpoint()).layer(
        CompressionLayer::new().zstd(true).compress_when(
            DefaultPredicate::new()
                // compression doesn't work for server-sent events
                // (https://github.com/tokio-rs/axum/discussions/2034)
                .and(NotForContentType::new("text/event-stream")),
        ),
    );

    info!("Starting API server on {:?}", local_addr);
    guard.into_spawn_task(wrap_start("api", local_addr, async {
        axum::serve(listener, app.into_make_service()).await
    }));

    Ok(local_addr.port())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionInfo {
    email: String,
    organization_id: String,
    organization_name: String,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum HttpErrorCode {
    Unauthorized,
    InvalidCredentials,
    ServerError,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HttpError {
    code: HttpErrorCode,
    message: String,
}

impl HttpError {
    pub fn new(code: HttpErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn login_error() -> Self {
        Self::new(
            HttpErrorCode::InvalidCredentials,
            "The username or password was incorrect",
        )
    }

    pub fn unauthorized_error() -> Self {
        Self::new(
            HttpErrorCode::Unauthorized,
            "You are not authorized to access this endpoint",
        )
    }

    pub fn server_error() -> Self {
        Self::new(HttpErrorCode::ServerError, "Something went wrong")
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> axum::response::Response {
        let status = match self.code {
            HttpErrorCode::InvalidCredentials | HttpErrorCode::Unauthorized => {
                StatusCode::UNAUTHORIZED
            }
            HttpErrorCode::ServerError => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let mut resp = Json(self).into_response();
        *resp.status_mut() = status;
        resp
    }
}

#[derive(OpenApi)]
#[openapi(
    info(title = "Arroyo REST API", version = "1.0.0"),
    servers((url = "/api/")),
    paths(
        ping,
        validate_query,
        validate_udf,
        create_pipeline,
        create_preview_pipeline,
        patch_pipeline,
        restart_pipeline,
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
        test_connection_profile,
        delete_connection_profile,
        get_connection_profile_autocomplete,
        get_connection_tables,
        create_connection_table,
        create_connection_profile,
        delete_connection_table,
        test_connection_table,
        test_schema,
        get_checkpoint_details,
        create_udf,
        get_udfs,
        delete_udf
    ),
    components(schemas(
        ErrorResp,
        PipelinePost,
        PreviewPost,
        PipelinePatch,
        PipelineRestart,
        Pipeline,
        PipelineGraph,
        PipelineNode,
        PipelineEdge,
        Job,
        StopType,
        PipelineCollection,
        JobCollection,
        JobLogMessage,
        JobLogMessageCollection,
        JobLogLevel,
        Checkpoint,
        CheckpointCollection,
        OutputData,
        MetricName,
        Metric,
        SubtaskMetrics,
        MetricGroup,
        OperatorMetricGroup,
        ConnectorCollection,
        Connector,
        ConnectionProfile,
        ConnectionProfilePost,
        ConnectionAutocompleteResp,
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
        JsonFormat,
        AvroFormat,
        ProtobufFormat,
        ParquetFormat,
        RawStringFormat,
        RawBytesFormat,
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
        ValidateQueryPost,
        QueryValidationResult,
        ValidateUdfPost,
        UdfValidationResult,
        Udf,
        UdfLanguage,
        UdfPost,
        GlobalUdf,
        GlobalUdfCollection,
        BadData,
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
