use anyhow::anyhow;
use axum::response::IntoResponse;
use axum::Json;
use deadpool_postgres::Pool;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::error::Error;
use time::OffsetDateTime;
use tokio_postgres::error::SqlState;
use tonic::transport::Channel;
use tracing::{error, info, warn};
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
    __path_create_pipeline, __path_delete_pipeline, __path_get_pipeline, __path_get_pipeline_jobs,
    __path_patch_pipeline, __path_restart_pipeline, __path_validate_query,
};
use crate::rest::__path_ping;
use crate::rest_utils::{bad_request, log_and_map, service_unavailable, ErrorResp};
use crate::udfs::{__path_create_udf, __path_delete_udf, __path_get_udfs, __path_validate_udf};
use arroyo_rpc::api_types::{checkpoints::*, connections::*, metrics::*, pipelines::*, udfs::*, *};
use arroyo_rpc::formats::*;
use arroyo_rpc::grpc::compiler_grpc_client::CompilerGrpcClient;
use arroyo_types::{
    default_controller_addr, grpc_port, ports, service_port, COMPILER_ADDR_ENV,
    CONTROLLER_ADDR_ENV, HTTP_PORT_ENV,
};

mod cloud;
mod connection_profiles;
mod connection_tables;
mod connectors;
mod jobs;
mod metrics;
mod pipelines;
pub mod rest;
mod rest_utils;
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

pub async fn compiler_service() -> Result<CompilerGrpcClient<Channel>, ErrorResp> {
    let compiler_addr = std::env::var(COMPILER_ADDR_ENV).unwrap_or_else(|_| {
        format!(
            "http://localhost:{}",
            grpc_port("compiler", ports::COMPILER_GRPC)
        )
    });

    // TODO: cache this
    CompilerGrpcClient::connect(compiler_addr.to_string())
        .await
        .map_err(|e| {
            error!("Failed to connect to compiler service: {}", e);
            service_unavailable("compiler-service")
        })
}

pub async fn start_server(pool: Pool) -> anyhow::Result<()> {
    let controller_addr =
        std::env::var(CONTROLLER_ADDR_ENV).unwrap_or_else(|_| default_controller_addr());

    let http_port = service_port("api", ports::API_HTTP, HTTP_PORT_ENV);
    let addr = format!("0.0.0.0:{}", http_port).parse().unwrap();

    let app = rest::create_rest_app(pool, &controller_addr);

    info!("Starting API server on {:?}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to start API server on {}: {}",
                addr,
                e.source()
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| e.to_string())
            )
        })
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
        MetricNames,
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
        ValidateQueryPost,
        QueryValidationResult,
        ValidateUdfPost,
        UdfValidationResult,
        Udf,
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
