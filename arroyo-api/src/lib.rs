use crate::jobs::{
    __path_get_job_checkpoints, __path_get_job_errors, __path_get_job_output, __path_get_jobs,
};
use crate::pipelines::__path_get_pipelines;
use crate::pipelines::__path_post_pipeline;
use crate::pipelines::{
    __path_delete_pipeline, __path_get_pipeline, __path_get_pipeline_jobs, __path_patch_pipeline,
    __path_validate_pipeline,
};
use crate::rest::__path_ping;
use crate::rest_types::{
    Checkpoint, CheckpointCollection, Job, JobCollection, JobLogLevel, JobLogMessage,
    JobLogMessageCollection, OutputData, Pipeline, PipelineCollection, PipelineEdge, PipelineGraph,
    PipelineNode, PipelinePatch, PipelinePost, StopType as StopTypeRest, Udf, UdfLanguage,
    ValidatePipelinePost,
};
use crate::rest_utils::ErrorResp;
use arroyo_connectors::connectors;
use arroyo_rpc::grpc::api::{
    api_grpc_server::ApiGrpc, CheckpointDetailsReq, CheckpointDetailsResp, ConfluentSchemaReq,
    ConfluentSchemaResp, CreateConnectionReq, CreateConnectionResp, CreateJobReq, CreateJobResp,
    CreatePipelineReq, CreatePipelineResp, GetConnectionsReq, GetConnectionsResp, GetJobsReq,
    GetJobsResp, GetPipelineReq, GrpcOutputSubscription, JobCheckpointsReq, JobCheckpointsResp,
    JobDetailsReq, JobDetailsResp, JobMetricsReq, JobMetricsResp, OperatorErrorsReq,
    OperatorErrorsRes, OutputData as OutputDataProto, PipelineDef, PipelineGraphReq,
    PipelineGraphResp, StopType, TestSourceMessage, UpdateJobReq, UpdateJobResp,
};
use arroyo_rpc::grpc::api::{
    CreateConnectionTableReq, CreateConnectionTableResp, DeleteConnectionReq, DeleteConnectionResp,
    DeleteConnectionTableReq, DeleteConnectionTableResp, DeleteJobReq, DeleteJobResp,
    GetConnectionTablesReq, GetConnectionTablesResp, GetConnectorsReq, GetConnectorsResp,
    PipelineProgram, TestSchemaReq, TestSchemaResp,
};
use arroyo_server_common::log_event;
use cornucopia_async::GenericClient;
use deadpool_postgres::{Object, Pool};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
use time::OffsetDateTime;
use tokio_postgres::error::SqlState;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{error, warn};
use utoipa::OpenApi;

mod cloud;
mod connection_tables;
mod connections;
mod jobs;
mod metrics;
mod optimizations;
mod pipelines;
pub mod rest;
mod rest_types;
mod rest_utils;

include!(concat!(env!("OUT_DIR"), "/api-sql.rs"));
const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(10);

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

fn handle_db_error(name: &str, err: tokio_postgres::Error) -> Status {
    if let Some(db) = &err.as_db_error() {
        if *db.code() == SqlState::UNIQUE_VIOLATION {
            // TODO improve error message
            warn!("SQL error: {}", db.message());
            return Status::invalid_argument(format!("A {} with that name already exists", name));
        }
    }

    log_and_map(err)
}

fn handle_delete(name: &str, users: &str, err: tokio_postgres::Error) -> Status {
    if let Some(db) = &err.as_db_error() {
        if *db.code() == SqlState::FOREIGN_KEY_VIOLATION {
            return Status::invalid_argument(format!(
                "Cannot delete {}; it is still being used by {}",
                name, users
            ));
        }
    }

    log_and_map(err)
}

fn log_and_map<E>(err: E) -> Status
where
    E: core::fmt::Debug,
{
    error!("Error while handling: {:?}", err);
    log_event("api_error", json!({ "error": format!("{:?}", err) }));
    Status::internal("Something went wrong")
}

pub(crate) fn to_micros(dt: OffsetDateTime) -> u64 {
    (dt.unix_timestamp_nanos() / 1_000) as u64
}

#[derive(Clone)]
pub struct ApiServer {
    pub pool: Pool,
    pub controller_addr: String,
}

// TODO: look in to using gRPC rich errors
pub fn required_field(field: &str) -> Status {
    Status::invalid_argument(format!("Field {} must be set", field))
}

impl ApiServer {
    async fn authenticate<T>(&self, request: Request<T>) -> Result<(Request<T>, AuthData), Status> {
        cloud::authenticate(self.client().await?, request).await
    }

    async fn client(&self) -> Result<Object, Status> {
        self.pool.get().await.map_err(log_and_map)
    }

    async fn start_or_preview(
        &self,
        req: CreatePipelineReq,
        pub_id: String,
        preview: bool,
        auth: AuthData,
    ) -> Result<Response<CreateJobResp>, ErrorResp> {
        let mut client = self.client().await?;
        let transaction = client.transaction().await.map_err(log_and_map)?;
        transaction
            .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
            .await
            .map_err(log_and_map)?;

        let pipeline_id =
            pipelines::create_pipeline(&req, &pub_id, auth.clone(), &transaction).await?;
        let create_job = CreateJobReq {
            pipeline_id: format!("{}", pipeline_id),
            checkpoint_interval_micros: DEFAULT_CHECKPOINT_INTERVAL.as_micros() as u64,
            preview,
        };

        let job_id =
            jobs::create_job(create_job, &req.name, &pipeline_id, auth, &transaction).await?;

        transaction.commit().await.map_err(log_and_map)?;
        log_event(
            "job_created",
            json!({"service": "api", "is_preview": preview, "job_id": job_id}),
        );

        Ok(Response::new(CreateJobResp { job_id }))
    }
}

#[tonic::async_trait]
impl ApiGrpc for ApiServer {
    // connections
    async fn get_connectors(
        &self,
        request: Request<GetConnectorsReq>,
    ) -> Result<Response<GetConnectorsResp>, Status> {
        let (_request, _auth) = self.authenticate(request).await?;

        let mut connectors: Vec<_> = connectors()
            .values()
            .map(|c| c.metadata())
            .filter(|metadata| !metadata.hidden)
            .collect();

        connectors.sort_by_cached_key(|c| c.name.clone());

        Ok(Response::new(GetConnectorsResp { connectors }))
    }

    async fn create_connection(
        &self,
        request: Request<CreateConnectionReq>,
    ) -> Result<Response<CreateConnectionResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        let resp =
            connections::create_connection(request.into_inner(), auth, &self.client().await?)
                .await?;

        Ok(Response::new(resp))
    }

    async fn get_connections(
        &self,
        request: Request<GetConnectionsReq>,
    ) -> Result<Response<GetConnectionsResp>, Status> {
        let (_, auth) = self.authenticate(request).await?;

        Ok(Response::new(GetConnectionsResp {
            connections: connections::get_connections(&auth, &self.client().await?).await?,
        }))
    }

    async fn delete_connection(
        &self,
        request: Request<DeleteConnectionReq>,
    ) -> Result<Response<DeleteConnectionResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        connections::delete_connection(request.into_inner(), auth, &self.client().await?).await?;

        Ok(Response::new(DeleteConnectionResp {}))
    }

    async fn create_connection_table(
        &self,
        request: Request<CreateConnectionTableReq>,
    ) -> Result<Response<CreateConnectionTableResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        connection_tables::create(request.into_inner(), auth, &self.pool).await?;

        Ok(Response::new(CreateConnectionTableResp {}))
    }

    // connection tables
    type TestConnectionTableStream = ReceiverStream<Result<TestSourceMessage, Status>>;

    async fn test_connection_table(
        &self,
        request: Request<CreateConnectionTableReq>,
    ) -> Result<Response<Self::TestConnectionTableStream>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        let rx = connection_tables::test(request.into_inner(), auth, &self.client().await?).await?;
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_connection_tables(
        &self,
        request: Request<GetConnectionTablesReq>,
    ) -> Result<Response<GetConnectionTablesResp>, Status> {
        let (_, auth) = self.authenticate(request).await?;

        let tables = connection_tables::get(&auth, &self.client().await?).await?;
        Ok(Response::new(GetConnectionTablesResp { tables }))
    }

    async fn delete_connection_table(
        &self,
        request: Request<DeleteConnectionTableReq>,
    ) -> Result<Response<DeleteConnectionTableResp>, Status> {
        let (req, auth) = self.authenticate(request).await?;

        connection_tables::delete(req.into_inner(), auth, &self.client().await?).await?;
        Ok(Response::new(DeleteConnectionTableResp {}))
    }

    async fn test_schema(
        &self,
        request: Request<TestSchemaReq>,
    ) -> Result<Response<TestSchemaResp>, Status> {
        let (request, _auth) = self.authenticate(request).await?;

        let errors = connection_tables::test_schema(request.into_inner()).await?;
        Ok(Response::new(TestSchemaResp {
            valid: errors.is_empty(),
            errors,
        }))
    }

    async fn get_confluent_schema(
        &self,
        request: Request<ConfluentSchemaReq>,
    ) -> Result<Response<ConfluentSchemaResp>, Status> {
        let (request, _) = self.authenticate(request).await?;

        Ok(Response::new(
            connection_tables::get_confluent_schema(request.into_inner()).await?,
        ))
    }

    // pipelines
    async fn create_pipeline(
        &self,
        _request: Request<CreatePipelineReq>,
    ) -> Result<Response<CreatePipelineResp>, Status> {
        Err(Status::unimplemented(
            "This functionality has been moved to the REST API.",
        ))
    }

    async fn graph_for_pipeline(
        &self,
        _request: Request<PipelineGraphReq>,
    ) -> Result<Response<PipelineGraphResp>, Status> {
        Err(Status::unimplemented(
            "This functionality has been moved to the REST API.",
        ))
    }

    async fn get_pipeline(
        &self,
        _request: Request<GetPipelineReq>,
    ) -> Result<Response<PipelineDef>, Status> {
        Err(Status::unimplemented(
            "This functionality has been moved to the REST API.",
        ))
    }

    async fn create_job(
        &self,
        _request: Request<CreateJobReq>,
    ) -> Result<Response<CreateJobResp>, Status> {
        Err(Status::unimplemented(
            "This functionality has been moved to the REST API.",
        ))
    }

    async fn delete_job(
        &self,
        _request: Request<DeleteJobReq>,
    ) -> Result<Response<DeleteJobResp>, Status> {
        Err(Status::unimplemented(
            "This functionality has been moved to the REST API.",
        ))
    }

    async fn start_pipeline(
        &self,
        _request: Request<CreatePipelineReq>,
    ) -> Result<Response<CreateJobResp>, Status> {
        Err(Status::unimplemented(
            "This functionality has been moved to the REST API.",
        ))
    }

    async fn preview_pipeline(
        &self,
        _request: Request<CreatePipelineReq>,
    ) -> Result<Response<CreateJobResp>, Status> {
        Err(Status::unimplemented(
            "This functionality has been moved to the REST API.",
        ))
    }

    async fn get_jobs(
        &self,
        _request: Request<GetJobsReq>,
    ) -> Result<Response<GetJobsResp>, Status> {
        Err(Status::unimplemented(
            "This functionality has been moved to the REST API.",
        ))
    }

    async fn get_job_details(
        &self,
        _request: Request<JobDetailsReq>,
    ) -> Result<Response<JobDetailsResp>, Status> {
        Err(Status::unimplemented(
            "This functionality has been moved to the REST API.",
        ))
    }

    async fn get_checkpoints(
        &self,
        _request: Request<JobCheckpointsReq>,
    ) -> Result<Response<JobCheckpointsResp>, Status> {
        Err(Status::unimplemented(
            "This functionality has been moved to the REST API.",
        ))
    }

    async fn get_checkpoint_detail(
        &self,
        request: Request<CheckpointDetailsReq>,
    ) -> Result<Response<CheckpointDetailsResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;
        let req = request.into_inner();

        jobs::checkpoint_details(&req.job_id, req.epoch, auth, &self.client().await?)
            .await
            .map(Response::new)
    }

    async fn get_operator_errors(
        &self,
        _request: Request<OperatorErrorsReq>,
    ) -> Result<Response<OperatorErrorsRes>, Status> {
        Err(Status::unimplemented(
            "This functionality has been moved to the REST API.",
        ))
    }

    async fn get_job_metrics(
        &self,
        request: Request<JobMetricsReq>,
    ) -> Result<Response<JobMetricsResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        Ok(Response::new(
            metrics::get_metrics(request.into_inner().job_id, auth, &self.client().await?).await?,
        ))
    }

    async fn update_job(
        &self,
        request: Request<UpdateJobReq>,
    ) -> Result<Response<UpdateJobResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;
        let req = request.into_inner();

        let interval = req.checkpoint_interval_micros.map(Duration::from_micros);

        let stop = req.stop.map(|_| match req.stop() {
            StopType::None => types::public::StopMode::none,
            StopType::Graceful => types::public::StopMode::graceful,
            StopType::Immediate => types::public::StopMode::immediate,
            StopType::Checkpoint => types::public::StopMode::checkpoint,
            StopType::Force => types::public::StopMode::force,
        });

        if let Some(interval) = interval {
            if interval < Duration::from_secs(1) || interval > Duration::from_secs(24 * 60 * 60) {
                return Err(Status::invalid_argument(
                    "checkpoint_interval_micros must be between 1 second and 1 day",
                ));
            }
        }

        let parallelism_overrides = if let Some(parallelism) = req.parallelism {
            let res = queries::api_queries::get_job_details()
                .bind(&self.client().await?, &auth.organization_id, &req.job_id)
                .opt()
                .await
                .map_err(log_and_map)?
                .ok_or_else(|| Status::not_found(format!("No job with id '{}'", req.job_id)))?;

            let program = PipelineProgram::decode(&res.program[..]).map_err(log_and_map)?;
            let map: HashMap<String, u32> = program
                .nodes
                .into_iter()
                .map(|node| (node.node_id, parallelism))
                .collect();

            Some(serde_json::to_value(map).map_err(log_and_map)?)
        } else {
            None
        };

        let res = queries::api_queries::update_job()
            .bind(
                &self.client().await?,
                &OffsetDateTime::now_utc(),
                &auth.user_id,
                &stop,
                &interval.map(|i| i.as_micros() as i64),
                &parallelism_overrides,
                &req.job_id,
                &auth.organization_id,
            )
            .await
            .map_err(log_and_map)?;

        if res == 0 {
            Err(Status::not_found(format!("No job with id {}", req.job_id)))
        } else {
            Ok(Response::new(UpdateJobResp {}))
        }
    }

    type SubscribeToOutputStream = ReceiverStream<Result<OutputDataProto, Status>>;

    async fn subscribe_to_output(
        &self,
        _request: Request<GrpcOutputSubscription>,
    ) -> Result<Response<Self::SubscribeToOutputStream>, Status> {
        Err(Status::unimplemented(
            "This functionality has been moved to the REST API.",
        ))
    }
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
        get_job_output
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
        OutputData
    )),
    tags(
        (name = "pipelines", description = "Pipeline management endpoints"),
        (name = "jobs", description = "Job management endpoints"),
        (name = "ping", description = "Ping endpoint"),
    )
)]
pub struct ApiDoc;
