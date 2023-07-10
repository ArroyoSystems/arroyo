use crate::pipelines::__path_get_pipelines;
use crate::pipelines::__path_post_pipeline;
use crate::pipelines::{
    __path_delete_pipeline, __path_get_jobs, __path_get_pipeline, __path_patch_pipeline,
};
use crate::rest::__path_ping;
use crate::rest_types::{
    Job, JobCollection, Pipeline, PipelineCollection, PipelinePatch, PipelinePost,
    StopType as StopTypeRest, Udf, UdfLanguage,
};
use arroyo_connectors::connectors;
use arroyo_rpc::grpc::api::{
    CreateConnectionTableReq, CreateConnectionTableResp, DeleteConnectionReq, DeleteConnectionResp,
    DeleteConnectionTableReq, DeleteConnectionTableResp, DeleteJobReq, DeleteJobResp,
    GetConnectionTablesReq, GetConnectionTablesResp, GetConnectorsReq, GetConnectorsResp,
    PipelineProgram, TestSchemaReq, TestSchemaResp,
};
use arroyo_rpc::grpc::{
    self,
    api::{
        api_grpc_server::ApiGrpc, CheckpointDetailsReq, CheckpointDetailsResp, ConfluentSchemaReq,
        ConfluentSchemaResp, CreateConnectionReq, CreateConnectionResp, CreateJobReq,
        CreateJobResp, CreatePipelineReq, CreatePipelineResp, GetConnectionsReq,
        GetConnectionsResp, GetJobsReq, GetJobsResp, GetPipelineReq, GrpcOutputSubscription,
        JobCheckpointsReq, JobCheckpointsResp, JobDetailsReq, JobDetailsResp, JobMetricsReq,
        JobMetricsResp, OperatorErrorsReq, OperatorErrorsRes, OutputData, PipelineDef,
        PipelineGraphReq, PipelineGraphResp, StopType, TestSourceMessage, UpdateJobReq,
        UpdateJobResp,
    },
    controller_grpc_client::ControllerGrpcClient,
};
use arroyo_rpc::public_ids::{generate_id, IdTypes};
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
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};
use utoipa::OpenApi;

mod cloud;
mod connection_tables;
mod connections;
mod job_log;
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
    ) -> Result<Response<CreateJobResp>, Status> {
        let mut client = self.client().await?;
        let transaction = client.transaction().await.map_err(log_and_map)?;
        transaction
            .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
            .await
            .map_err(log_and_map)?;

        let pipeline_id =
            pipelines::create_pipeline(req, &pub_id, auth.clone(), &transaction).await?;
        let create_job = CreateJobReq {
            pipeline_id: format!("{}", pipeline_id),
            checkpoint_interval_micros: DEFAULT_CHECKPOINT_INTERVAL.as_micros() as u64,
            preview,
        };

        let job_id = jobs::create_job(create_job, auth, &transaction).await?;

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

        let mut connectors: Vec<_> = connectors().values().map(|c| c.metadata()).collect();

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
        request: Request<CreatePipelineReq>,
    ) -> Result<Response<CreatePipelineResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        let mut client = self.client().await?;
        let transaction = client.transaction().await.map_err(log_and_map)?;

        let id = pipelines::create_pipeline(
            request.into_inner(),
            &generate_id(IdTypes::Pipeline),
            auth,
            &transaction,
        )
        .await?;

        transaction.commit().await.map_err(log_and_map)?;

        log_event("pipeline_created", json!({"service": "api"}));

        Ok(Response::new(CreatePipelineResp {
            pipeline_id: format!("{}", id),
        }))
    }

    async fn graph_for_pipeline(
        &self,
        request: Request<PipelineGraphReq>,
    ) -> Result<Response<PipelineGraphResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        Ok(Response::new(
            pipelines::sql_graph(request.into_inner(), auth, &self.client().await?).await?,
        ))
    }

    async fn get_pipeline(
        &self,
        request: Request<GetPipelineReq>,
    ) -> Result<Response<PipelineDef>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        let def = pipelines::query_pipeline(
            &request.into_inner().pipeline_id,
            &auth,
            &self.client().await?,
        )
        .await?;
        Ok(Response::new(def))
    }

    async fn create_job(
        &self,
        request: Request<CreateJobReq>,
    ) -> Result<Response<CreateJobResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        let mut client = self.client().await?;
        let transaction = client.transaction().await.map_err(log_and_map)?;
        transaction
            .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
            .await
            .map_err(log_and_map)?;

        let id = jobs::create_job(request.into_inner(), auth, &transaction).await?;

        transaction.commit().await.map_err(log_and_map)?;

        log_event("job_created", json!({"service": "api"}));

        Ok(Response::new(CreateJobResp { job_id: id }))
    }

    async fn delete_job(
        &self,
        request: Request<DeleteJobReq>,
    ) -> Result<Response<DeleteJobResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        jobs::delete_job(&request.into_inner().job_id, auth, &self.pool).await?;
        Ok(Response::new(DeleteJobResp {}))
    }

    async fn start_pipeline(
        &self,
        request: Request<CreatePipelineReq>,
    ) -> Result<Response<CreateJobResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;
        self.start_or_preview(
            request.into_inner(),
            generate_id(IdTypes::Pipeline),
            false,
            auth,
        )
        .await
    }

    async fn preview_pipeline(
        &self,
        request: Request<CreatePipelineReq>,
    ) -> Result<Response<CreateJobResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        log_event("pipeline_preview", json!({"service": "api"}));
        self.start_or_preview(
            request.into_inner(),
            generate_id(IdTypes::Pipeline),
            true,
            auth,
        )
        .await
    }

    async fn get_jobs(
        &self,
        request: Request<GetJobsReq>,
    ) -> Result<Response<GetJobsResp>, Status> {
        let (_, auth) = self.authenticate(request).await?;

        let jobs = jobs::get_jobs(&auth, &self.client().await?).await?;

        Ok(Response::new(GetJobsResp { jobs }))
    }

    async fn get_job_details(
        &self,
        request: Request<JobDetailsReq>,
    ) -> Result<Response<JobDetailsResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;
        let req = request.into_inner();

        jobs::get_job_details(&req.job_id, &auth, &self.client().await?)
            .await
            .map(Response::new)
    }

    async fn get_checkpoints(
        &self,
        request: Request<JobCheckpointsReq>,
    ) -> Result<Response<JobCheckpointsResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;
        let req = request.into_inner();

        jobs::get_job_checkpoints(&req.job_id, auth, &self.client().await?)
            .await
            .map(|checkpoints| Response::new(JobCheckpointsResp { checkpoints }))
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
        request: Request<OperatorErrorsReq>,
    ) -> Result<Response<OperatorErrorsRes>, Status> {
        let (request, auth) = self.authenticate(request).await?;
        let job_id = request.into_inner().job_id;
        let client = self.client().await?;

        // validate that the job exists and user can access it
        let _ = jobs::get_job_details(&job_id, &auth, &client).await?;

        let messages = job_log::get_operator_errors(&job_id, &client).await?;

        Ok(Response::new(OperatorErrorsRes { messages }))
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

    type SubscribeToOutputStream = ReceiverStream<Result<OutputData, Status>>;

    async fn subscribe_to_output(
        &self,
        request: Request<GrpcOutputSubscription>,
    ) -> Result<Response<Self::SubscribeToOutputStream>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        let job_id = request.into_inner().job_id;
        // validate that the job exists, the user has access, and the graph has a GrpcSink
        let details = jobs::get_job_details(&job_id, &auth, &self.client().await?).await?;

        if !details
            .job_graph
            .unwrap()
            .nodes
            .iter()
            .any(|n| n.operator.contains("WebSink"))
        {
            // TODO: make this check more robust
            return Err(Status::invalid_argument(format!(
                "Job {} does not have a web sink",
                job_id
            )));
        }

        let (tx, rx) = tokio::sync::mpsc::channel(32);

        let mut controller = ControllerGrpcClient::connect(self.controller_addr.clone())
            .await
            .map_err(log_and_map)?;

        info!("connected to controller");

        let mut stream = controller
            .subscribe_to_output(Request::new(grpc::GrpcOutputSubscription {
                job_id: job_id.clone(),
            }))
            .await
            .map_err(log_and_map)?
            .into_inner();

        info!("subscribed to output");
        tokio::spawn(async move {
            let _controller = controller;
            while let Some(d) = stream.next().await {
                if d.as_ref().map(|t| t.done).unwrap_or(false) {
                    info!("Stream done for {}", job_id);
                    break;
                }

                let v = d.map(|d| OutputData {
                    operator_id: d.operator_id,
                    timestamp: d.timestamp,
                    key: d.key,
                    value: d.value,
                });

                if tx.send(v).await.is_err() {
                    break;
                }
            }

            info!("Closing watch stream for {}", job_id);
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[derive(OpenApi)]
#[openapi(
    info(title = "Arroyo REST API", version = "1.0.0"),
    servers((url = "/api/")),
    paths(ping, post_pipeline, patch_pipeline, get_pipeline, delete_pipeline, get_pipelines, get_jobs),
    components(schemas(PipelinePost, PipelinePatch, Pipeline, Job, StopTypeRest, Udf, UdfLanguage, PipelineCollection, JobCollection)),
    tags(
        (name = "pipelines", description = "Pipeline management endpoints"),
        (name = "ping", description = "Ping endpoint"),
    )
)]
pub struct ApiDoc;
