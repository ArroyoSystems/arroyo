use ::time::OffsetDateTime;
use arroyo_rpc::grpc::api::{
    DeleteConnectionReq, DeleteConnectionResp, DeleteJobReq, DeleteJobResp, DeleteSinkReq,
    DeleteSinkResp, DeleteSourceReq, DeleteSourceResp, GetSinksReq, GetSinksResp, PipelineProgram,
    SourceMetadataResp,
};
use arroyo_rpc::grpc::{
    self,
    api::{
        api_grpc_server::{ApiGrpc, ApiGrpcServer},
        CheckpointDetailsReq, CheckpointDetailsResp, ConfluentSchemaReq, ConfluentSchemaResp,
        CreateConnectionReq, CreateConnectionResp, CreateJobReq, CreateJobResp, CreatePipelineReq,
        CreatePipelineResp, CreateSinkReq, CreateSinkResp, CreateSourceReq, CreateSourceResp,
        GetConnectionsReq, GetConnectionsResp, GetJobsReq, GetJobsResp, GetPipelineReq,
        GetSourcesReq, GetSourcesResp, GrpcOutputSubscription, JobCheckpointsReq,
        JobCheckpointsResp, JobDetailsReq, JobDetailsResp, JobMetricsReq, JobMetricsResp,
        OutputData, PipelineDef, PipelineGraphReq, PipelineGraphResp, StopType, TestSchemaResp,
        TestSourceMessage, UpdateJobReq, UpdateJobResp,
    },
    controller_grpc_client::ControllerGrpcClient,
};
use arroyo_server_common::start_admin_server;
use arroyo_types::{grpc_port, ports, DatabaseConfig, ASSET_DIR_ENV, CONTROLLER_ADDR_ENV};
use axum::{response::IntoResponse, routing::get_service, Json, Router};
use cornucopia_async::GenericClient;
use deadpool_postgres::{ManagerConfig, Object, Pool, RecyclingMethod};
use http::StatusCode;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::{io, time::Duration};
use tokio::{select, sync::broadcast};
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Response, Status};
use tower_http::{
    cors::{self, CorsLayer},
    services::{ServeDir, ServeFile},
};
use tracing::{error, info, warn};

use crate::jobs::get_job_details;
use queries::api_queries;

mod cloud;
mod connections;
mod jobs;
mod json_schema;
mod metrics;
mod optimizations;
mod pipelines;
mod sinks;
mod sources;
mod testers;

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
struct OrgMetadata {
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
struct AuthData {
    pub user_id: String,
    pub organization_id: String,
    pub role: String,
    pub org_metadata: OrgMetadata,
}

#[tokio::main]
pub async fn main() {
    let config = DatabaseConfig::load();
    let mut cfg = deadpool_postgres::Config::new();
    cfg.dbname = Some(config.name);
    cfg.host = Some(config.host);
    cfg.port = Some(config.port);
    cfg.user = Some(config.user);
    cfg.password = Some(config.password);
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg
        .create_pool(Some(deadpool_postgres::Runtime::Tokio1), NoTls)
        .expect("Unable t connect to database");

    server(pool).await;
}

fn to_micros(dt: OffsetDateTime) -> u64 {
    (dt.unix_timestamp_nanos() / 1_000) as u64
}

async fn handle_error(_err: io::Error) -> impl IntoResponse {
    (StatusCode::INTERNAL_SERVER_ERROR, "Something went wrong...")
}

async fn server(pool: Pool) {
    let _guard = arroyo_server_common::init_logging("api");

    let asset_dir = env::var(ASSET_DIR_ENV).unwrap_or_else(|_| "arroyo-console/dist".to_string());

    let serve_dir = ServeDir::new(&asset_dir)
        .not_found_service(ServeFile::new(format!("{}/index.html", asset_dir)));
    let serve_dir = get_service(serve_dir).handle_error(handle_error);

    // TODO: enable in development only!!!
    let cors = CorsLayer::new()
        .allow_headers(cors::Any)
        .allow_origin(cors::Any);

    let app = Router::new()
        .nest_service("/", serve_dir.clone())
        .fallback_service(serve_dir)
        .layer(cors);

    let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);

    start_admin_server(
        "arroyo-api".to_string(),
        ports::API_ADMIN,
        shutdown_rx.resubscribe(),
    );

    let addr = format!("0.0.0.0:{}", ports::API_HTTP).parse().unwrap();
    info!("Starting http server on {:?}", addr);

    tokio::spawn(async move {
        select! {
            result = axum::Server::bind(&addr)
            .serve(app.into_make_service()) => {
                result.unwrap();
            }
            _ = shutdown_rx.recv() => {

            }
        }
    });

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(arroyo_rpc::grpc::API_FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    let controller_addr = std::env::var(CONTROLLER_ADDR_ENV)
        .unwrap_or_else(|_| format!("http://localhost:{}", ports::CONTROLLER_GRPC));

    let server = ApiServer {
        pool,
        controller_addr,
    };

    let addr = format!("0.0.0.0:{}", grpc_port(ports::API_GRPC))
        .parse()
        .unwrap();
    info!("Starting gRPC server on {:?}", addr);

    arroyo_server_common::grpc_server()
        .accept_http1(true)
        .add_service(
            tonic_web::config()
                .allow_all_origins()
                .enable(ApiGrpcServer::new(server)),
        )
        .add_service(reflection)
        .serve(addr)
        .await
        .unwrap();

    shutdown_tx.send(0).unwrap();
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

struct ApiServer {
    pool: Pool,
    controller_addr: String,
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
        preview: bool,
        auth: AuthData,
    ) -> Result<Response<CreateJobResp>, Status> {
        let mut client = self.client().await?;
        let transaction = client.transaction().await.map_err(log_and_map)?;
        transaction
            .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
            .await
            .map_err(log_and_map)?;

        let pipeline_id = pipelines::create_pipeline(req, auth.clone(), &transaction).await?;
        let create_job = CreateJobReq {
            pipeline_id: format!("{}", pipeline_id),
            checkpoint_interval_micros: DEFAULT_CHECKPOINT_INTERVAL.as_micros() as u64,
            preview,
        };

        let job_id = jobs::create_job(create_job, auth, &transaction).await?;

        transaction.commit().await.map_err(log_and_map)?;

        Ok(Response::new(CreateJobResp { job_id }))
    }
}

fn log_and_map<E>(err: E) -> Status
where
    E: core::fmt::Debug,
{
    error!("Error while handling: {:?}", err);
    Status::internal("Something went wrong")
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

#[tonic::async_trait]
impl ApiGrpc for ApiServer {
    // connections
    async fn create_connection(
        &self,
        request: Request<CreateConnectionReq>,
    ) -> Result<Response<CreateConnectionResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        connections::create_connection(request.into_inner(), auth, &self.client().await?).await?;

        Ok(Response::new(CreateConnectionResp {}))
    }

    async fn test_connection(
        &self,
        request: Request<CreateConnectionReq>,
    ) -> Result<Response<TestSourceMessage>, Status> {
        let (request, _) = self.authenticate(request).await?;

        Ok(Response::new(
            connections::test_connection(request.into_inner()).await?,
        ))
    }

    async fn get_connections(
        &self,
        request: Request<GetConnectionsReq>,
    ) -> Result<Response<GetConnectionsResp>, Status> {
        let (_, auth) = self.authenticate(request).await?;

        Ok(Response::new(GetConnectionsResp {
            connections: connections::get_connections(auth, &self.client().await?).await?,
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

    // sources
    async fn create_source(
        &self,
        request: Request<CreateSourceReq>,
    ) -> Result<Response<CreateSourceResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        sources::create_source(request.into_inner(), auth, &self.pool).await?;

        Ok(Response::new(CreateSourceResp {}))
    }

    async fn get_sources(
        &self,
        request: Request<GetSourcesReq>,
    ) -> Result<Response<GetSourcesResp>, Status> {
        let (_, auth) = self.authenticate(request).await?;

        Ok(Response::new(GetSourcesResp {
            sources: sources::get_sources(&auth, &self.client().await?).await?,
        }))
    }

    async fn delete_source(
        &self,
        request: Request<DeleteSourceReq>,
    ) -> Result<Response<DeleteSourceResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        sources::delete_source(request.into_inner(), auth, &self.client().await?).await?;

        Ok(Response::new(DeleteSourceResp {}))
    }

    async fn get_confluent_schema(
        &self,
        request: Request<ConfluentSchemaReq>,
    ) -> Result<Response<ConfluentSchemaResp>, Status> {
        let (request, _) = self.authenticate(request).await?;

        Ok(Response::new(
            sources::get_confluent_schema(request.into_inner()).await?,
        ))
    }

    async fn test_schema(
        &self,
        request: Request<CreateSourceReq>,
    ) -> Result<Response<TestSchemaResp>, Status> {
        let (request, _auth) = self.authenticate(request).await?;

        let errors = sources::test_schema(request.into_inner()).await?;
        Ok(Response::new(TestSchemaResp {
            valid: errors.is_empty(),
            errors,
        }))
    }

    async fn get_source_metadata(
        &self,
        request: Request<CreateSourceReq>,
    ) -> Result<Response<SourceMetadataResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        Ok(Response::new(
            sources::get_source_metadata(request.into_inner(), auth, &self.client().await?).await?,
        ))
    }

    type TestSourceStream = ReceiverStream<Result<TestSourceMessage, Status>>;

    async fn test_source(
        &self,
        request: Request<CreateSourceReq>,
    ) -> Result<Response<Self::TestSourceStream>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        let rx = sources::test_source(request.into_inner(), auth, &self.client().await?).await?;
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // sinks
    async fn create_sink(
        &self,
        request: Request<CreateSinkReq>,
    ) -> Result<Response<CreateSinkResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        sinks::create_sink(request.into_inner(), auth, &self.pool).await?;

        Ok(Response::new(CreateSinkResp {}))
    }

    async fn get_sinks(
        &self,
        request: Request<GetSinksReq>,
    ) -> Result<Response<GetSinksResp>, Status> {
        let (_, auth) = self.authenticate(request).await?;

        Ok(Response::new(GetSinksResp {
            sinks: sinks::get_sinks(&auth, &self.client().await?).await?,
        }))
    }

    async fn delete_sink(
        &self,
        request: Request<DeleteSinkReq>,
    ) -> Result<Response<DeleteSinkResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        sinks::delete_sink(request.into_inner(), auth, &self.client().await?).await?;

        Ok(Response::new(DeleteSinkResp {}))
    }

    // pipelines
    async fn create_pipeline(
        &self,
        request: Request<CreatePipelineReq>,
    ) -> Result<Response<CreatePipelineResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        let mut client = self.client().await?;
        let transaction = client.transaction().await.map_err(log_and_map)?;

        let id = pipelines::create_pipeline(request.into_inner(), auth, &transaction).await?;

        transaction.commit().await.map_err(log_and_map)?;

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

        let def = pipelines::get_pipeline(
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

        Ok(Response::new(CreateJobResp { job_id: id }))
    }

    async fn start_pipeline(
        &self,
        request: Request<CreatePipelineReq>,
    ) -> Result<Response<CreateJobResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;
        self.start_or_preview(request.into_inner(), false, auth)
            .await
    }

    async fn preview_pipeline(
        &self,
        request: Request<CreatePipelineReq>,
    ) -> Result<Response<CreateJobResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;
        self.start_or_preview(request.into_inner(), true, auth)
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
            let res = api_queries::get_job_details()
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

        let res = api_queries::update_job()
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

    async fn delete_job(
        &self,
        request: Request<DeleteJobReq>,
    ) -> Result<Response<DeleteJobResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        jobs::delete_job(&request.into_inner().job_id, auth, &self.pool).await?;
        Ok(Response::new(DeleteJobResp {}))
    }

    type SubscribeToOutputStream = ReceiverStream<Result<OutputData, Status>>;

    async fn subscribe_to_output(
        &self,
        request: Request<GrpcOutputSubscription>,
    ) -> Result<Response<Self::SubscribeToOutputStream>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        let job_id = request.into_inner().job_id;
        // validate that the job exists, the user has access, and the graph has a GrpcSink
        let details = get_job_details(&job_id, &auth, &self.client().await?).await?;
        if !details
            .job_graph
            .unwrap()
            .nodes
            .iter()
            .any(|n| n.operator.contains("GrpcSink"))
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
