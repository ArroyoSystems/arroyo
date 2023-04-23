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
        OutputData, PipelineDef, PipelineGraphReq, PipelineGraphResp, RefreshSampleReq,
        RefreshSampleResp, StopType, TestSchemaResp, TestSourceMessage, UpdateJobReq,
        UpdateJobResp,
    },
    controller_grpc_client::ControllerGrpcClient,
};
use arroyo_server_common::start_admin_server;
use arroyo_types::{
    grpc_port, ports, service_port, DatabaseConfig, API_ENDPOINT_ENV, ASSET_DIR_ENV,
    CONTROLLER_ADDR_ENV, HTTP_PORT_ENV,
};
use axum::body::Body;
use axum::{response::IntoResponse, Json, Router};
use cornucopia_async::GenericClient;
use deadpool_postgres::{ManagerConfig, Object, Pool, RecyclingMethod};
use http::StatusCode;
use once_cell::sync::Lazy;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use tokio::{select, sync::broadcast};
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Response, Status};
use tower::service_fn;
use tower_http::{
    cors::{self, CorsLayer},
    services::ServeDir,
};
use tracing::{error, info, warn};

use crate::{jobs::get_job_details, pipelines::start_or_preview};
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
        .expect("Unable to connect to database");

    server(pool).await;
}

fn to_micros(dt: OffsetDateTime) -> u64 {
    (dt.unix_timestamp_nanos() / 1_000) as u64
}

async fn server(pool: Pool) {
    let _guard = arroyo_server_common::init_logging("api");

    let asset_dir = env::var(ASSET_DIR_ENV).unwrap_or_else(|_| "arroyo-console/dist".to_string());

    static INDEX_HTML: Lazy<String> = Lazy::new(|| {
        let asset_dir =
            env::var(ASSET_DIR_ENV).unwrap_or_else(|_| "arroyo-console/dist".to_string());

        let endpoint = env::var(API_ENDPOINT_ENV).unwrap_or_else(|_| String::new());

        std::fs::read_to_string(PathBuf::from_str(&asset_dir).unwrap()
            .join("index.html"))
            .expect("Could not find index.html in asset dir (you may need to build the console sources)")
            .replace("{{API_ENDPOINT}}", &endpoint)
    });

    let fallback = service_fn(|_: http::Request<_>| async move {
        let body = Body::from(INDEX_HTML.as_str());
        let res = http::Response::new(body);
        Ok::<_, _>(res)
    });

    let serve_dir = ServeDir::new(&asset_dir).not_found_service(fallback.clone());

    // TODO: enable in development only!!!
    let cors = CorsLayer::new()
        .allow_headers(cors::Any)
        .allow_origin(cors::Any);

    let app = Router::new()
        .route_service("/", fallback)
        .fallback_service(serve_dir)
        .layer(cors);

    let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);

    start_admin_server("api", ports::API_ADMIN, shutdown_rx.resubscribe());

    let http_port = service_port("api", ports::API_HTTP, HTTP_PORT_ENV);
    let addr = format!("0.0.0.0:{}", http_port).parse().unwrap();
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

    let addr = format!("0.0.0.0:{}", grpc_port("api", ports::API_GRPC))
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

        let mut client = self.client().await?;
        let transaction = client.transaction().await.map_err(log_and_map)?;
        transaction
            .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
            .await
            .map_err(log_and_map)?;

        let resp = start_or_preview(request.into_inner(), false, auth, &transaction).await;

        transaction.commit().await.map_err(log_and_map)?;

        resp
    }

    async fn preview_pipeline(
        &self,
        request: Request<CreatePipelineReq>,
    ) -> Result<Response<CreateJobResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        let mut client = self.client().await?;
        let transaction = client.transaction().await.map_err(log_and_map)?;
        transaction
            .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
            .await
            .map_err(log_and_map)?;

        let resp = start_or_preview(request.into_inner(), true, auth, &transaction).await;

        transaction.commit().await.map_err(log_and_map)?;

        resp
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

    async fn refresh_sample(
        &self,
        request: Request<RefreshSampleReq>,
    ) -> Result<Response<RefreshSampleResp>, Status> {
        let (request, auth) = self.authenticate(request).await?;
        // start the pipeline associated with the source
        let job_id = api_queries::get_source_pipeline() // TODO: when it's possible to have two jobs for one pipeline, how does this look?
            .bind(
                &self.client().await?,
                &(request.into_inner().source_id as i64),
            )
            .one()
            .await
            .map_err(log_and_map)?;

        self.update_job(Request::new(UpdateJobReq {
            job_id: job_id.clone(),
            stop: Some(StopType::None.into()),
            checkpoint_interval_micros: None,
            parallelism: None,
        }))
        .await?;

        // get the job stream
        let controller = ControllerGrpcClient::connect(self.controller_addr.clone())
            .await
            .map_err(log_and_map)?;

        let output_lines = jobs::get_lines_from_job_output(
            jobs::get_job_stream(job_id.clone(), controller, auth, &self.client().await?).await?,
            20,
        )
        .await;

        self.update_job(Request::new(UpdateJobReq {
            job_id: job_id.clone(),
            stop: Some(StopType::Immediate.into()),
            checkpoint_interval_micros: None,
            parallelism: None,
        }))
        .await?;

        Ok(Response::new(RefreshSampleResp {
            rando: output_lines
                .iter()
                .map(|record| record.clone().unwrap())
                .collect(),
        }))
    }

    type SubscribeToOutputStream = ReceiverStream<Result<OutputData, Status>>;

    async fn subscribe_to_output(
        &self,
        request: Request<GrpcOutputSubscription>,
    ) -> Result<Response<Self::SubscribeToOutputStream>, Status> {
        let (request, auth) = self.authenticate(request).await?;

        let controller = ControllerGrpcClient::connect(self.controller_addr.clone())
            .await
            .map_err(log_and_map)?;

        let stream = jobs::get_job_stream(
            request.into_inner().job_id,
            controller,
            auth,
            &self.client().await?,
        )
        .await?;

        Ok(Response::new(
            jobs::get_stream_from_job_output(stream).await,
        ))
    }
}
