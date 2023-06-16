use std::collections::HashMap;
use std::time::Duration;

use deadpool_postgres::{Object, Pool};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::OffsetDateTime;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use arroyo_rpc::grpc::api::{
    DeleteConnectionReq, DeleteConnectionResp, DeleteJobReq, DeleteJobResp, DeleteSinkReq,
    DeleteSinkResp, DeleteSourceReq, DeleteSourceResp, GetSinksReq, GetSinksResp, PipelineProgram,
    SourceMetadataResp,
};
use arroyo_rpc::grpc::{
    self,
    api::{
        api_grpc_server::ApiGrpc, CheckpointDetailsReq, CheckpointDetailsResp, ConfluentSchemaReq,
        ConfluentSchemaResp, CreateConnectionReq, CreateConnectionResp, CreateJobReq,
        CreateJobResp, CreatePipelineReq, CreatePipelineResp, CreateSinkReq, CreateSinkResp,
        CreateSourceReq, CreateSourceResp, GetConnectionsReq, GetConnectionsResp, GetJobsReq,
        GetJobsResp, GetPipelineReq, GetSourcesReq, GetSourcesResp, GrpcOutputSubscription,
        JobCheckpointsReq, JobCheckpointsResp, JobDetailsReq, JobDetailsResp, JobMetricsReq,
        JobMetricsResp, OperatorErrorsReq, OperatorErrorsRes, OutputData, PipelineDef,
        PipelineGraphReq, PipelineGraphResp, StopType, TestSchemaResp, TestSourceMessage,
        UpdateJobReq, UpdateJobResp,
    },
    controller_grpc_client::ControllerGrpcClient,
};
use arroyo_server_common::log_event;

use crate::jobs::get_job_details;
use crate::queries::api_queries;
use crate::{cloud, connections, job_log, jobs, metrics, pipelines, sinks, sources, types};

fn log_and_map<E>(err: E) -> Status
where
    E: core::fmt::Debug,
{
    error!("Error while handling: {:?}", err);
    log_event("api_error", json!({ "error": format!("{:?}", err) }));
    Status::internal("Something went wrong")
}

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

#[derive(Clone)]
pub(crate) struct ApiServer {
    pub(crate) pool: Pool,
    pub(crate) controller_addr: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub(crate) struct OrgMetadata {
    #[serde(default)]
    pub(crate) can_create_programs: bool,

    #[serde(default = "default_max_nexmark_qps")]
    pub(crate) max_nexmark_qps: f64,

    #[serde(default = "default_max_impulse_qps")]
    pub(crate) max_impulse_qps: f64,

    #[serde(default = "default_max_parallelism")]
    pub(crate) max_parallelism: u32,

    #[serde(default = "default_max_operators")]
    pub(crate) max_operators: u32,

    #[serde(default = "default_max_running_jobs")]
    pub(crate) max_running_jobs: u32,

    #[serde(default = "default_kafka_qps")]
    pub(crate) kafka_qps: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct AuthData {
    pub user_id: String,
    pub organization_id: String,
    pub role: String,
    pub org_metadata: OrgMetadata,
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

#[tonic::async_trait]
impl ApiGrpc for ApiServer {
    // connections
    async fn create_connection(
        &self,
        _: Request<CreateConnectionReq>,
    ) -> Result<Response<CreateConnectionResp>, Status> {
        Err(Status::unimplemented(
            "This feature has been moved ot the REST API",
        ))
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

        log_event("job_created", json!({"service": "api"}));

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

        log_event("pipeline_preview", json!({"service": "api"}));
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
