#![allow(clippy::new_without_default)]
// TODO: factor out complex types
#![allow(clippy::type_complexity)]
// https://github.com/rust-lang/rust-clippy/issues/12908
#![allow(clippy::needless_lifetimes)]

use anyhow::Result;
use arroyo_rpc::config::config;
use arroyo_rpc::errors::ErrorDomain;
use arroyo_rpc::grpc::rpc::controller_grpc_server::{ControllerGrpc, ControllerGrpcServer};
use arroyo_rpc::grpc::rpc::{
    GrpcOutputSubscription, HeartbeatNodeReq, HeartbeatNodeResp, HeartbeatReq, HeartbeatResp,
    JobMetricsReq, JobMetricsResp, OutputData, RegisterNodeReq, RegisterNodeResp,
    RegisterWorkerReq, RegisterWorkerResp, TaskCheckpointCompletedReq, TaskCheckpointCompletedResp,
    TaskFailedReq, TaskFailedResp, TaskFinishedReq, TaskFinishedResp, TaskStartedReq,
    TaskStartedResp, WorkerFinishedReq, WorkerFinishedResp, WorkerInitializationCompleteReq,
    WorkerInitializationCompleteResp,
};
use arroyo_rpc::grpc::rpc::{
    NonfatalErrorReq, RetryHint, SinkDataReq, SinkDataResp, TaskCheckpointEventReq,
    TaskCheckpointEventResp, WorkerErrorRes,
};
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use arroyo_rpc::{config, errors};
use arroyo_server_common::shutdown::ShutdownGuard;
use arroyo_server_common::wrap_start;
use arroyo_types::{from_micros, MachineId, WorkerId};
use cornucopia_async::DatabaseSource;
use lazy_static::lazy_static;
use prometheus::{register_gauge, Gauge};
use states::{Created, State, StateMachine};
use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

//pub mod compiler;
pub mod job_controller;
pub mod schedulers;
mod states;

const TTL_PIPELINE_CLEANUP_TIME: Duration = Duration::from_secs(60 * 60);

include!(concat!(env!("OUT_DIR"), "/controller-sql.rs"));

use crate::job_controller::job_metrics::JobMetrics;
use crate::schedulers::{NodeScheduler, ProcessScheduler, Scheduler};
use types::public::LogLevel;
use types::public::{RestartMode, StopMode};

pub const CHECKPOINTS_TO_KEEP: u32 = 5;

lazy_static! {
    static ref ACTIVE_PIPELINES: Gauge = register_gauge!(
        "arroyo_controller_active_pipelines",
        "number of active pipelines in arroyo-controller"
    )
    .unwrap();
    pub static ref COMPACTION_TUPLES_IN: Gauge = register_gauge!(
        "arroyo_controller_compaction_tuples_in",
        "Number of tuples being considered for compaction"
    )
    .unwrap();
    static ref COMPACTION_TUPLES_OUT: Gauge = register_gauge!(
        "arroyo_controller_compaction_tuples_in",
        "Number of tuples being considered for compaction"
    )
    .unwrap();
}

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct JobConfig {
    id: Arc<String>,
    organization_id: String,
    pipeline_name: String,
    pipeline_id: i64,
    stop_mode: StopMode,
    checkpoint_interval: Duration,
    ttl: Option<Duration>,
    parallelism_overrides: HashMap<u32, usize>,
    restart_nonce: i32,
    restart_mode: RestartMode,
}

#[derive(Clone, Debug)]
pub struct JobStatus {
    id: Arc<String>,
    run_id: u64,
    state: String,
    start_time: Option<OffsetDateTime>,
    finish_time: Option<OffsetDateTime>,
    tasks: Option<i32>,
    failure_message: Option<String>,
    failure_domain: Option<String>,
    restarts: i32,
    pipeline_path: Option<String>,
    wasm_path: Option<String>,
    restart_nonce: i32,
}

impl JobStatus {
    pub async fn update_db(&self, database: &DatabaseSource) -> Result<(), String> {
        let c = database.client().await.map_err(|e| format!("{e:?}"))?;
        let res = queries::controller_queries::execute_update_job_status(
            &c,
            &self.state,
            &self.start_time,
            &self.finish_time,
            &self.tasks,
            &self.failure_message,
            &self.failure_domain,
            &self.restarts,
            &self.pipeline_path,
            &self.wasm_path,
            &(self.run_id as i64),
            &self.restart_nonce,
            &*self.id,
        )
        .await
        .map_err(|e| format!("{e:?}"))?;

        if res == 0 {
            Err("Job status does not exist".to_string())
        } else {
            Ok(())
        }
    }
}

fn job_in_final_state(config: &JobConfig, status: &JobStatus) -> bool {
    match status.state.as_str() {
        "Stopped" | "Finished" => config.stop_mode != StopMode::none,
        "Failed" => config.restart_nonce == status.restart_nonce,
        _ => false,
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TaskFailedEvent {
    worker_id: WorkerId,
    node_id: u32,
    operator_id: String,
    subtask_index: u32,
    reason: String,
    error_domain: errors::ErrorDomain,
    retry_hint: errors::RetryHint,
}

#[derive(Debug)]
pub enum RunningMessage {
    TaskCheckpointEvent(TaskCheckpointEventReq),
    TaskCheckpointFinished(TaskCheckpointCompletedReq),
    TaskFinished {
        worker_id: WorkerId,
        time: SystemTime,
        node_id: u32,
        subtask_index: u32,
    },
    TaskFailed(TaskFailedEvent),
    WorkerHeartbeat {
        worker_id: WorkerId,
        time: Instant,
    },
    WorkerFinished {
        worker_id: WorkerId,
    },
}

#[derive(Debug)]
pub enum JobMessage {
    ConfigUpdate(JobConfig),
    WorkerConnect {
        worker_id: WorkerId,
        machine_id: MachineId,
        run_id: u64,
        rpc_address: String,
        data_address: String,
        slots: usize,
    },
    WorkerInitializationComplete {
        worker_id: WorkerId,
        success: bool,
        error_message: Option<String>,
    },
    TaskStarted {
        worker_id: WorkerId,
        node_id: u32,
        operator_subtask: u64,
    },
    RunningMessage(RunningMessage),
}

#[derive(Clone)]
pub struct ControllerServer {
    job_state: Arc<tokio::sync::Mutex<HashMap<String, StateMachine>>>,
    data_txs: Arc<tokio::sync::Mutex<HashMap<String, Vec<Sender<Result<OutputData, Status>>>>>>,
    scheduler: Arc<dyn Scheduler>,
    metrics: Arc<RwLock<HashMap<Arc<String>, JobMetrics>>>,
    db: DatabaseSource,
}

#[tonic::async_trait]
impl ControllerGrpc for ControllerServer {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerReq>,
    ) -> Result<Response<RegisterWorkerResp>, Status> {
        info!(
            "Worker registered: {:?} -- {:?}",
            request.get_ref(),
            request.remote_addr()
        );

        let req = request.into_inner();
        let worker = req
            .worker_info
            .ok_or_else(|| Status::invalid_argument("missing worker_info"))?;

        self.send_to_job_queue(
            &worker.job_id,
            JobMessage::WorkerConnect {
                worker_id: WorkerId(worker.worker_id),
                machine_id: MachineId(worker.machine_id.into()),
                run_id: worker.run_id,
                rpc_address: req.rpc_address,
                data_address: req.data_address,
                slots: req.slots as usize,
            },
        )
        .await?;

        Ok(Response::new(RegisterWorkerResp {}))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatReq>,
    ) -> Result<Response<HeartbeatResp>, Status> {
        let req = request.into_inner();

        self.send_to_job_queue(
            &req.job_id,
            JobMessage::RunningMessage(RunningMessage::WorkerHeartbeat {
                worker_id: WorkerId(req.worker_id),
                time: Instant::now(),
            }),
        )
        .await?;

        return Ok(Response::new(HeartbeatResp {}));
    }

    async fn task_started(
        &self,
        request: Request<TaskStartedReq>,
    ) -> Result<Response<TaskStartedResp>, Status> {
        let req = request.into_inner();
        info!("task started: {:?}", req);

        self.send_to_job_queue(
            &req.job_id,
            JobMessage::TaskStarted {
                worker_id: WorkerId(req.worker_id),
                node_id: req.node_id,
                operator_subtask: req.operator_subtask,
            },
        )
        .await?;

        Ok(Response::new(TaskStartedResp {}))
    }

    async fn task_checkpoint_event(
        &self,
        request: Request<TaskCheckpointEventReq>,
    ) -> Result<Response<TaskCheckpointEventResp>, Status> {
        let req = request.into_inner();

        debug!("received task checkpoint event {:?}", req);
        let job_id = req.job_id.clone();
        self.send_to_job_queue(
            &job_id,
            JobMessage::RunningMessage(RunningMessage::TaskCheckpointEvent(req)),
        )
        .await?;

        Ok(Response::new(TaskCheckpointEventResp {}))
    }

    async fn task_checkpoint_completed(
        &self,
        request: Request<TaskCheckpointCompletedReq>,
    ) -> Result<Response<TaskCheckpointCompletedResp>, Status> {
        let req = request.into_inner();

        debug!("received task checkpoint completed {:?}", req);
        let job_id = req.job_id.clone();

        self.send_to_job_queue(
            &job_id,
            JobMessage::RunningMessage(RunningMessage::TaskCheckpointFinished(req)),
        )
        .await?;

        Ok(Response::new(TaskCheckpointCompletedResp {}))
    }

    async fn task_finished(
        &self,
        request: Request<TaskFinishedReq>,
    ) -> Result<Response<TaskFinishedResp>, Status> {
        let req = request.into_inner();

        self.send_to_job_queue(
            &req.job_id,
            JobMessage::RunningMessage(RunningMessage::TaskFinished {
                worker_id: WorkerId(req.worker_id),
                time: from_micros(req.time),
                node_id: req.node_id,
                subtask_index: req.operator_subtask as u32,
            }),
        )
        .await?;

        Ok(Response::new(TaskFinishedResp {}))
    }

    async fn task_failed(
        &self,
        request: Request<TaskFailedReq>,
    ) -> Result<Response<TaskFailedResp>, Status> {
        let req = request.into_inner();
        let err = req
            .error
            .ok_or_else(|| Status::invalid_argument("TaskFailedReq missing error"))?;

        self.send_to_job_queue(
            &err.job_id,
            JobMessage::RunningMessage(RunningMessage::TaskFailed(TaskFailedEvent {
                worker_id: WorkerId(req.worker_id),
                node_id: err.node_id,
                subtask_index: err.operator_subtask as u32,
                error_domain: err.error_domain().into(),
                retry_hint: err.retry_hint().into(),
                operator_id: err.operator_id,
                reason: err.error,
            })),
        )
        .await?;

        Ok(Response::new(TaskFailedResp {}))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeReq>,
    ) -> Result<Response<RegisterNodeResp>, Status> {
        let req = request.into_inner();
        info!(
            "Received node registration from {} at {} with {} slots",
            req.machine_id, req.addr, req.task_slots
        );

        self.scheduler.register_node(req).await;

        Ok(Response::new(RegisterNodeResp {}))
    }

    async fn heartbeat_node(
        &self,
        request: Request<HeartbeatNodeReq>,
    ) -> Result<Response<HeartbeatNodeResp>, Status> {
        self.scheduler.heartbeat_node(request.into_inner()).await?;
        Ok(Response::new(HeartbeatNodeResp {}))
    }

    async fn worker_finished(
        &self,
        request: Request<WorkerFinishedReq>,
    ) -> Result<Response<WorkerFinishedResp>, Status> {
        self.scheduler.worker_finished(request.into_inner()).await;
        Ok(Response::new(WorkerFinishedResp {}))
    }

    async fn send_sink_data(
        &self,
        request: Request<SinkDataReq>,
    ) -> Result<Response<SinkDataResp>, Status> {
        let req = request.into_inner();
        let mut data_txs = self.data_txs.lock().await;
        if let Some(v) = data_txs.get_mut(&req.job_id) {
            let output = OutputData {
                operator_id: req.operator_id,
                subtask_idx: req.subtask_index,
                timestamps: req.timestamps,
                batch: req.batch,
                start_id: req.start_id,
                done: req.done,
            };

            let mut remove = HashSet::new();
            for (i, tx) in v.iter().enumerate() {
                match tx.try_send(Ok(output.clone())) {
                    Ok(_) => {}
                    Err(TrySendError::Closed(_)) => {
                        remove.insert(i);
                    }
                    Err(TrySendError::Full(_)) => {
                        debug!("queue full");
                    }
                }
            }

            let mut i = 0;
            v.retain(|_tx| {
                i += 1;
                !remove.contains(&(i - 1))
            });
        }
        Ok(Response::new(SinkDataResp::default()))
    }

    type SubscribeToOutputStream = ReceiverStream<Result<OutputData, Status>>;

    async fn subscribe_to_output(
        &self,
        request: Request<GrpcOutputSubscription>,
    ) -> Result<Response<Self::SubscribeToOutputStream>, Status> {
        let job_id = request.into_inner().job_id;
        if self
            .job_state
            .lock()
            .await
            .get(&job_id)
            .ok_or_else(|| Status::not_found(format!("Job {job_id} does not exist")))?
            .state
            .read()
            .unwrap()
            .as_str()
            != "Running"
        {
            return Err(Status::failed_precondition(
                "Job must be running to read output",
            ));
        }

        let (tx, rx) = tokio::sync::mpsc::channel(32);

        let mut data_txs = self.data_txs.lock().await;
        data_txs.entry(job_id).or_default().push(tx);

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn nonfatal_error(
        &self,
        request: Request<NonfatalErrorReq>,
    ) -> Result<Response<WorkerErrorRes>, Status> {
        let req = request.into_inner();
        let err = req
            .error
            .ok_or_else(|| Status::invalid_argument("NonfatalErrorReq missing error"))?;

        info!(
            job_id = err.job_id,
            operator_id = err.operator_id,
            message = "operator error",
            error_message = err.error,
            error_details = err.details
        );

        let client = self.db.client().await.unwrap();
        match queries::controller_queries::execute_create_job_log_message(
            &client,
            &generate_id(IdTypes::JobLogMessage),
            &err.job_id,
            &err.operator_id,
            &(err.operator_subtask as i64),
            &LogLevel::error,
            &err.error,
            &err.details,
            &errors::ErrorDomain::from(err.error_domain()).as_str(),
            &errors::RetryHint::from(err.retry_hint()).as_str(),
        )
        .await
        {
            Ok(_) => Ok(Response::new(WorkerErrorRes {})),
            Err(db_err) => Err(Status::from_error(Box::new(db_err))),
        }
    }

    async fn job_metrics(
        &self,
        request: Request<JobMetricsReq>,
    ) -> Result<Response<JobMetricsResp>, Status> {
        let job_id = request.into_inner().job_id;
        let metrics = self
            .metrics
            .read()
            .await
            .get(&job_id)
            .ok_or_else(|| Status::not_found("No metrics for job"))?
            .clone();

        // TODO: send this over in a more efficient format like protobuf
        Ok(Response::new(JobMetricsResp {
            metrics: serde_json::to_string(&metrics.get_groups().await).unwrap(),
        }))
    }

    async fn worker_initialization_complete(
        &self,
        request: Request<WorkerInitializationCompleteReq>,
    ) -> Result<Response<WorkerInitializationCompleteResp>, Status> {
        let req = request.into_inner();
        info!(
            "Worker {} initialization completed: success={}, error={:?}",
            req.worker_id, req.success, req.error_message
        );

        self.send_to_job_queue(
            &req.job_id,
            JobMessage::WorkerInitializationComplete {
                worker_id: WorkerId(req.worker_id),
                success: req.success,
                error_message: req.error_message,
            },
        )
        .await?;

        Ok(Response::new(WorkerInitializationCompleteResp {}))
    }
}

impl ControllerServer {
    pub async fn new(database: DatabaseSource) -> Self {
        let scheduler: Arc<dyn Scheduler> = match &config().controller.scheduler {
            config::Scheduler::Node => {
                info!("Using node scheduler");
                Arc::new(NodeScheduler::new())
            }
            config::Scheduler::Kubernetes => {
                info!("Using kubernetes scheduler");
                Arc::new(schedulers::kubernetes::KubernetesScheduler::new().await)
            }
            config::Scheduler::Embedded => {
                info!("Using embedded scheduler");
                Arc::new(schedulers::embedded::EmbeddedScheduler::new())
            }
            config::Scheduler::Process => {
                info!("Using process scheduler");
                Arc::new(ProcessScheduler::new())
            }
        };

        Self {
            scheduler,
            data_txs: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            job_state: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            db: database,
            metrics: Default::default(),
        }
    }

    async fn send_to_job_queue(&self, job_id: &str, msg: JobMessage) -> Result<(), Status> {
        let mut jobs = self.job_state.lock().await;

        if let Some(sm) = jobs.get_mut(job_id) {
            if let Err(e) = sm.send(msg).await {
                Err(Status::failed_precondition(format!(
                    "Cannot handle message for {job_id}: {e}"
                )))
            } else {
                Ok(())
            }
        } else {
            warn!(message = "Received message for unknown job id", job_id);
            Err(Status::failed_precondition(format!(
                "No job with id {job_id}"
            )))
        }
    }

    fn start_updater(&self, guard: ShutdownGuard) {
        let db = self.db.clone();
        let jobs = Arc::clone(&self.job_state);
        let scheduler = Arc::clone(&self.scheduler);
        let metrics = Arc::clone(&self.metrics);

        let token = guard.token();

        let mut cleaned_at = Instant::now();

        let our_guard = guard.child("update-thread");
        our_guard.into_spawn_task(async move {
            while !token.is_cancelled() {
                let client = db.client().await?;
                let res = queries::controller_queries::fetch_all_jobs(&client).await?;
                for p in res {
                    let id = Arc::new(p.id);
                    let config = JobConfig {
                        id: id.clone(),
                        organization_id: p.org_id,
                        pipeline_name: p.pipeline_name,
                        pipeline_id: p.pipeline_id,
                        stop_mode: p.stop,
                        checkpoint_interval: Duration::from_micros(
                            p.checkpoint_interval_micros as u64,
                        ),
                        ttl: p.ttl_micros.map(|t| Duration::from_micros(t as u64)),
                        parallelism_overrides: p
                            .parallelism_overrides
                            .as_object()
                            .unwrap()
                            .into_iter()
                            .filter_map(|(k, v)| {
                                Some((u32::from_str(k).ok()?, v.as_u64()? as usize))
                            })
                            .collect(),
                        restart_nonce: p.config_restart_nonce,
                        restart_mode: p.restart_mode,
                    };

                    let mut jobs = jobs.lock().await;

                    let status = JobStatus {
                        id: id.clone(),
                        run_id: p.run_id.unwrap_or(0).max(0) as u64,
                        state: p.state.unwrap_or_else(|| Created {}.name().to_string()),
                        start_time: p.start_time,
                        finish_time: p.finish_time,
                        tasks: p.tasks,
                        failure_message: p.failure_message,
                        failure_domain: p.failure_domain,
                        restarts: p.restarts,
                        pipeline_path: p.pipeline_path,
                        wasm_path: p.wasm_path,
                        restart_nonce: p.status_restart_nonce,
                    };

                    if let Some(sm) = jobs.get_mut(&*id) {
                        sm.update(config, status, &guard).await;
                    } else if !job_in_final_state(&config, &status) {
                        jobs.insert(
                            (*id).clone(),
                            StateMachine::new(
                                config,
                                status,
                                db.clone(),
                                scheduler.clone(),
                                guard.clone_temporary(),
                                metrics.clone(),
                            )
                            .await,
                        );
                    }
                }

                if cleaned_at.elapsed() > Duration::from_secs(5) {
                    let res = queries::controller_queries::execute_clean_preview_pipelines(
                        &client,
                        &(OffsetDateTime::now_utc() - TTL_PIPELINE_CLEANUP_TIME),
                    )
                    .await?;
                    if res > 0 {
                        info!("Cleaned {res} preview pipelines from database");
                    }
                    cleaned_at = Instant::now();
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok(())
        });
    }

    pub async fn start(self, guard: ShutdownGuard) -> anyhow::Result<u16> {
        // let reflection = tonic_reflection::server::Builder::configure()
        //     .register_encoded_file_descriptor_set(arroyo_rpc::grpc::API_FILE_DESCRIPTOR_SET)
        //     .build_v1()
        //     .unwrap();

        let config = config();
        let addr = SocketAddr::new(config.controller.bind_address, config.controller.rpc_port);

        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;

        let service = ControllerGrpcServer::new(self.clone())
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd);

        self.start_updater(guard.child("updater"));

        if let Some(tls_config) = config.get_tls_config(&config.controller.tls) {
            info!("Starting arroyo-controller with TLS on {}", local_addr);

            let server = arroyo_server_common::grpc_server_with_tls(tls_config)
                .await?
                .accept_http1(true)
                .add_service(service);
            // TODO: re-enable once tonic 0.13 is released
            //.add_service(reflection);

            guard.into_spawn_task(wrap_start(
                "controller",
                local_addr,
                server.serve_with_incoming(TcpListenerStream::new(listener)),
            ));
        } else {
            info!("Starting arroyo-controller on {}", local_addr);

            guard.into_spawn_task(wrap_start(
                "controller",
                local_addr,
                arroyo_server_common::grpc_server()
                    .accept_http1(true)
                    .add_service(service)
                    // TODO: re-enable once tonic 0.13 is released
                    //.add_service(reflection)
                    .serve_with_incoming(TcpListenerStream::new(listener)),
            ));
        }

        Ok(local_addr.port())
    }
}
