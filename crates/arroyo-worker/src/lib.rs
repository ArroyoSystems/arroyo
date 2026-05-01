// TODO: factor out complex types
#![allow(clippy::type_complexity)]

use crate::engine::{Engine, Program, SubtaskNode};
use crate::job_controller::{RetireWorkerLeader, RunningMessage, TaskFailedEvent, WorkerContext};
use crate::network_manager::NetworkManager;
use anyhow::{Context, Result, anyhow};

use arroyo_rpc::grpc::rpc::worker_grpc_server::{WorkerGrpc, WorkerGrpcServer};
use arroyo_rpc::grpc::rpc::{
    CheckpointManifest, CheckpointReq, CheckpointResp, CommitReq, CommitResp, GetWorkerPhaseReq,
    GetWorkerPhaseResp, HeartbeatReq, HeartbeatResp, JobControllerInitReq, JobControllerInitResp,
    JobFinishedReq, JobFinishedResp, JobMetricsReq, JobMetricsResp, JobStatus, JobStatusReq,
    JobStatusResp, JobStopMode, LoadCompactedDataReq, LoadCompactedDataRes, MetricFamily,
    MetricsReq, MetricsResp, NonfatalErrorReq, RegisterWorkerReq, StartExecutionReq,
    StartExecutionResp, StopExecutionReq, StopExecutionResp, StopJobReq, StopJobResp,
    TaskAssignment, TaskCheckpointCompletedReq, TaskCheckpointCompletedResp,
    TaskCheckpointEventReq, TaskCheckpointEventResp, TaskFailedReq, TaskFailedResp,
    TaskFinishedReq, TaskFinishedResp, TaskStartedReq, WorkerErrorRes,
    WorkerInitializationCompleteReq, WorkerPhase, WorkerResources,
};
use arroyo_types::{
    CLUSTER_ID_ENV, CheckpointBarrier, CheckpointFilePathLayout, GENERATION_ENV, JOB_ID_ENV, JobId,
    MachineId, PIPELINE_ID_ENV, PipelineId, WorkerId, from_micros, from_millis, to_micros,
};
use rand::random;

use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::mem;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::job_controller::controller::WorkerJobController;
use crate::utils::to_d2;
use arroyo_datastream::logical::LogicalProgram;
use arroyo_planner::physical::new_registry;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::job_controller_grpc_server::{
    JobControllerGrpc, JobControllerGrpcServer,
};
use arroyo_rpc::grpc::rpc::job_status_grpc_server::{JobStatusGrpc, JobStatusGrpcServer};
use arroyo_rpc::{
    CompactionResult, ControlMessage, ControlResp, controller_client, job_controller_client,
    local_address, retry,
};

use arroyo_server_common::shutdown::{CancellationToken, ShutdownGuard};
use arroyo_server_common::wrap_start;
use arroyo_state::get_storage_provider;
use arroyo_state_protocol::store::read_protobuf;
use arroyo_state_protocol::types::CheckpointRef;
pub use ordered_float::OrderedFloat;
use prometheus::{Encoder, ProtobufEncoder};
use prost::Message;
use uuid::Uuid;

pub mod arrow;

pub mod engine;
pub mod job_controller;
mod network_manager;
pub mod utils;

pub static TIMER_TABLE: char = '[';

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum LogicalEdge {
    Forward,
    Shuffle,
    ShuffleJoin(usize),
}

impl Display for LogicalEdge {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalEdge::Forward => write!(f, "→"),
            LogicalEdge::Shuffle => write!(f, "⤨"),
            LogicalEdge::ShuffleJoin(order) => write!(f, "{order}⤨"),
        }
    }
}

#[derive(Clone)]
pub struct LogicalNode {
    pub id: String,
    pub description: String,
    pub create_fn: Box<fn(usize, usize) -> SubtaskNode>,
    pub initial_parallelism: usize,
}

impl Display for LogicalNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl Debug for LogicalNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

enum WorkerExecutionPhase {
    Idle,
    Initializing {
        started_at: SystemTime,
    },
    WaitingOnLeader {
        control_rx: Receiver<ControlResp>,
        job_controller_addr: String,
        engine_state: EngineState,
    },
    Running(EngineState),
    Failed {
        started_at: SystemTime,
        error_message: String,
    },
}

impl Display for WorkerExecutionPhase {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                WorkerExecutionPhase::Idle => "idle",
                WorkerExecutionPhase::Initializing { .. } => "initializing",
                WorkerExecutionPhase::WaitingOnLeader { .. } => "waiting_on_leader",
                WorkerExecutionPhase::Running(_) => "running",
                WorkerExecutionPhase::Failed { .. } => "failed",
            }
        )
    }
}

struct EngineState {
    sources: Vec<Sender<ControlMessage>>,
    sinks: Vec<Sender<ControlMessage>>,
    operator_to_node: HashMap<String, u32>,
    operator_controls: HashMap<u32, Vec<Sender<ControlMessage>>>, // node_id -> vec of control tx
    shutdown_guard: ShutdownGuard,
}

pub struct LocalRunner {
    program: Program,
    control_rx: Receiver<ControlResp>,
}

impl LocalRunner {
    pub fn new(program: Program, control_rx: Receiver<ControlResp>) -> Self {
        Self {
            program,
            control_rx,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let total_nodes = self.program.total_nodes();
        let engine = Engine::for_local(
            self.program,
            "pipe-local".to_string(),
            "job-local".to_string(),
        )
        .await?;

        let _running_engine = engine.start().await;

        let mut finished_nodes = HashSet::new();

        loop {
            while let Some(control_message) = self.control_rx.recv().await {
                debug!("received {:?}", control_message);
                if let ControlResp::TaskFinished {
                    task_id: node_id,
                    subtask_idx: task_index,
                } = control_message
                {
                    finished_nodes.insert((node_id, task_index));
                    if finished_nodes.len() == total_nodes {
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct WorkerState {
    worker_context: WorkerContext,
    phase: Arc<Mutex<WorkerExecutionPhase>>,
    network: Arc<Mutex<Option<NetworkManager>>>,
    // used to send messages to the local job controller -- only on the leader node
    job_controller_tx: Arc<OnceLock<Sender<RunningMessage>>>,
    job_status: Arc<Mutex<JobStatus>>,
}

impl WorkerState {
    async fn initialize(self, shutdown_guard: ShutdownGuard, req: StartExecutionReq) {
        let worker_context = self.worker_context.clone();

        let phase = self.phase.clone();

        let error_message = if let Err(e) = self.initialize_inner(shutdown_guard, req).await {
            let mut phase_guard = phase.lock().unwrap();
            *phase_guard = WorkerExecutionPhase::Failed {
                started_at: SystemTime::now(),
                error_message: e.to_string(),
            };

            Some(e.to_string())
        } else {
            None
        };

        if let Ok(mut client) = controller_client("worker", &config().worker.tls).await
            && let Err(e) = client
                .worker_initialization_complete(Request::new(WorkerInitializationCompleteReq {
                    worker_context: Some(worker_context.as_proto()),
                    time: to_micros(SystemTime::now()),
                    success: error_message.is_none(),
                    error_message,
                }))
                .await
        {
            error!(
                "failed to notify controller of schedule completion: {:?}",
                e
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn initialize_job_controller(
        &self,
        program: Arc<LogicalProgram>,
        tasks: &[TaskAssignment],
        epoch: u64,
        min_epoch: u64,
        shutdown: &ShutdownGuard,
        checkpoint_interval: Duration,
        parent: Option<(CheckpointRef, CheckpointManifest)>,
    ) -> anyhow::Result<bool> {
        // runs only on the leader

        let (tx, rx) = channel(128);

        self.job_controller_tx.set(tx).map_err(|_| {
            anyhow!("tried to initialize job controller but it was already initialized!")
        })?;

        debug!(
            "[{:?}] initialized job controller",
            self.worker_context.worker_id
        );

        let controller = match WorkerJobController::init(
            self.worker_context.clone(),
            program,
            tasks,
            epoch,
            min_epoch,
            rx,
            self.job_status.clone(),
            checkpoint_interval,
            parent,
        )
        .await
        {
            Ok(controller) => controller,
            Err(err) if err.downcast_ref::<RetireWorkerLeader>().is_some() => {
                info!(
                    worker_id = self.worker_context.worker_id.0,
                    generation = self.worker_context.generation,
                    error = format!("{:?}", err),
                    "worker leader retired during initialization"
                );
                shutdown.cancel();
                return Ok(false);
            }
            Err(err) => return Err(err),
        };

        controller.start(shutdown);

        Ok(true)
    }

    async fn initialize_inner(
        self,
        shutdown_guard: ShutdownGuard,
        req: StartExecutionReq,
    ) -> Result<()> {
        let mut registry = new_registry();
        let logical = Arc::new(
            LogicalProgram::try_from(req.program.expect("Program is None"))
                .map_err(|e| Status::internal(format!("Failed to create LogicalProgram: {}", e)))?,
        );

        debug!(
            "Starting execution for graph\n{}",
            to_d2(&logical)
                .await
                .unwrap_or_else(|e| format!("Failed to generate pipeline visualization: {e}"))
        );

        for (udf_name, dylib_config) in &logical.program_config.udf_dylibs {
            info!("Loading UDF {}", udf_name);
            registry
                .load_dylib(udf_name, dylib_config)
                .await
                .with_context(|| format!("loading UDF {udf_name}"))?;
            if dylib_config.is_async {
                continue;
            }
        }

        for (udf_name, python_udf) in &logical.program_config.python_udfs {
            info!("Loading Python UDF {}", udf_name);
            registry
                .add_python_udf(python_udf)
                .await
                .with_context(|| format!("loading Python UDF {udf_name}"))?;
        }

        let (control_tx, control_rx) = channel(128);

        let parent_ref = req
            .checkpoint_manifest_ref
            .map(CheckpointRef::new)
            .transpose()?;

        let parent = if let Some(parent_ref) = parent_ref {
            let manifest = read_protobuf::<_, CheckpointManifest>(
                get_storage_provider().await?.as_ref(),
                &parent_ref,
            )
            .await?
            .ok_or_else(|| anyhow!("could not find restoration checkpoint {:?}", parent_ref))?;

            Some((parent_ref, manifest))
        } else {
            None
        };

        if req.is_leader {
            // TODO: the leader needs to load checkpoint metadata and figure out epochs/min epochs
            //  itself from object storage
            if !self
                .initialize_job_controller(
                    logical.clone(),
                    &req.tasks,
                    req.start_epoch,
                    req.min_epoch,
                    &shutdown_guard,
                    Duration::from_micros(req.checkpoint_interval_micros),
                    parent.clone(),
                )
                .await?
            {
                return Ok(());
            }
        }

        let engine = {
            let network_manager = {
                self.network
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("Network manager not available"))?
            };

            let file_path_layout = if req.wait_for_leader || req.is_leader {
                CheckpointFilePathLayout::Protocol {
                    pipeline_id: self.worker_context.pipeline_id.clone(),
                    generation: self.worker_context.generation,
                }
            } else {
                CheckpointFilePathLayout::Legacy
            };

            let program = Program::from_logical(
                &self.worker_context.job_id,
                &logical.graph,
                &req.tasks,
                registry,
                req.restore_epoch,
                parent.map(|(_, m)| m),
                file_path_layout,
                control_tx.clone(),
            )
            .await?;

            let engine = Engine::new(
                program,
                self.worker_context.clone(),
                network_manager,
                req.tasks,
            );
            engine.start().await
        };

        let sources = engine.source_controls();
        let sinks = engine.sink_controls();
        let operator_controls = engine.operator_controls();
        let operator_to_node = engine.operator_to_node();

        let engine_state = EngineState {
            sources,
            sinks,
            operator_to_node,
            operator_controls,
            shutdown_guard: shutdown_guard.child("engine-state"),
        };

        let mut phase_guard = self.phase.lock().unwrap();

        let job_controller_addr = req
            .job_controller_addr
            .clone()
            .unwrap_or_else(|| config().controller_endpoint());

        if req.wait_for_leader {
            info!("waiting for leader");
            *phase_guard = WorkerExecutionPhase::WaitingOnLeader {
                control_rx,
                engine_state,
                job_controller_addr,
            };
            drop(phase_guard);
        } else {
            info!("Worker moving to running phase");
            *phase_guard = WorkerExecutionPhase::Running(engine_state);
            drop(phase_guard);

            let cancel_token = shutdown_guard.token();
            shutdown_guard
                .child("control-thread")
                .into_spawn_task(async move {
                    self.run_control_loop(cancel_token, control_rx, job_controller_addr, false)
                        .await
                });
        }

        info!("Initialization completed successfully");
        Ok(())
    }

    async fn run_control_loop(
        self,
        cancel_token: CancellationToken,
        control_rx: Receiver<ControlResp>,
        job_controller_addr: String,
        is_worker_job_controller: bool,
    ) -> Result<()> {
        // TODO: We need this just for sending TaskStarted notifications to the actual controller for
        //  scheduling; it would be nicer if we didn't need to retain it for the entire job lifecycle
        let mut controller = controller_client("worker", &config().worker.tls).await?;
        let mut job_controller = job_controller_client(
            "worker",
            &config().worker.tls,
            job_controller_addr,
            is_worker_job_controller,
        )
        .await?;

        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut control_rx = control_rx;

        loop {
            select! {
                msg = control_rx.recv() => {
                    let err = match msg {
                        Some(ControlResp::CheckpointEvent(c)) => {
                            job_controller.task_checkpoint_event(Request::new(
                                TaskCheckpointEventReq {
                                    worker_context: Some(self.worker_context.as_proto()),
                                    time: to_micros(c.time),
                                    operator_id: c.operator_id,
                                    subtask_idx: c.subtask_idx,
                                    epoch: c.checkpoint_epoch,
                                    event_type: c.event_type as i32,
                                }
                            )).await.err()
                        }
                        Some(ControlResp::CheckpointCompleted(c)) => {
                            job_controller.task_checkpoint_completed(Request::new(
                                TaskCheckpointCompletedReq {
                                    worker_context: Some(self.worker_context.as_proto()),
                                    time: c.subtask_metadata.finish_time,
                                    operator_id: c.operator_id,
                                    epoch: c.checkpoint_epoch,
                                    needs_commit: false,
                                    metadata: Some(c.subtask_metadata),
                                }
                            )).await.err()
                        }
                        Some(ControlResp::TaskFinished { task_id, subtask_idx }) => {
                            info!(message = "Task finished", task_id, subtask_idx);
                            job_controller.task_finished(Request::new(
                                TaskFinishedReq {
                                    worker_context: Some(self.worker_context.as_proto()),
                                    time: to_micros(SystemTime::now()),
                                    task_id,
                                    subtask_idx,
                                }
                            )).await.err()
                        }
                        Some(ControlResp::TaskFailed { task_id, subtask_idx, error }) => {
                            job_controller.task_failed(Request::new(
                                TaskFailedReq {
                                    worker_context: Some(self.worker_context.as_proto()),
                                    time: to_micros(SystemTime::now()),
                                    error: Some(rpc::TaskError {
                                        task_id,
                                        subtask_idx,
                                        error: error.message,
                                        error_domain: rpc::ErrorDomain::from(error.domain) as i32,
                                        retry_hint: rpc::RetryHint::from(error.retry_hint) as i32,
                                        operator_id: error.operator_id.unwrap_or_default(),
                                        details: error.details.unwrap_or_default(),
                                    }),
                                }
                            )).await.err()
                        }
                        Some(ControlResp::Error { task_id, operator_id, subtask_idx, message, details}) => {
                            job_controller.nonfatal_error(Request::new(
                                NonfatalErrorReq {
                                    worker_context: Some(self.worker_context.as_proto()),
                                    time: to_micros(SystemTime::now()),
                                    error: Some(rpc::TaskError {
                                        task_id,
                                        operator_id,
                                        subtask_idx,
                                        error: message,
                                        error_domain: rpc::ErrorDomain::External as i32,
                                        retry_hint: rpc::RetryHint::NoRetry as i32,
                                        details,
                                    }),
                                }
                            )).await.err()
                        }
                        Some(ControlResp::TaskStarted {task_id, subtask_idx, start_time}) => {
                            controller.task_started(Request::new(
                                TaskStartedReq {
                                    worker_context: Some(self.worker_context.as_proto()),
                                    time: to_micros(start_time),
                                    task_id,
                                    subtask_idx,
                                }
                            )).await.err()
                        }
                        None => {
                            // TODO: remove the control queue from the select at this point
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            None
                        }
                    };
                    if let Some(err) = err {
                        error!("encountered control message failure {}", err);
                        cancel_token.cancel();
                    }
                }
                _ = tick.tick() => {
                    if matches!(*self.phase.lock().unwrap(), WorkerExecutionPhase::Running { .. }) {
                        let result = job_controller.heartbeat(Request::new(HeartbeatReq {
                            worker_context: Some(self.worker_context.as_proto()),
                            time: to_micros(SystemTime::now()),
                        })).await;
                        if let Err(err) = result {
                            error!("heartbeat failed {:?}", err);
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub struct WorkerServer {
    state: WorkerState,
    shutdown_guard: ShutdownGuard,
}

impl WorkerServer {
    pub fn from_config(shutdown_guard: ShutdownGuard) -> Result<Self> {
        let worker_id = WorkerId(config().worker.id.unwrap_or_else(random));
        let cluster_id =
            std::env::var(CLUSTER_ID_ENV).unwrap_or_else(|_| panic!("{CLUSTER_ID_ENV} is not set"));
        let job_id =
            std::env::var(JOB_ID_ENV).unwrap_or_else(|_| panic!("{JOB_ID_ENV} is not set"));

        let machine_id = MachineId(Arc::new(
            config()
                .worker
                .machine_id
                .clone()
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
        ));

        let run_id = std::env::var(GENERATION_ENV)
            .unwrap_or_else(|_| panic!("{GENERATION_ENV} is not set"))
            .parse()
            .unwrap_or_else(|_| panic!("{GENERATION_ENV} must be an unsigned int"));

        let pipeline_id = std::env::var(PIPELINE_ID_ENV)
            .unwrap_or_else(|_| panic!("{PIPELINE_ID_ENV} is not set"));

        arroyo_server_common::set_cluster_id(&cluster_id);

        Ok(WorkerServer::new(
            machine_id,
            worker_id,
            PipelineId(pipeline_id.into()),
            JobId(job_id.into()),
            run_id,
            shutdown_guard,
        ))
    }

    pub fn new(
        machine_id: MachineId,
        worker_id: WorkerId,
        pipeline_id: PipelineId,
        job_id: JobId,
        run_id: u64,
        shutdown_guard: ShutdownGuard,
    ) -> Self {
        Self {
            state: WorkerState {
                phase: Arc::new(Mutex::new(WorkerExecutionPhase::Idle)),
                network: Arc::new(Mutex::new(None)),
                job_controller_tx: Arc::new(OnceLock::new()),
                worker_context: WorkerContext {
                    worker_id,
                    machine_id,
                    pipeline_id,
                    job_id,
                    generation: run_id,
                },
                job_status: Arc::new(Mutex::new(JobStatus {
                    job_state: rpc::JobState::JobInitializing.into(),
                    updated_at: to_micros(SystemTime::now()),
                    transitioned_at: to_micros(SystemTime::now()),
                    last_checkpoint: None,
                    job_failure: None,
                })),
            },
            shutdown_guard,
        }
    }

    pub fn id(&self) -> WorkerId {
        self.state.worker_context.worker_id
    }

    pub fn job_id(&self) -> &str {
        &self.state.worker_context.job_id
    }

    pub async fn start_async(self) -> Result<()> {
        let config = config();
        let listener = TcpListener::bind(SocketAddr::new(
            config.worker.bind_address,
            config.worker.rpc_port,
        ))
        .await?;
        let local_addr = listener.local_addr()?;

        let mut client = retry!(
            controller_client("worker", &config.worker.tls).await,
            2,
            Duration::from_millis(100),
            Duration::from_secs(2),
            |e| warn!("Failed to connect to controller: {e}, retrying...")
        )?;

        let mut network = NetworkManager::new(0).await?;
        let data_port = network
            .open_listener(self.shutdown_guard.child("network-manager"))
            .await;

        *self.state.network.lock().unwrap() = Some(network);

        let context = self.state.worker_context.clone();

        let hostname = local_address(config.worker.bind_address);
        let rpc_address = format!("http://{}:{}", hostname, local_addr.port());

        let data_address = format!("{hostname}:{data_port}");

        let leader = LeaderServer {
            state: self.state.clone(),
        };

        self.shutdown_guard
            .child("grpc")
            .into_spawn_task(wrap_start(
                "worker",
                local_addr,
                if let Some(tls) = config.get_tls_config(&config.worker.tls) {
                    info!("Started worker-rpc with TLS on {}", local_addr);
                    arroyo_server_common::grpc_server_with_tls(tls)
                        .await?
                        .add_service(WorkerGrpcServer::new(self))
                        .add_service(JobControllerGrpcServer::new(leader.clone()))
                        .add_service(JobStatusGrpcServer::new(leader))
                        .serve_with_incoming(TcpListenerStream::new(listener))
                } else {
                    info!("Started worker-rpc on {}", local_addr);
                    arroyo_server_common::grpc_server()
                        .add_service(WorkerGrpcServer::new(self))
                        .add_service(JobControllerGrpcServer::new(leader.clone()))
                        .add_service(JobStatusGrpcServer::new(leader))
                        .serve_with_incoming(TcpListenerStream::new(listener))
                },
            ));

        // ideally, get a signal when the server is started...
        tokio::time::sleep(Duration::from_millis(50)).await;

        client
            .register_worker(Request::new(RegisterWorkerReq {
                time: to_micros(SystemTime::now()),
                worker_context: Some(context.as_proto()),
                rpc_address,
                data_address,
                resources: Some(WorkerResources {
                    slots: std::thread::available_parallelism().unwrap().get() as u64,
                }),
                slots: config.worker.task_slots as u64,
            }))
            .await?;

        Ok(())
    }

    #[tokio::main]
    pub async fn start(self) -> Result<()> {
        self.start_async().await
    }
}

#[tonic::async_trait]
impl WorkerGrpc for WorkerServer {
    async fn get_worker_phase(
        &self,
        _req: Request<GetWorkerPhaseReq>,
    ) -> Result<Response<GetWorkerPhaseResp>, Status> {
        let phase = self.state.phase.lock().unwrap();

        let (phase_enum, phase_started_at, error_message) = match &*phase {
            WorkerExecutionPhase::Idle => (WorkerPhase::Idle as i32, None, None),
            WorkerExecutionPhase::Initializing { started_at } => (
                WorkerPhase::Initializing as i32,
                Some(to_micros(*started_at)),
                None,
            ),
            WorkerExecutionPhase::Running(_) => (WorkerPhase::Running as i32, None, None),
            WorkerExecutionPhase::Failed {
                started_at,
                error_message,
            } => (
                WorkerPhase::Failed as i32,
                Some(to_micros(*started_at)),
                Some(error_message.clone()),
            ),
            WorkerExecutionPhase::WaitingOnLeader { .. } => {
                (WorkerPhase::WaitingOnLeader as i32, None, None)
            }
        };

        Ok(Response::new(GetWorkerPhaseResp {
            phase: phase_enum,
            phase_started_at,
            error_message,
        }))
    }

    async fn start_execution(
        &self,
        request: Request<StartExecutionReq>,
    ) -> Result<Response<StartExecutionResp>, Status> {
        let mut phase = self.state.phase.lock().unwrap();

        match &*phase {
            WorkerExecutionPhase::Idle => {
                *phase = WorkerExecutionPhase::Initializing {
                    started_at: SystemTime::now(),
                };

                // Spawn async initialization
                let state = self.state.clone();
                let req = request.into_inner();
                let shutdown_guard = self.shutdown_guard.clone_temporary();

                self.shutdown_guard.spawn_temporary(async move {
                    state.initialize(shutdown_guard, req).await;
                    Ok(())
                });

                Ok(Response::new(StartExecutionResp {}))
            }
            WorkerExecutionPhase::Initializing { .. } => {
                Err(Status::unavailable("Worker is initializing"))
            }
            WorkerExecutionPhase::WaitingOnLeader { .. } => {
                Err(Status::failed_precondition("Worker is waiting for leader"))
            }
            WorkerExecutionPhase::Running(_) => {
                Err(Status::failed_precondition("Worker is already running"))
            }
            WorkerExecutionPhase::Failed { .. } => {
                Err(Status::failed_precondition("Worker is in failed state"))
            }
        }
    }

    async fn checkpoint(
        &self,
        request: Request<CheckpointReq>,
    ) -> Result<Response<CheckpointResp>, Status> {
        let req = request.into_inner();

        let (sinks, sources) = {
            let phase = self.state.phase.lock().unwrap();
            match &*phase {
                WorkerExecutionPhase::Running(engine_state) => {
                    (engine_state.sinks.clone(), engine_state.sources.clone())
                }
                _ => {
                    return Err(Status::failed_precondition("Worker not in running phase"));
                }
            }
        };

        if req.is_commit {
            for sender in &sinks {
                sender
                    .send(ControlMessage::Commit {
                        epoch: req.epoch,
                        commit_data: HashMap::new(),
                    })
                    .await
                    .unwrap();
            }
            return Ok(Response::new(CheckpointResp {}));
        }

        let barrier = CheckpointBarrier {
            epoch: req.epoch,
            min_epoch: req.min_epoch,
            timestamp: from_millis(req.timestamp),
            then_stop: req.then_stop,
        };

        for n in &sources {
            n.send(ControlMessage::Checkpoint(barrier)).await.unwrap();
        }

        Ok(Response::new(CheckpointResp {}))
    }

    async fn commit(&self, request: Request<CommitReq>) -> Result<Response<CommitResp>, Status> {
        let req = request.into_inner();
        debug!("received commit request {:?}", req);

        let sender_commit_map_pairs = {
            let phase = self.state.phase.lock().unwrap();
            let engine_state = match &*phase {
                WorkerExecutionPhase::WaitingOnLeader { engine_state, .. }
                | WorkerExecutionPhase::Running(engine_state) => engine_state,
                _ => {
                    return Err(Status::failed_precondition("Worker not in running phase"));
                }
            };

            let mut sender_commit_map_pairs = vec![];
            for (operator_id, commit_operator) in req.committing_data {
                let node_id = engine_state
                    .operator_to_node
                    .get(&operator_id)
                    .unwrap_or_else(|| panic!("Could not find node for operator id {operator_id}"));
                let nodes = engine_state.operator_controls.get(node_id).unwrap().clone();
                let commit_map: HashMap<_, _> = commit_operator
                    .committing_data
                    .into_iter()
                    .map(|(table, backend_data)| (table, backend_data.commit_data_by_subtask))
                    .collect();
                sender_commit_map_pairs.push((nodes, commit_map));
            }
            sender_commit_map_pairs
        };

        for (senders, commit_map) in sender_commit_map_pairs {
            for sender in senders {
                sender
                    .send(ControlMessage::Commit {
                        epoch: req.epoch,
                        commit_data: commit_map.clone(),
                    })
                    .await
                    .unwrap();
            }
        }
        Ok(Response::new(CommitResp {}))
    }

    async fn load_compacted_data(
        &self,
        request: Request<LoadCompactedDataReq>,
    ) -> Result<Response<LoadCompactedDataRes>, Status> {
        let req = request.into_inner();

        let nodes = {
            let phase = self.state.phase.lock().unwrap();
            let engine_state = match &*phase {
                WorkerExecutionPhase::Running(engine_state) => engine_state,
                _ => {
                    return Err(Status::failed_precondition("Worker not in running phase"));
                }
            };
            let node_id = engine_state
                .operator_to_node
                .get(&req.operator_id)
                .ok_or_else(|| {
                    Status::not_found(format!("No node found for operator_id {}", req.operator_id))
                })?;
            engine_state.operator_controls.get(node_id).unwrap().clone()
        };

        let compacted: CompactionResult = req.into();

        for s in nodes {
            if let Err(e) = s
                .send(ControlMessage::LoadCompacted {
                    compacted: compacted.clone(),
                })
                .await
            {
                warn!(
                    "Failed to send LoadCompacted message to operator {}: {}",
                    compacted.operator_id, e
                );
            }
        }

        Ok(Response::new(LoadCompactedDataRes {}))
    }

    async fn stop_execution(
        &self,
        request: Request<StopExecutionReq>,
    ) -> Result<Response<StopExecutionResp>, Status> {
        let sources = {
            let phase = self.state.phase.lock().unwrap();
            let engine_state = match &*phase {
                WorkerExecutionPhase::Running(engine_state) => engine_state,
                _ => {
                    return Err(Status::failed_precondition("Worker not in running phase"));
                }
            };
            engine_state.sources.clone()
        };

        let req = request.into_inner();
        for s in sources {
            // if this fails, it means the source has already shut down
            let _ = s
                .send(ControlMessage::Stop {
                    mode: req.stop_mode(),
                })
                .await;
        }

        Ok(Response::new(StopExecutionResp {}))
    }

    async fn job_finished(
        &self,
        _request: Request<JobFinishedReq>,
    ) -> Result<Response<JobFinishedResp>, Status> {
        let mut phase = self.state.phase.lock().unwrap();
        if let WorkerExecutionPhase::Running(engine_state) = &*phase {
            engine_state.shutdown_guard.cancel();
        }
        *phase = WorkerExecutionPhase::Idle;

        let token = self.shutdown_guard.token();
        tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            token.cancel();
        });

        Ok(Response::new(JobFinishedResp {}))
    }

    async fn get_metrics(
        &self,
        _req: Request<MetricsReq>,
    ) -> Result<Response<MetricsResp>, Status> {
        // we have to round-trip through bytes because rust-prometheus doesn't use prost
        let encoder = ProtobufEncoder::new();
        let registry = prometheus::default_registry();
        let mut buf = vec![];
        encoder.encode(&registry.gather(), &mut buf).map_err(|e| {
            Status::failed_precondition(format!("Failed to generate metrics: {e:?}"))
        })?;

        let mut metrics = vec![];

        let mut input = &buf[..];
        while !input.is_empty() {
            metrics.push(
                MetricFamily::decode_length_delimited(&mut input).map_err(|e| {
                    Status::failed_precondition(format!(
                        "Incompatible protobuf format for metrics: {e:?}"
                    ))
                })?,
            );
        }

        Ok(Response::new(MetricsResp { metrics }))
    }

    async fn job_controller_init(
        &self,
        _: Request<JobControllerInitReq>,
    ) -> std::result::Result<Response<JobControllerInitResp>, Status> {
        let mut phase = self.state.phase.lock().unwrap();
        let mut tmp_phase = WorkerExecutionPhase::Idle;
        mem::swap(&mut *phase, &mut tmp_phase);

        let (job_controller_addr, control_rx) = match tmp_phase {
            WorkerExecutionPhase::Running(e) => {
                debug!("job_controller_init called on worker but already in running phase");
                *phase = WorkerExecutionPhase::Running(e);
                return Ok(Response::new(JobControllerInitResp {}));
            }
            WorkerExecutionPhase::WaitingOnLeader {
                control_rx,
                engine_state,
                job_controller_addr,
            } => {
                *phase = WorkerExecutionPhase::Running(engine_state);
                (job_controller_addr, control_rx)
            }
            p => {
                let msg = format!("job_controller_init called on worker in {p} phase");
                *phase = p;
                return Err(Status::failed_precondition(msg));
            }
        };

        info!("Worker moving to running phase");
        let cancel_token = self.shutdown_guard.token();
        let state = self.state.clone();
        self.shutdown_guard
            .child("control-thread")
            .into_spawn_task(async move {
                state
                    .run_control_loop(cancel_token, control_rx, job_controller_addr, true)
                    .await
            });

        Ok(Response::new(JobControllerInitResp {}))
    }
}

#[derive(Clone)]
pub struct LeaderServer {
    state: WorkerState,
}

impl LeaderServer {
    async fn send_job_message(&self, msg: RunningMessage) -> Result<(), Status> {
        self.state
            .job_controller_tx
            .get()
            .ok_or_else(|| {
                Status::internal(format!(
                    "[{:?}] tried to handle job message on non-leader node!",
                    self.state.worker_context.worker_id
                ))
            })?
            .send(msg)
            .await
            .map_err(|_| Status::internal("could not process request, internal queue is closed"))
    }

    fn validate_req(&self, ctx: Option<&rpc::WorkerContext>) -> Result<(), Status> {
        let Some(ctx) = ctx else {
            return Err(Status::invalid_argument(
                "request is missing worker context",
            ));
        };

        if *self.state.worker_context.job_id != ctx.job_id
            || self.state.worker_context.generation != ctx.generation
        {
            return Err(Status::permission_denied(format!(
                "received message for incorrect job or generation ({}, {}) != ({}, {})",
                self.state.worker_context.job_id,
                self.state.worker_context.generation,
                ctx.job_id,
                ctx.generation
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl JobControllerGrpc for LeaderServer {
    async fn task_checkpoint_event(
        &self,
        request: Request<TaskCheckpointEventReq>,
    ) -> Result<Response<TaskCheckpointEventResp>, Status> {
        let req = request.into_inner();

        self.validate_req(req.worker_context.as_ref())?;

        debug!(req =? req, "received task checkpoint event");

        self.send_job_message(RunningMessage::TaskCheckpointEvent(req))
            .await?;

        Ok(Response::new(TaskCheckpointEventResp {}))
    }

    async fn task_checkpoint_completed(
        &self,
        request: Request<TaskCheckpointCompletedReq>,
    ) -> Result<Response<TaskCheckpointCompletedResp>, Status> {
        let req = request.into_inner();

        self.validate_req(req.worker_context.as_ref())?;

        debug!("received task checkpoint completed {:?}", req);

        self.send_job_message(RunningMessage::TaskCheckpointFinished(req))
            .await?;

        Ok(Response::new(TaskCheckpointCompletedResp {}))
    }

    async fn task_finished(
        &self,
        request: Request<TaskFinishedReq>,
    ) -> Result<Response<TaskFinishedResp>, Status> {
        let req = request.into_inner();

        self.validate_req(req.worker_context.as_ref())?;

        let ctx = req
            .worker_context
            .ok_or_else(|| Status::invalid_argument("missing worker_context"))?;

        self.send_job_message(RunningMessage::TaskFinished {
            worker_id: WorkerId(ctx.worker_id),
            time: from_micros(req.time),
            task_id: req.task_id,
            subtask_idx: req.subtask_idx,
        })
        .await?;

        Ok(Response::new(TaskFinishedResp {}))
    }

    async fn task_failed(
        &self,
        request: Request<TaskFailedReq>,
    ) -> Result<Response<TaskFailedResp>, Status> {
        let req = request.into_inner();

        self.validate_req(req.worker_context.as_ref())?;

        let ctx = req
            .worker_context
            .ok_or_else(|| Status::invalid_argument("TaskFailedReq missing worker_context"))?;
        let err = req
            .error
            .ok_or_else(|| Status::invalid_argument("TaskFailedReq missing error"))?;

        self.send_job_message(RunningMessage::TaskFailed(TaskFailedEvent {
            worker_id: WorkerId(ctx.worker_id),
            task_id: err.task_id,
            subtask_idx: err.subtask_idx,
            error_domain: err.error_domain().into(),
            retry_hint: err.retry_hint().into(),
            operator_id: err.operator_id,
            reason: err.error,
            details: err.details,
        }))
        .await?;

        Ok(Response::new(TaskFailedResp {}))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatReq>,
    ) -> Result<Response<HeartbeatResp>, Status> {
        let req = request.into_inner();

        self.validate_req(req.worker_context.as_ref())?;

        debug!(
            "[{:?}] Received heartbeat {:?}",
            self.state.worker_context.worker_id, req
        );

        self.send_job_message(RunningMessage::WorkerHeartbeat {
            worker_id: WorkerId(req.worker_context.as_ref().unwrap().worker_id),
            time: Instant::now(),
        })
        .await?;

        Ok(Response::new(HeartbeatResp {}))
    }

    async fn nonfatal_error(
        &self,
        request: Request<NonfatalErrorReq>,
    ) -> Result<Response<WorkerErrorRes>, Status> {
        let req = request.into_inner();

        self.validate_req(req.worker_context.as_ref())?;

        let ctx = req
            .worker_context
            .ok_or_else(|| Status::invalid_argument("NonfatalErrorReq missing worker_context"))?;
        let err = req
            .error
            .ok_or_else(|| Status::invalid_argument("NonfatalErrorReq missing error"))?;

        error!(
            job_id = ctx.job_id,
            operator_id = err.operator_id,
            message = "non-fatal operator error",
            error_message = err.error,
            error_details = err.details
        );

        Ok(Response::new(WorkerErrorRes {}))
    }

    async fn job_metrics(
        &self,
        _request: Request<JobMetricsReq>,
    ) -> Result<Response<JobMetricsResp>, Status> {
        todo!("metric support")
    }
}

#[async_trait]
impl JobStatusGrpc for LeaderServer {
    async fn get_job_status(
        &self,
        request: Request<JobStatusReq>,
    ) -> std::result::Result<Response<JobStatusResp>, Status> {
        if self.state.job_controller_tx.get().is_none() {
            return Err(Status::failed_precondition(
                "requested job status from non-leader node",
            ));
        }

        let job_id = (*self.state.worker_context.job_id.0).clone();
        let req = request.into_inner();
        if job_id != req.job_id {
            return Err(Status::failed_precondition(format!(
                "requested job status for invalid job {} - expected {}",
                req.job_id, job_id
            )));
        }

        Ok(Response::new(JobStatusResp {
            job_id,
            generation: self.state.worker_context.generation,
            job_status: Some((*self.state.job_status.lock().unwrap()).clone()),
        }))
    }

    async fn stop_job(
        &self,
        request: Request<StopJobReq>,
    ) -> std::result::Result<Response<StopJobResp>, Status> {
        let req = request.into_inner();
        let stop_mode = JobStopMode::try_from(req.stop_mode).map_err(|_| {
            Status::invalid_argument(format!("invalid stop mode: {}", req.stop_mode))
        })?;

        info!(
            message = "received stop job request",
            worker_id = ?self.state.worker_context.worker_id,
            stop_mode = ?stop_mode,
        );

        self.send_job_message(RunningMessage::Stop { stop_mode })
            .await?;

        Ok(Response::new(StopJobResp {}))
    }
}
