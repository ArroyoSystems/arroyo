// TODO: factor out complex types
#![allow(clippy::type_complexity)]

use crate::engine::{Engine, Program, SubtaskNode};
use crate::network_manager::NetworkManager;
use anyhow::{Context, Result};

use arroyo_rpc::grpc::rpc::worker_grpc_server::{WorkerGrpc, WorkerGrpcServer};
use arroyo_rpc::grpc::rpc::{
    CheckpointReq, CheckpointResp, CommitReq, CommitResp, GetWorkerPhaseReq, GetWorkerPhaseResp,
    HeartbeatReq, JobFinishedReq, JobFinishedResp, LoadCompactedDataReq, LoadCompactedDataRes,
    MetricFamily, MetricsReq, MetricsResp, RegisterWorkerReq, StartExecutionReq,
    StartExecutionResp, StopExecutionReq, StopExecutionResp, TaskCheckpointCompletedReq,
    TaskCheckpointEventReq, TaskFailedReq, TaskFinishedReq, TaskStartedReq, WorkerErrorReq,
    WorkerInfo, WorkerInitializationCompleteReq, WorkerPhase, WorkerResources,
};
use arroyo_types::{
    from_millis, to_micros, CheckpointBarrier, MachineId, WorkerId, JOB_ID_ENV, RUN_ID_ENV,
};
use rand::random;

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::utils::to_d2;
use arroyo_datastream::logical::LogicalProgram;
use arroyo_planner::physical::new_registry;
use arroyo_rpc::config::config;
use arroyo_rpc::controller_client;
use arroyo_rpc::{local_address, retry, CompactionResult, ControlMessage, ControlResp};
use arroyo_server_common::shutdown::{CancellationToken, ShutdownGuard};
use arroyo_server_common::wrap_start;
pub use ordered_float::OrderedFloat;
use prometheus::{Encoder, ProtobufEncoder};
use prost::Message;
use uuid::Uuid;

pub mod arrow;

pub mod engine;
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
    Running(EngineState),
    Failed {
        started_at: SystemTime,
        error_message: String,
    },
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
        let name = format!("{}-0", self.program.name);
        let total_nodes = self.program.total_nodes();
        let engine = Engine::for_local(self.program, name).await?;

        let _running_engine = engine.start().await;

        let mut finished_nodes = HashSet::new();

        loop {
            while let Some(control_message) = self.control_rx.recv().await {
                debug!("received {:?}", control_message);
                if let ControlResp::TaskFinished {
                    node_id,
                    task_index,
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

pub struct WorkerServer {
    id: WorkerId,
    job_id: String,
    run_id: u64,
    name: &'static str,
    phase: Arc<Mutex<WorkerExecutionPhase>>,
    network: Arc<Mutex<Option<NetworkManager>>>,
    shutdown_guard: ShutdownGuard,
}

impl WorkerServer {
    pub fn from_config(shutdown_guard: ShutdownGuard) -> Result<Self> {
        let id = WorkerId(config().worker.id.unwrap_or_else(random));
        let job_id =
            std::env::var(JOB_ID_ENV).unwrap_or_else(|_| panic!("{JOB_ID_ENV} is not set"));

        let run_id = std::env::var(RUN_ID_ENV)
            .unwrap_or_else(|_| panic!("{RUN_ID_ENV} is not set"))
            .parse()
            .unwrap_or_else(|_| panic!("{RUN_ID_ENV} must be an unsigned int"));

        Ok(WorkerServer::new(
            "program",
            id,
            job_id,
            run_id,
            shutdown_guard,
        ))
    }

    pub fn new(
        name: &'static str,
        worker_id: WorkerId,
        job_id: String,
        run_id: u64,
        shutdown_guard: ShutdownGuard,
    ) -> Self {
        Self {
            id: worker_id,
            name,
            job_id,
            run_id,
            phase: Arc::new(Mutex::new(WorkerExecutionPhase::Idle)),
            network: Arc::new(Mutex::new(None)),
            shutdown_guard,
        }
    }

    pub fn id(&self) -> WorkerId {
        self.id
    }

    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    pub async fn start_async(self) -> Result<()> {
        let machine_id = MachineId(Arc::new(
            config()
                .worker
                .machine_id
                .clone()
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
        ));

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

        *self.network.lock().unwrap() = Some(network);

        let id = self.id;

        let hostname = local_address(config.worker.bind_address);
        let rpc_address = format!("http://{}:{}", hostname, local_addr.port());

        let data_address = format!("{hostname}:{data_port}");
        let job_id = self.job_id.clone();
        let run_id = self.run_id;

        self.shutdown_guard
            .child("grpc")
            .into_spawn_task(wrap_start(
                "worker",
                local_addr,
                if let Some(tls) = config.get_tls_config(&config.worker.tls) {
                    info!(
                        "Started worker-rpc with TLS for {} on {}",
                        self.name, local_addr
                    );
                    arroyo_server_common::grpc_server_with_tls(tls)
                        .await?
                        .add_service(WorkerGrpcServer::new(self))
                        .serve_with_incoming(TcpListenerStream::new(listener))
                } else {
                    info!("Started worker-rpc for {} on {}", self.name, local_addr);
                    arroyo_server_common::grpc_server()
                        .add_service(WorkerGrpcServer::new(self))
                        .serve_with_incoming(TcpListenerStream::new(listener))
                },
            ));

        // ideally, get a signal when the server is started...
        tokio::time::sleep(Duration::from_millis(50)).await;

        client
            .register_worker(Request::new(RegisterWorkerReq {
                worker_info: Some(WorkerInfo {
                    worker_id: id.0,
                    machine_id: machine_id.to_string(),
                    job_id,
                    run_id,
                }),
                rpc_address,
                data_address,
                resources: Some(WorkerResources {
                    slots: std::thread::available_parallelism().unwrap().get() as u64,
                }),
                slots: config.worker.task_slots as u64,
            }))
            .await
            .unwrap();

        Ok(())
    }

    #[tokio::main]
    pub async fn start(self) -> Result<()> {
        self.start_async().await
    }

    async fn run_control_loop(
        cancel_token: CancellationToken,
        control_rx: Receiver<ControlResp>,
        worker_id: WorkerId,
        job_id: String,
    ) -> Result<()> {
        let mut controller = controller_client("worker", &config().worker.tls)
            .await
            .expect("Unable to connect to controller");

        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut control_rx = control_rx;

        loop {
            select! {
                msg = control_rx.recv() => {
                    let err = match msg {
                        Some(ControlResp::CheckpointEvent(c)) => {
                            controller.task_checkpoint_event(Request::new(
                                TaskCheckpointEventReq {
                                    worker_id: worker_id.0,
                                    time: to_micros(c.time),
                                    job_id: job_id.clone(),
                                    node_id: c.node_id,
                                    operator_id: c.operator_id,
                                    subtask_index: c.subtask_index,
                                    epoch: c.checkpoint_epoch,
                                    event_type: c.event_type as i32,
                                }
                            )).await.err()
                        }
                        Some(ControlResp::CheckpointCompleted(c)) => {
                            controller.task_checkpoint_completed(Request::new(
                                TaskCheckpointCompletedReq {
                                    worker_id: worker_id.0,
                                    time: c.subtask_metadata.finish_time,
                                    job_id: job_id.clone(),
                                    node_id: c.node_id,
                                    operator_id: c.operator_id,
                                    epoch: c.checkpoint_epoch,
                                    needs_commit: false,
                                    metadata: Some(c.subtask_metadata),
                                }
                            )).await.err()
                        }
                        Some(ControlResp::TaskFinished { node_id, task_index }) => {
                            info!(message = "Task finished", node_id, task_index);
                            controller.task_finished(Request::new(
                                TaskFinishedReq {
                                    worker_id: worker_id.0,
                                    job_id: job_id.clone(),
                                    time: to_micros(SystemTime::now()),
                                    node_id,
                                    operator_subtask: task_index as u64,
                                }
                            )).await.err()
                        }
                        Some(ControlResp::TaskFailed { node_id, task_index, error }) => {
                            controller.task_failed(Request::new(
                                TaskFailedReq {
                                    worker_id: worker_id.0,
                                    time: to_micros(SystemTime::now()),
                                    error: Some(rpc::TaskError {
                                        job_id: job_id.clone(),
                                        node_id,
                                        operator_subtask: task_index as u64,
                                        error: error.message,
                                        error_domain: rpc::ErrorDomain::from(error.domain) as i32,
                                        retry_hint: rpc::RetryHint::from(error.retry_hint) as i32,
                                        operator_id: error.operator_id,
                                        details: error.details.unwrap_or_default(),
                                    }),
                                }
                            )).await.err()
                        }
                        Some(ControlResp::Error { node_id, operator_id, task_index, message, details}) => {
                            controller.nonfatal_error(Request::new(
                                NonfatalErrorReq {
                                    worker_id: worker_id.0,
                                    time: to_micros(SystemTime::now()),
                                    error: Some(rpc::TaskError {
                                        job_id: job_id.clone(),
                                        node_id,
                                        operator_id,
                                        operator_subtask: task_index as u64,
                                        error: message,
                                        error_domain: rpc::ErrorDomain::External as i32,
                                        retry_hint: rpc::RetryHint::NoRetry as i32,
                                        details,
                                    }),
                                }
                            )).await.err()
                        }
                        Some(ControlResp::TaskStarted {node_id, task_index, start_time}) => {
                            controller.task_started(Request::new(
                                TaskStartedReq {
                                    worker_id: worker_id.0,
                                    job_id: job_id.clone(),
                                    time: to_micros(start_time),
                                    node_id,
                                    operator_subtask: task_index as u64,
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
                    let result = controller.heartbeat(Request::new(HeartbeatReq {
                        job_id: job_id.clone(),
                        time: to_micros(SystemTime::now()),
                        worker_id: worker_id.0,
                    })).await;
                    if let Err(err) = result {
                        error!("heartbeat failed {:?}", err);
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn initialize(
        phase: Arc<Mutex<WorkerExecutionPhase>>,
        network: Arc<Mutex<Option<NetworkManager>>>,
        shutdown_guard: ShutdownGuard,
        worker_id: WorkerId,
        job_id: String,
        run_id: u64,
        name: &'static str,
        req: StartExecutionReq,
    ) {
        let error_message = match Self::initialize_inner(
            Arc::clone(&network),
            shutdown_guard,
            worker_id,
            &job_id,
            run_id,
            name,
            req,
        )
        .await
        {
            Ok(engine_state) => {
                let mut phase_guard = phase.lock().unwrap();
                *phase_guard = WorkerExecutionPhase::Running(engine_state);

                None
            }
            Err(e) => {
                let mut phase_guard = phase.lock().unwrap();
                *phase_guard = WorkerExecutionPhase::Failed {
                    started_at: SystemTime::now(),
                    error_message: e.to_string(),
                };

                Some(e.to_string())
            }
        };

        if let Ok(mut client) = controller_client("worker", &config().worker.tls).await {
            if let Err(e) = client
                .worker_initialization_complete(Request::new(WorkerInitializationCompleteReq {
                    worker_id: worker_id.0,
                    job_id,
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
    }

    async fn initialize_inner(
        network: Arc<Mutex<Option<NetworkManager>>>,
        shutdown_guard: ShutdownGuard,
        worker_id: WorkerId,
        job_id: &str,
        run_id: u64,
        name: &'static str,
        req: StartExecutionReq,
    ) -> Result<EngineState> {
        let mut registry = new_registry();

        let logical = LogicalProgram::try_from(req.program.expect("Program is None"))
            .map_err(|e| anyhow::anyhow!("Failed to create LogicalProgram: {}", e))?;

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

        let engine = {
            let network_manager = {
                network
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("Network manager not available"))?
            };

            let program = Program::from_logical(
                name.to_string(),
                job_id,
                &logical.graph,
                &req.tasks,
                registry,
                req.restore_epoch,
                control_tx.clone(),
            )
            .await;

            let engine = Engine::new(
                program,
                worker_id,
                job_id.to_string(),
                run_id,
                network_manager,
                req.tasks,
            );
            engine.start().await
        };

        let job_id_owned = job_id.to_string();
        let cancel_token = shutdown_guard.token();
        shutdown_guard
            .child("control-thread")
            .into_spawn_task(async move {
                Self::run_control_loop(cancel_token, control_rx, worker_id, job_id_owned).await
            });

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

        info!("[{:?}] Initialization completed successfully", worker_id);
        Ok(engine_state)
    }
}

#[tonic::async_trait]
impl WorkerGrpc for WorkerServer {
    async fn start_execution(
        &self,
        request: Request<StartExecutionReq>,
    ) -> Result<Response<StartExecutionResp>, Status> {
        let mut phase = self.phase.lock().unwrap();

        match &*phase {
            WorkerExecutionPhase::Idle => {
                let started_at = SystemTime::now();
                *phase = WorkerExecutionPhase::Initializing { started_at };

                // Spawn async initialization
                let phase = Arc::clone(&self.phase);
                let network = Arc::clone(&self.network);
                let shutdown_guard = self.shutdown_guard.clone_temporary();
                let worker_id = self.id;
                let job_id = self.job_id.clone();
                let run_id = self.run_id;
                let name = self.name;
                let req = request.into_inner();

                self.shutdown_guard.spawn_temporary(async move {
                    Self::initialize(
                        phase,
                        network,
                        shutdown_guard,
                        worker_id,
                        job_id,
                        run_id,
                        name,
                        req,
                    )
                    .await;
                    Ok(())
                });

                Ok(Response::new(StartExecutionResp {}))
            }
            WorkerExecutionPhase::Initializing { .. } => {
                Err(Status::unavailable("Worker is initializing"))
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
            let phase = self.phase.lock().unwrap();
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
            let phase = self.phase.lock().unwrap();
            let engine_state = match &*phase {
                WorkerExecutionPhase::Running(engine_state) => engine_state,
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
            let phase = self.phase.lock().unwrap();
            let engine_state = match &*phase {
                WorkerExecutionPhase::Running(engine_state) => engine_state,
                _ => {
                    return Err(Status::failed_precondition("Worker not in running phase"));
                }
            };
            engine_state
                .operator_controls
                .get(&req.node_id)
                .unwrap()
                .clone()
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
            let phase = self.phase.lock().unwrap();
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
            s.send(ControlMessage::Stop {
                mode: req.stop_mode(),
            })
            .await
            .unwrap();
        }

        Ok(Response::new(StopExecutionResp {}))
    }

    async fn job_finished(
        &self,
        _request: Request<JobFinishedReq>,
    ) -> Result<Response<JobFinishedResp>, Status> {
        let mut phase = self.phase.lock().unwrap();
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

    async fn get_worker_phase(
        &self,
        _req: Request<GetWorkerPhaseReq>,
    ) -> Result<Response<GetWorkerPhaseResp>, Status> {
        let phase = self.phase.lock().unwrap();

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
        };

        Ok(Response::new(GetWorkerPhaseResp {
            phase: phase_enum,
            phase_started_at,
            error_message,
        }))
    }
}
