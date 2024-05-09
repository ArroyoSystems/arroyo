//#![allow(clippy::new_without_default)]
// TODO: factor out complex types
#![allow(clippy::type_complexity)]

use crate::engine::{Engine, Program, StreamConfig, SubtaskNode};
use crate::network_manager::NetworkManager;
use anyhow::Result;

use arroyo_rpc::grpc::controller_grpc_client::ControllerGrpcClient;
use arroyo_rpc::grpc::worker_grpc_server::{WorkerGrpc, WorkerGrpcServer};
use arroyo_rpc::grpc::{
    api, CheckpointReq, CheckpointResp, CommitReq, CommitResp, HeartbeatReq, JobFinishedReq,
    JobFinishedResp, LoadCompactedDataReq, LoadCompactedDataRes, RegisterWorkerReq,
    StartExecutionReq, StartExecutionResp, StopExecutionReq, StopExecutionResp,
    TaskCheckpointCompletedReq, TaskCheckpointEventReq, TaskFailedReq, TaskFinishedReq,
    TaskStartedReq, WorkerErrorReq, WorkerResources,
};
use arroyo_types::{
    default_controller_addr, from_millis, grpc_port, to_micros, CheckpointBarrier, NodeId,
    WorkerId, ARROYO_PROGRAM_ENV, ARROYO_PROGRAM_FILE_ENV, JOB_ID_ENV, RUN_ID_ENV,
};
use local_ip_address::local_ip;
use rand::random;

use base64::engine::general_purpose;
use base64::Engine as Base64Engine;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use arroyo_rpc::{CompactionResult, ControlMessage, ControlResp};
pub use ordered_float::OrderedFloat;
use prost::Message;

use arroyo_datastream::logical::{LogicalGraph, LogicalProgram, ProgramConfig};
use arroyo_df::physical::new_registry;
use arroyo_server_common::shutdown::ShutdownGuard;
use arroyo_server_common::wrap_start;

pub mod arrow;

pub mod engine;
mod network_manager;

pub const PROMETHEUS_PUSH_GATEWAY: &str = "localhost:9091";
pub const METRICS_PUSH_INTERVAL: Duration = Duration::from_secs(1);

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
            LogicalEdge::ShuffleJoin(order) => write!(f, "{}⤨", order),
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

struct EngineState {
    sources: Vec<Sender<ControlMessage>>,
    sinks: Vec<Sender<ControlMessage>>,
    operator_controls: HashMap<String, Vec<Sender<ControlMessage>>>, // operator_id -> vec of control tx
    shutdown_guard: ShutdownGuard,
}

pub struct LocalRunner {
    program: Program,
}

impl LocalRunner {
    pub fn new(program: Program) -> Self {
        Self { program }
    }

    pub async fn run(self) {
        let name = format!("{}-0", self.program.name);
        let total_nodes = self.program.total_nodes();
        let engine = Engine::for_local(self.program, name);
        let (_running_engine, mut control_rx) = engine
            .start(StreamConfig {
                restore_epoch: None,
            })
            .await;

        let mut finished_nodes = HashSet::new();

        loop {
            while let Some(control_message) = control_rx.recv().await {
                debug!("received {:?}", control_message);
                if let ControlResp::TaskFinished {
                    operator_id,
                    task_index,
                } = control_message
                {
                    finished_nodes.insert((operator_id, task_index));
                    if finished_nodes.len() == total_nodes {
                        return;
                    }
                }
            }
        }
    }
}

pub struct WorkerServer {
    id: WorkerId,
    job_id: String,
    run_id: String,
    name: &'static str,
    controller_addr: String,
    logical_graph: LogicalGraph,
    program_config: ProgramConfig,
    state: Arc<Mutex<Option<EngineState>>>,
    network: Arc<Mutex<Option<NetworkManager>>>,
    shutdown_guard: ShutdownGuard,
}

impl WorkerServer {
    fn get_program() -> String {
        match (
            env::var(ARROYO_PROGRAM_FILE_ENV),
            env::var(ARROYO_PROGRAM_ENV),
        ) {
            (Ok(file), Err(_)) => std::fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("Could not read program from {}: {:?}", file, e)),
            (Err(_), Ok(program)) => program,
            (Err(_), Err(_)) => {
                panic!(
                    "One of {} or {} must be set",
                    ARROYO_PROGRAM_FILE_ENV, ARROYO_PROGRAM_ENV
                )
            }
            _ => {
                panic!(
                    "Both {} and {} are set; only one may be used",
                    ARROYO_PROGRAM_FILE_ENV, ARROYO_PROGRAM_ENV
                )
            }
        }
    }

    pub fn from_env(shutdown_guard: ShutdownGuard) -> Self {
        let graph = Self::get_program();
        let graph = general_purpose::STANDARD_NO_PAD
            .decode(graph)
            .expect("Program is not valid base64");

        let graph = api::ArrowProgram::decode(&graph[..]).expect("Program is not a valid protobuf");

        let logical = LogicalProgram::try_from(graph).expect("Failed to create LogicalProgram");

        let id = WorkerId::from_env().unwrap_or_else(|| WorkerId(random()));
        let job_id =
            std::env::var(JOB_ID_ENV).unwrap_or_else(|_| panic!("{} is not set", JOB_ID_ENV));

        let run_id =
            std::env::var(RUN_ID_ENV).unwrap_or_else(|_| panic!("{} is not set", RUN_ID_ENV));

        let controller_addr = std::env::var(arroyo_types::CONTROLLER_ADDR_ENV)
            .unwrap_or_else(|_| default_controller_addr());

        WorkerServer::new(
            "program",
            id,
            job_id,
            run_id,
            controller_addr,
            logical,
            shutdown_guard,
        )
    }

    pub fn new(
        name: &'static str,
        worker_id: WorkerId,
        job_id: String,
        run_id: String,
        controller_addr: String,
        logical: LogicalProgram,
        shutdown_guard: ShutdownGuard,
    ) -> Self {
        Self {
            id: worker_id,
            name,
            job_id,
            run_id,
            controller_addr,
            logical_graph: logical.graph,
            program_config: logical.program_config,
            state: Arc::new(Mutex::new(None)),
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

    pub async fn start_async(self) -> Result<(), Box<dyn std::error::Error>> {
        let slots = std::env::var(arroyo_types::TASK_SLOTS_ENV)
            .map(|s| usize::from_str(&s).unwrap())
            .unwrap_or(8);

        let node_id = NodeId::from_env();

        let grpc_port = grpc_port("worker", 0);

        let listener = TcpListener::bind(format!("0.0.0.0:{}", grpc_port)).await?;
        let local_addr = listener.local_addr()?;

        info!("Started worker-rpc for {} on {}", self.name, local_addr);
        let mut client = ControllerGrpcClient::connect(self.controller_addr.clone()).await?;

        let mut network = NetworkManager::new(0);
        let data_port = network
            .open_listener(self.shutdown_guard.child("network-manager"))
            .await;

        *self.network.lock().unwrap() = Some(network);

        info!(
            "Started worker data for {} on 0.0.0.0:{}",
            self.name, data_port
        );

        let id = self.id;
        let local_ip = local_ip().unwrap();

        let rpc_address = format!("http://{}:{}", local_ip, local_addr.port());
        let data_address = format!("{}:{}", local_ip, data_port);
        let job_id = self.job_id.clone();

        self.shutdown_guard
            .child("grpc")
            .into_spawn_task(wrap_start(
                "worker",
                local_addr,
                arroyo_server_common::grpc_server()
                    .add_service(WorkerGrpcServer::new(self))
                    .serve_with_incoming(TcpListenerStream::new(listener)),
            ));

        // ideally, get a signal when the server is started...
        tokio::time::sleep(Duration::from_millis(50)).await;

        client
            .register_worker(Request::new(RegisterWorkerReq {
                worker_id: id.0,
                node_id: node_id.map(|n| n.0).unwrap_or(1),
                job_id,
                rpc_address,
                data_address,
                resources: Some(WorkerResources {
                    slots: std::thread::available_parallelism().unwrap().get() as u64,
                }),
                slots: slots as u64,
            }))
            .await
            .unwrap();

        Ok(())
    }

    #[tokio::main]
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error>> {
        self.start_async().await
    }

    fn start_control_thread(
        &self,
        mut control_rx: Receiver<ControlResp>,
        worker_id: WorkerId,
        job_id: String,
    ) -> impl Future<Output = Result<()>> {
        let addr = self.controller_addr.clone();

        let cancel_token = self.shutdown_guard.token();

        async move {
            let mut controller = ControllerGrpcClient::connect(addr.clone())
                .await
                .expect("Unable to connect to controller");
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
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
                                        operator_id: c.operator_id,
                                        epoch: c.checkpoint_epoch,
                                        needs_commit: false,
                                        metadata: Some(c.subtask_metadata),
                                    }
                                )).await.err()
                            }
                            Some(ControlResp::TaskFinished { operator_id, task_index }) => {
                                info!(message = "Task finished", operator_id, task_index);
                                controller.task_finished(Request::new(
                                    TaskFinishedReq {
                                        worker_id: worker_id.0,
                                        job_id: job_id.clone(),
                                        time: to_micros(SystemTime::now()),
                                        operator_id: operator_id.to_string(),
                                        operator_subtask: task_index as u64,
                                    }
                                )).await.err()
                            }
                            Some(ControlResp::TaskFailed { operator_id, task_index, error }) => {
                                controller.task_failed(Request::new(
                                    TaskFailedReq {
                                        worker_id: worker_id.0,
                                        job_id: job_id.clone(),
                                        time: to_micros(SystemTime::now()),
                                        operator_id: operator_id.to_string(),
                                        operator_subtask: task_index as u64,
                                        error,
                                    }
                                )).await.err()
                            }
                            Some(ControlResp::Error { operator_id, task_index, message, details}) => {
                                controller.worker_error(Request::new(
                                    WorkerErrorReq {
                                        job_id: job_id.clone(),
                                        operator_id,
                                        task_index: task_index as u32,
                                        message,
                                        details
                                    }
                                )).await.err()
                            }
                            Some(ControlResp::TaskStarted {operator_id, task_index, start_time}) => {
                                controller.task_started(Request::new(
                                    TaskStartedReq {
                                        worker_id: worker_id.0,
                                        job_id: job_id.clone(),
                                        time: to_micros(start_time),
                                        operator_id: operator_id.to_string(),
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
    }
}

#[tonic::async_trait]
impl WorkerGrpc for WorkerServer {
    async fn start_execution(
        &self,
        request: Request<StartExecutionReq>,
    ) -> Result<Response<StartExecutionResp>, Status> {
        {
            let state = self.state.lock().unwrap();

            if state.is_some() {
                return Err(Status::failed_precondition(
                    "Job is already running on this worker",
                ));
            }
        }

        let req = request.into_inner();
        let mut registry = new_registry();

        for (udf_name, dylib_config) in &self.program_config.udf_dylibs {
            info!("Loading UDF {}", udf_name);
            registry
                .load_dylib(udf_name, dylib_config)
                .await
                .map_err(|e| {
                    Status::failed_precondition(
                        e.context(format!("loading UDF {udf_name}")).to_string(),
                    )
                })?;
            if dylib_config.is_async {
                continue;
            }
        }

        let (engine, control_rx) = {
            let network = { self.network.lock().unwrap().take().unwrap() };

            let program = Program::from_logical(
                self.name.to_string(),
                &self.logical_graph,
                &req.tasks,
                registry,
            );

            let engine = Engine::new(
                program,
                self.id,
                self.job_id.clone(),
                self.run_id.clone(),
                network,
                req.tasks,
            );
            engine
                .start(StreamConfig {
                    restore_epoch: req.restore_epoch,
                })
                .await
        };

        self.shutdown_guard
            .child("control-thread")
            .into_spawn_task(self.start_control_thread(control_rx, self.id, self.job_id.clone()));

        let sources = engine.source_controls();
        let sinks = engine.sink_controls();
        let operator_controls = engine.operator_controls();

        let mut state = self.state.lock().unwrap();
        *state = Some(EngineState {
            sources,
            sinks,
            operator_controls,
            shutdown_guard: self.shutdown_guard.child("engine-state"),
        });

        info!("[{:?}] Started execution", self.id);

        Ok(Response::new(StartExecutionResp {}))
    }

    async fn checkpoint(
        &self,
        request: Request<CheckpointReq>,
    ) -> Result<Response<CheckpointResp>, Status> {
        let req = request.into_inner();

        if req.is_commit {
            let senders = {
                let state = self.state.lock().unwrap();

                if let Some(state) = state.as_ref() {
                    state.sinks.clone()
                } else {
                    return Err(Status::failed_precondition(
                        "Worker has not yet started execution",
                    ));
                }
            };
            for sender in &senders {
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

        let senders = {
            let state = self.state.lock().unwrap();

            if let Some(state) = state.as_ref() {
                state.sources.clone()
            } else {
                return Err(Status::failed_precondition(
                    "Worker has not yet started execution",
                ));
            }
        };

        let barrier = CheckpointBarrier {
            epoch: req.epoch,
            min_epoch: req.min_epoch,
            timestamp: from_millis(req.timestamp),
            then_stop: req.then_stop,
        };

        for n in &senders {
            n.send(ControlMessage::Checkpoint(barrier)).await.unwrap();
        }

        Ok(Response::new(CheckpointResp {}))
    }

    async fn commit(&self, request: Request<CommitReq>) -> Result<Response<CommitResp>, Status> {
        let req = request.into_inner();
        info!("received commit request {:?}", req);
        let sender_commit_map_pairs = {
            let state_mutex = self.state.lock().unwrap();
            let Some(state) = state_mutex.as_ref() else {
                return Err(Status::failed_precondition(
                    "Worker has not yet started execution",
                ));
            };
            let mut sender_commit_map_pairs = vec![];
            for (operator_id, commit_operator) in req.committing_data {
                let nodes = state.operator_controls.get(&operator_id).unwrap().clone();
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
            let state = self.state.lock().unwrap();
            let s = state.as_ref().unwrap();
            s.operator_controls.get(&req.operator_id).unwrap().clone()
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

        return Ok(Response::new(LoadCompactedDataRes {}));
    }

    async fn stop_execution(
        &self,
        request: Request<StopExecutionReq>,
    ) -> Result<Response<StopExecutionResp>, Status> {
        let sources = {
            let state = self.state.lock().unwrap();
            state.as_ref().unwrap().sources.clone()
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
        let mut state = self.state.lock().unwrap();
        if let Some(engine) = state.as_mut() {
            engine.shutdown_guard.cancel();
        }

        let token = self.shutdown_guard.token();
        tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            token.cancel();
        });

        Ok(Response::new(JobFinishedResp {}))
    }
}
