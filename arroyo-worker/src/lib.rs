#![allow(clippy::new_without_default)]
// TODO: factor out complex types
#![allow(clippy::type_complexity)]
extern crate core;

use crate::engine::{Engine, Program, StreamConfig, SubtaskNode};
use crate::network_manager::NetworkManager;
use arroyo_rpc::grpc::controller_grpc_client::ControllerGrpcClient;
use arroyo_rpc::grpc::worker_grpc_server::{WorkerGrpc, WorkerGrpcServer};
use arroyo_rpc::grpc::{
    CheckpointReq, CheckpointResp, JobFinishedReq, JobFinishedResp, RegisterWorkerReq,
    StartExecutionReq, StartExecutionResp, StopExecutionReq, StopExecutionResp, WorkerResources,
};
use arroyo_rpc::ControlMessage;
use arroyo_server_common::start_admin_server;
use arroyo_types::{
    admin_port, from_millis, grpc_port, ports, CheckpointBarrier, NodeId, WorkerId, ADMIN_PORT_ENV,
    JOB_ID_ENV, RUN_ID_ENV,
};
use engine::RunningEngine;
use lazy_static::lazy_static;
use local_ip_address::local_ip;
use petgraph::graph::DiGraph;
use rand::Rng;
use std::env;
use std::fmt::{Debug, Display, Formatter};
use std::process::exit;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tracing::info;

pub mod engine;
mod inq_reader;
mod network_manager;
pub mod operators;
mod process_fn;

pub const PROMETHEUS_PUSH_GATEWAY: &str = "localhost:9091";
pub const METRICS_PUSH_INTERVAL: Duration = Duration::from_secs(1);

lazy_static! {
    pub static ref LOCAL_CONTROLLER_ADDR: String =
        format!("http://localhost:{}", ports::CONTROLLER_GRPC);
}

pub static TIMER_TABLE: char = '[';

pub enum SourceFinishType {
    // stop messages should be propagated through the dataflow
    Graceful,
    // shuts down the operator immediately, triggering immediate shut-downs across the dataflow
    Immediate,
    // EndOfData messages are propagated, causing MAX_WATERMARK and flushing all timers
    Final,
}

pub enum ControlOutcome {
    Continue,
    Stop,
    Finish,
}

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
    running_engine: RunningEngine,
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
        let engine = Engine::for_local(self.program, name);
        engine
            .start(StreamConfig {
                restore_epoch: None,
            })
            .await;

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

pub struct WorkerServer {
    id: WorkerId,
    job_id: String,
    run_id: String,
    name: &'static str,
    hash: &'static str,
    controller_addr: String,
    logical: DiGraph<LogicalNode, LogicalEdge>,
    state: Arc<Mutex<Option<EngineState>>>,
    network: Arc<Mutex<Option<NetworkManager>>>,
}

impl WorkerServer {
    pub fn new(
        name: &'static str,
        hash: &'static str,
        logical: DiGraph<LogicalNode, LogicalEdge>,
    ) -> Self {
        let controller_addr = std::env::var(arroyo_types::CONTROLLER_ADDR_ENV)
            .unwrap_or_else(|_| LOCAL_CONTROLLER_ADDR.clone());

        let id = WorkerId::from_env().unwrap_or_else(|| WorkerId(rand::thread_rng().gen()));
        let job_id =
            std::env::var(JOB_ID_ENV).unwrap_or_else(|_| panic!("{} is not set", JOB_ID_ENV));

        let run_id =
            std::env::var(RUN_ID_ENV).unwrap_or_else(|_| panic!("{} is not set", RUN_ID_ENV));

        Self {
            id,
            name,
            job_id,
            run_id,
            hash,
            controller_addr,
            logical,
            state: Arc::new(Mutex::new(None)),
            network: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn start_async(self) -> Result<(), Box<dyn std::error::Error>> {
        let _guard =
            arroyo_server_common::init_logging(&format!("worker-{}-{}", self.id.0, self.job_id));

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
        let data_port = network.open_listener().await;

        (*self.network.lock().unwrap()) = Some(network);

        info!(
            "Started worker data for {} on 0.0.0.0:{}",
            self.name, data_port
        );

        let id = self.id;
        let local_ip = local_ip().unwrap();

        let rpc_address = format!("http://{}:{}", local_ip, local_addr.port());
        let data_address = format!("{}:{}", local_ip, data_port);
        let hash = self.hash;
        let job_id = self.job_id.clone();

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        start_admin_server("api", 0, shutdown_rx);

        tokio::spawn(async move {
            // ideally, get a signal when the server is started...
            tokio::time::sleep(Duration::from_secs(2)).await;

            client
                .register_worker(Request::new(RegisterWorkerReq {
                    worker_id: id.0,
                    node_id: node_id.0,
                    job_id,
                    rpc_address,
                    data_address,
                    resources: Some(WorkerResources {
                        slots: std::thread::available_parallelism().unwrap().get() as u64,
                    }),
                    job_hash: hash.to_string(),
                    slots: slots as u64,
                }))
                .await
                .unwrap();
        });

        arroyo_server_common::grpc_server()
            .add_service(WorkerGrpcServer::new(self))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await?;

        shutdown_tx.send(0).unwrap();

        Ok(())
    }

    #[tokio::main]
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error>> {
        self.start_async().await
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

        let program = Program::from_logical(self.name.to_string(), &self.logical, &req.tasks);

        let engine = {
            let network = { self.network.lock().unwrap().take().unwrap() };

            let engine = Engine::new(
                program,
                self.id,
                self.job_id.clone(),
                self.run_id.clone(),
                self.controller_addr.clone(),
                network,
                req.tasks,
            );
            engine
                .start(StreamConfig {
                    restore_epoch: req.restore_epoch,
                })
                .await
        };

        let sources = engine.source_controls();

        let mut state = self.state.lock().unwrap();
        *state = Some(EngineState {
            sources,
            running_engine: engine,
        });

        info!("[{:?}] Started execution", self.id);

        Ok(Response::new(StartExecutionResp {}))
    }

    async fn checkpoint(
        &self,
        request: Request<CheckpointReq>,
    ) -> Result<Response<CheckpointResp>, Status> {
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

        let req = request.into_inner();

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
            engine.running_engine.stop();
        }

        tokio::task::spawn(async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            exit(0);
        });

        Ok(Response::new(JobFinishedResp {}))
    }
}
