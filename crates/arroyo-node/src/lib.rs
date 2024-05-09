use std::env::{current_exe, temp_dir};
use std::net::SocketAddr;
use std::{
    collections::HashMap,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use anyhow::{anyhow, bail};
use arroyo_rpc::grpc::{
    controller_grpc_client::ControllerGrpcClient, node_grpc_server::NodeGrpc,
    node_grpc_server::NodeGrpcServer, GetWorkersReq, GetWorkersResp, HeartbeatNodeReq,
    RegisterNodeReq, StartWorkerReq, StartWorkerResp, StopWorkerReq, StopWorkerResp,
    StopWorkerStatus, WorkerFinishedReq,
};
use arroyo_server_common::shutdown::ShutdownGuard;
use arroyo_server_common::wrap_start;
use arroyo_types::{
    default_controller_addr, grpc_port, ports, to_millis, to_nanos, NodeId, WorkerId,
    ARROYO_PROGRAM_FILE_ENV, CONTROLLER_ADDR_ENV, JOB_ID_ENV, NODE_ID_ENV, RUN_ID_ENV,
    TASK_SLOTS_ENV, WORKER_ID_ENV,
};
use base64::engine::general_purpose;
use base64::Engine;
use lazy_static::lazy_static;
use prometheus::{register_gauge, Gauge};
use rand::random;
use std::process::exit;
use tokio::sync::mpsc::{channel, Sender};
use tokio::{process::Command, select};
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

lazy_static! {
    static ref WORKERS: Gauge = register_gauge!(
        "arroyo_node_running_workers",
        "number of workers managed by this node"
    )
    .unwrap();
}

pub struct WorkerStatus {
    name: String,
    job_id: String,
    slots: usize,
    running: bool,
    pid: u32,
}

pub struct NodeServer {
    id: NodeId,
    worker_finished_tx: Sender<WorkerFinishedReq>,
    workers: Arc<Mutex<HashMap<WorkerId, WorkerStatus>>>,
}

async fn signal_process(signal: &str, pid: u32) -> bool {
    tokio::process::Command::new("kill")
        .arg("-s")
        .arg(signal)
        .arg(pid.to_string())
        .status()
        .await
        .map(|t| t.success())
        .unwrap_or(false)
}

impl NodeServer {
    async fn start_worker_int(&self, req: StartWorkerReq) -> anyhow::Result<WorkerId> {
        if req.node_id != self.id.0 {
            warn!(
                "incorrect node id for job {}, expected {}, got {}",
                req.job_id, self.id.0, req.node_id
            );
            bail!(
                "incorrect node_id, expected {}, got {}",
                self.id.0,
                req.node_id
            );
        }

        let dir =
            PathBuf::from_str(&format!("/tmp/arroyo-node-{}/{}", self.id.0, req.job_id,)).unwrap();
        tokio::fs::create_dir_all(&dir).await.unwrap();

        info!("Starting worker for job {}", req.job_id);

        // TODO: Check that we have enough slots to schedule this
        let slots = req.slots;
        let state = Arc::clone(&self.workers);
        let worker_id = WorkerId(random());
        let node_id = self.id;
        let finished_tx = self.worker_finished_tx.clone();
        let program = general_purpose::STANDARD_NO_PAD.encode(&req.program);

        let program_file = temp_dir()
            .join("arroyo")
            .join(&req.job_id)
            .join(format!("{}.program", to_nanos(SystemTime::now())));

        tokio::fs::create_dir_all(&program_file.parent().unwrap())
            .await
            .map_err(|e| anyhow!("Failed to create tmp dir for program file: {:?}", e))?;
        tokio::fs::write(&program_file, &program)
            .await
            .map_err(|e| anyhow!("Failed to write program file to tmp dir: {:?}", e))?;

        let mut workers = self.workers.lock().unwrap();

        let mut command = Command::new(current_exe().expect("Could not get path of worker binary"));

        for (env, value) in req.env_vars {
            command.env(env, value);
        }

        let mut child = command
            .arg("worker")
            .env("RUST_LOG", "info")
            .env(WORKER_ID_ENV, format!("{}", worker_id.0))
            .env(NODE_ID_ENV, format!("{}", node_id.0))
            .env(JOB_ID_ENV, req.job_id.clone())
            .env(TASK_SLOTS_ENV, format!("{}", slots))
            .env(RUN_ID_ENV, format!("{}", req.run_id))
            .env(ARROYO_PROGRAM_FILE_ENV, program_file.to_str().unwrap())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| Status::internal(format!("Failed to start worker: {:?}", e)))?;

        workers.insert(
            worker_id,
            WorkerStatus {
                name: req.name,
                job_id: req.job_id.clone(),
                slots: slots as usize,
                running: true,
                pid: child
                    .id()
                    .ok_or_else(|| Status::internal("Could not get pid from process"))?,
            },
        );

        let job_id = req.job_id;
        tokio::spawn(async move {
            let status = child.wait().await;

            info!(
                message = "child exited",
                code = status.map(|s| s.code()).unwrap_or(None),
                job_id
            );

            WORKERS.dec();
            finished_tx
                .send(WorkerFinishedReq {
                    node_id: node_id.0,
                    worker_id: worker_id.0,
                    slots,
                    job_id,
                })
                .await
                .unwrap();

            let mut workers = state.lock().unwrap();
            workers.get_mut(&worker_id).unwrap().running = false;
        });

        WORKERS.inc();

        Ok(worker_id)
    }
}

#[tonic::async_trait]
impl NodeGrpc for NodeServer {
    async fn start_worker(
        &self,
        request: Request<StartWorkerReq>,
    ) -> Result<Response<StartWorkerResp>, Status> {
        let req = request.into_inner();

        let worker_id = self
            .start_worker_int(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(StartWorkerResp {
            worker_id: worker_id.0,
        }))
    }

    async fn stop_worker(
        &self,
        request: Request<StopWorkerReq>,
    ) -> Result<Response<StopWorkerResp>, Status> {
        let req = request.into_inner();

        let (running, pid, job_id) = {
            let workers = self.workers.lock().unwrap();

            let Some(worker) = workers.get(&WorkerId(req.worker_id)) else {
                return Ok(Response::new(StopWorkerResp {
                    status: StopWorkerStatus::NotFound.into(),
                }));
            };

            (worker.running, worker.pid, worker.job_id.clone())
        };

        let status = if running {
            info!(
                message = "stopping worker",
                worker_id = req.worker_id,
                job_id,
                force = req.force
            );

            let signal = if req.force { "KILL" } else { "TERM" };
            signal_process(signal, pid).await
        } else {
            info!(
                message = "not stopping worker; already stopped",
                worker_id = req.worker_id,
                job_id
            );
            true
        };
        let status = if status {
            StopWorkerStatus::Stopped
        } else {
            StopWorkerStatus::StopFailed
        };

        Ok(Response::new(StopWorkerResp {
            status: status.into(),
        }))
    }

    async fn get_workers(
        &self,
        _: Request<GetWorkersReq>,
    ) -> Result<Response<GetWorkersResp>, Status> {
        let workers = self.workers.lock().unwrap();

        let statuses: Vec<_> = workers
            .values()
            .map(|w| arroyo_rpc::grpc::WorkerStatus {
                name: w.name.clone(),
                slots: w.slots as u64,
                running: w.running,
            })
            .collect();

        Ok(Response::new(GetWorkersResp { statuses }))
    }
}

pub async fn start_server(guard: ShutdownGuard) -> NodeId {
    let controller_addr =
        std::env::var(CONTROLLER_ADDR_ENV).unwrap_or_else(|_| default_controller_addr());

    let task_slots = std::env::var("NODE_SLOTS")
        .map(|s| usize::from_str(&s).unwrap())
        .unwrap_or(16);

    let grpc = grpc_port("node", ports::NODE_GRPC);

    let node_id = NodeId(random());

    let (worker_finished_tx, mut worker_finished_rx) = channel(128);

    let server = NodeServer {
        id: node_id,
        workers: Arc::new(Mutex::new(HashMap::new())),
        worker_finished_tx,
    };

    let bind_addr: SocketAddr = format!("0.0.0.0:{}", grpc).parse().unwrap();
    info!(
        "Starting node server on {} with {} slots",
        bind_addr, task_slots
    );

    guard.spawn_task(
        "grpc",
        wrap_start(
            "node",
            bind_addr.clone(),
            arroyo_server_common::grpc_server()
                .max_frame_size(Some((1 << 24) - 1)) // 16MB
                .add_service(NodeGrpcServer::new(server))
                .serve(bind_addr),
        ),
    );

    let req_addr = format!("{}:{}", local_ip_address::local_ip().unwrap(), grpc);

    // TODO: replace this with some sort of hook on server startup
    tokio::time::sleep(Duration::from_millis(100)).await;

    if guard.is_cancelled() {
        // don't register if the server failed to bind
        guard.cancel();
        return node_id;
    }

    guard.into_spawn_task(async move {
        let mut attempts = 0;
        loop {
            match ControllerGrpcClient::connect(controller_addr.clone()).await {
                Ok(mut controller) => {
                    controller
                        .register_node(Request::new(RegisterNodeReq {
                            node_id: node_id.0,
                            task_slots: task_slots as u64,
                            addr: req_addr.clone(),
                        }))
                        .await
                        .unwrap();

                    info!("Connected to controller");
                    loop {
                        select! {
                            _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                            msg = worker_finished_rx.recv() => {
                                controller.worker_finished(Request::new(msg.unwrap())).await
                                .unwrap_or_else(|err| {
                                    error!("shutting down: controller failed to report finished worker with {:?}", err);
                                    exit(1);
                                });
                            }
                        }

                        if let Err(e) = controller
                            .heartbeat_node(Request::new(HeartbeatNodeReq {
                                node_id: node_id.0,
                                time: to_millis(SystemTime::now()),
                            }))
                            .await
                        {
                            bail!("shutting down: controller failed heartbeat with {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    if attempts % 50 == 0 {
                        info!(
                            "failed to connect to controller on {}..., {:?}",
                            controller_addr, e
                        );
                    }

                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        #[allow(unreachable_code)]
        Ok(())
    });

    node_id
}
