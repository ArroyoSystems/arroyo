use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use anyhow::{anyhow, bail};
use arroyo_rpc::grpc::{
    controller_grpc_client::ControllerGrpcClient, node_grpc_server::NodeGrpc,
    node_grpc_server::NodeGrpcServer, start_worker_req, GetWorkersReq, GetWorkersResp,
    HeartbeatNodeReq, RegisterNodeReq, StartWorkerReq, StartWorkerResp, StopWorkerReq,
    StopWorkerResp, StopWorkerStatus, WorkerFinishedReq,
};
use arroyo_server_common::shutdown::Shutdown;
use arroyo_types::{
    grpc_port, ports, to_millis, NodeId, WorkerId, CONTROLLER_ADDR_ENV, JOB_ID_ENV, NODE_ID_ENV,
    RUN_ID_ENV, TASK_SLOTS_ENV, WORKER_ID_ENV,
};
use lazy_static::lazy_static;
use prometheus::{register_gauge, Gauge};
use rand::Rng;
use std::os::unix::fs::PermissionsExt;
use std::process::exit;
use tokio::sync::mpsc::{channel, Sender};
use tokio::{fs::File, io::AsyncWriteExt, process::Command, select};
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, warn};

const MAX_BIN_SIZE: usize = 300 * 1024 * 1024;

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

async fn create_file_if_needed(path: &Path, contents: &[u8], mode: Option<u32>) {
    for i in 0..10 {
        if path.exists() {
            return;
        }
        match File::create(&path).await {
            Ok(mut file) => {
                file.write_all(contents).await.unwrap();
                if let Some(mode) = mode {
                    let mut perms = file.metadata().await.unwrap().permissions();
                    perms.set_mode(mode);
                    file.set_permissions(perms).await.unwrap();
                }
                return;
            }
            Err(err) => {
                if err.kind() == tokio::io::ErrorKind::AlreadyExists {
                    return;
                } else {
                    warn!("Failed to create file on attempt {}: {:?}", i, err);
                }
            }
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Exhausted attempts to create file");
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
    async fn start_worker_int(&self, mut s: Streaming<StartWorkerReq>) -> anyhow::Result<WorkerId> {
        let start_worker_req::Msg::Header(header) = s
            .next()
            .await
            .ok_or_else(|| anyhow!("Didn't receive header"))??
            .msg
            .unwrap()
        else {
            bail!("First message was not a header");
        };

        if header.node_id != self.id.0 {
            warn!(
                "incorrect node id for job {}, expected {}, got {}",
                header.job_id, self.id.0, header.node_id
            );
            bail!(
                "incorrect node_id, expected {}, got {}",
                self.id.0,
                header.node_id
            );
        }

        info!("Receiving binary for job {}", header.job_id);

        let dir = PathBuf::from_str(&format!("/tmp/arroyo-node-{}/{}", self.id.0, header.job_id,))
            .unwrap();
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let wasm = dir.join("wasm_fns_bg.wasm");
        create_file_if_needed(&wasm, &header.wasm, None).await;

        // TODO: write the file as bytes are streamed in

        let mut buf = vec![0; (header.binary_size as usize).min(MAX_BIN_SIZE)];
        let mut bytes = 0;
        let mut next_part = 0;
        loop {
            let next = s
                .next()
                .await
                .ok_or_else(|| anyhow!("Closed before sending all parts"))??;

            let start_worker_req::Msg::Data(data) = next.msg.unwrap() else {
                bail!("Expected data message");
            };

            if next_part != data.part {
                bail!("Expected part {}, received part {}", next_part, data.part);
            }
            next_part += 1;

            buf[bytes..bytes + data.data.len()].copy_from_slice(&data.data);
            bytes += data.data.len();

            if !data.has_more {
                break;
            }
        }

        let bin = dir.join("pipeline");
        create_file_if_needed(&bin, &buf, Some(0o776)).await;
        drop(buf);

        info!("Starting worker for job {}", header.job_id);

        // TODO: Check that we have enough slots to schedule this
        let slots = header.slots;
        let state = Arc::clone(&self.workers);
        let worker_id = WorkerId(rand::thread_rng().gen());
        let node_id = self.id;
        let finished_tx = self.worker_finished_tx.clone();

        let mut workers = self.workers.lock().unwrap();

        let mut command = Command::new("./pipeline");
        for (env, value) in header.env_vars {
            command.env(env, value);
        }
        let mut child = command
            .env("RUST_LOG", "info")
            .env(WORKER_ID_ENV, format!("{}", worker_id.0))
            .env(NODE_ID_ENV, format!("{}", node_id.0))
            .env(JOB_ID_ENV, header.job_id.clone())
            .env(TASK_SLOTS_ENV, format!("{}", slots))
            .env(RUN_ID_ENV, format!("{}", header.run_id))
            .current_dir(&dir)
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| Status::internal(format!("Failed to start worker: {:?}", e)))?;

        workers.insert(
            worker_id,
            WorkerStatus {
                name: header.name,
                job_id: header.job_id.clone(),
                slots: slots as usize,
                running: true,
                pid: child
                    .id()
                    .ok_or_else(|| Status::internal("Could not get pid from process"))?,
            },
        );

        let job_id = header.job_id;
        tokio::spawn(async move {
            let status = child.wait().await;

            info!(
                message = "child exited",
                path = format!("{:?}", bin),
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
        request: Request<Streaming<StartWorkerReq>>,
    ) -> Result<Response<StartWorkerResp>, Status> {
        let in_stream = request.into_inner();

        let worker_id = self
            .start_worker_int(in_stream)
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

#[tokio::main]
pub async fn main() {
    let controller_addr =
        std::env::var(CONTROLLER_ADDR_ENV).expect("CONTROLLER_ADDR env variable not set");

    let task_slots = std::env::var("NODE_SLOTS")
        .map(|s| usize::from_str(&s).unwrap())
        .unwrap_or(16);

    let grpc = grpc_port("node", ports::NODE_GRPC);

    let node_id = NodeId(rand::thread_rng().gen());

    let _guard = arroyo_server_common::init_logging(&format!("node-{}", node_id.0));
    let (worker_finished_tx, mut worker_finished_rx) = channel(128);

    let server = NodeServer {
        id: node_id,
        workers: Arc::new(Mutex::new(HashMap::new())),
        worker_finished_tx,
    };

    let bind_addr = format!("0.0.0.0:{}", grpc);
    info!(
        "Starting node server on {} with {} slots",
        bind_addr, task_slots
    );

    let shutdown = Shutdown::new("node");

    shutdown.spawn_task(
        "admin",
        arroyo_server_common::start_admin_server("node", ports::NODE_ADMIN),
    );

    shutdown.spawn_task(
        "grpc",
        arroyo_server_common::grpc_server()
            .max_frame_size(Some((1 << 24) - 1)) // 16MB
            .add_service(NodeGrpcServer::new(server))
            .serve(bind_addr.parse().unwrap()),
    );

    let req_addr = format!("{}:{}", local_ip_address::local_ip().unwrap(), grpc);

    // TODO: replace this with some sort of hook on server startup
    tokio::time::sleep(Duration::from_secs(1)).await;

    if shutdown.is_canceled() {
        // don't register if the server failed to bind
        exit(1);
    }

    shutdown.spawn_task("connect-thread", async move {
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
                            error!("shutting down: controller failed heartbeat with {:?}", e);
                            return;
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
    });

    let _ = shutdown.wait_for_shutdown(Duration::from_secs(30)).await;
}
