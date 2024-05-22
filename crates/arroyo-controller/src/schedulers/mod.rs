use anyhow::bail;
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::grpc::node_grpc_client::NodeGrpcClient;
use arroyo_rpc::grpc::{
    api, HeartbeatNodeReq, RegisterNodeReq, StartWorkerReq, StopWorkerReq, StopWorkerStatus,
    WorkerFinishedReq,
};
use arroyo_types::{
    to_nanos, NodeId, WorkerId, ARROYO_PROGRAM_FILE_ENV, JOB_ID_ENV, NODE_ID_ENV, RUN_ID_ENV,
    SLOTS_PER_NODE, TASK_SLOTS_ENV, WORKER_ID_ENV,
};
use base64::{engine::general_purpose, Engine as _};
use lazy_static::lazy_static;
use prometheus::{register_gauge, Gauge};
use prost::Message;
use std::collections::HashMap;
use std::env::{current_exe, temp_dir};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::process::Command;
use tokio::sync::{oneshot, Mutex};
use tonic::{Request, Status};
use tracing::{info, warn};
pub mod embedded;
pub mod kubernetes;

lazy_static! {
    static ref FREE_SLOTS: Gauge =
        register_gauge!("arroyo_controller_free_slots", "number of free task slots").unwrap();
    static ref REGISTERED_SLOTS: Gauge = register_gauge!(
        "arroyo_controller_registered_slots",
        "total number of registered task slots"
    )
    .unwrap();
    static ref REGISTERED_NODES: Gauge = register_gauge!(
        "arroyo_controller_registered_nodes",
        "total number of registered nodes"
    )
    .unwrap();
}

#[async_trait::async_trait]
pub trait Scheduler: Send + Sync {
    async fn start_workers(
        &self,
        start_pipeline_req: StartPipelineReq,
    ) -> Result<(), SchedulerError>;

    async fn register_node(&self, req: RegisterNodeReq);
    async fn heartbeat_node(&self, req: HeartbeatNodeReq) -> Result<(), Status>;
    async fn worker_finished(&self, req: WorkerFinishedReq);
    async fn stop_workers(
        &self,
        job_id: &str,
        run_id: Option<i64>,
        force: bool,
    ) -> anyhow::Result<()>;
    async fn workers_for_job(
        &self,
        job_id: &str,
        run_id: Option<i64>,
    ) -> anyhow::Result<Vec<WorkerId>>;
}

pub struct ProcessWorker {
    job_id: Arc<String>,
    run_id: i64,
    shutdown_tx: oneshot::Sender<()>,
}

/// This Scheduler starts new processes to run the worker nodes
pub struct ProcessScheduler {
    workers: Arc<Mutex<HashMap<WorkerId, ProcessWorker>>>,
    worker_counter: AtomicU64,
}

impl ProcessScheduler {
    pub fn new() -> Self {
        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            worker_counter: AtomicU64::new(100),
        }
    }
}

const DEFAULT_SLOTS_PER_NODE: u32 = 16;

pub struct StartPipelineReq {
    pub name: String,
    pub program: LogicalProgram,
    pub wasm_path: String,
    pub job_id: Arc<String>,
    pub hash: String,
    pub run_id: i64,
    pub slots: usize,
    pub env_vars: HashMap<String, String>,
}

#[async_trait::async_trait]
impl Scheduler for ProcessScheduler {
    async fn start_workers(
        &self,
        start_pipeline_req: StartPipelineReq,
    ) -> Result<(), SchedulerError> {
        let slots_per_node = arroyo_types::u32_config(SLOTS_PER_NODE, DEFAULT_SLOTS_PER_NODE);

        let workers = (start_pipeline_req.slots as f32 / slots_per_node as f32).ceil() as usize;

        let mut slots_scheduled = 0;

        let base_path = PathBuf::from_str(&format!(
            "/tmp/arroyo-process/{}",
            start_pipeline_req.job_id
        ))
        .unwrap();

        let program = start_pipeline_req.program.clone();
        let program = general_purpose::STANDARD_NO_PAD
            .encode(api::ArrowProgram::from(program).encode_to_vec());

        let program_file = temp_dir()
            .join("arroyo")
            .join(&*start_pipeline_req.job_id)
            .join(format!("{}.program", to_nanos(SystemTime::now())));

        tokio::fs::create_dir_all(&program_file.parent().unwrap())
            .await
            .map_err(|e| {
                SchedulerError::Other(format!(
                    "Failed to create tmp dir for program file: {:?}",
                    e
                ))
            })?;
        tokio::fs::write(&program_file, &program)
            .await
            .map_err(|e| {
                SchedulerError::Other(format!("Failed to write program file to tmp dir: {:?}", e))
            })?;

        for _ in 0..workers {
            let path = base_path.clone();

            let slots_here =
                (start_pipeline_req.slots - slots_scheduled).min(slots_per_node as usize);

            let worker_id = self.worker_counter.fetch_add(1, Ordering::SeqCst);

            let (tx, rx) = oneshot::channel();

            {
                let mut workers = self.workers.lock().await;
                workers.insert(
                    WorkerId(worker_id),
                    ProcessWorker {
                        job_id: start_pipeline_req.job_id.clone(),
                        run_id: start_pipeline_req.run_id,
                        shutdown_tx: tx,
                    },
                );
            }

            slots_scheduled += slots_here;
            let job_id = start_pipeline_req.job_id.clone();
            let workers = self.workers.clone();
            let env_map = start_pipeline_req.env_vars.clone();
            let program_file = program_file.to_str().unwrap().to_string();

            tokio::spawn(async move {
                let mut command =
                    Command::new(current_exe().expect("Could not get path of worker binary"));

                for (env, value) in env_map {
                    command.env(env, value);
                }
                let mut child = command
                    .arg("worker")
                    .env("RUST_LOG", "info")
                    .env(TASK_SLOTS_ENV, format!("{}", slots_here))
                    .env(WORKER_ID_ENV, format!("{}", worker_id)) // start at 100 to make same length
                    .env(JOB_ID_ENV, &*job_id)
                    .env(NODE_ID_ENV, format!("{}", 1))
                    .env(RUN_ID_ENV, format!("{}", start_pipeline_req.run_id))
                    .env(ARROYO_PROGRAM_FILE_ENV, program_file)
                    .kill_on_drop(true)
                    .spawn()
                    .unwrap();

                tokio::select! {
                    status = child.wait() => {
                        info!("Child ({:?}) exited with status {:?}", path, status);
                    }
                    _ = rx => {
                        info!(message = "Killing child", worker_id = worker_id, job_id = *job_id);
                        child.kill().await.unwrap();
                    }
                }

                let mut state = workers.lock().await;
                state.remove(&WorkerId(worker_id));
            });
        }

        Ok(())
    }

    async fn register_node(&self, _: RegisterNodeReq) {}
    async fn heartbeat_node(&self, _: HeartbeatNodeReq) -> Result<(), Status> {
        Ok(())
    }
    async fn worker_finished(&self, _: WorkerFinishedReq) {}

    async fn workers_for_job(
        &self,
        job_id: &str,
        run_id: Option<i64>,
    ) -> anyhow::Result<Vec<WorkerId>> {
        Ok(self
            .workers
            .lock()
            .await
            .iter()
            .filter(|(_, w)| {
                *w.job_id == job_id && (run_id.is_none() || w.run_id == run_id.unwrap())
            })
            .map(|(k, _)| *k)
            .collect())
    }

    async fn stop_workers(
        &self,
        job_id: &str,
        run_id: Option<i64>,
        _force: bool,
    ) -> anyhow::Result<()> {
        for worker_id in self.workers_for_job(job_id, run_id).await? {
            let worker = {
                let mut state = self.workers.lock().await;
                let Some(worker) = state.remove(&worker_id) else {
                    return Ok(());
                };
                worker
            };

            let _ = worker.shutdown_tx.send(());
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct NodeStatus {
    id: NodeId,
    free_slots: usize,
    scheduled_slots: HashMap<WorkerId, usize>,
    addr: String,
    last_heartbeat: Instant,
}

impl NodeStatus {
    fn new(id: NodeId, slots: usize, addr: String) -> NodeStatus {
        FREE_SLOTS.add(slots as f64);
        REGISTERED_SLOTS.add(slots as f64);

        NodeStatus {
            id,
            free_slots: slots,
            scheduled_slots: HashMap::new(),
            addr,
            last_heartbeat: Instant::now(),
        }
    }

    fn take_slots(&mut self, worker: WorkerId, slots: usize) {
        if let Some(v) = self.free_slots.checked_sub(slots) {
            FREE_SLOTS.sub(slots as f64);
            self.free_slots = v;
            self.scheduled_slots.insert(worker, slots);
        } else {
            panic!(
                "Attempted to schedule more slots than are available on node {} ({} < {})",
                self.addr, self.free_slots, slots
            );
        }
    }

    fn release_slots(&mut self, worker_id: WorkerId, slots: usize) {
        if let Some(freed) = self.scheduled_slots.remove(&worker_id) {
            assert_eq!(freed, slots,
                "Controller and node disagree about how many slots are scheduled for worker {:?} ({} != {})",
                worker_id, freed, slots);

            self.free_slots += slots;

            FREE_SLOTS.add(slots as f64);
        } else {
            warn!(
                "Received release request for unknown worker {:?}",
                worker_id
            );
        }
    }
}

#[derive(Clone)]
struct NodeWorker {
    job_id: Arc<String>,
    node_id: NodeId,
    run_id: i64,
    running: bool,
}

#[derive(Default)]
pub struct NodeSchedulerState {
    nodes: HashMap<NodeId, NodeStatus>,
    workers: HashMap<WorkerId, NodeWorker>,
}

impl NodeSchedulerState {
    fn expire_nodes(&mut self, expiration_time: Instant) {
        let expired_nodes: Vec<_> = self
            .nodes
            .iter()
            .filter_map(|(node_id, status)| {
                if status.last_heartbeat >= expiration_time {
                    None
                } else {
                    Some(*node_id)
                }
            })
            .collect();
        for node_id in expired_nodes {
            warn!("expiring node {:?} from scheduler state", node_id);
            self.nodes.remove(&node_id);
        }
    }
}

pub struct NodeScheduler {
    state: Arc<Mutex<NodeSchedulerState>>,
}

pub enum SchedulerError {
    NotEnoughSlots { slots_needed: usize },
    Other(String),
    CompilationNeeded,
}

impl NodeScheduler {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(NodeSchedulerState::default())),
        }
    }

    async fn stop_worker(
        &self,
        job_id: &str,
        worker_id: WorkerId,
        force: bool,
    ) -> anyhow::Result<Option<WorkerId>> {
        let state = self.state.lock().await;

        let Some(worker) = state.workers.get(&worker_id) else {
            // assume it's already finished
            return Ok(Some(worker_id));
        };

        let Some(node) = state.nodes.get(&worker.node_id) else {
            warn!(
                message = "node not found for stop worker",
                node_id = worker.node_id.0
            );
            return Ok(Some(worker_id));
        };

        let worker = worker.clone();
        let node = node.clone();
        drop(state);

        info!(
            message = "stopping worker",
            job_id = *worker.job_id,
            node_id = worker.node_id.0,
            node_addr = node.addr,
            worker_id = worker_id.0
        );

        let Ok(mut client) = NodeGrpcClient::connect(format!("http://{}", node.addr)).await else {
            warn!("Failed to connect to worker to stop; this likely means it is dead");
            return Ok(Some(worker_id));
        };

        let Ok(resp) = client
            .stop_worker(Request::new(StopWorkerReq {
                job_id: job_id.to_string(),
                worker_id: worker_id.0,
                force,
            }))
            .await
        else {
            warn!("Failed to connect to worker to stop; this likely means it is dead");
            return Ok(Some(worker_id));
        };

        match (resp.get_ref().status(), force) {
            (StopWorkerStatus::NotFound, false) => {
                bail!("couldn't find worker, will only continue if force")
            }
            (StopWorkerStatus::StopFailed, _) => bail!("tried to kill and couldn't"),
            _ => Ok(None),
        }
    }
}

#[async_trait::async_trait]
impl Scheduler for NodeScheduler {
    async fn register_node(&self, req: RegisterNodeReq) {
        let mut state = self.state.lock().await;
        if let std::collections::hash_map::Entry::Vacant(e) = state.nodes.entry(NodeId(req.node_id))
        {
            e.insert(NodeStatus::new(
                NodeId(req.node_id),
                req.task_slots as usize,
                req.addr,
            ));
        }
    }

    async fn heartbeat_node(&self, req: HeartbeatNodeReq) -> Result<(), Status> {
        let mut state = self.state.lock().await;
        if let Some(node) = state.nodes.get_mut(&NodeId(req.node_id)) {
            node.last_heartbeat = Instant::now();
            Ok(())
        } else {
            warn!(
                "Received heartbeat for unregistered node {}, failing request",
                req.node_id
            );
            Err(Status::not_found(format!(
                "node {} not in scheduler's collection of nodes",
                req.node_id
            )))
        }
    }

    async fn worker_finished(&self, req: WorkerFinishedReq) {
        let mut state = self.state.lock().await;
        let worker_id = WorkerId(req.worker_id);

        if let Some(node) = state.nodes.get_mut(&NodeId(req.node_id)) {
            node.release_slots(worker_id, req.slots as usize);
        } else {
            warn!(
                "Got worker finished message for unknown node {}",
                req.node_id
            );
        }

        if state.workers.remove(&worker_id).is_none() {
            warn!(
                "Got worker finished message for unknown worker {}",
                worker_id.0
            );
        }
    }

    async fn workers_for_job(
        &self,
        job_id: &str,
        run_id: Option<i64>,
    ) -> anyhow::Result<Vec<WorkerId>> {
        let state = self.state.lock().await;
        Ok(state
            .workers
            .iter()
            .filter(|(_, v)| {
                *v.job_id == job_id
                    && v.running
                    && (run_id.is_none() || v.run_id == run_id.unwrap())
            })
            .map(|(w, _)| *w)
            .collect())
    }

    #[allow(unreachable_code, unused)]
    async fn start_workers(
        &self,
        start_pipeline_req: StartPipelineReq,
    ) -> Result<(), SchedulerError> {
        // TODO: make this locking more fine-grained
        let mut state = self.state.lock().await;

        state.expire_nodes(Instant::now() - Duration::from_secs(30));

        let free_slots = state.nodes.values().map(|n| n.free_slots).sum::<usize>();
        let slots = start_pipeline_req.slots;
        if slots > free_slots {
            return Err(SchedulerError::NotEnoughSlots {
                slots_needed: slots - free_slots,
            });
        }

        let mut to_schedule = slots;
        let mut slots_assigned = vec![];
        while to_schedule > 0 {
            // find the node with the most free slots and fill it
            let node = {
                if let Some(status) = state
                    .nodes
                    .values()
                    .filter(|n| {
                        n.free_slots > 0 && n.last_heartbeat.elapsed() < Duration::from_secs(30)
                    })
                    .max_by_key(|n| n.free_slots)
                    .cloned()
                {
                    status
                } else {
                    unreachable!();
                }
            };

            let slots_for_this_one = node.free_slots.min(to_schedule);
            info!(
                "Scheduling {} slots on node {}",
                slots_for_this_one, node.addr
            );

            let mut client = NodeGrpcClient::connect(format!("http://{}", node.addr))
                .await
                // TODO: handle this issue more gracefully by moving trying other nodes
                .map_err(|e| {
                    // release back slots already scheduled.
                    slots_assigned
                        .iter()
                        .for_each(|(node_id, worker_id, slots)| {
                            state
                                .nodes
                                .get_mut(node_id)
                                .unwrap()
                                .release_slots(*worker_id, *slots);
                        });
                    SchedulerError::Other(format!(
                        "Failed to connect to node {}: {:?}",
                        node.addr, e
                    ))
                })?;

            let req = StartWorkerReq {
                name: start_pipeline_req.name.clone(),
                job_id: (*start_pipeline_req.job_id).clone(),
                slots: slots_for_this_one as u64,
                node_id: node.id.0,
                run_id: start_pipeline_req.run_id as u64,
                env_vars: start_pipeline_req.env_vars.clone(),
                program: api::ArrowProgram::from(start_pipeline_req.program.clone())
                    .encode_to_vec(),
            };

            let res = client
                .start_worker(Request::new(req))
                .await
                .map_err(|e| {
                    // release back slots already scheduled.
                    slots_assigned
                        .iter()
                        .for_each(|(node_id, worker_id, slots)| {
                            state
                                .nodes
                                .get_mut(node_id)
                                .unwrap()
                                .release_slots(*worker_id, *slots);
                        });
                    SchedulerError::Other(format!(
                        "Failed to start worker on node {}: {:?}",
                        node.addr, e
                    ))
                })?
                .into_inner();

            state
                .nodes
                .get_mut(&node.id)
                .unwrap()
                .take_slots(WorkerId(res.worker_id), slots_for_this_one);

            state.workers.insert(
                WorkerId(res.worker_id),
                NodeWorker {
                    job_id: start_pipeline_req.job_id.clone(),
                    run_id: start_pipeline_req.run_id,
                    node_id: node.id,
                    running: true,
                },
            );

            slots_assigned.push((node.id, WorkerId(res.worker_id), slots_for_this_one));

            to_schedule -= slots_for_this_one;
        }
        Ok(())
    }

    async fn stop_workers(
        &self,
        job_id: &str,
        run_id: Option<i64>,
        force: bool,
    ) -> anyhow::Result<()> {
        // iterate through all of the workers from workers_for_job and stop them in parallel
        let workers = self.workers_for_job(job_id, run_id).await?;
        let mut futures = vec![];
        for worker_id in workers {
            futures.push(self.stop_worker(job_id, worker_id, force));
        }

        for f in futures {
            match f.await? {
                Some(worker_id) => {
                    let mut state = self.state.lock().await;
                    if let Some(worker) = state.workers.get_mut(&worker_id) {
                        worker.running = false;
                    }
                }
                None => {
                    bail!("Failed to stop worker");
                }
            }
        }

        Ok(())
    }
}
