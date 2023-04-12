use anyhow::bail;
use arroyo_rpc::grpc::node_grpc_client::NodeGrpcClient;
use arroyo_rpc::grpc::{
    HeartbeatNodeReq, RegisterNodeReq, StartWorkerReq, StopWorkerReq, WorkerFinishedReq,
};
use arroyo_types::{
    NodeId, WorkerId, CONTROLLER_ADDR_ENV, JOB_ID_ENV, NODE_ID_ENV, NOMAD_DC_ENV,
    NOMAD_ENDPOINT_ENV, RUN_ID_ENV, TASK_SLOTS_ENV, WORKER_ID_ENV,
};
use lazy_static::lazy_static;
use prometheus::{register_gauge, Gauge};
use rand::Rng;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::os::unix::prelude::PermissionsExt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::process::Command;
use tokio::sync::{oneshot, Mutex};
use tonic::{Request, Status};
use tracing::{info, warn};

use crate::get_from_object_store;

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
    async fn stop_worker(&self, req: StopWorkerReq) -> anyhow::Result<()>;
    async fn workers_for_job(&self, job_id: &str) -> anyhow::Result<Vec<WorkerId>>;

    async fn clean_cluster(&self, job_id: &str) -> anyhow::Result<()> {
        for worker in self.workers_for_job(job_id).await? {
            self.stop_worker(StopWorkerReq {
                job_id: job_id.to_string(),
                worker_id: worker.0,
                force: true,
            })
            .await?;
        }

        Ok(())
    }
}

pub struct ProcessWorker {
    job_id: String,
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

const SLOTS_PER_NODE: usize = 16;

const SLOTS_PER_NOMAD_NODE: usize = 15;
const MEMORY_PER_SLOT_MB: usize = 60_000 / SLOTS_PER_NOMAD_NODE;
const CPU_PER_SLOT_MHZ: usize = 3400;

pub struct StartPipelineReq {
    pub name: String,
    pub pipeline_path: String,
    pub wasm_path: String,
    pub job_id: String,
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
        let workers = (start_pipeline_req.slots as f32 / SLOTS_PER_NODE as f32).ceil() as usize;

        let mut slots_scheduled = 0;

        let pipeline = get_from_object_store(&start_pipeline_req.pipeline_path)
            .await
            .unwrap();
        let wasm = get_from_object_store(&start_pipeline_req.wasm_path)
            .await
            .unwrap();
        let base_path = PathBuf::from_str(&format!(
            "/tmp/arroyo-process/{}",
            start_pipeline_req.job_id
        ))
        .unwrap();
        tokio::fs::create_dir_all(&base_path).await.unwrap();

        let pipeline_path = base_path.join("pipeline");

        if !pipeline_path.exists() {
            tokio::fs::write(&pipeline_path, pipeline).await.unwrap();
            let file = tokio::fs::File::open(&pipeline_path).await.unwrap();

            let mut perms = file.metadata().await.unwrap().permissions();
            perms.set_mode(0o776);
            file.set_permissions(perms).await.unwrap();

            tokio::fs::write(&base_path.join("wasm_fns_bg.wasm"), wasm)
                .await
                .unwrap();
        }

        for _ in 0..workers {
            let path = base_path.clone();

            let slots_here =
                (start_pipeline_req.slots as usize - slots_scheduled).min(SLOTS_PER_NODE);

            let worker_id = self.worker_counter.fetch_add(1, Ordering::SeqCst);

            let (tx, rx) = oneshot::channel();

            {
                let mut workers = self.workers.lock().await;
                workers.insert(
                    WorkerId(worker_id),
                    ProcessWorker {
                        job_id: start_pipeline_req.job_id.clone(),
                        shutdown_tx: tx,
                    },
                );
            }

            slots_scheduled += slots_here;
            let job_id = start_pipeline_req.job_id.clone();
            println!("Starting in path {:?}", path);
            let workers = self.workers.clone();
            let env_map = start_pipeline_req.env_vars.clone();
            tokio::spawn(async move {
                let mut command = Command::new("./pipeline");
                for (env, value) in env_map {
                    command.env(env, value);
                }
                let mut child = command
                    .current_dir(&path)
                    .env("RUST_LOG", "info")
                    .env(TASK_SLOTS_ENV, format!("{}", slots_here))
                    .env(WORKER_ID_ENV, format!("{}", worker_id)) // start at 100 to make same length
                    .env(JOB_ID_ENV, &job_id)
                    .env(NODE_ID_ENV, format!("{}", 1))
                    .env(RUN_ID_ENV, format!("{}", start_pipeline_req.run_id))
                    .kill_on_drop(true)
                    .spawn()
                    .unwrap();

                tokio::select! {
                    status = child.wait() => {
                        info!("Child ({:?}) exited with status {:?}", path, status);
                    }
                    _ = rx => {
                        info!(message = "Killing child", worker_id = worker_id, job_id = job_id);
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

    async fn workers_for_job(&self, job_id: &str) -> anyhow::Result<Vec<WorkerId>> {
        Ok(self
            .workers
            .lock()
            .await
            .iter()
            .filter(|(_, w)| w.job_id == job_id)
            .map(|(k, _)| *k)
            .collect())
    }

    async fn stop_worker(&self, req: StopWorkerReq) -> anyhow::Result<()> {
        let worker = {
            let mut state = self.workers.lock().await;
            let Some(worker) = state.remove(&WorkerId(req.worker_id)) else {
                return Ok(());
            };
            worker
        };

        let _ = worker.shutdown_tx.send(());

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

struct NodeWorker {
    job_id: String,
    node_id: NodeId,
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
                    Some(node_id.clone())
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
}

impl NodeScheduler {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(NodeSchedulerState::default())),
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

    async fn workers_for_job(&self, job_id: &str) -> anyhow::Result<Vec<WorkerId>> {
        let state = self.state.lock().await;
        Ok(state
            .workers
            .iter()
            .filter(|(_, v)| v.job_id == job_id)
            .map(|(w, _)| *w)
            .collect())
    }

    async fn start_workers(
        &self,
        start_pipeline_req: StartPipelineReq,
    ) -> Result<(), SchedulerError> {
        let binary = get_from_object_store(&start_pipeline_req.pipeline_path)
            .await
            .unwrap();
        let wasm = get_from_object_store(&start_pipeline_req.wasm_path)
            .await
            .unwrap();

        // TODO: make this locking more fine-grained
        let mut state = self.state.lock().await;

        state.expire_nodes(Instant::now() - Duration::from_secs(30));

        let free_slots = state.nodes.values().map(|n| n.free_slots).sum::<usize>();
        let slots = start_pipeline_req.slots as usize;
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

            let res = client
                .start_worker(Request::new(StartWorkerReq {
                    name: start_pipeline_req.name.clone(),
                    job_id: start_pipeline_req.job_id.clone(),
                    binary: binary.clone(),
                    wasm: wasm.clone(),
                    job_hash: start_pipeline_req.hash.clone(),
                    slots: slots_for_this_one as u64,
                    node_id: node.id.0,
                    run_id: start_pipeline_req.run_id as u64,
                    env_vars: start_pipeline_req.env_vars.clone(),
                }))
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
                    node_id: node.id,
                },
            );

            slots_assigned.push((node.id, WorkerId(res.worker_id), slots_for_this_one));

            to_schedule -= slots_for_this_one;
        }
        Ok(())
    }

    async fn stop_worker(&self, req: StopWorkerReq) -> anyhow::Result<()> {
        let state = self.state.lock().await;

        let Some(worker) = state.workers.get(&WorkerId(req.worker_id)) else {
            // assume it's already finished
            return Ok(());
        };

        let Some(node) = state.nodes.get(&worker.node_id) else {
            warn!(message = "node not found for stop worker", node_id = worker.node_id.0);
            return Ok(());
        };

        info!(
            message = "stopping worker",
            job_id = worker.job_id,
            node_id = worker.node_id.0,
            node_addr = node.addr,
            worker_id = req.worker_id
        );

        let mut client = NodeGrpcClient::connect(format!("http://{}", node.addr)).await?;

        if !client
            .stop_worker(Request::new(req))
            .await?
            .get_ref()
            .stopped
        {
            bail!("failed to stop worker");
        }

        Ok(())
    }
}

pub struct NomadScheduler {
    client: reqwest::Client,
    base: String,
    datacenter: String,
}

impl NomadScheduler {
    pub fn new() -> Self {
        let endpoint = std::env::var(NOMAD_ENDPOINT_ENV)
            .unwrap_or_else(|_| "http://localhost:4646".to_string());

        let datacenter = std::env::var(NOMAD_DC_ENV).unwrap_or_else(|_| "dc1".to_string());

        Self {
            client: reqwest::Client::new(),
            base: endpoint,
            datacenter,
        }
    }
}

#[async_trait::async_trait]
impl Scheduler for NomadScheduler {
    async fn start_workers(
        &self,
        start_pipeline_req: StartPipelineReq,
    ) -> Result<(), SchedulerError> {
        let slots = start_pipeline_req.slots as usize;
        let workers = (slots as f32 / SLOTS_PER_NOMAD_NODE as f32).ceil() as usize;
        let mut slots_scheduled = 0;

        for _ in 0..workers {
            let slots_here = (slots - slots_scheduled).min(SLOTS_PER_NOMAD_NODE);

            let worker_id: u32 = rand::thread_rng().gen();

            slots_scheduled += slots_here;

            let mut env_vars = HashMap::new();
            env_vars.insert("RUST_LOG".to_string(), "info".to_string());
            env_vars.insert("PROD".to_string(), "true".to_string());
            env_vars.insert(TASK_SLOTS_ENV.to_string(), slots_here.to_string());
            env_vars.insert(WORKER_ID_ENV.to_string(), worker_id.to_string());
            env_vars.insert(NODE_ID_ENV.to_string(), "1".to_string());
            env_vars.insert(
                RUN_ID_ENV.to_string(),
                format!("{}", start_pipeline_req.run_id),
            );
            env_vars.insert(
                CONTROLLER_ADDR_ENV.to_string(),
                std::env::var(CONTROLLER_ADDR_ENV).unwrap_or_else(|_| "".to_string()),
            );
            for (key, value) in start_pipeline_req.env_vars.iter() {
                env_vars.insert(key.to_string(), value.to_string());
            }

            let job = json!({
                "Job": {
                    "ID": format!("{}-{}", start_pipeline_req.job_id, worker_id),
                    "Type": "batch",
                    "Datacenters": [&self.datacenter],
                    "Meta": {
                        "job_id": start_pipeline_req.job_id,
                        "worker_id": worker_id.to_string(),
                        "job_name": start_pipeline_req.name,
                        "job_hash": start_pipeline_req.hash,
                    },

                    // disable restarting and rescheduling as the controller takes care of handling failures
                    "Restart":  {
                        "Attempts": 0,
                        "Mode": "fail"
                    },
                    "Reschedule": {
                        "Attempts": 0
                    },

                    "TaskGroups": [
                        {
                            "Name": "worker",
                            "Count": 1,
                            "Tasks": [
                                {
                                    "Name": "worker",
                                    "Driver": "exec",
                                    "Config": {
                                        "command": "pipeline"
                                    },
                                    "Artifacts": [{
                                        "GetterSource": start_pipeline_req.pipeline_path,
                                    }],
                                    "Env": env_vars,
                                    "Resources": {
                                        "CPU": CPU_PER_SLOT_MHZ * slots_here,
                                        "MemoryMB": MEMORY_PER_SLOT_MB * slots_here,
                                    }
                                }
                            ],
                        }
                    ]
                }
            });

            let resp = self
                .client
                .post(format!("{}/v1/jobs", self.base))
                .json(&job)
                .send()
                .await
                .map_err(|e| SchedulerError::Other(format!("{:?}", e)))?;

            if !resp.status().is_success() {
                return match resp.bytes().await {
                    Ok(bytes) => Err(SchedulerError::Other(format!(
                        "Error scheduling: {:?}",
                        String::from_utf8_lossy(&bytes)
                    ))),
                    Err(e) => Err(SchedulerError::Other(format!("Error scheduling: {:?}", e))),
                };
            }
        }

        Ok(())
    }

    async fn workers_for_job(&self, job_id: &str) -> anyhow::Result<Vec<WorkerId>> {
        let resp: Value = self
            .client
            // TODO: use the filter API instead
            .get(format!(
                "{}/v1/jobs?meta=true&prefix={}-",
                self.base, job_id
            ))
            .send()
            .await?
            .json()
            .await?;

        let jobs = resp
            .as_array()
            .ok_or(anyhow::anyhow!("invalid response from nomad api"))?;

        let mut workers = vec![];

        for job in jobs {
            let worker_id = job
                .pointer("/Meta/worker_id")
                .ok_or(anyhow::anyhow!("missing worker id"))?;

            if job
                .get("Status")
                .ok_or(anyhow::anyhow!("missing status"))?
                .as_str()
                == Some("dead")
            {
                continue;
            }

            workers.push(WorkerId(u64::from_str(
                worker_id
                    .as_str()
                    .ok_or(anyhow::anyhow!("worker id is not a string"))?,
            )?));
        }

        Ok(workers)
    }

    async fn register_node(&self, _req: RegisterNodeReq) {
        // ignore
    }

    async fn heartbeat_node(&self, _req: HeartbeatNodeReq) -> Result<(), Status> {
        // ignore
        Ok(())
    }

    async fn worker_finished(&self, _req: WorkerFinishedReq) {
        // ignore
    }

    async fn stop_worker(&self, req: StopWorkerReq) -> anyhow::Result<()> {
        info!(
            message = "stopping worker",
            job_id = req.job_id,
            worker_id = req.worker_id
        );
        let resp = self
            .client
            .delete(format!(
                "{}/v1/job/{}-{}",
                self.base, req.job_id, req.worker_id
            ))
            .send()
            .await?;

        if !resp.status().is_success() {
            warn!(
                message = "failed to stop worker",
                job_id = req.job_id,
                worker_id = req.worker_id,
                status = resp.status().as_u16()
            );
        }

        Ok(())
    }
}
