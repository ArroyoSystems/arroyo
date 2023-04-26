use crate::schedulers::{Scheduler, SchedulerError, StartPipelineReq};
use arroyo_rpc::grpc::{HeartbeatNodeReq, RegisterNodeReq, WorkerFinishedReq};
use arroyo_types::{
    WorkerId, CONTROLLER_ADDR_ENV, JOB_ID_ENV, NODE_ID_ENV, NOMAD_DC_ENV, NOMAD_ENDPOINT_ENV,
    RUN_ID_ENV, TASK_SLOTS_ENV, WORKER_ID_ENV,
};
use rand::Rng;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::str::FromStr;
use tonic::Status;
use tracing::{info, warn};

const SLOTS_PER_NOMAD_NODE: usize = 15;
const MEMORY_PER_SLOT_MB: usize = 60_000 / SLOTS_PER_NOMAD_NODE;
const CPU_PER_SLOT_MHZ: usize = 3400;

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

    async fn stop_worker(
        &self,
        job_id: &str,
        worker_id: WorkerId,
        _force: bool,
    ) -> anyhow::Result<()> {
        let resp = self
            .client
            .delete(format!("{}/v1/job/{}-{}", self.base, job_id, worker_id.0))
            .send()
            .await?;

        if !resp.status().is_success() {
            warn!(
                message = "failed to stop worker",
                job_id = job_id,
                worker_id = worker_id.0,
                status = resp.status().as_u16()
            );
        }

        Ok(())
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
            env_vars.insert(JOB_ID_ENV.to_string(), start_pipeline_req.job_id.clone());
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
                    "ID": format!("{}-{}-{}", start_pipeline_req.job_id, start_pipeline_req.run_id, worker_id),
                    "Type": "batch",
                    "Datacenters": [&self.datacenter],
                    "Meta": {
                        "job_id": start_pipeline_req.job_id,
                        "worker_id": worker_id.to_string(),
                        "run_id": start_pipeline_req.run_id,
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

    async fn workers_for_job(
        &self,
        job_id: &str,
        run_id: Option<i64>,
    ) -> anyhow::Result<Vec<WorkerId>> {
        let prefix = if let Some(run_id) = run_id {
            format!("{}-{}-", job_id, run_id)
        } else {
            format!("{}-", job_id)
        };

        let resp: Value = self
            .client
            // TODO: use the filter API instead
            .get(format!("{}/v1/jobs?meta=true&prefix={}", self.base, prefix))
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

    async fn stop_workers(
        &self,
        job_id: &str,
        run_id: Option<i64>,
        force: bool,
    ) -> anyhow::Result<()> {
        info!(message = "stopping workers", job_id = job_id,);

        let futures = self
            .workers_for_job(job_id, run_id)
            .await?
            .iter()
            .map(|worker| self.stop_worker(job_id, *worker, force))
            .collect::<Vec<_>>();

        for future in futures {
            future.await?;
        }

        Ok(())
    }
}
