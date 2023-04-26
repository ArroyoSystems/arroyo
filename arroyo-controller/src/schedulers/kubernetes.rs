use crate::schedulers::{Scheduler, SchedulerError, StartPipelineReq};
use anyhow::bail;
use arroyo_rpc::grpc::{HeartbeatNodeReq, RegisterNodeReq, WorkerFinishedReq};
use arroyo_types::{
    WorkerId, CONTROLLER_ADDR_ENV, GRPC_PORT_ENV, JOB_ID_ENV, K8S_IMAGE_ENV, K8S_POD_CPU_ENV,
    K8S_POD_MEM_MB_ENV, K8S_POD_SLOTS_ENV, NODE_ID_ENV, OUTPUT_DIR_ENV, RUN_ID_ENV, TASK_SLOTS_ENV,
};
use async_trait::async_trait;
use k8s_openapi::api::apps::v1::ReplicaSet;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{DeleteParams, ListParams};
use kube::{Api, Client};
use serde_json::json;
use std::env;
use std::str::FromStr;
use std::time::Duration;
use tonic::Status;

const JOB_ID_LABEL: &'static str = "arroyo.dev/job-id";
const RUN_ID_LABEL: &'static str = "arroyo.dev/run-id";
const JOB_NAME_LABEL: &'static str = "arroyo.dev/job-name";

const DEFAULT_CPU: u32 = 800;
const DEFAULT_MEM_MB: u32 = 400;
const DEFAULT_SLOTS: u32 = 4;

pub struct KubernetesScheduler {
    client: Client,
}

impl KubernetesScheduler {
    pub async fn new() -> Self {
        Self {
            client: Client::try_default().await.unwrap(),
        }
    }
}

#[async_trait]
impl Scheduler for KubernetesScheduler {
    async fn start_workers(&self, req: StartPipelineReq) -> Result<(), SchedulerError> {
        let api: Api<ReplicaSet> = Api::default_namespaced(self.client.clone());

        let image = env::var(K8S_IMAGE_ENV)
            .unwrap_or_else(|_| "ghcr.io/arroyosystems/arroyo-worker:amd64".to_string());

        let cpu = env::var(K8S_POD_CPU_ENV)
            .map(|t| u32::from_str(&t).unwrap_or(DEFAULT_CPU))
            .unwrap_or(DEFAULT_CPU);

        let mem = env::var(K8S_POD_MEM_MB_ENV)
            .map(|t| u32::from_str(&t).unwrap_or(DEFAULT_MEM_MB))
            .unwrap_or(DEFAULT_MEM_MB);

        let slots_per_pod = env::var(K8S_POD_SLOTS_ENV)
            .map(|t| u32::from_str(&t).unwrap_or(DEFAULT_SLOTS))
            .unwrap_or(DEFAULT_SLOTS);

        let slots = req.slots as usize;
        let replicas = (slots as f32 / slots_per_pod as f32).ceil() as usize;

        let mut volumes = vec![];
        let mut volume_mounts = vec![];

        if let Ok(output_dir) = env::var(OUTPUT_DIR_ENV) {
            volumes.push(json!(
                {
                    "name": "checkpoints",
                    "hostPath": {
                        "path": &output_dir,
                        "type": "Directory",
                    }
                }
            ));

            volume_mounts.push(json!(
                {
                    "name": "checkpoints",
                    "mountPath": output_dir,
                }
            ));
        }

        let labels = json!({
            JOB_ID_LABEL: req.job_id,
            RUN_ID_LABEL: format!("{}", req.run_id),
            JOB_NAME_LABEL: req.name,
        });

        let mut env = json!([
            {
                "name": "PROD", "value": "true",
            },
            {
                "name": TASK_SLOTS_ENV, "value": format!("{}", slots_per_pod),
            },
            {
                "name": NODE_ID_ENV, "value": "1",
            },
            {
                "name": JOB_ID_ENV, "value": req.job_id,
            },
            {
                "name": RUN_ID_ENV, "value": format!("{}", req.run_id),
            },
            {
                "name": GRPC_PORT_ENV, "value": "8000",
            },
            {
                "name": "WORKER_BIN",
                "value": req.pipeline_path,
            },
            {
                "name": "WASM_BIN",
                "value": req.wasm_path
            },
        ]);

        if let Ok(addr) = std::env::var(CONTROLLER_ADDR_ENV) {
            env.as_array_mut().unwrap().push(json!({
                "name": CONTROLLER_ADDR_ENV,
                "value": addr,
            }));
        }

        for (key, value) in req.env_vars.into_iter() {
            env.as_array_mut().unwrap().push(json!({
                "name": key,
                "value": value,
            }));
        }

        let rs: ReplicaSet = serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": format!("arroyo-{}-{}", req.job_id, req.run_id),
                "labels": labels,
            },
            "spec": {
                "replicas": replicas,
                "selector": {
                    "matchLabels": {
                        JOB_ID_LABEL: req.job_id,
                        RUN_ID_LABEL: format!("{}", req.run_id),
                    }
                },
                "template": {
                    "metadata": {
                        "labels": labels
                    },
                    "spec": {
                        "volumes": volumes,
                        "containers": [
                            {
                                "name": "worker",
                                "image": image,
                                "imagePullPolicy": "Always",
                                "resources": {
                                    "requests": {
                                        "cpu": format!("{}m", cpu),
                                        "memory": format!("{}Mi", mem),
                                    }
                                },
                                "ports": [
                                    {
                                        "containerPort": 8000,
                                        "name": "grpc",
                                    }
                                ],
                                "env": env,
                                "volumeMounts": volume_mounts
                            }
                        ]
                    }
                }
            }
        }))
        .unwrap();

        api.create(&Default::default(), &rs)
            .await
            .map_err(|e| SchedulerError::Other(e.to_string()))?;

        Ok(())
    }

    async fn register_node(&self, _req: RegisterNodeReq) {
        // n/a
    }

    async fn heartbeat_node(&self, _req: HeartbeatNodeReq) -> Result<(), Status> {
        // n/a
        Ok(())
    }

    async fn worker_finished(&self, _req: WorkerFinishedReq) {
        // n/a
    }

    async fn stop_workers(
        &self,
        job_id: &str,
        run_id: Option<i64>,
        force: bool,
    ) -> anyhow::Result<()> {
        let api: Api<ReplicaSet> = Api::default_namespaced(self.client.clone());

        let mut labels = format!("{}={}", JOB_ID_LABEL, job_id);
        if let Some(run_id) = run_id {
            labels.push_str(&format!(",{}={}", RUN_ID_LABEL, run_id));
        }

        let delete_params = if force {
            DeleteParams::default().grace_period(0)
        } else {
            DeleteParams::default()
        };

        let result = api
            .delete_collection(&delete_params, &ListParams::default().labels(&labels))
            .await?;

        if let Some(status) = result.right() {
            if status.is_failure() {
                bail!("Failed to clean cluster: {:?}", status);
            }
        }

        // wait for workers to stop
        for i in 0..20 {
            tokio::time::sleep(Duration::from_millis(i * 10)).await;

            if self.workers_for_job(&job_id, run_id).await?.len() == 0 {
                return Ok(());
            }
        }

        bail!("workers failed to shut down");
    }

    async fn workers_for_job(
        &self,
        job_id: &str,
        run_id: Option<i64>,
    ) -> anyhow::Result<Vec<WorkerId>> {
        // get the pods associated with the replica set for the given job_id and run_id
        let api: Api<Pod> = Api::default_namespaced(self.client.clone());

        // label selector for job_id and optional run_id

        let mut selector = format!("{}={}", JOB_ID_LABEL, job_id);
        if let Some(run_id) = run_id {
            selector.push_str(&format!(",{}={}", RUN_ID_LABEL, run_id));
        }

        api.list(&ListParams::default().labels(&selector))
            .await?
            .iter()
            .filter(|pod| pod.metadata.deletion_timestamp.is_none())
            .map(|_: &Pod| {
                // TODO: figure out how to evolve this API in such a way that makes sense given that
                //   we don't have static access to worker ids
                Ok(WorkerId(1))
            })
            .collect()
    }
}
