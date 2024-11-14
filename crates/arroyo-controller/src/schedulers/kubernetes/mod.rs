mod quantities;

use crate::schedulers::kubernetes::quantities::QuantityParser;
use crate::schedulers::{Scheduler, SchedulerError, StartPipelineReq};
use anyhow::bail;
use arroyo_rpc::config::{config, KubernetesSchedulerConfig, ResourceMode};
use arroyo_rpc::grpc::rpc::{HeartbeatNodeReq, RegisterNodeReq, WorkerFinishedReq};
use arroyo_types::{WorkerId, JOB_ID_ENV, RUN_ID_ENV};
use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::{DeleteParams, ListParams};
use kube::{Api, Client};
use serde_json::json;
use std::time::Duration;
use tonic::Status;
use tracing::{info, warn};

const CLUSTER_LABEL: &str = "cluster";
const JOB_ID_LABEL: &str = "job_id";
const RUN_ID_LABEL: &str = "run_id";
const JOB_NAME_LABEL: &str = "job_name";

pub struct KubernetesScheduler {
    client: Option<Client>,
    config: KubernetesSchedulerConfig,
}

impl KubernetesScheduler {
    pub async fn new() -> Self {
        Self::with_config(
            Some(Client::try_default().await.unwrap()),
            config().kubernetes_scheduler.clone(),
        )
    }

    pub fn with_config(client: Option<Client>, config: KubernetesSchedulerConfig) -> Self {
        Self { client, config }
    }

    fn make_pod(&self, req: &StartPipelineReq, number: usize, slots: usize) -> Pod {
        let c = &self.config;

        let resources = match c.resource_mode {
            ResourceMode::PerSlot => {
                let mut r = c.worker.resources.clone();

                for (name, rs) in [("limits", &mut r.limits), ("requests", &mut r.requests)] {
                    if let Some(l) = rs {
                        if let Some(cpu) = l.get_mut("cpu") {
                            let v = cpu.parse_cpu().unwrap_or_else(|e| {
                                if number == 0 {
                                    warn!("Invalid value '{}' for \
                                kubernetes_scheduler.worker.resources.{}.cpu: {}; defaulting to 900m", cpu.0, name, e);
                                }
                                Quantity("900m".to_string()).parse_cpu().unwrap()
                            });

                            *cpu = Quantity((v * slots as i64).to_canonical());
                        }

                        if let Some(mem) = l.get_mut("memory") {
                            let v = mem.parse_memory().unwrap_or_else(|e| {
                                if number == 0 {
                                    warn!("Invalid value '{}' for \
                                kubernetes_scheduler.worker.resources.{}.memory: {}; defaulting to 500Mi", mem.0, name, e);
                                }
                                Quantity("500Mi".to_string()).parse_memory().unwrap()
                            });

                            *mem = Quantity((v * slots as i64).to_canonical());
                        }
                    }
                }

                r
            }
            ResourceMode::PerPod => c.worker.resources.clone(),
        };

        let mut labels = c.worker.labels.clone();
        labels.insert(CLUSTER_LABEL.to_string(), c.worker.name());
        labels.insert(JOB_ID_LABEL.to_string(), (*req.job_id).clone());
        labels.insert(RUN_ID_LABEL.to_string(), format!("{}", req.run_id));
        labels.insert(JOB_NAME_LABEL.to_string(), req.name.clone());

        let mut env = json!([
            {
                "name": "PROD", "value": "true",
            },
            {
                "name": "ARROYO__WORKER__TASK_SLOTS", "value": format!("{}", slots),
            },
            {
                "name": JOB_ID_ENV, "value": req.job_id,
            },
            {
                "name": RUN_ID_ENV, "value": format!("{}", req.run_id),
            },
            {
                "name": "ARROYO__WORKER__RPC_PORT", "value": "6900",
            },
            {
                "name": "ARROYO__ADMIN__HTTP_PORT", "value": "6901",
            },
            {
                "name": "WASM_BIN",
                "value": req.wasm_path
            }
        ]);

        for (key, value) in req.env_vars.iter() {
            env.as_array_mut().unwrap().push(json!({
                "name": key,
                "value": value,
            }));
        }

        for var in &config().kubernetes_scheduler.worker.env {
            env.as_array_mut()
                .unwrap()
                .push(serde_json::to_value(var).unwrap());
        }

        let owner: Vec<_> = config()
            .kubernetes_scheduler
            .controller
            .iter()
            .cloned()
            .collect();

        let command: Vec<_> = shlex::split(&c.worker.command).expect("cannot split command");

        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": format!("{}-{}-{}-{}", c.worker.name(), req.job_id.to_ascii_lowercase().replace('_', "-"), req.run_id, number),
                "namespace": c.namespace,
                "labels": labels,
                "annotations": c.worker.annotations,
                "ownerReferences": owner,
            },
            "spec": {
                "volumes": c.worker.volumes,
                "restartPolicy": "Never",
                "imagePullSecrets": c.worker.image_pull_secrets,
                "containers": [
                    {
                        "name": "worker",
                        "image": c.worker.image,
                        "command": command,
                        "imagePullPolicy": c.worker.image_pull_policy,
                        "resources": resources,
                        "ports": [
                            {
                                "containerPort": 6900,
                                "name": "grpc",
                            },
                            {
                                "containerPort": 6901,
                                "name": "admin",
                            }
                        ],
                        "env": env,
                        "volumeMounts": c.worker.volume_mounts,
                    }
                ],
                "serviceAccountName": c.worker.service_account_name,
            }
        })).unwrap()
    }
}

#[async_trait]
impl Scheduler for KubernetesScheduler {
    async fn start_workers(&self, req: StartPipelineReq) -> Result<(), SchedulerError> {
        let api: Api<Pod> = Api::default_namespaced(self.client.as_ref().unwrap().clone());

        let replicas = (req.slots as f32 / config().kubernetes_scheduler.worker.task_slots as f32)
            .ceil() as usize;

        info!(
            job_id = *req.job_id,
            message = "starting workers on k8s",
            replicas,
            task_slots = req.slots
        );

        let max_slots_per_pod = config().kubernetes_scheduler.worker.task_slots as usize;
        let mut slots_scheduled = 0;
        let mut pods = vec![];
        for i in 0..replicas {
            let slots_here = (req.slots - slots_scheduled).min(max_slots_per_pod);
            pods.push(self.make_pod(&req, i, slots_here));
            slots_scheduled += slots_here;
        }

        info!(
            job_id = *req.job_id,
            message = "starting workers on k8s",
            replicas = pods.len(),
            task_slots = req.slots
        );

        for pod in pods {
            info!(
                job_id = *req.job_id,
                message = "starting worker",
                pod = pod.metadata.name
            );
            api.create(&Default::default(), &pod)
                .await
                .map_err(|e| SchedulerError::Other(e.to_string()))?;
        }

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
        let api: Api<Pod> = Api::default_namespaced(self.client.as_ref().unwrap().clone());

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

            if self.workers_for_job(job_id, run_id).await?.is_empty() {
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
        let api: Api<Pod> = Api::default_namespaced(self.client.as_ref().unwrap().clone());

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

#[cfg(test)]
mod test {
    use arroyo_datastream::logical::LogicalProgram;
    use arroyo_rpc::config::config;
    use serde_json::json;
    use std::sync::Arc;

    use crate::schedulers::kubernetes::KubernetesScheduler;
    use crate::schedulers::StartPipelineReq;

    #[test]
    fn test_resource_creation() {
        let req = StartPipelineReq {
            name: "test_pipeline".to_string(),
            program: LogicalProgram::default(),
            wasm_path: "file:///wasm".to_string(),
            job_id: Arc::new("job123".to_string()),
            hash: "12123123h".to_string(),
            run_id: 1,
            slots: 8,
            env_vars: Default::default(),
        };

        let mut config = config().kubernetes_scheduler.clone();

        config.worker.env.push(
            serde_json::from_value(json!({
                "name": "MY_TEST_VAR",
                "value": "my value"
            }))
            .unwrap(),
        );

        config.worker.volume_mounts.push(
            serde_json::from_value(json!({
                "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount",
                "name": "kube-api-access-sx6mp",
                "readOnly": true
            }))
            .unwrap(),
        );

        config.worker.volumes.push(
            serde_json::from_value(json!({
                "hostPath": {
                    "path": "/tmp/arroyo-test",
                    "type": "DirectoryOrCreate"
                },
                "name": "checkpoints"
            }))
            .unwrap(),
        );

        KubernetesScheduler::with_config(None, config)
            // test that we don't panic when creating the replicaset
            .make_pod(&req, 3, 4);
    }
}
