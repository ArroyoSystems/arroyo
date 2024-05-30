use crate::schedulers::{Scheduler, SchedulerError, StartPipelineReq};
use anyhow::bail;
use arroyo_rpc::config::{config, KubernetesSchedulerConfig};
use arroyo_rpc::grpc::{api, HeartbeatNodeReq, RegisterNodeReq, WorkerFinishedReq};
use arroyo_types::{WorkerId, ARROYO_PROGRAM_ENV, JOB_ID_ENV, RUN_ID_ENV};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use k8s_openapi::api::apps::v1::ReplicaSet;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{DeleteParams, ListParams};
use kube::{Api, Client};
use prost::Message;
use serde_json::json;
use std::time::Duration;
use tonic::Status;

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

    fn make_replicaset(&self, req: StartPipelineReq) -> ReplicaSet {
        let c = &self.config;
        let replicas = (req.slots as f32 / c.worker.task_slots as f32).ceil() as usize;

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
                "name": "ARROYO_WORKER_TASK_SLOTS", "value": format!("{}", c.worker.task_slots),
            },
            {
                "name": JOB_ID_ENV, "value": req.job_id,
            },
            {
                "name": RUN_ID_ENV, "value": format!("{}", req.run_id),
            },
            {
                "name": "ARROYO_WORKER_RPC_PORT", "value": "6900",
            },
            {
                "name": "ARROYO_ADMIN_HTTP_PORT", "value": "6901",
            },
            {
                "name": ARROYO_PROGRAM_ENV,
                "value": general_purpose::STANDARD_NO_PAD
                    .encode(api::ArrowProgram::from(req.program).encode_to_vec()),
            },
            {
                "name": "WASM_BIN",
                "value": req.wasm_path
            },
            {
                "name": "ARROYO_CONTROLLER_ENDPOINT",
                "value": config().controller_endpoint(),
            }
        ]);

        for (key, value) in req.env_vars.into_iter() {
            env.as_array_mut().unwrap().push(json!({
                "name": key,
                "value": value,
            }));
        }

        serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": format!("{}-{}-{}", c.worker.name(), req.job_id.to_ascii_lowercase().replace('_', "-"), req.run_id),
                "namespace": c.namespace,
                "labels": labels,
                "annotations": c.worker.annotations,
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
                        "labels": labels,
                        "annotations": c.worker.annotations,
                    },
                    "spec": {
                        "volumes": c.worker.volumes,
                        "containers": [
                            {
                                "name": "worker",
                                "image": c.worker.image,
                                "command": ["/app/arroyo-bin", "worker"],
                                "imagePullPolicy": c.worker.image_pull_policy,
                                "resources": c.worker.resources,
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
                }
            }
        })).unwrap()
    }
}

#[async_trait]
impl Scheduler for KubernetesScheduler {
    async fn start_workers(&self, req: StartPipelineReq) -> Result<(), SchedulerError> {
        let api: Api<ReplicaSet> = Api::default_namespaced(self.client.as_ref().unwrap().clone());

        let rs = self.make_replicaset(req);

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
        let api: Api<ReplicaSet> = Api::default_namespaced(self.client.as_ref().unwrap().clone());

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
            .make_replicaset(req);
    }
}
