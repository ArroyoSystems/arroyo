use crate::schedulers::{Scheduler, SchedulerError, StartPipelineReq};
use anyhow::bail;
use arroyo_rpc::grpc::{api, HeartbeatNodeReq, RegisterNodeReq, WorkerFinishedReq};
use arroyo_types::{
    string_config, u32_config, WorkerId, ADMIN_PORT_ENV, ARROYO_PROGRAM_ENV, CONTROLLER_ADDR_ENV,
    GRPC_PORT_ENV, JOB_ID_ENV, K8S_NAMESPACE_ENV, K8S_WORKER_ANNOTATIONS_ENV,
    K8S_WORKER_CONFIG_MAP_ENV, K8S_WORKER_IMAGE_ENV, K8S_WORKER_IMAGE_PULL_POLICY_ENV,
    K8S_WORKER_LABELS_ENV, K8S_WORKER_NAME_ENV, K8S_WORKER_RESOURCES_ENV,
    K8S_WORKER_SERVICE_ACCOUNT_NAME_ENV, K8S_WORKER_SLOTS_ENV, K8S_WORKER_VOLUMES_ENV,
    K8S_WORKER_VOLUME_MOUNTS_ENV, NODE_ID_ENV, RUN_ID_ENV, TASK_SLOTS_ENV,
};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use k8s_openapi::api::apps::v1::ReplicaSet;
use k8s_openapi::api::core::v1::{Pod, ResourceRequirements, Volume, VolumeMount};
use kube::api::{DeleteParams, ListParams};
use kube::{Api, Client};
use prost::Message;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::env;
use std::time::Duration;
use tonic::Status;

const CLUSTER_LABEL: &str = "cluster";
const JOB_ID_LABEL: &str = "job_id";
const RUN_ID_LABEL: &str = "run_id";
const JOB_NAME_LABEL: &str = "job_name";

pub struct KubernetesScheduler {
    client: Option<Client>,
    namespace: String,
    name: String,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
    image: String,
    image_pull_policy: String,
    service_account_name: String,
    resources: ResourceRequirements,
    slots_per_pod: u32,
    volumes: Vec<Volume>,
    volume_mounts: Vec<VolumeMount>,
    config_map: Option<String>,
}

fn yaml_config<T: DeserializeOwned>(var: &str, default: T) -> T {
    env::var(var)
        .map(|s| {
            serde_yaml::from_str(&s)
                .unwrap_or_else(|e| panic!("Invalid configuration for '{}': {:?}", var, e))
        })
        .unwrap_or(default)
}

impl KubernetesScheduler {
    pub async fn from_env() -> Self {
        Self::new(Some(Client::try_default().await.unwrap()))
    }

    pub fn new(client: Option<Client>) -> Self {
        Self {
            client,
            namespace: string_config(K8S_NAMESPACE_ENV, "default"),
            name: format!("{}-worker", string_config(K8S_WORKER_NAME_ENV, "arroyo")),
            image: string_config(K8S_WORKER_IMAGE_ENV, "ghcr.io/arroyosystems/arroyo:latest"),
            labels: yaml_config(K8S_WORKER_LABELS_ENV, BTreeMap::new()),
            annotations: yaml_config(K8S_WORKER_ANNOTATIONS_ENV, BTreeMap::new()),
            image_pull_policy: string_config(K8S_WORKER_IMAGE_PULL_POLICY_ENV, "IfNotPresent"),
            service_account_name: string_config(K8S_WORKER_SERVICE_ACCOUNT_NAME_ENV, "default"),
            resources: yaml_config(
                K8S_WORKER_RESOURCES_ENV,
                ResourceRequirements {
                    claims: None,
                    limits: None,
                    requests: Some(
                        [
                            ("cpu".to_string(), serde_json::from_str("\"400m\"").unwrap()),
                            (
                                "memory".to_string(),
                                serde_json::from_str("\"200Mi\"").unwrap(),
                            ),
                        ]
                        .into(),
                    ),
                },
            ),
            slots_per_pod: u32_config(K8S_WORKER_SLOTS_ENV, 4),
            volumes: yaml_config(K8S_WORKER_VOLUMES_ENV, vec![]),
            volume_mounts: yaml_config(K8S_WORKER_VOLUME_MOUNTS_ENV, vec![]),
            config_map: env::var(K8S_WORKER_CONFIG_MAP_ENV).ok(),
        }
    }

    fn make_replicaset(&self, req: StartPipelineReq) -> ReplicaSet {
        let replicas = (req.slots as f32 / self.slots_per_pod as f32).ceil() as usize;

        let mut labels = json!({
            CLUSTER_LABEL: self.name,
            JOB_ID_LABEL: req.job_id,
            RUN_ID_LABEL: format!("{}", req.run_id),
            JOB_NAME_LABEL: req.name,
        });
        for (k, v) in &self.labels {
            labels
                .as_object_mut()
                .unwrap()
                .insert(k.clone(), Value::String(v.clone()));
        }

        let mut annotations = json!({});
        for (k, v) in &self.annotations {
            annotations
                .as_object_mut()
                .unwrap()
                .insert(k.clone(), Value::String(v.clone()));
        }

        let mut env = json!([
            {
                "name": "PROD", "value": "true",
            },
            {
                "name": TASK_SLOTS_ENV, "value": format!("{}", self.slots_per_pod),
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
                "name": GRPC_PORT_ENV, "value": "6900",
            },
            {
                "name": ADMIN_PORT_ENV, "value": "6901",
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

        serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": format!("{}-{}-{}", self.name, req.job_id.to_ascii_lowercase().replace('_', "-"), req.run_id),
                "namespace": self.namespace,
                "labels": labels,
                "annotations": annotations,
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
                        "annotations": annotations,
                    },
                    "spec": {
                        "volumes": self.volumes,
                        "containers": [
                            {
                                "name": "worker",
                                "image": self.image,
                                "command": ["/app/arroyo-bin", "worker"],
                                "imagePullPolicy": self.image_pull_policy,
                                "resources": self.resources,
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
                                "volumeMounts": self.volume_mounts,
                                "envFrom": self.config_map.as_ref().map(|name| {
                                  json!([
                                    {"configMapRef": {
                                         "name": name
                                    }}
                                  ])
                                }).unwrap_or_else(|| json!([]))
                            }
                        ],
                        "serviceAccountName": self.service_account_name,
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

        KubernetesScheduler::new(None)
            // test that we don't panic when creating the replicaset
            .make_replicaset(req);
    }
}
