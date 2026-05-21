mod quantities;

use crate::schedulers::kubernetes::quantities::QuantityParser;
use crate::schedulers::{Scheduler, SchedulerError, StartPipelineReq};
use anyhow::bail;
use arroyo_rpc::api_types::jobs::SchedulerConfigSpec;
use arroyo_rpc::config::{KubernetesSchedulerConfig, ResourceMode, config};
use arroyo_rpc::grpc::rpc::{HeartbeatNodeReq, RegisterNodeReq, WorkerFinishedReq};
use arroyo_types::{GENERATION_ENV, JOB_ID_ENV, PIPELINE_ID_ENV, WorkerId};
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
const GENERATION_LABEL: &str = "generation";
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

    /// Return a [`KubernetesSchedulerConfig`] with the per-job
    /// configuration from `req` overlaid on top of `self.config`.
    ///
    /// Merge semantics by field:
    ///   - `image`, `image_pull_policy`, `command`,
    ///     `service_account_name`, `task_slots`, `resources` → replace
    ///   - `labels`, `annotations`, `node_selector` → merge map
    ///     (per-job wins on key collisions)
    ///   - `image_pull_secrets`, `volumes`, `volume_mounts`,
    ///     `tolerations`, `env` → append (global first, per-job last)
    ///
    /// Fields with no per-job override pass through unchanged.
    fn resolved_config(&self, req: &StartPipelineReq) -> KubernetesSchedulerConfig {
        let mut config = self.config.clone();
        let job_config = if let Some(job_config) = &req.scheduler_config {
            match &job_config.spec {
                SchedulerConfigSpec::Kubernetes(job_config) => job_config,
            }
        } else {
            return config;
        };

        if let Some(image) = &job_config.image {
            config.worker.image = image.clone();
        }
        if let Some(ipp) = &job_config.image_pull_policy {
            config.worker.image_pull_policy = ipp.clone();
        }
        if let Some(cmd) = &job_config.command {
            config.worker.command = cmd.clone();
        }
        if let Some(sa) = &job_config.service_account_name {
            config.worker.service_account_name = sa.clone();
        }
        if let Some(ts) = job_config.task_slots {
            config.worker.task_slots = ts;
        }
        if let Some(r) = &job_config.resources {
            config.worker.resources = r.clone();
            config.resource_mode = ResourceMode::PerPod;
        }

        // Merge-style maps (per-job wins on collision).
        if let Some(extra) = &job_config.labels {
            config
                .worker
                .labels
                .extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        if let Some(extra) = &job_config.annotations {
            config
                .worker
                .annotations
                .extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        if let Some(extra) = &job_config.node_selector {
            config
                .worker
                .node_selector
                .extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
        }

        // Append-style lists.
        if let Some(extra) = &job_config.image_pull_secrets {
            config
                .worker
                .image_pull_secrets
                .extend(extra.iter().cloned());
        }
        if let Some(extra) = &job_config.volumes {
            config.worker.volumes.extend(extra.iter().cloned());
        }
        if let Some(extra) = &job_config.volume_mounts {
            config.worker.volume_mounts.extend(extra.iter().cloned());
        }
        if let Some(extra) = &job_config.tolerations {
            config.worker.tolerations.extend(extra.iter().cloned());
        }
        if let Some(extra) = &job_config.env {
            config.worker.env.extend(extra.iter().cloned());
        }

        config
    }

    fn make_pod(
        &self,
        c: &KubernetesSchedulerConfig,
        req: &StartPipelineReq,
        number: usize,
        slots: usize,
    ) -> Pod {
        let resources = match &c.resource_mode {
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
        labels.insert(GENERATION_LABEL.to_string(), format!("{}", req.generation));
        labels.insert(JOB_NAME_LABEL.to_string(), req.name.clone());

        let pod_name = format!(
            "{}-{}-{}-{}",
            c.worker.name(),
            req.job_id.to_ascii_lowercase().replace('_', "-"),
            req.generation,
            number
        );

        let mut env = json!([
            {
                "name": "PROD", "value": "true",
            },
            {
                "name": "ARROYO__WORKER__TASK_SLOTS", "value": format!("{}", slots),
            },
            {
                "name": PIPELINE_ID_ENV, "value": *req.pipeline_id,
            },
            {
                "name": JOB_ID_ENV, "value": *req.job_id,
            },
            {
                "name": GENERATION_ENV, "value": format!("{}", req.generation),
            },
            {
                "name": "ARROYO__WORKER__MACHINE_ID", "value": pod_name,
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

        if config().is_tls_enabled(&config().worker.tls) {
            // if TLS is enabled, we'll need to refer to the workers by their hostnames
            env.as_array_mut().unwrap().push(json!({
                "name": "ARROYO__HOSTNAME",
                "value": format!("{}.{}.pod.local", pod_name, c.namespace)
            }));
        }

        for (key, value) in req.env_vars.iter() {
            env.as_array_mut().unwrap().push(json!({
                "name": key,
                "value": value,
            }));
        }

        for var in &c.worker.env {
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
                "name": pod_name,
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
                "nodeSelector": c.worker.node_selector,
                "tolerations": c.worker.tolerations,
            }
        }))
        .unwrap()
    }
}

#[async_trait]
impl Scheduler for KubernetesScheduler {
    async fn start_workers(&self, req: StartPipelineReq) -> Result<(), SchedulerError> {
        let api: Api<Pod> = Api::default_namespaced(self.client.as_ref().unwrap().clone());

        let c = self.resolved_config(&req);
        let replicas = (req.slots as f32 / c.worker.task_slots as f32).ceil() as usize;
        let max_slots_per_pod = c.worker.task_slots as usize;
        let mut slots_scheduled = 0;
        let mut pods = vec![];
        for i in 0..replicas {
            let slots_here = (req.slots - slots_scheduled).min(max_slots_per_pod);
            pods.push(self.make_pod(&c, &req, i, slots_here));
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
        generation: Option<u64>,
        force: bool,
    ) -> anyhow::Result<()> {
        let api: Api<Pod> = Api::default_namespaced(self.client.as_ref().unwrap().clone());

        let mut labels = format!("{JOB_ID_LABEL}={job_id}");
        if let Some(generation) = generation {
            labels.push_str(&format!(",{GENERATION_LABEL}={generation}"));
        }

        let delete_params = if force {
            DeleteParams::default().grace_period(0)
        } else {
            DeleteParams::default()
        };

        let result = api
            .delete_collection(&delete_params, &ListParams::default().labels(&labels))
            .await?;

        if let Some(status) = result.right()
            && status.is_failure()
        {
            bail!("Failed to clean cluster: {:?}", status);
        }

        // wait for workers to stop
        for i in 0..20 {
            tokio::time::sleep(Duration::from_millis(i * 10)).await;

            if self.workers_for_job(job_id, generation).await?.is_empty() {
                return Ok(());
            }
        }

        bail!("workers failed to shut down");
    }

    async fn workers_for_job(
        &self,
        job_id: &str,
        generation: Option<u64>,
    ) -> anyhow::Result<Vec<WorkerId>> {
        // get the pods associated with the replica set for the given job_id and generation
        let api: Api<Pod> = Api::default_namespaced(self.client.as_ref().unwrap().clone());

        // label selector for job_id and optional generation

        let mut selector = format!("{JOB_ID_LABEL}={job_id}");
        if let Some(generation) = generation {
            selector.push_str(&format!(",{GENERATION_LABEL}={generation}"));
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
    use crate::schedulers::StartPipelineReq;
    use crate::schedulers::kubernetes::{JOB_ID_LABEL, KubernetesScheduler};
    use arroyo_datastream::logical::LogicalProgram;
    use arroyo_rpc::config::config;
    use arroyo_types::{JobId, PipelineId};
    use serde_json::json;

    #[test]
    fn test_resource_creation() {
        let req = StartPipelineReq {
            name: "test_pipeline".to_string(),
            program: LogicalProgram::default(),
            wasm_path: "file:///wasm".to_string(),
            pipeline_id: PipelineId("pipe-123".to_string().into()),
            job_id: JobId("job123".to_string().into()),
            hash: "12123123h".to_string(),
            generation: 1,
            slots: 8,
            env_vars: Default::default(),
            pipeline_tags: Default::default(),
            scheduler_config: None,
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

        let scheduler = KubernetesScheduler::with_config(None, config);
        let resolved = scheduler.resolved_config(&req);
        // test that we don't panic when creating the replicaset
        scheduler.make_pod(&resolved, &req, 3, 4);
    }

    #[test]
    fn test_per_job_config_applies_to_pod() {
        use arroyo_rpc::api_types::jobs::{
            KubernetesJobSchedulerConfig, SchedulerConfig, SchedulerConfigSpec,
        };
        use std::collections::BTreeMap;

        let mut node_selector = BTreeMap::new();
        node_selector.insert("workload".to_string(), "gpu".to_string());

        let mut labels = BTreeMap::new();
        labels.insert("team".to_string(), "data".to_string());

        let mut annotations = BTreeMap::new();
        annotations.insert("scheduler.k8s/preempt".to_string(), "false".to_string());

        let job_cfg = KubernetesJobSchedulerConfig {
            image: Some("my-registry/custom-worker:v2".to_string()),
            image_pull_policy: Some("Always".to_string()),
            image_pull_secrets: Some(vec![
                serde_json::from_value(json!({ "name": "private-registry" })).unwrap(),
            ]),
            command: Some("/usr/local/bin/custom-worker --foo".to_string()),
            service_account_name: Some("job-specific-sa".to_string()),
            env: Some(vec![
                serde_json::from_value(json!({
                    "name": "PER_JOB_FOO",
                    "value": "bar"
                }))
                .unwrap(),
            ]),
            node_selector: Some(node_selector),
            labels: Some(labels),
            annotations: Some(annotations),
            tolerations: Some(vec![
                serde_json::from_value(json!({
                    "key": "gpu",
                    "operator": "Exists",
                    "effect": "NoSchedule"
                }))
                .unwrap(),
            ]),
            volumes: Some(vec![
                serde_json::from_value(json!({
                    "name": "job-secret",
                    "secret": { "secretName": "my-job-secret" }
                }))
                .unwrap(),
            ]),
            volume_mounts: Some(vec![
                serde_json::from_value(json!({
                    "name": "job-secret",
                    "mountPath": "/secrets",
                    "readOnly": true
                }))
                .unwrap(),
            ]),
            task_slots: Some(2),
            resources: None,
        };

        let req = StartPipelineReq {
            name: "test_pipeline".to_string(),
            program: LogicalProgram::default(),
            wasm_path: "file:///wasm".to_string(),
            pipeline_id: PipelineId("pipe-123".to_string().into()),
            job_id: JobId("job123".to_string().into()),
            hash: "12123123h".to_string(),
            generation: 1,
            slots: 4,
            env_vars: Default::default(),
            pipeline_tags: Default::default(),
            scheduler_config: Some(SchedulerConfig::new(SchedulerConfigSpec::Kubernetes(
                job_cfg,
            ))),
        };

        let cfg = config().kubernetes_scheduler.clone();
        let scheduler = KubernetesScheduler::with_config(None, cfg);
        let resolved = scheduler.resolved_config(&req);

        // Resolved task_slots should use the per-job override.
        assert_eq!(resolved.worker.task_slots, 2);

        let pod = scheduler.make_pod(&resolved, &req, 0, 2);
        let spec = pod.spec.as_ref().unwrap();
        let container = &spec.containers[0];

        // Image, pull policy, command are replaced.
        assert_eq!(
            container.image.as_deref().unwrap(),
            "my-registry/custom-worker:v2"
        );
        assert_eq!(container.image_pull_policy.as_deref().unwrap(), "Always");
        let cmd = container.command.as_ref().unwrap();
        assert_eq!(cmd[0], "/usr/local/bin/custom-worker");
        assert!(cmd.iter().any(|s| s == "--foo"));

        // Service account is replaced.
        assert_eq!(
            spec.service_account_name.as_deref().unwrap(),
            "job-specific-sa"
        );

        // Image pull secrets are appended (we just verify presence).
        assert!(
            spec.image_pull_secrets
                .as_ref()
                .unwrap()
                .iter()
                .any(|s| s.name == "private-registry")
        );

        // Node selector merged in.
        let ns = spec.node_selector.as_ref().unwrap();
        assert_eq!(ns.get("workload").unwrap(), "gpu");

        // Labels include the per-job entry, plus intrinsic labels are
        // preserved (cluster/job-id/generation/job-name).
        let pod_labels = pod.metadata.labels.as_ref().unwrap();
        assert_eq!(pod_labels.get("team").unwrap(), "data");
        assert_eq!(pod_labels.get(JOB_ID_LABEL).unwrap(), "job123");

        // Annotations include the per-job entry.
        let pod_annotations = pod.metadata.annotations.as_ref().unwrap();
        assert_eq!(
            pod_annotations.get("scheduler.k8s/preempt").unwrap(),
            "false"
        );

        // Tolerations are appended.
        let tolerations = spec.tolerations.as_ref().unwrap();
        assert!(tolerations.iter().any(|t| t.key.as_deref() == Some("gpu")));

        // Volumes / volume mounts are appended.
        assert!(
            spec.volumes
                .as_ref()
                .unwrap()
                .iter()
                .any(|v| v.name == "job-secret")
        );
        assert!(
            container
                .volume_mounts
                .as_ref()
                .unwrap()
                .iter()
                .any(|m| m.name == "job-secret")
        );

        // Per-job env var is present.
        let env = container.env.as_ref().unwrap();
        assert!(env.iter().any(|e| e.name == "PER_JOB_FOO"));
    }

    #[test]
    fn test_resolved_config_falls_back_to_global() {
        let req = StartPipelineReq {
            name: "test_pipeline".to_string(),
            program: LogicalProgram::default(),
            wasm_path: "file:///wasm".to_string(),
            pipeline_id: PipelineId("pipe-123".to_string().into()),
            job_id: JobId("job123".to_string().into()),
            hash: "12123123h".to_string(),
            generation: 1,
            slots: 8,
            env_vars: Default::default(),
            pipeline_tags: Default::default(),
            scheduler_config: None,
        };

        let cfg = config().kubernetes_scheduler.clone();
        let global_task_slots = cfg.worker.task_slots;
        let global_image = cfg.worker.image.clone();
        let scheduler = KubernetesScheduler::with_config(None, cfg);
        let resolved = scheduler.resolved_config(&req);

        assert_eq!(resolved.worker.task_slots, global_task_slots);
        assert_eq!(resolved.worker.image, global_image);
    }
}
