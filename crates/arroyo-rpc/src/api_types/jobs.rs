use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{
    EnvVar, LocalObjectReference, ResourceRequirements, Toleration, Volume, VolumeMount,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Per-job scheduler configuration.
///
/// Persisted on `job_configs.scheduler_config` and applied at scheduling
/// time on top of the controller's global scheduler config.
///
/// A JSON payload looks like:
/// ```json
/// { "version": 1, "type": "kubernetes", "image": "..." }
/// ```
/// `version` is mandatory on the wire (no implicit default) so that
/// future schema changes can be detected at deserialize time rather
/// than silently misinterpreting old payloads. The current schema is
/// [`SchedulerConfig::CURRENT_VERSION`]. The variant in `spec` must
/// match the scheduler the controller is running; mismatched variants
/// are rejected at job creation time.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct SchedulerConfig {
    pub version: u32,

    /// The scheduler-specific configuration, discriminated on `type`.
    #[serde(flatten)]
    pub spec: SchedulerConfigSpec,
}

impl SchedulerConfig {
    pub const CURRENT_VERSION: u32 = 1;

    pub fn new(spec: SchedulerConfigSpec) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            spec,
        }
    }

    pub fn ensure_supported_version(&self) -> anyhow::Result<()> {
        if self.version == Self::CURRENT_VERSION {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "unsupported scheduler_config version {} (this binary supports {})",
                self.version,
                Self::CURRENT_VERSION,
            ))
        }
    }
}

/// The scheduler-specific portion of a [`SchedulerConfig`]. Internally
/// tagged on `type`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SchedulerConfigSpec {
    Kubernetes(KubernetesJobSchedulerConfig),
}

/// Per-job configuration for the Kubernetes scheduler.
///
/// All fields are optional: `None` means "use the global default from
/// `kubernetes-scheduler.worker.*`".
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, ToSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct KubernetesJobSchedulerConfig {
    /// Worker container image. Replaces `kubernetes-scheduler.worker.image`.
    pub image: Option<String>,

    /// Worker container imagePullPolicy. Replaces
    /// `kubernetes-scheduler.worker.image-pull-policy`.
    pub image_pull_policy: Option<String>,

    /// Extra image pull secrets. Appended to
    /// `kubernetes-scheduler.worker.image-pull-secrets`.
    #[schema(value_type = Vec<Object>)]
    pub image_pull_secrets: Option<Vec<LocalObjectReference>>,

    /// Worker container command (shell-style; split by `shlex`). Replaces
    /// `kubernetes-scheduler.worker.command`.
    pub command: Option<String>,

    /// Pod `serviceAccountName`. Replaces
    /// `kubernetes-scheduler.worker.service-account-name`.
    pub service_account_name: Option<String>,

    /// Worker resource requests/limits. Replaces
    /// `kubernetes-scheduler.worker.resources`.
    #[schema(value_type = Object)]
    pub resources: Option<ResourceRequirements>,

    /// Slots per worker pod. Replaces
    /// `kubernetes-scheduler.worker.task-slots`.
    pub task_slots: Option<u32>,

    /// Extra env vars. Appended to `kubernetes-scheduler.worker.env`
    /// (and the controller's intrinsic env). Per-job values win on
    /// name collision via kubelet's last-wins resolution.
    #[schema(value_type = Vec<Object>)]
    pub env: Option<Vec<EnvVar>>,

    /// Extra pod volumes. Appended to
    /// `kubernetes-scheduler.worker.volumes`. Typically paired with
    /// `volume_mounts`.
    #[schema(value_type = Vec<Object>)]
    pub volumes: Option<Vec<Volume>>,

    /// Extra container volume mounts. Appended to
    /// `kubernetes-scheduler.worker.volume-mounts`.
    #[schema(value_type = Vec<Object>)]
    pub volume_mounts: Option<Vec<VolumeMount>>,

    /// Extra node selector entries. Merged with the global node selector
    /// (per-job keys win).
    pub node_selector: Option<BTreeMap<String, String>>,

    /// Extra tolerations. Appended to the global tolerations.
    #[schema(value_type = Vec<Object>)]
    pub tolerations: Option<Vec<Toleration>>,

    /// Extra pod labels. Merged with the global labels (per-job keys
    /// win). The controller's intrinsic labels (cluster/job-id/
    /// generation/job-name) are still applied last and cannot be
    /// overridden.
    pub labels: Option<BTreeMap<String, String>>,

    /// Extra pod annotations. Merged with the global annotations
    /// (per-job keys win).
    pub annotations: Option<BTreeMap<String, String>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serializes_with_version_and_type_siblings() {
        let sc = SchedulerConfig::new(SchedulerConfigSpec::Kubernetes(
            KubernetesJobSchedulerConfig {
                image: Some("foo:bar".to_string()),
                ..Default::default()
            },
        ));

        let v = serde_json::to_value(&sc).unwrap();
        assert_eq!(v["version"], json!(1));
        assert_eq!(v["type"], json!("kubernetes"));
        assert_eq!(v["image"], json!("foo:bar"));
    }

    #[test]
    fn deserializes_v1_kubernetes_payload() {
        let v = json!({
            "version": 1,
            "type": "kubernetes",
            "image": "foo:bar",
        });
        let sc: SchedulerConfig = serde_json::from_value(v).unwrap();
        assert_eq!(sc.version, 1);
        sc.ensure_supported_version().unwrap();
        match sc.spec {
            SchedulerConfigSpec::Kubernetes(k) => {
                assert_eq!(k.image.as_deref(), Some("foo:bar"));
            }
        }
    }

    #[test]
    fn missing_version_field_is_a_deserialize_error() {
        // version is non-optional, so a payload without it must fail.
        let v = json!({
            "type": "kubernetes",
            "image": "foo:bar",
        });
        assert!(serde_json::from_value::<SchedulerConfig>(v).is_err());
    }

    #[test]
    fn future_version_parses_but_ensure_supported_rejects() {
        let v = json!({
            "version": 99,
            "type": "kubernetes",
            "image": "foo:bar",
        });
        let sc: SchedulerConfig = serde_json::from_value(v).unwrap();
        assert_eq!(sc.version, 99);
        let err = sc.ensure_supported_version().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("99"),
            "error message missing got version: {msg}"
        );
        assert!(
            msg.contains(&SchedulerConfig::CURRENT_VERSION.to_string()),
            "error message missing supported version: {msg}"
        );
    }

    #[test]
    fn typos_in_variant_fields_are_still_a_deserialize_error() {
        let v = json!({
            "version": 1,
            "type": "kubernetes",
            "imagee": "typo",  // not "image"
        });
        assert!(serde_json::from_value::<SchedulerConfig>(v).is_err());
    }
}
