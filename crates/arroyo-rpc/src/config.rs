use anyhow::{anyhow, bail};
use arc_swap::ArcSwapOption;
use figment::providers::{Env, Format, Json, Toml, Yaml};
use figment::Figment;
use k8s_openapi::api::core::v1::{
    EnvVar, LocalObjectReference, ResourceRequirements, Toleration, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use log::warn;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fmt::{Debug, Formatter};
use std::fs;
use std::net::IpAddr;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

const DEFAULT_CONFIG: &str = include_str!("../default.toml");
const SENSITIVE_MASK: &str = "*********";

static CONFIG: ArcSwapOption<Config> = ArcSwapOption::const_empty();

pub fn initialize_config(path: Option<&Path>, dir: Option<&Path>) {
    let mut paths = vec![];
    if let Some(path) = path {
        if !path.exists() {
            eprintln!(
                "Cannot load configuration from {}; file does not exist",
                path.to_string_lossy()
            );
            exit(1);
        }
        paths.push(path.to_path_buf());
    }

    // find config files in directory
    if let Some(dir) = dir {
        if let Ok(rd) = fs::read_dir(dir) {
            paths.extend(rd.filter_map(|f| Some(f.ok()?.path())).filter(|p| {
                p.extension()
                    .map(|ext| {
                        &*ext.to_string_lossy() == "yaml" || &*ext.to_string_lossy() == "toml"
                    })
                    .unwrap_or(false)
            }));
        } else {
            warn!("Invalid configuration directory '{}", dir.to_string_lossy());
        }
    }

    let current = CONFIG.load();

    let mut config: Config = match load_config(&paths).extract() {
        Ok(config) => config,
        Err(errors) => {
            eprintln!("Configuration is invalid!");
            for err in errors {
                eprintln!("  • {err}");
            }

            exit(1);
        }
    };

    config.config_path = path.map(|p| p.to_path_buf());
    config.config_dir = dir.map(|p| p.to_path_buf());

    if current.is_none()
        && CONFIG
            .compare_and_swap(current, Some(Arc::new(config)))
            .is_none()
    {
        return;
    }

    panic!("Unable to initialize configuration; it's already initialized!");
}

pub fn update<F: Fn(&mut Config)>(f: F) {
    CONFIG.rcu(|c| {
        let mut new = (**c
            .as_ref()
            .expect("tried to update config; but not yet loaded!"))
        .clone();
        f(&mut new);
        Some(Arc::new(new))
    });
}

pub fn config() -> Arc<Config> {
    let cur = CONFIG.load();
    if cur.is_none() {
        warn!("Config accessed before initialization! This should only happen in tests.");
        CONFIG.compare_and_swap(cur, Some(load_config(&[]).extract().unwrap()));
    } else {
        drop(cur);
    }

    CONFIG.load_full().unwrap()
}

fn add_legacy_str(config: Figment, old: &str, new: &str) -> Figment {
    if let Ok(v) = std::env::var(old) {
        warn!(
            "Using deprecated config option '{old}' -- will be removed in 0.12; \
        see the config docs to migrate https://doc.arroyo.dev/config"
        );
        config.merge((new, v))
    } else {
        config
    }
}

fn add_legacy_int(config: Figment, old: &str, new: &str) -> Figment {
    if let Ok(v) = std::env::var(old) {
        warn!(
            "Using deprecated config option '{old}' -- will be removed in 0.12; \
        see the config docs to migrate https://doc.arroyo.dev/config"
        );
        match v.parse::<u16>() {
            Ok(v) => {
                return config.merge((new, v));
            }
            Err(_) => {
                warn!("Invalid config for {old} -- expected a number");
            }
        }
    }

    config
}

fn load_config(paths: &[PathBuf]) -> Figment {
    // Priority (from highest--overriding--to lowest--overridden) is:
    //   1. ARROYO__* environment variables
    //   2. The config file specified in <path>
    //   3. Any *.toml or *.yaml files specified in <dir>
    //   4. arroyo.toml in the current directory
    //   5. $(user conf dir)/arroyo/config.{toml,yaml}
    //   6. ../default.toml
    let mut figment = Figment::from(Toml::string(DEFAULT_CONFIG));

    // support a few legacy configs with warnings -- to be removed in 0.12.
    figment = add_legacy_str(figment, "CHECKPOINT_URL", "checkpoint-url");
    figment = add_legacy_str(figment, "ARTIFACT_URL", "compiler.artifact-url");
    figment = add_legacy_str(figment, "SCHEDULER", "controller.scheduler");
    figment = add_legacy_str(figment, "DATABASE_HOST", "database.postgres.host");
    figment = add_legacy_int(figment, "DATABASE_PORT", "database.postgres.port");
    figment = add_legacy_str(figment, "DATABASE_USER", "database.postgres.user");
    figment = add_legacy_str(figment, "DATABASE_PASSWORD", "database.postgres.password");
    figment = add_legacy_str(figment, "DATABASE_NAME", "database.postgres.database-name");
    figment = add_legacy_str(figment, "CONTROLLER_ADDR", "controller-endpoint");
    figment = add_legacy_str(figment, "COMPILER_ADDR", "compiler-endpoint");

    if let Some(config_dir) = dirs::config_dir() {
        figment = figment
            .admerge(Yaml::file(config_dir.join("arroyo/config.yaml")))
            .admerge(Toml::file(config_dir.join("arroyo/config.toml")));
    }

    figment = figment
        .admerge(Yaml::file("arroyo.yaml"))
        .admerge(Toml::file("arroyo.toml"));

    for path in paths {
        match path.extension().and_then(OsStr::to_str) {
            Some("yaml") => {
                figment = figment.admerge(Yaml::file(path));
            }
            Some("json") => {
                figment = figment.admerge(Json::file(path));
            }
            _ => {
                figment = figment.admerge(Toml::file(path));
            }
        }
    }

    figment.admerge(
        Env::prefixed("ARROYO__").map(|p| p.as_str().replace("__", ".").replace("_", "-").into()),
    )
}

/// Arroyo configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct Config {
    /// API service configuration
    pub api: ApiConfig,

    /// Controller service configuration
    pub controller: ControllerConfig,

    /// Compiler service configuration
    pub compiler: CompilerConfig,

    /// Node service configuration
    pub node: NodeConfig,

    /// Worker configuration
    pub worker: WorkerConfig,

    /// Admin service configuration
    pub admin: AdminConfig,

    /// Global TLS configuration
    #[serde(default)]
    pub tls: TlsConfig,

    /// Default pipeline configuration
    pub pipeline: PipelineConfig,

    /// Database configuration
    pub database: DatabaseConfig,

    /// Process scheduler configuration
    pub process_scheduler: ProcessSchedulerConfig,

    // Kubernetes scheduler configuration
    pub kubernetes_scheduler: KubernetesSchedulerConfig,

    // Logging config
    pub logging: LogConfig,

    /// URL of an object store or filesystem for storing checkpoints
    pub checkpoint_url: String,

    /// Default interval for checkpointing
    pub default_checkpoint_interval: HumanReadableDuration,

    /// The endpoint of the controller, used by other services to connect to it. This must be set
    /// if running the controller on a separate machine from the other services or on a separate
    /// process with a non-standard port.
    pub controller_endpoint: Option<Url>,

    // The endpoint of the API service, used by the Web UI
    pub api_endpoint: Option<Url>,

    /// The endpoint of the compiler, used by the API server to connect to it. This must be set
    /// if running the compiler on a separate machine from the other services or on a separate
    /// process with a non-standard port.
    compiler_endpoint: Option<Url>,

    /// Hostname for this node; if set, this will be used for connections made to this node,
    /// otherwise we will attempt to determine the local ip address. Setting this will generally
    /// be useful for TLS.
    pub hostname: Option<String>,

    /// Path to the config file
    pub config_path: Option<PathBuf>,

    /// Directory to look for config files in
    pub config_dir: Option<PathBuf>,

    /// Run options
    #[serde(default)]
    pub run: RunConfig,

    /// Telemetry config
    #[serde(default)]
    pub disable_telemetry: bool,
}

impl Config {
    pub fn hostname(&self) -> &str {
        self.hostname.as_deref().unwrap_or("localhost")
    }

    pub fn controller_endpoint(&self) -> String {
        self.controller_endpoint
            .as_ref()
            .map(|t| t.to_string())
            .unwrap_or_else(|| format!("http://{}:{}", self.hostname(), self.controller.rpc_port))
    }

    pub fn compiler_endpoint(&self) -> String {
        self.compiler_endpoint
            .as_ref()
            .map(|t| t.to_string())
            .unwrap_or_else(|| format!("http://{}:{}", self.hostname(), self.compiler.rpc_port))
    }

    /// Get effective TLS configuration for a service, falling back to global config
    pub fn get_tls_config<'a>(
        &'a self,
        service_tls: &'a Option<TlsConfig>,
    ) -> Option<&'a TlsConfig> {
        if !self.is_tls_enabled(service_tls) {
            return None;
        };

        service_tls.as_ref().or(Some(&self.tls))
    }

    /// Check if TLS is enabled for a service
    pub fn is_tls_enabled(&self, service_tls: &Option<TlsConfig>) -> bool {
        service_tls
            .as_ref()
            .map(|tls| tls.enabled)
            .unwrap_or(self.tls.enabled)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "kebab-case", deny_unknown_fields, tag = "type")]
pub enum ApiAuthMode {
    #[default]
    None,
    Mtls {
        #[serde(rename = "ca-cert-file")]
        ca_cert_file: Option<PathBuf>,
    },
    StaticApiKey {
        #[serde(rename = "api-key")]
        api_key: Sensitive<String>,
    },
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ApiConfig {
    /// The host the API service should bind to
    pub bind_address: IpAddr,

    /// The HTTP port for the API service
    pub http_port: u16,

    /// The HTTP port for the API service in run mode; defaults to a random port
    pub run_http_port: Option<u16>,

    /// TLS configuration for API service
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    #[serde(default)]
    pub auth_mode: ApiAuthMode,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ControllerConfig {
    /// The host the controller should bind to
    pub bind_address: IpAddr,

    /// The RPC port for the controller
    pub rpc_port: u16,

    /// The scheduler to use
    pub scheduler: Scheduler,

    /// TLS configuration for controller gRPC service
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct CompactionConfig {
    /// Whether to enable compaction for checkpoints
    pub enabled: bool,

    /// The number of outstanding checkpoints that will trigger compaction
    pub checkpoints_to_compact: u32,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct CompilerConfig {
    /// Bind address for the compiler
    pub bind_address: IpAddr,

    /// RPC port for the compiler
    pub rpc_port: u16,

    /// Whether the compiler should attempt to install clang if it's not already installed
    pub install_clang: bool,

    /// Whether the compiler should attempt to install rustc if it's not already installed
    pub install_rustc: bool,

    /// Where to store compilation artifacts
    pub artifact_url: String,

    /// Directory to build artifacts in
    pub build_dir: String,

    /// Whether to use a local version of the UDF library or the published crate (only
    /// enable in development environments)
    #[serde(default)]
    pub use_local_udf_crate: bool,

    /// TLS configuration for compiler gRPC service
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct WorkerConfig {
    /// Bind address for the worker RPC socket
    pub bind_address: IpAddr,

    /// RPC port for the worker to listen on; set to 0 to use a random available port
    pub rpc_port: u16,

    /// Data port for the worker to listen on; set to 0 to use a random available port
    pub data_port: u16,

    /// Number of task slots for this worker
    pub task_slots: u32,

    /// ID for this worker
    #[serde(default)]
    pub id: Option<u64>,

    /// ID for the machine this worker is running on
    #[serde(default)]
    pub machine_id: Option<String>,

    /// Name to identify this worker (e.g., e.g., its hostname or a pod name)
    pub name: Option<String>,

    /// Size of the queues between nodes in the dataflow graph
    pub queue_size: u32,

    /// TLS configuration for worker TCP shuffling
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct NodeConfig {
    /// ID for this node
    pub id: Option<String>,

    /// Bind address for the node service
    pub bind_address: IpAddr,

    /// RPC port for the node service
    pub rpc_port: u16,

    /// Number of task slots for this node
    pub task_slots: u32,

    /// TLS configuration for Node gRPC
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

impl NodeConfig {
    pub fn id(&self) -> u64 {
        todo!()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct AdminConfig {
    /// Bind address for the admin service
    pub bind_address: IpAddr,

    /// HTTP port the admin service will listen on
    pub http_port: u16,

    /// TLS configuration for admin service
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    #[serde(default)]
    pub auth_mode: ApiAuthMode,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "kebab-case")]
pub enum DefaultSink {
    #[default]
    Preview,
    Stdout,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct PipelineConfig {
    /// Batch size
    pub source_batch_size: usize,

    /// Batch linger time (how long to wait before flushing)
    pub source_batch_linger: HumanReadableDuration,

    /// How often to flush aggregates
    pub update_aggregate_flush_interval: HumanReadableDuration,

    /// How many restarts to allow before moving to failed (-1 for infinite)
    pub allowed_restarts: i32,

    /// After this amount of time, we consider the job to be healthy and reset the restarts counter
    pub healthy_duration: HumanReadableDuration,

    /// Number of seconds to wait for a worker heartbeat before considering it dead
    pub worker_heartbeat_timeout: HumanReadableDuration,

    /// Amount of time to wait for workers to start up before considering them failed
    pub worker_startup_time: HumanReadableDuration,

    /// Amount of time to wait for tasks to startup before considering it failed
    pub task_startup_time: HumanReadableDuration,

    /// Initial backoff delay for retryable state errors
    pub state_initial_backoff: HumanReadableDuration,

    /// Maximum backoff delay for retryable state errors
    pub state_max_backoff: HumanReadableDuration,

    /// Default sink, for when none is specified
    #[serde(default)]
    pub default_sink: DefaultSink,

    pub chaining: ChainingConfig,

    pub compaction: CompactionConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ChainingConfig {
    /// Whether to enable operator chaining
    pub enabled: bool,
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum DatabaseType {
    Postgres,
    Sqlite,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct DatabaseConfig {
    pub r#type: DatabaseType,
    pub postgres: PostgresConfig,
    #[serde(default)]
    pub sqlite: SqliteConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct PostgresConfig {
    pub database_name: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Sensitive<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct SqliteConfig {
    pub path: PathBuf,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: dirs::config_dir()
                .map(|p| p.join("arroyo/config.sqlite"))
                .unwrap_or_else(|| PathBuf::from_str("config.sqlite").unwrap()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum Scheduler {
    Embedded,
    Process,
    Node,
    Kubernetes,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ProcessSchedulerConfig {
    pub slots_per_process: u32,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum ResourceMode {
    /// In per-slot mode, tasks are packed onto workers up to the `task-slots` config, and for each
    /// slot the amount of resources specified in `resources` is provided
    PerSlot,
    /// In per-pod mode, every pod has exactly `task-slots` slots, and exactly the resources in
    /// `resources`, even if it is scheduled for fewer slots. This mirrors the behavior before 0.11.
    PerPod,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct KubernetesSchedulerConfig {
    pub namespace: String,
    pub controller: Option<OwnerReference>,
    pub resource_mode: ResourceMode,
    pub worker: KubernetesWorkerConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct KubernetesWorkerConfig {
    name_prefix: String,

    #[serde(default)]
    name: Option<String>,

    pub image: String,

    pub image_pull_policy: String,

    #[serde(default)]
    pub image_pull_secrets: Vec<LocalObjectReference>,

    pub service_account_name: String,

    #[serde(default)]
    pub labels: BTreeMap<String, String>,

    #[serde(default)]
    pub annotations: BTreeMap<String, String>,

    #[serde(default)]
    pub env: Vec<EnvVar>,

    pub resources: ResourceRequirements,

    pub task_slots: u32,

    #[serde(default)]
    pub volumes: Vec<Volume>,

    #[serde(default)]
    pub volume_mounts: Vec<VolumeMount>,

    pub command: String,

    pub node_selector: BTreeMap<String, String>,

    pub tolerations: Vec<Toleration>,
}

impl KubernetesWorkerConfig {
    pub fn name(&self) -> String {
        self.name
            .as_ref()
            .cloned()
            .unwrap_or_else(|| format!("{}-worker", self.name_prefix))
    }
}

#[derive(Clone)]
pub struct HumanReadableDuration {
    duration: Duration,
    original: String,
}

impl From<Duration> for HumanReadableDuration {
    fn from(value: Duration) -> Self {
        Self {
            duration: value,
            original: format!("{}ns", value.as_nanos()),
        }
    }
}

impl Deref for HumanReadableDuration {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.duration
    }
}

impl Debug for HumanReadableDuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.original.fmt(f)
    }
}

impl Serialize for HumanReadableDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.original)
    }
}

impl<'de> Deserialize<'de> for HumanReadableDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str = String::deserialize(deserializer)?;

        let r = Regex::new(r"^(\d+)\s*([a-zA-Zµ]+)$").unwrap();
        let captures = r
            .captures(&str)
            .ok_or_else(|| de::Error::custom(format!("invalid duration specification '{str}'")))?;
        let mut capture = captures.iter();

        capture.next();

        let n: u64 = capture.next().unwrap().unwrap().as_str().parse().unwrap();
        let unit = capture.next().unwrap().unwrap().as_str();

        let duration = match unit {
            "ns" | "nanos" => Duration::from_nanos(n),
            "µs" | "micros" => Duration::from_micros(n),
            "ms" | "millis" => Duration::from_millis(n),
            "s" | "secs" | "seconds" => Duration::from_secs(n),
            "m" | "mins" | "minutes" => Duration::from_secs(n * 60),
            "h" | "hrs" | "hours" => Duration::from_secs(n * 60 * 60),
            x => return Err(de::Error::custom(format!("unknown time unit '{x}'"))),
        };

        Ok(HumanReadableDuration {
            duration,
            original: str,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct LogConfig {
    /// Set the log format
    #[serde(default)]
    pub format: LogFormat,

    /// Nonblocking logging may reduce tail latency at the cost of higher memory usage
    #[serde(default)]
    pub nonblocking: bool,

    /// Set the number of lines to buffer before dropping logs or exerting backpressure on senders
    /// Only valid when nonblocking is set to true
    pub buffered_lines_limit: usize,

    /// Set switch whether record file line number in log
    #[serde(default)]
    pub enable_file_line: bool,

    /// Set switch whether record file name in log
    #[serde(default)]
    pub enable_file_name: bool,

    /// Static logging fields
    #[serde(default)]
    pub static_fields: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum LogFormat {
    #[default]
    Plaintext,
    Json,
    Logfmt,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct RunConfig {
    /// Supplies the default query for `arroyo run`; otherwise the query is read from the command
    /// line or from stdin
    pub query: Option<String>,

    /// Sets the state directory, where state will be read from and written to
    pub state_dir: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Sensitive<T: Serialize + DeserializeOwned + Debug + Clone>(T);

impl<'de, T: Serialize + DeserializeOwned + Debug + Clone> Deserialize<'de> for Sensitive<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Sensitive(T::deserialize(deserializer)?))
    }
}

impl<T: Serialize + DeserializeOwned + Debug + Clone> Serialize for Sensitive<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(SENSITIVE_MASK)
    }
}

impl<T: Serialize + DeserializeOwned + Debug + Clone> Deref for Sensitive<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Serialize + DeserializeOwned + Debug + Clone> std::fmt::Display for Sensitive<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{SENSITIVE_MASK}")
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct TlsConfig {
    /// Whether TLS is enabled
    pub enabled: bool,

    /// Path to the certificate file (PEM format)
    pub cert_file: Option<PathBuf>,

    /// Path to the private key file (PEM format)
    pub key_file: Option<PathBuf>,

    /// Path to a CA cert; if set mTLS will be enabled on all links
    pub mtls_ca_file: Option<PathBuf>,
}

pub struct LoadedTlsConfig {
    pub cert: Vec<u8>,
    pub key: Vec<u8>,
    pub mtls_ca: Option<Vec<u8>>,
}

impl TlsConfig {
    async fn read(name: &str, path: Option<&PathBuf>) -> anyhow::Result<Vec<u8>> {
        let Some(path) = path else {
            bail!("TLS enabled but no {name} specified");
        };

        tokio::fs::read(path)
            .await
            .map_err(|e| anyhow!("could not read {name} file: {e}"))
    }

    pub async fn load(&self) -> anyhow::Result<LoadedTlsConfig> {
        let mtls_ca = if let Some(mtls_file) = &self.mtls_ca_file {
            Some(Self::read("mtls-ca-file", Some(mtls_file)).await?)
        } else {
            None
        };

        Ok(LoadedTlsConfig {
            cert: Self::read("cert-file", self.cert_file.as_ref()).await?,
            key: Self::read("key-file", self.key_file.as_ref()).await?,
            mtls_ca,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{load_config, Config, DatabaseType, Scheduler, SqliteConfig};
    use url::Url;

    #[test]
    fn test_config() {
        figment::Jail::expect_with(|jail| {
            // test default loading
            let _config: Config = load_config(&[]).extract().unwrap();

            // try overriding database by config file
            jail.create_file(
                "arroyo.toml",
                r#"
            [database]
            type = "sqlite"
            "#,
            )
            .unwrap();

            let config: Config = load_config(&[]).extract().unwrap();
            assert_eq!(config.database.sqlite.path, SqliteConfig::default().path);
            assert_eq!(config.database.r#type, DatabaseType::Sqlite);

            // try overriding with environment variables
            jail.set_env("ARROYO__ADMIN__HTTP_PORT", 9111);
            let config: Config = load_config(&[]).extract().unwrap();
            assert_eq!(config.admin.http_port, 9111);

            Ok(())
        });
    }

    #[test]
    fn test_legacy_config() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("SCHEDULER", "kubernetes");
            jail.set_env("CONTROLLER_ADDR", "http://localhost:9092");
            jail.set_env("CHECKPOINT_URL", "s3:///checkpoint/path");
            jail.set_env("ARTIFACT_URL", "s3:///artifact/path");
            jail.set_env("DATABASE_PORT", "5000");

            let config: Config = load_config(&[]).extract().unwrap();
            assert_eq!(config.controller.scheduler, Scheduler::Kubernetes);
            assert_eq!(
                config.controller_endpoint,
                Some(Url::parse("http://localhost:9092").unwrap())
            );
            assert_eq!(config.checkpoint_url, "s3:///checkpoint/path");
            assert_eq!(config.compiler.artifact_url, "s3:///artifact/path");
            assert_eq!(config.database.postgres.port, 5000);
            Ok(())
        });
    }

    #[test]
    fn test_sensitive_config() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("ARROYO__DATABASE__POSTGRES__PASSWORD", "sup3r-s3cr3t-p@ss");

            let config: Config = load_config(&[]).extract().unwrap();
            assert_eq!(&*config.database.postgres.password, "sup3r-s3cr3t-p@ss");

            let v: serde_json::Value =
                serde_json::from_str(&serde_json::to_string(&config).unwrap()).unwrap();
            assert_eq!(
                v.pointer("/database/postgres/password")
                    .unwrap()
                    .as_str()
                    .unwrap(),
                "*********"
            );

            assert_eq!(
                std::format!("{}", config.database.postgres.password),
                "*********"
            );

            Ok(())
        });
    }
}
