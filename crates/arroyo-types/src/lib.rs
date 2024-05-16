use crate::ports::CONTROLLER_GRPC;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_array::RecordBatch;
use bincode::{Decode, Encode};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::env;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::ops::{Range, RangeInclusive};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Copy, Hash, Debug, Clone, Eq, PartialEq, Encode, Decode, PartialOrd, Ord, Deserialize)]
pub struct Window {
    pub start: SystemTime,
    pub end: SystemTime,
}

impl Window {
    pub fn new(start: SystemTime, end: SystemTime) -> Self {
        Self { start, end }
    }

    pub fn session(start: SystemTime, gap: Duration) -> Self {
        Self {
            start,
            end: start + gap,
        }
    }

    pub fn contains(&self, t: SystemTime) -> bool {
        self.start <= t && t < self.end
    }

    pub fn size(&self) -> Duration {
        self.end.duration_since(self.start).unwrap_or_default()
    }

    pub fn extend(&self, new_end: SystemTime, max_size: Duration) -> Window {
        let new_end = self.end.max(new_end);
        Window {
            start: self.start,
            end: if new_end.duration_since(self.start).unwrap_or_default() > max_size {
                self.start + max_size
            } else {
                new_end
            },
        }
    }
}

impl From<Range<SystemTime>> for Window {
    fn from(value: Range<SystemTime>) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}

impl Serialize for Window {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("Window", 2)?;

        state.serialize_field("start", &to_millis(self.start))?;
        state.serialize_field("end", &to_millis(self.end))?;

        state.end()
    }
}

pub const DEFAULT_LINGER: Duration = Duration::from_millis(100);
pub const DEFAULT_BATCH_SIZE: usize = 512;
pub const QUEUE_SIZE_ENV: &str = "QUEUE_SIZE";
pub const DEFAULT_QUEUE_SIZE: u32 = 8 * 1024;
pub const TASK_SLOTS_ENV: &str = "TASK_SLOTS";
pub const CONTROLLER_ADDR_ENV: &str = "CONTROLLER_ADDR";
pub const API_ADDR_ENV: &str = "API_ADDR";
pub const NODE_ID_ENV: &str = "NODE_ID_ENV";
pub const WORKER_ID_ENV: &str = "WORKER_ID_ENV";
pub const JOB_ID_ENV: &str = "JOB_ID_ENV";
pub const RUN_ID_ENV: &str = "RUN_ID_ENV";
pub const ARROYO_PROGRAM_ENV: &str = "ARROYO_PROGRAM";
pub const ARROYO_PROGRAM_FILE_ENV: &str = "ARROYO_PROGRAM_FILE";
pub const COMPILER_ADDR_ENV: &str = "COMPILER_ADDR";
pub const NOMAD_ENDPOINT_ENV: &str = "NOMAD_ENDPOINT";
pub const NOMAD_DC_ENV: &str = "NOMAD_DC";

pub const DATABASE_ENV: &str = "DATABASE";
// postgres configs
pub const DATABASE_NAME_ENV: &str = "DATABASE_NAME";
pub const DATABASE_HOST_ENV: &str = "DATABASE_HOST";
pub const DATABASE_PORT_ENV: &str = "DATABASE_PORT";
pub const DATABASE_USER_ENV: &str = "DATABASE_USER";
pub const DATABASE_PASSWORD_ENV: &str = "DATABASE_PASSWORD";
// sqlite configs
pub const DATABASE_PATH_ENV: &str = "DATABASE_PATH";

pub const ADMIN_PORT_ENV: &str = "ADMIN_PORT";
pub const GRPC_PORT_ENV: &str = "GRPC_PORT";
pub const HTTP_PORT_ENV: &str = "HTTP_PORT";
pub const COMPILER_PORT_ENV: &str = "COMPILER_PORT";

pub const UPDATE_AGGREGATE_FLUSH_MS_ENV: &str = "UPDATE_AGGREGATE_FLUSH_MS";
pub const BATCH_SIZE_ENV: &str = "BATCH_SIZE";
pub const BATCH_LINGER_MS_ENV: &str = "BATCH_LINGER_MS";
pub const USE_LOCAL_UDF_LIB_ENV: &str = "USE_LOCAL_UDF_LIB";

pub const ASSET_DIR_ENV: &str = "ASSET_DIR";
// Endpoint that the frontend should query for the API
pub const API_ENDPOINT_ENV: &str = "API_ENDPOINT";
// The rate parameter (e.g., "15s") used by the API when querying prometheus metrics -- this should
// be at least 4x the configured scrape interval for your prometheus config
pub const API_METRICS_RATE_ENV: &str = "API_METRICS_RATE";

// storage configuration
pub const S3_ENDPOINT_ENV: &str = "S3_ENDPOINT";
pub const S3_REGION_ENV: &str = "S3_REGION";
pub const CHECKPOINT_URL_ENV: &str = "CHECKPOINT_URL";

// compiler service
pub const ARTIFACT_URL_ENV: &str = "ARTIFACT_URL";
pub const ARTIFACT_URL_DEFAULT: &str = "/tmp/arroyo/artifacts";
pub const COMPILER_FEATURES_ENV: &str = "COMPILER_FEATURES";
pub const INSTALL_CLANG_ENV: &str = "INSTALL_CLANG";
pub const INSTALL_RUSTC_ENV: &str = "INSTALL_RUSTC";

// process scheduler
pub const SLOTS_PER_NODE: &str = "SLOTS_PER_NODE";

// kubernetes scheduler configuration
pub const K8S_NAMESPACE_ENV: &str = "K8S_NAMESPACE";
pub const K8S_WORKER_NAME_ENV: &str = "K8S_WORKER_NAME";
pub const K8S_WORKER_IMAGE_ENV: &str = "K8S_WORKER_IMAGE";
pub const K8S_WORKER_IMAGE_PULL_POLICY_ENV: &str = "K8S_WORKER_IMAGE_PULL_POLICY";
pub const K8S_WORKER_SERVICE_ACCOUNT_NAME_ENV: &str = "K8S_WORKER_SERVICE_ACCOUNT_NAME";
pub const K8S_WORKER_LABELS_ENV: &str = "K8S_WORKER_LABELS";
pub const K8S_WORKER_ANNOTATIONS_ENV: &str = "K8S_WORKER_ANNOTATIONS";
pub const K8S_WORKER_RESOURCES_ENV: &str = "K8S_WORKER_RESOURCES";
pub const K8S_WORKER_SLOTS_ENV: &str = "K8S_WORKER_SLOTS";
pub const K8S_WORKER_VOLUMES_ENV: &str = "K8S_WORKER_VOLUMES";
pub const K8S_WORKER_VOLUME_MOUNTS_ENV: &str = "K8S_WORKER_VOLUME_MOUNTS";
pub const K8S_WORKER_CONFIG_MAP_ENV: &str = "K8S_WORKER_CONFIG_MAP";

// node
pub const NODE_SLOTS_ENV: &str = "NODE_SLOTS";

// telemetry configuration
pub const DISABLE_TELEMETRY_ENV: &str = "DISABLE_TELEMETRY";
pub const POSTHOG_KEY: &str = "phc_ghJo7Aa9QOo4inoWFYZP7o2aKszllEUyH77QeFgznUe";
pub fn telemetry_enabled() -> bool {
    match env::var(DISABLE_TELEMETRY_ENV) {
        Ok(val) => val != "true",
        Err(_) => true,
    }
}

pub fn bool_config(var: &str, default: bool) -> bool {
    if let Ok(v) = env::var(var) {
        match v.to_lowercase().as_str() {
            "true" | "yes" | "1" => {
                return true;
            }
            "false" | "no" | "0" => {
                return false;
            }
            _ => {}
        }
    };
    default
}

pub fn string_config(var: &str, default: &str) -> String {
    env::var(var).unwrap_or_else(|_| default.to_string())
}

pub fn u32_config(var: &str, default: u32) -> u32 {
    env::var(var)
        .map(|s| u32::from_str(&s).unwrap_or(default))
        .unwrap_or(default)
}

pub fn duration_millis_config(var: &str, default: Duration) -> Duration {
    env::var(var)
        .map(|s| {
            u64::from_str(&s)
                .map(Duration::from_millis)
                .unwrap_or(default)
        })
        .unwrap_or(default)
}

// These seeds were randomly generated; changing them will break existing state
pub const HASH_SEEDS: [u64; 4] = [
    5093852630788334730,
    1843948808084437226,
    8049205638242432149,
    17942305062735447798,
];

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
}

impl Display for DatabaseConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}:{}/{}", self.user, self.host, self.port, self.name)
    }
}

impl DatabaseConfig {
    pub fn load() -> Self {
        DatabaseConfig {
            name: env::var(DATABASE_NAME_ENV).unwrap_or_else(|_| "arroyo".to_string()),
            host: env::var(DATABASE_HOST_ENV).unwrap_or_else(|_| "localhost".to_string()),
            port: u16::from_str(
                &env::var(DATABASE_PORT_ENV).unwrap_or_else(|_| "5432".to_string()),
            )
            .unwrap_or(5432),
            user: env::var(DATABASE_USER_ENV).unwrap_or_else(|_| "arroyo".to_string()),
            password: env::var(DATABASE_PASSWORD_ENV).unwrap_or_else(|_| "arroyo".to_string()),
        }
    }
}

// default ports for development; overridden in production to
pub mod ports {
    pub const CONTROLLER_GRPC: u16 = 9190;
    pub const CONTROLLER_ADMIN: u16 = 9191;

    pub const NODE_GRPC: u16 = 9290;
    pub const NODE_ADMIN: u16 = 9291;

    pub const API_HTTP: u16 = 8000;
    pub const API_ADMIN: u16 = 8001;

    pub const COMPILER_GRPC: u16 = 9000;
    pub const COMPILER_ADMIN: u16 = 9001;
}

pub fn grpc_port(service: &str, default: u16) -> u16 {
    service_port(service, default, GRPC_PORT_ENV)
}

pub fn admin_port(service: &str, default: u16) -> u16 {
    service_port(service, default, ADMIN_PORT_ENV)
}

pub fn service_port(service: &str, default: u16, env_var: &str) -> u16 {
    env::var(format!("{}_{}", service.to_uppercase(), env_var))
        .ok()
        .or(env::var(env_var).ok())
        .map(|s| u16::from_str(&s).unwrap_or_else(|_| panic!("Invalid setting for {}", env_var)))
        .unwrap_or(default)
}

pub fn default_controller_addr() -> String {
    format!(
        "http://localhost:{}",
        grpc_port("controller", CONTROLLER_GRPC)
    )
}

#[derive(Debug, Hash, Eq, PartialEq, Copy, Clone)]
pub struct WorkerId(pub u64);

impl WorkerId {
    pub fn from_env() -> Option<WorkerId> {
        std::env::var(WORKER_ID_ENV)
            .map(|s| u64::from_str(&s).unwrap())
            .ok()
            .map(WorkerId)
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Copy, Clone)]
pub struct NodeId(pub u64);

impl NodeId {
    pub fn from_env() -> Option<NodeId> {
        std::env::var(NODE_ID_ENV)
            .map(|s| NodeId(u64::from_str(&s).unwrap()))
            .ok()
    }
}

pub fn to_millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub fn to_micros(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
}

pub fn from_millis(ts: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ts)
}

pub fn from_micros(ts: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(ts)
}

pub fn to_nanos(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

pub fn from_nanos(ts: u128) -> SystemTime {
    UNIX_EPOCH
        + Duration::from_secs((ts / 1_000_000_000) as u64)
        + Duration::from_nanos((ts % 1_000_000_000) as u64)
}

pub fn print_time(time: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(time)
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

pub fn single_item_hash_map<I: Into<K>, K: Hash + Eq, V>(key: I, value: V) -> HashMap<K, V> {
    let mut map = HashMap::new();
    map.insert(key.into(), value);
    map
}

// used for avro serialization -- returns the number of days since the UNIX EPOCH
pub fn days_since_epoch(time: SystemTime) -> i32 {
    time.duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .div_euclid(86400) as i32
}

pub fn string_to_map(s: &str, pair_delimeter: char) -> Option<HashMap<String, String>> {
    if s.trim().is_empty() {
        return Some(HashMap::new());
    }

    s.split(',')
        .map(|s| {
            let mut kv = s.trim().split(pair_delimeter);
            Some((kv.next()?.trim().to_string(), kv.next()?.trim().to_string()))
        })
        .collect()
}

pub trait Key: Debug + Clone + Encode + Decode + Hash + PartialEq + Eq + Send + 'static {}
impl<T: Debug + Clone + Encode + Decode + Hash + PartialEq + Eq + Send + 'static> Key for T {}

pub trait Data: Debug + Clone + Encode + Decode + Send + 'static {}
impl<T: Debug + Clone + Encode + Decode + Send + 'static> Data for T {}

#[derive(Debug, Copy, Clone, Encode, Decode, PartialEq, Eq)]
pub enum Watermark {
    EventTime(SystemTime),
    Idle,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArrowMessage {
    Data(RecordBatch),
    Signal(SignalMessage),
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum SignalMessage {
    Barrier(CheckpointBarrier),
    Watermark(Watermark),
    Stop,
    EndOfData,
}

impl ArrowMessage {
    pub fn is_end(&self) -> bool {
        matches!(
            self,
            ArrowMessage::Signal(SignalMessage::Stop)
                | ArrowMessage::Signal(SignalMessage::EndOfData)
        )
    }
}

#[derive(Debug, Clone)]
pub struct UserError {
    pub name: String,
    pub details: String,
}

impl UserError {
    pub fn new(name: impl Into<String>, details: impl Into<String>) -> UserError {
        UserError {
            name: name.into(),
            details: details.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceError {
    BadData { details: String },
    Other { name: String, details: String },
}

impl SourceError {
    pub fn bad_data(details: impl Into<String>) -> SourceError {
        SourceError::BadData {
            details: details.into(),
        }
    }
    pub fn other(name: impl Into<String>, details: impl Into<String>) -> SourceError {
        SourceError::Other {
            name: name.into(),
            details: details.into(),
        }
    }

    pub fn details(&self) -> &String {
        match self {
            SourceError::BadData { details } | SourceError::Other { details, .. } => details,
        }
    }
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Serialize, Deserialize)]
pub enum UpdatingData<T: Data> {
    Retract(T),
    Update { old: T, new: T },
    Append(T),
}

impl<T: Data> UpdatingData<T> {
    pub fn lower(&self) -> T {
        match self {
            UpdatingData::Retract(_) => {
                panic!("cannot lower retractions")
            }
            UpdatingData::Update { new, .. } => new.clone(),
            UpdatingData::Append(t) => t.clone(),
        }
    }

    pub fn unwrap_append(&self) -> &T {
        match self {
            UpdatingData::Append(t) => t,
            _ => panic!("UpdatingData is not an append"),
        }
    }
}

#[derive(Clone, Encode, Decode, Debug, Serialize, Deserialize, PartialEq)]
#[serde(try_from = "DebeziumShadow<T>")]
pub struct Debezium<T: Data> {
    pub before: Option<T>,
    pub after: Option<T>,
    pub op: DebeziumOp,
}

// Use a shadow type to perform post-deserialization validation that the expected fields
// are set; see https://github.com/serde-rs/serde/issues/642#issuecomment-683276351
#[derive(Clone, Encode, Decode, Debug, Serialize, Deserialize, PartialEq)]
struct DebeziumShadow<T: Data> {
    before: Option<T>,
    after: Option<T>,
    op: DebeziumOp,
}

impl<T: Data> TryFrom<DebeziumShadow<T>> for Debezium<T> {
    type Error = &'static str;

    fn try_from(value: DebeziumShadow<T>) -> Result<Self, Self::Error> {
        match (value.op, &value.before, &value.after) {
            (DebeziumOp::Create, _, None) => {
                Err("`after` must be set for Debezium create messages")
            }
            (DebeziumOp::Update, None, _) => {
                Err("`before` must be set for Debezium update messages; for Postgres you may need to set REPLICA IDENTIFY FULL on your database")
            }
            (DebeziumOp::Update, _, None) => {
                Err("`after` must be set for Debezium update messages")
            }
            (DebeziumOp::Delete, None, _) => {
                Err("`before` must be set for Debezium delete messages")
            }
            _ => Ok(Debezium {
                before: value.before,
                after: value.after,
                op: value.op,
            }),
        }
    }
}

//Debezium ops with single character serialization
#[derive(Copy, Clone, Encode, Decode, Debug, PartialEq)]
pub enum DebeziumOp {
    Create,
    Update,
    Delete,
}

impl ToString for DebeziumOp {
    fn to_string(&self) -> String {
        match self {
            DebeziumOp::Create => "c",
            DebeziumOp::Update => "u",
            DebeziumOp::Delete => "d",
        }
        .to_string()
    }
}

impl<'de> Deserialize<'de> for DebeziumOp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "c" => Ok(DebeziumOp::Create),
            "u" => Ok(DebeziumOp::Update),
            "d" => Ok(DebeziumOp::Delete),
            "r" => Ok(DebeziumOp::Create),
            _ => Err(serde::de::Error::custom(format!(
                "Invalid DebeziumOp {}",
                s
            ))),
        }
    }
}

impl Serialize for DebeziumOp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            DebeziumOp::Create => serializer.serialize_str("c"),
            DebeziumOp::Update => serializer.serialize_str("u"),
            DebeziumOp::Delete => serializer.serialize_str("d"),
        }
    }
}

#[derive(Clone, Encode, Decode, Debug, Serialize, Deserialize, PartialEq)]
pub enum JoinType {
    /// Inner Join
    Inner,
    /// Left Join
    Left,
    /// Right Join
    Right,
    /// Full Join
    Full,
}

pub trait RecordBatchBuilder: Default + Debug + Sync + Send + 'static {
    type Data: Data;
    fn nullable() -> Self;
    fn add_data(&mut self, data: Option<Self::Data>);
    fn flush(&mut self) -> RecordBatch;
    fn as_struct_array(&mut self) -> arrow::array::StructArray;
    fn schema(&self) -> SchemaRef;
}

/// A reference-counted reference to a [TaskInfo].
pub type TaskInfoRef = Arc<TaskInfo>;

#[derive(Eq, PartialEq, Hash, Debug, Clone, Encode, Decode)]
pub struct TaskInfo {
    pub job_id: String,
    pub operator_name: String,
    pub operator_id: String,
    pub task_index: usize,
    pub parallelism: usize,
    pub key_range: RangeInclusive<u64>,
}

impl TaskInfo {
    pub fn for_test(job_id: &str, operator_id: &str) -> Self {
        Self {
            job_id: job_id.to_string(),
            operator_name: "op".to_string(),
            operator_id: operator_id.to_string(),
            task_index: 0,
            parallelism: 1,
            key_range: 0..=u64::MAX,
        }
    }

    pub fn metric_label_map(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.insert("operator_id".to_string(), self.operator_id.clone());
        labels.insert("subtask_idx".to_string(), format!("{}", self.task_index));
        labels.insert("operator_name".to_string(), self.operator_name.clone());
        labels
    }
}

pub fn get_test_task_info() -> TaskInfo {
    TaskInfo {
        job_id: "instance-1".to_string(),
        operator_name: "test-operator".to_string(),
        operator_id: "test-operator-1".to_string(),
        task_index: 0,
        parallelism: 1,
        key_range: 0..=u64::MAX,
    }
}

#[derive(Copy, Clone, Debug, Encode, Decode, Hash, PartialEq, Eq, serde::Serialize)]
pub struct GlobalKey {}

#[derive(Encode, Decode, Debug, Copy, Clone, Eq, PartialEq, serde::Serialize)]
pub struct ImpulseEvent {
    pub counter: u64,
    pub subtask_index: u64,
}

#[derive(Encode, Decode, Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RawJson {
    pub value: String,
}

pub fn raw_schema() -> Schema {
    Schema::new(vec![ArroyoExtensionType::add_metadata(
        Some(ArroyoExtensionType::JSON),
        Field::new("value", DataType::Utf8, false),
    )])
}

pub static MESSAGES_RECV: &str = "arroyo_worker_messages_recv";
pub static MESSAGES_SENT: &str = "arroyo_worker_messages_sent";
pub static BYTES_RECV: &str = "arroyo_worker_bytes_recv";
pub static BYTES_SENT: &str = "arroyo_worker_bytes_sent";
pub static BATCHES_RECV: &str = "arroyo_worker_batches_recv";
pub static BATCHES_SENT: &str = "arroyo_worker_batches_sent";
pub static TX_QUEUE_SIZE: &str = "arroyo_worker_tx_queue_size";
pub static TX_QUEUE_REM: &str = "arroyo_worker_tx_queue_rem";
pub static DESERIALIZATION_ERRORS: &str = "arroyo_worker_deserialization_errors";

#[derive(Debug, Copy, Clone, Encode, Decode, PartialEq, Eq)]
pub struct CheckpointBarrier {
    pub epoch: u32,
    pub min_epoch: u32,
    pub timestamp: SystemTime,
    pub then_stop: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Hash, Serialize)]
pub enum DatePart {
    Year,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
    DayOfWeek,
    DayOfYear,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd, Serialize)]
pub enum DateTruncPrecision {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
}

impl TryFrom<&str> for DatePart {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let value_lower = value.to_lowercase();
        match value_lower.as_str() {
            "year" => Ok(DatePart::Year),
            "month" => Ok(DatePart::Month),
            "week" => Ok(DatePart::Week),
            "day" => Ok(DatePart::Day),
            "hour" => Ok(DatePart::Hour),
            "minute" => Ok(DatePart::Minute),
            "second" => Ok(DatePart::Second),
            "millisecond" => Ok(DatePart::Millisecond),
            "microsecond" => Ok(DatePart::Microsecond),
            "nanosecond" => Ok(DatePart::Nanosecond),
            "dow" => Ok(DatePart::DayOfWeek),
            "doy" => Ok(DatePart::DayOfYear),
            _ => Err(format!("'{}' is not a valid DatePart", value)),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ArroyoExtensionType {
    JSON,
}

impl ArroyoExtensionType {
    pub fn from_map(map: &HashMap<String, String>) -> Option<Self> {
        match map.get("ARROW:extension:name")?.as_str() {
            "arroyo.json" => Some(Self::JSON),
            _ => None,
        }
    }

    pub fn add_metadata(v: Option<Self>, field: Field) -> Field {
        if let Some(v) = v {
            let mut m = HashMap::new();
            match v {
                ArroyoExtensionType::JSON => {
                    m.insert(
                        "ARROW:extension:name".to_string(),
                        "arroyo.json".to_string(),
                    );
                }
            }
            field.with_metadata(m)
        } else {
            field
        }
    }
}

impl TryFrom<&str> for DateTruncPrecision {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let value_lower = value.to_lowercase();
        match value_lower.as_str() {
            "year" => Ok(DateTruncPrecision::Year),
            "quarter" => Ok(DateTruncPrecision::Quarter),
            "month" => Ok(DateTruncPrecision::Month),
            "week" => Ok(DateTruncPrecision::Week),
            "day" => Ok(DateTruncPrecision::Day),
            "hour" => Ok(DateTruncPrecision::Hour),
            "minute" => Ok(DateTruncPrecision::Minute),
            "second" => Ok(DateTruncPrecision::Second),

            _ => Err(format!("'{}' is not a valid DateTruncPrecision", value)),
        }
    }
}

pub fn server_for_hash(x: u64, n: usize) -> usize {
    if n == 1 {
        0
    } else {
        let range_size = (u64::MAX / (n as u64)) + 1;
        (x / range_size) as usize
    }
}

pub fn range_for_server(i: usize, n: usize) -> RangeInclusive<u64> {
    if n == 1 {
        return 0..=u64::MAX;
    }
    let range_size = (u64::MAX / (n as u64)) + 1;
    let start = range_size * (i as u64);
    let end = if i + 1 == n {
        u64::MAX
    } else {
        start + range_size - 1
    };
    start..=end
}

pub fn should_flush(size: usize, time: Instant) -> bool {
    static FLUSH_SIZE: OnceLock<usize> = OnceLock::new();
    let flush_size =
        FLUSH_SIZE.get_or_init(|| u32_config(BATCH_SIZE_ENV, DEFAULT_BATCH_SIZE as u32) as usize);

    static FLUSH_LINGER: OnceLock<Duration> = OnceLock::new();
    let flush_linger =
        FLUSH_LINGER.get_or_init(|| duration_millis_config(BATCH_LINGER_MS_ENV, DEFAULT_LINGER));

    size > 0 && (size >= *flush_size || time.elapsed() >= *flush_linger)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_for_server() {
        let n = 6;

        for i in 0..(n - 1) {
            let range1 = range_for_server(i, n);
            let range2 = range_for_server(i + 1, n);

            assert_eq!(*range1.end() + 1, *range2.start(), "Ranges not adjacent");
            assert_eq!(
                i,
                server_for_hash(*range1.start(), n),
                "start not assigned to range."
            );
            assert_eq!(
                i,
                server_for_hash(*range1.end(), n),
                "end not assigned to range."
            );
        }

        let last_range = range_for_server(n - 1, n);
        assert_eq!(
            *last_range.end(),
            u64::MAX,
            "Last range does not contain u64::MAX"
        );
        assert_eq!(
            n - 1,
            server_for_hash(u64::MAX, n),
            "u64::MAX not in last range"
        );
    }

    #[test]
    fn test_server_for_hash() {
        let n = 2;
        let x = u64::MAX;

        let server_index = server_for_hash(x, n);
        let server_range = range_for_server(server_index, n);

        assert!(
            server_range.contains(&x),
            "u64::MAX is not in the correct range"
        );
    }
}
