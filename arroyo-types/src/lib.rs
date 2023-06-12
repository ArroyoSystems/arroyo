use bincode::{config, Decode, Encode};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Copy, Hash, Debug, Clone, Eq, PartialEq, Encode, Decode, PartialOrd, Ord, Deserialize)]
pub struct Window {
    pub start_time: SystemTime,
    pub end_time: SystemTime,
}

impl Serialize for Window {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("Window", 2)?;

        state.serialize_field("start", &to_millis(self.start_time))?;
        state.serialize_field("end", &to_millis(self.end_time))?;

        state.end()
    }
}

static BINCODE_CONF: config::Configuration = config::standard();

pub const TASK_SLOTS_ENV: &str = "TASK_SLOTS";
pub const CONTROLLER_ADDR_ENV: &str = "CONTROLLER_ADDR";
pub const API_ADDR_ENV: &str = "API_ADDR";
pub const NODE_ID_ENV: &str = "NODE_ID_ENV";
pub const WORKER_ID_ENV: &str = "WORKER_ID_ENV";
pub const JOB_ID_ENV: &str = "JOB_ID_ENV";
pub const RUN_ID_ENV: &str = "RUN_ID_ENV";
pub const REMOTE_COMPILER_ENDPOINT_ENV: &str = "REMOTE_COMPILER_ENDPOINT";
pub const NOMAD_ENDPOINT_ENV: &str = "NOMAD_ENDPOINT";
pub const NOMAD_DC_ENV: &str = "NOMAD_DC";

pub const DATABASE_NAME_ENV: &str = "DATABASE_NAME";
pub const DATABASE_HOST_ENV: &str = "DATABASE_HOST";
pub const DATABASE_PORT_ENV: &str = "DATABASE_PORT";
pub const DATABASE_USER_ENV: &str = "DATABASE_USER";
pub const DATABASE_PASSWORD_ENV: &str = "DATABASE_PASSWORD";

pub const ADMIN_PORT_ENV: &str = "ADMIN_PORT";
pub const GRPC_PORT_ENV: &str = "GRPC_PORT";
pub const HTTP_PORT_ENV: &str = "HTTP_PORT";

pub const ASSET_DIR_ENV: &str = "ASSET_DIR";
// Endpoint that the frontend should query for the API
pub const API_ENDPOINT_ENV: &str = "API_ENDPOINT";
// The rate parameter (e.g., "15s") used by the API when querying prometheus metrics -- this should
// be at least 4x the configured scrape interval for your prometheus config
pub const API_METRICS_RATE_ENV: &str = "API_METRICS_RATE";

pub const S3_REGION_ENV: &str = "S3_REGION";
pub const S3_BUCKET_ENV: &str = "S3_BUCKET";
pub const OUTPUT_DIR_ENV: &str = "OUTPUT_DIR";

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

// telemetry configuration
pub const DISABLE_TELEMETRY_ENV: &str = "DISABLE_TELEMETRY";
pub const POSTHOG_KEY: &str = "phc_ghJo7Aa9QOo4inoWFYZP7o2aKszllEUyH77QeFgznUe";
pub fn telemetry_enabled() -> bool {
    match env::var(DISABLE_TELEMETRY_ENV) {
        Ok(val) => val != "true",
        Err(_) => true,
    }
}

pub fn string_config(var: &str, default: &str) -> String {
    env::var(var).unwrap_or_else(|_| default.to_string())
}

pub fn u32_config(var: &str, default: u32) -> u32 {
    env::var(var)
        .map(|s| u32::from_str(&s).unwrap_or(default))
        .unwrap_or(default)
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
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
    pub const API_GRPC: u16 = 8001;
    pub const API_ADMIN: u16 = 8002;

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
    pub fn from_env() -> NodeId {
        NodeId(
            std::env::var(NODE_ID_ENV)
                .map(|s| u64::from_str(&s).unwrap())
                .unwrap_or_else(|_| panic!("{} not set", NODE_ID_ENV)),
        )
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
    UNIX_EPOCH + Duration::from_nanos(ts as u64)
}

pub fn string_to_map(s: &str) -> Option<HashMap<String, String>> {
    if s.trim().is_empty() {
        return Some(HashMap::new());
    }

    s.split(',')
        .map(|s| {
            let mut kv = s.trim().split(':');
            Some((kv.next()?.trim().to_string(), kv.next()?.trim().to_string()))
        })
        .collect()
}

pub trait Key: Debug + Clone + Encode + Decode + Hash + PartialEq + Eq + Send + 'static {}
impl<T: Debug + Clone + Encode + Decode + Hash + PartialEq + Eq + Send + 'static> Key for T {}

pub trait Data: Debug + Clone + Encode + Decode + Send + PartialEq + 'static {}
impl<T: Debug + Clone + Encode + Decode + Send + PartialEq + 'static> Data for T {}

#[derive(Debug, Clone, Encode, Decode)]
pub enum Message<K: Key, T: Data> {
    Record(Record<K, T>),
    Barrier(CheckpointBarrier),
    Watermark(SystemTime),
    Stop,
    EndOfData,
}

impl<K: Key, T: Data> Message<K, T> {
    pub fn is_end(&self) -> bool {
        matches!(self, Message::Stop | Message::EndOfData)
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Record<K: Key, T: Data> {
    pub timestamp: SystemTime,
    pub key: Option<K>,
    pub value: T,
}

unsafe impl<K: Key, T: Data> Sync for Record<K, T> {}

impl<K: Key, T: Data> Record<K, T> {
    pub fn from_value(timestamp: SystemTime, value: T) -> Option<Record<(), T>> {
        Some(Record {
            timestamp,
            key: None,
            value,
        })
    }

    pub fn from_bytes(bs: &[u8]) -> Result<Record<K, T>, bincode::error::DecodeError> {
        let (record, len) = bincode::decode_from_slice(bs, BINCODE_CONF)?;

        if len != bs.len() {
            return Err(bincode::error::DecodeError::ArrayLengthMismatch {
                required: bs.len(),
                found: len,
            });
        }

        Ok(record)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        bincode::encode_to_vec(self, BINCODE_CONF)
    }
}

#[derive(Debug, Clone, Encode, Decode)]
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

pub mod nexmark {
    use bincode::{Decode, Encode};

    #[derive(
        Debug,
        Clone,
        Encode,
        Decode,
        Eq,
        PartialEq,
        Hash,
        PartialOrd,
        Ord,
        serde::Serialize,
        serde::Deserialize,
    )]
    pub struct Person {
        pub id: i64,
        pub name: String,
        pub email_address: String,
        pub credit_card: String,
        pub city: String,
        pub state: String,
        pub datetime: std::time::SystemTime,
        pub extra: String,
    }

    #[derive(
        Debug,
        Clone,
        Encode,
        Decode,
        Eq,
        PartialEq,
        Hash,
        PartialOrd,
        Ord,
        serde::Serialize,
        serde::Deserialize,
    )]
    pub struct Auction {
        pub id: i64,
        pub item_name: String,
        pub description: String,
        pub initial_bid: i64,
        pub reserve: i64,
        pub datetime: std::time::SystemTime,
        pub expires: std::time::SystemTime,
        pub seller: i64,
        pub category: i64,
        pub extra: String,
    }

    #[derive(
        Debug,
        Clone,
        Encode,
        Decode,
        Eq,
        PartialEq,
        Hash,
        PartialOrd,
        Ord,
        serde::Serialize,
        serde::Deserialize,
    )]
    pub struct Bid {
        pub auction: i64,
        pub bidder: i64,
        pub price: i64,
        pub channel: String,
        pub url: String,
        pub datetime: std::time::SystemTime,
        pub extra: String,
    }

    // TODO: Use an enum once Data Fusion supports Unions.
    #[derive(
        Debug,
        Clone,
        Encode,
        Decode,
        Eq,
        PartialEq,
        Hash,
        PartialOrd,
        Ord,
        serde::Serialize,
        serde::Deserialize,
    )]
    pub struct Event {
        pub person: Option<Person>,
        pub bid: Option<Bid>,
        pub auction: Option<Auction>,
    }

    impl Event {
        pub fn person(person: Person) -> Event {
            Event {
                person: Some(person),
                bid: None,
                auction: None,
            }
        }
        pub fn bid(bid: Bid) -> Event {
            Event {
                person: None,
                bid: Some(bid),
                auction: None,
            }
        }
        pub fn auction(auction: Auction) -> Event {
            Event {
                person: None,
                bid: None,
                auction: Some(auction),
            }
        }
    }
}

pub static MESSAGES_RECV: &str = "arroyo_worker_messages_recv";
pub static MESSAGES_SENT: &str = "arroyo_worker_messages_sent";
pub static BYTES_RECV: &str = "arroyo_worker_bytes_recv";
pub static BYTES_SENT: &str = "arroyo_worker_bytes_sent";
pub static TX_QUEUE_SIZE: &str = "arroyo_worker_tx_queue_size";
pub static TX_QUEUE_REM: &str = "arroyo_worker_tx_queue_rem";

#[derive(Debug, Copy, Clone, Encode, Decode)]
pub struct CheckpointBarrier {
    pub epoch: u32,
    pub min_epoch: u32,
    pub timestamp: SystemTime,
    pub then_stop: bool,
}
