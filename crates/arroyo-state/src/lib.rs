use arrow_array::RecordBatch;
use arroyo_rpc::errors::{StateError, StorageError};
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, ExpiringKeyedTimeTableConfig, GlobalKeyedTableConfig,
    OperatorCheckpointMetadata, TableCheckpointMetadata, TableConfig, TableEnum,
};
use arroyo_types::single_item_hash_map;
use async_trait::async_trait;
use bincode::config::Configuration;
use bincode::{Decode, Encode};

use arroyo_rpc::config::config;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_storage::StorageProvider;
pub use arroyo_storage::StorageProviderFor;
use prost::Message;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;

mod metrics;
pub mod parquet;
pub(crate) mod schemas;
pub mod tables;

pub const BINCODE_CONFIG: Configuration = bincode::config::standard();
pub const FULL_KEY_RANGE: RangeInclusive<u64> = 0..=u64::MAX;

#[derive(Debug)]
pub enum StateMessage {
    Checkpoint(CheckpointMessage),
    Compaction(HashMap<String, TableCheckpointMetadata>),
    TableData { table: String, data: TableData },
}
#[derive(Debug)]
pub struct CheckpointMessage {
    epoch: u32,
    time: SystemTime,
    watermark: Option<SystemTime>,
    then_stop: bool,
}

#[derive(Debug)]
pub enum TableData {
    RecordBatch(RecordBatch),
    CommitData { data: Vec<u8> },
    KeyedData { key: Vec<u8>, value: Vec<u8> },
}

pub type StateBackend = parquet::ParquetBackend;

pub fn global_table_config(
    name: impl Into<String>,
    description: impl Into<String>,
) -> HashMap<String, TableConfig> {
    global_table_config_with_version(name, description, 0)
}

pub fn global_table_config_with_version(
    name: impl Into<String>,
    description: impl Into<String>,
    state_version: u32,
) -> HashMap<String, TableConfig> {
    let name = name.into();
    single_item_hash_map(
        name.clone(),
        TableConfig {
            table_type: TableEnum::GlobalKeyValue.into(),
            config: GlobalKeyedTableConfig {
                table_name: name,
                description: description.into(),
                uses_two_phase_commit: false,
            }
            .encode_to_vec(),
            state_version,
        },
    )
}

pub fn timestamp_table_config(
    name: impl Into<String>,
    description: impl Into<String>,
    retention: Duration,
    generational: bool,
    schema: ArroyoSchema,
) -> TableConfig {
    TableConfig {
        table_type: TableEnum::ExpiringKeyedTimeTable.into(),
        config: ExpiringKeyedTimeTableConfig {
            table_name: name.into(),
            description: description.into(),
            retention_micros: retention.as_micros() as u64,
            generational,
            schema: Some(schema.into()),
        }
        .encode_to_vec(),
        state_version: 0,
    }
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub struct DeleteTimeKeyOperation {
    pub timestamp: SystemTime,
    pub key: Vec<u8>,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub struct DeleteKeyOperation {
    pub key: Vec<u8>,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub struct DeleteValueOperation {
    pub key: Vec<u8>,
    pub timestamp: SystemTime,
    pub value: Vec<u8>,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub struct DeleteTimeRangeOperation {
    pub key: Vec<u8>,
    pub start: SystemTime,
    pub end: SystemTime,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub enum DataOperation {
    Insert,
    DeleteTimeKey(DeleteTimeKeyOperation), // delete single key of a TimeKeyMap
    DeleteKey(DeleteKeyOperation),         // delete all data for a key in a KeyTimeMultiMap
    DeleteValue(DeleteValueOperation),     // delete single value of a KeyTimeMultiMap
    DeleteTimeRange(DeleteTimeRangeOperation), // delete all values for key in range (only for KeyTimeMultiMap)
}
#[async_trait]
pub trait BackingStore {
    /// loads the checkpoint metadata for a given job id and epoch
    async fn load_checkpoint_metadata(
        role: &StorageProviderFor,
        job_id: &str,
        epoch: u32,
    ) -> Result<CheckpointMetadata, StateError>;

    /// loads the operator checkpoint metadata for a given job id, operator id, and epoch
    async fn load_operator_metadata(
        role: &StorageProviderFor,
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>, StateError>;

    /// returns the name of the BackingStore implementation
    fn name() -> &'static str;

    /// writes the operator checkpoint metadata to the backing store
    async fn write_operator_checkpoint_metadata(
        role: &StorageProviderFor,
        metadata: OperatorCheckpointMetadata,
    ) -> Result<(), StateError>;

    /// writes the checkpoint metadata to the backing store
    async fn write_checkpoint_metadata(
        role: &StorageProviderFor,
        metadata: CheckpointMetadata,
    ) -> Result<(), StateError>;

    /// cleans up a checkpoint by deleting data that is no longer needed
    async fn cleanup_checkpoint(
        role: &StorageProviderFor,
        metadata: CheckpointMetadata,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<(), StateError>;
}

pub fn hash_key<K: Hash>(key: &K) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Per-URL cache of [`StorageProvider`] instances.
fn storage_provider_cache() -> &'static Mutex<HashMap<String, Arc<StorageProvider>>> {
    static CACHE: std::sync::OnceLock<Mutex<HashMap<String, Arc<StorageProvider>>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Returns a cached [`StorageProvider`] for the given role.
///
/// Workers use [`StorageProviderFor::Worker`], which reads `config().checkpoint_url`
/// (set per-pipeline via the `ARROYO__CHECKPOINT_URL` env var at worker startup).
///
/// Controllers manage many pipelines that may have different storage URLs and
/// pass [`StorageProviderFor::Controller`] with the pipeline's `state_url`.
/// When `state_url` is `None`, falls back to `config().checkpoint_url`.
pub async fn get_storage_provider(
    role: &StorageProviderFor,
) -> Result<Arc<StorageProvider>, StorageError> {
    let storage_url = match role {
        StorageProviderFor::Controller {
            storage_url: Some(url),
        } => url,
        StorageProviderFor::Worker | StorageProviderFor::Controller { storage_url: None } => {
            &config().checkpoint_url
        }
    };
    let mut cache = storage_provider_cache().lock().await;

    if let Some(storage_provider) = cache.get(storage_url) {
        Ok(storage_provider.clone())
    } else {
        let storage_provider = Arc::new(StorageProvider::for_url(storage_url).await?);
        cache.insert(storage_url.clone(), storage_provider.clone());
        Ok(storage_provider)
    }
}
