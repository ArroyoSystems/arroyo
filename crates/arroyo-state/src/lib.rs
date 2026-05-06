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

// TODO: Tweak these numbers and maybe introduce a config knob
const STORAGE_PROVIDER_CACHE_CAPACITY: u64 = 5_000;
const STORAGE_PROVIDER_CACHE_TTI: Duration = Duration::from_secs(60 * 60);

/// Per-URL cache of [`StorageProvider`] instances.
fn storage_provider_cache() -> &'static moka::future::Cache<String, Arc<StorageProvider>> {
    static CACHE: std::sync::OnceLock<moka::future::Cache<String, Arc<StorageProvider>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| {
        moka::future::Cache::builder()
            .max_capacity(STORAGE_PROVIDER_CACHE_CAPACITY)
            .time_to_idle(STORAGE_PROVIDER_CACHE_TTI)
            .build()
    })
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
    let storage_url: String = match role {
        StorageProviderFor::Controller {
            storage_url: Some(url),
        } => url.clone(),
        StorageProviderFor::Worker | StorageProviderFor::Controller { storage_url: None } => {
            config().checkpoint_url.clone()
        }
    };

    storage_provider_cache()
        .try_get_with(storage_url.clone(), async move {
            StorageProvider::for_url(&storage_url).await.map(Arc::new)
        })
        .await
        .map_err(|e: Arc<StorageError>| StorageError::PathError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_local_url(suffix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("file:///tmp/arroyo-state-test-{nanos}-{suffix}")
    }

    #[tokio::test]
    async fn cache_returns_same_arc_for_same_url() {
        let url = unique_local_url("same");
        let role = StorageProviderFor::Controller {
            storage_url: Some(url),
        };

        let a = get_storage_provider(&role).await.unwrap();
        let b = get_storage_provider(&role).await.unwrap();
        assert!(
            Arc::ptr_eq(&a, &b),
            "cache hit on same URL should return the same Arc"
        );
    }

    #[tokio::test]
    async fn cache_returns_distinct_arcs_for_different_urls() {
        let role_a = StorageProviderFor::Controller {
            storage_url: Some(unique_local_url("a")),
        };
        let role_b = StorageProviderFor::Controller {
            storage_url: Some(unique_local_url("b")),
        };

        let a = get_storage_provider(&role_a).await.unwrap();
        let b = get_storage_provider(&role_b).await.unwrap();
        assert!(
            !Arc::ptr_eq(&a, &b),
            "different URLs should resolve to different cached Arcs"
        );
    }

    #[tokio::test]
    async fn controller_with_none_falls_back_to_worker_url() {
        let worker = StorageProviderFor::Worker;
        let controller_none = StorageProviderFor::Controller { storage_url: None };

        let from_worker = get_storage_provider(&worker).await.unwrap();
        let from_none = get_storage_provider(&controller_none).await.unwrap();
        assert!(
            Arc::ptr_eq(&from_worker, &from_none),
            "Worker and Controller{{None}} should share the cached provider"
        );
    }
}
