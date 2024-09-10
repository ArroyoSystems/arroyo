use anyhow::{Context, Result};
use arrow_array::RecordBatch;
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
use prost::Message;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub mod checkpoint_state;
pub mod committing_state;
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
    /// prepares a checkpoint to be loaded, e.g., by deleting future data
    async fn prepare_checkpoint_load(metadata: &CheckpointMetadata) -> Result<()>;

    /// loads the checkpoint metadata for a given job id and epoch
    async fn load_checkpoint_metadata(job_id: &str, epoch: u32) -> Result<CheckpointMetadata>;

    /// loads the operator checkpoint metadata for a given job id, operator id, and epoch
    async fn load_operator_metadata(
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>>;

    /// returns the name of the BackingStore implementation
    fn name() -> &'static str;

    /// writes the operator checkpoint metadata to the backing store
    async fn write_operator_checkpoint_metadata(metadata: OperatorCheckpointMetadata)
        -> Result<()>;

    /// writes the checkpoint metadata to the backing store
    async fn write_checkpoint_metadata(metadata: CheckpointMetadata) -> Result<()>;

    /// cleans up a checkpoint by deleting data that is no longer needed
    async fn cleanup_checkpoint(
        metadata: CheckpointMetadata,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<()>;
}

pub fn hash_key<K: Hash>(key: &K) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

static STORAGE_PROVIDER: tokio::sync::OnceCell<Arc<StorageProvider>> =
    tokio::sync::OnceCell::const_new();

pub(crate) async fn get_storage_provider() -> Result<&'static Arc<StorageProvider>> {
    // TODO: this should be encoded in the config so that the controller doesn't need
    // to be synchronized with the workers

    STORAGE_PROVIDER
        .get_or_try_init(|| async {
            let storage_url = &config().checkpoint_url;

            StorageProvider::for_url(storage_url)
                .await
                .context(format!(
                    "failed to construct checkpoint backend for URL {}",
                    storage_url
                ))
                .map(Arc::new)
        })
        .await
}
