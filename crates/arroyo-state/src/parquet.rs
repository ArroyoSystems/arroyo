use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::tables::global_keyed_map::GlobalKeyedTable;
use crate::tables::{CompactionConfig, ErasedTable};
use crate::{get_storage_provider, BackingStore};
use anyhow::{bail, Result};
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, OperatorCheckpointMetadata, TableCheckpointMetadata,
};
use futures::stream::FuturesUnordered;
use futures::StreamExt;

use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;
use tracing::{debug, info};

pub const FULL_KEY_RANGE: RangeInclusive<u64> = 0..=u64::MAX;
pub const GENERATIONS_TO_COMPACT: u32 = 1; // only compact generation 0 files

pub struct ParquetBackend;

fn base_path(job_id: &str, epoch: u32) -> String {
    format!("{}/checkpoints/checkpoint-{:0>7}", job_id, epoch)
}

fn metadata_path(path: &str) -> String {
    format!("{}/metadata", path)
}

fn operator_path(job_id: &str, epoch: u32, operator: &str) -> String {
    format!("{}/operator-{}", base_path(job_id, epoch), operator)
}

#[async_trait::async_trait]
impl BackingStore for ParquetBackend {
    fn name() -> &'static str {
        "parquet"
    }

    async fn load_checkpoint_metadata(job_id: &str, epoch: u32) -> Result<CheckpointMetadata> {
        let storage_client = get_storage_provider().await?;
        let data = storage_client
            .get(metadata_path(&base_path(job_id, epoch)).as_str())
            .await?;
        let metadata = CheckpointMetadata::decode(&data[..])?;
        Ok(metadata)
    }

    async fn load_operator_metadata(
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>> {
        let storage_client = get_storage_provider().await?;
        storage_client
            .get_if_present(metadata_path(&operator_path(job_id, epoch, operator_id)).as_str())
            .await?
            .map(|data| Ok(OperatorCheckpointMetadata::decode(&data[..])?))
            .transpose()
    }

    async fn write_operator_checkpoint_metadata(
        metadata: OperatorCheckpointMetadata,
    ) -> Result<()> {
        let storage_client = get_storage_provider().await?;
        let operator_metadata = metadata
            .operator_metadata
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing operator metadata"))?;
        let path = metadata_path(&operator_path(
            &operator_metadata.job_id,
            operator_metadata.epoch,
            &operator_metadata.operator_id,
        ));
        storage_client
            .put(path.as_str(), metadata.encode_to_vec())
            .await?;
        // TODO: propagate error
        Ok(())
    }

    async fn write_checkpoint_metadata(metadata: CheckpointMetadata) -> Result<()> {
        debug!("writing checkpoint {:?}", metadata);
        let storage_client = get_storage_provider().await?;
        let path = metadata_path(&base_path(&metadata.job_id, metadata.epoch));
        storage_client
            .put(path.as_str(), metadata.encode_to_vec())
            .await?;
        Ok(())
    }

    async fn prepare_checkpoint_load(_metadata: &CheckpointMetadata) -> anyhow::Result<()> {
        Ok(())
    }

    async fn cleanup_checkpoint(
        mut metadata: CheckpointMetadata,
        old_min_epoch: u32,
        min_epoch: u32,
    ) -> Result<()> {
        info!(
            message = "Cleaning checkpoint",
            min_epoch,
            job_id = metadata.job_id
        );

        let mut futures: FuturesUnordered<_> = metadata
            .operator_ids
            .iter()
            .map(|operator_id| {
                Self::cleanup_operator(
                    metadata.job_id.clone(),
                    operator_id.clone(),
                    old_min_epoch,
                    min_epoch,
                )
            })
            .collect();

        let storage_client = Mutex::new(get_storage_provider().await?);

        // wait for all of the futures to complete
        while let Some(result) = futures.next().await {
            let operator_id = result?;

            for epoch_to_remove in old_min_epoch..min_epoch {
                let path = metadata_path(&operator_path(
                    &metadata.job_id,
                    epoch_to_remove,
                    &operator_id,
                ));
                storage_client.lock().await.delete_if_present(path).await?;
            }
            debug!(
                message = "Finished cleaning operator",
                job_id = metadata.job_id,
                operator_id,
                min_epoch
            );
        }

        for epoch_to_remove in old_min_epoch..min_epoch {
            storage_client
                .lock()
                .await
                .delete_if_present(metadata_path(&base_path(&metadata.job_id, epoch_to_remove)))
                .await?;
        }
        metadata.min_epoch = min_epoch;
        Self::write_checkpoint_metadata(metadata).await?;
        Ok(())
    }
}

impl ParquetBackend {
    /// Called after a checkpoint is committed
    pub async fn compact_operator(
        job_id: Arc<String>,
        operator_id: String,
        epoch: u32,
    ) -> Result<HashMap<String, TableCheckpointMetadata>> {
        let min_files_to_compact = config().pipeline.compaction.checkpoints_to_compact as usize;

        let operator_checkpoint_metadata =
            Self::load_operator_metadata(&job_id, &operator_id, epoch)
                .await?
                .expect("expect operator metadata to still be present");
        let storage_provider = get_storage_provider().await?;
        let compaction_config = CompactionConfig {
            compact_generations: vec![0].into_iter().collect(),
            min_compaction_epochs: min_files_to_compact,
            storage_provider: Arc::clone(storage_provider),
        };
        let operator_metadata = operator_checkpoint_metadata.operator_metadata.unwrap();

        let mut result = HashMap::new();

        for (table, table_metadata) in operator_checkpoint_metadata.table_checkpoint_metadata {
            let table_config = operator_checkpoint_metadata
                .table_configs
                .get(&table)
                .unwrap()
                .clone();
            if let Some(compacted_metadata) = match table_metadata.table_type() {
                rpc::TableEnum::MissingTableType => bail!("should have table type"),
                rpc::TableEnum::GlobalKeyValue => {
                    GlobalKeyedTable::compact_data(
                        table_config,
                        &compaction_config,
                        &operator_metadata,
                        table_metadata,
                    )
                    .await?
                }
                rpc::TableEnum::ExpiringKeyedTimeTable => {
                    ExpiringTimeKeyTable::compact_data(
                        table_config,
                        &compaction_config,
                        &operator_metadata,
                        table_metadata,
                    )
                    .await?
                }
            } {
                result.insert(table, compacted_metadata);
            }
        }
        Ok(result)
    }

    /// Delete files no longer referenced by the new min epoch
    pub async fn cleanup_operator(
        job_id: String,
        operator_id: String,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<String> {
        let operator_metadata = Self::load_operator_metadata(&job_id, &operator_id, new_min_epoch)
            .await?
            .expect("expect new_min_epoch metadata to still be present");
        let paths_to_keep: HashSet<String> = operator_metadata
            .table_checkpoint_metadata
            .iter()
            .flat_map(|(table_name, metadata)| {
                let table_config = operator_metadata
                    .table_configs
                    .get(table_name)
                    .unwrap()
                    .clone();

                match table_config.table_type() {
                    rpc::TableEnum::MissingTableType => todo!("should handle error"),
                    rpc::TableEnum::GlobalKeyValue => {
                        GlobalKeyedTable::files_to_keep(table_config, metadata.clone()).unwrap()
                    }
                    rpc::TableEnum::ExpiringKeyedTimeTable => {
                        ExpiringTimeKeyTable::files_to_keep(table_config, metadata.clone()).unwrap()
                    }
                }
            })
            .collect();

        let mut deleted_paths = HashSet::new();
        let storage_client = get_storage_provider().await?;

        for epoch_to_remove in old_min_epoch..new_min_epoch {
            let Some(operator_metadata) =
                Self::load_operator_metadata(&job_id, &operator_id, epoch_to_remove).await?
            else {
                continue;
            };

            // delete any files that are not in the new min epoch
            for file in operator_metadata
                .table_checkpoint_metadata
                .iter()
                // TODO: factor this out
                .flat_map(|(table_name, metadata)| {
                    let table_config = operator_metadata
                        .table_configs
                        .get(table_name)
                        .ok_or_else(|| anyhow::anyhow!("missing table config for operator {}, table {}, metadata is {:?}, operator_metadata is {:?}",
                         operator_id, table_name, metadata, operator_metadata)).unwrap()
                        .clone();

                    match table_config.table_type() {
                        rpc::TableEnum::MissingTableType => todo!("should handle error"),
                        rpc::TableEnum::GlobalKeyValue => {
                            GlobalKeyedTable::files_to_keep(table_config, metadata.clone()).unwrap()
                        }
                        rpc::TableEnum::ExpiringKeyedTimeTable => {
                            ExpiringTimeKeyTable::files_to_keep(table_config, metadata.clone())
                                .unwrap()
                        }
                    }
                })
            {
                if !paths_to_keep.contains(&file) && !deleted_paths.contains(&file) {
                    deleted_paths.insert(file.clone());
                    storage_client.delete_if_present(file).await?;
                }
            }
        }

        Ok(operator_id)
    }
}

#[derive(Debug)]
pub struct ParquetStats {
    pub max_timestamp: SystemTime,
    pub min_routing_key: u64,
    pub max_routing_key: u64,
}

impl Default for ParquetStats {
    fn default() -> Self {
        Self {
            max_timestamp: SystemTime::UNIX_EPOCH,
            min_routing_key: u64::MAX,
            max_routing_key: u64::MIN,
        }
    }
}

impl ParquetStats {
    pub fn merge(&mut self, other: ParquetStats) {
        self.max_timestamp = self.max_timestamp.max(other.max_timestamp);
        self.min_routing_key = self.min_routing_key.min(other.min_routing_key);
        self.max_routing_key = self.max_routing_key.max(other.max_routing_key);
    }
}
