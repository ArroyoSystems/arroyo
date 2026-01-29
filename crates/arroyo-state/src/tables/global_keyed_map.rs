use crate::{CheckpointMessage, StateMessage, TableData};
use arrow_array::{BinaryArray, RecordBatch};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{
    GlobalKeyedTableSubtaskCheckpointMetadata, GlobalKeyedTableTaskCheckpointMetadata,
    OperatorMetadata, TableEnum,
};
use arroyo_storage::StorageProviderRef;
use arroyo_types::{Data, Key, TaskInfo, to_micros};
use bincode::config;

use once_cell::sync::Lazy;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::{
    arrow::ArrowWriter,
    basic::ZstdLevel,
    file::properties::{EnabledStatistics, WriterProperties},
};
use tracing::debug;

use std::iter::Zip;

use arroyo_rpc::grpc::rpc::GlobalKeyedTableConfig;
use std::time::SystemTime;
use std::{
    collections::{BTreeMap, HashMap},
    mem,
    sync::Arc,
};
use tokio::sync::mpsc::Sender;

use super::{
    CheckpointParquetMetadata, CompactionConfig, MigratableState, Table, TableEpochCheckpointer,
    table_checkpoint_path,
};
use tracing::info;
static GLOBAL_KEY_VALUE_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields = vec![
        Field::new("key", DataType::Binary, false), // non-nullable BinaryArray for 'key'
        Field::new("value", DataType::Binary, false), // non-nullable BinaryArray for 'value'
    ];
    Arc::new(Schema::new(fields))
});

#[derive(Debug, Clone)]
pub struct GlobalKeyedTable {
    table_name: String,
    pub task_info: Arc<TaskInfo>,
    storage_provider: StorageProviderRef,
    pub files: Vec<String>,
    pub state_version: u32,
}

impl GlobalKeyedTable {
    #[allow(clippy::type_complexity)]
    fn get_key_value_iterator<'a>(
        &self,
        record_batch: &'a RecordBatch,
    ) -> Result<
        Zip<impl Iterator<Item = Option<&'a [u8]>>, impl Iterator<Item = Option<&'a [u8]>>>,
        StateError,
    > {
        let key_column = record_batch.column_by_name("key").ok_or_else(|| {
            StateError::ArrowError(ArrowError::SchemaError("missing column 'key'".to_string()))
        })?;
        let value_column = record_batch.column_by_name("value").ok_or_else(|| {
            StateError::ArrowError(ArrowError::SchemaError(
                "missing column 'value'".to_string(),
            ))
        })?;
        let cast_key_column = key_column
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| {
                StateError::ArrowError(ArrowError::CastError(
                    "failed to downcast key to binary".to_string(),
                ))
            })?;
        let cast_value_column = value_column
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| {
                StateError::ArrowError(ArrowError::CastError(
                    "failed to downcast value column to BinaryArray".to_string(),
                ))
            })?;
        Ok(cast_key_column.into_iter().zip(cast_value_column))
    }

    async fn load_with_version<K: Key, V: Data>(
        &self,
        state_tx: Sender<StateMessage>,
        version: u32,
    ) -> Result<GlobalKeyedView<K, V>, StateError> {
        let mut data = HashMap::new();
        for file in &self.files {
            let contents = self.storage_provider.get(file.as_str()).await?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(contents)?.build()?;

            for batch in reader {
                for (key, value) in self.get_key_value_iterator(&batch?)? {
                    let key = key.ok_or_else(|| StateError::Other {
                        table: self.table_name.clone(),
                        error: "unexpected null key from record batch".to_string(),
                    })?;
                    let value = value.ok_or_else(|| StateError::Other {
                        table: self.table_name.clone(),
                        error: "unexpected null value from record batch".to_string(),
                    })?;
                    data.insert(
                        bincode::decode_from_slice(key, config::standard())?.0,
                        bincode::decode_from_slice(value, config::standard())?.0,
                    );
                }
            }
        }
        Ok(GlobalKeyedView {
            table_name: self.table_name.to_string(),
            data,
            state_tx,
            version,
        })
    }

    pub async fn memory_view<K: Key, V: Data>(
        &self,
        state_tx: Sender<StateMessage>,
    ) -> Result<GlobalKeyedView<K, V>, StateError> {
        self.load_with_version(state_tx, 0).await
    }

    /// Load state with version-aware migration support.
    pub async fn memory_view_migratable<K: Key, V: MigratableState>(
        &self,
        state_tx: Sender<StateMessage>,
    ) -> Result<GlobalKeyedView<K, V>, StateError> {
        let mut data = HashMap::new();
        for file in &self.files {
            let contents = self.storage_provider.get(file.as_str()).await?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(contents)?;

            let metadata = CheckpointParquetMetadata::from(
                reader.metadata().file_metadata().key_value_metadata(),
            );

            let migrating = if metadata.state_version == V::VERSION {
                false
            } else if metadata.state_version + 1 == V::VERSION {
                info!(
                    "Migrating state for table '{}' in {} from version {} to version {}",
                    self.table_name,
                    file,
                    metadata.state_version,
                    V::VERSION
                );
                true
            } else {
                // we only support 1 step migration at this point
                return Err(StateError::UnsupportedStateVersion {
                    table: self.table_name.clone(),
                    found: metadata.state_version,
                    expected: V::VERSION,
                });
            };

            for batch in reader.build()? {
                for (key, value) in self.get_key_value_iterator(&batch?)? {
                    let key = key.ok_or_else(|| StateError::Other {
                        table: self.table_name.clone(),
                        error: "unexpected null key from record batch".to_string(),
                    })?;
                    let value = value.ok_or_else(|| StateError::Other {
                        table: self.table_name.clone(),
                        error: "unexpected null value from record batch".to_string(),
                    })?;

                    let (k, v) = if migrating {
                        let decoded_key: K = bincode::decode_from_slice(key, config::standard())?.0;
                        let old_value: V::PreviousVersion =
                            bincode::decode_from_slice(value, config::standard())?.0;

                        let new_value = V::migrate(old_value)?;
                        (decoded_key, new_value)
                    } else {
                        (
                            bincode::decode_from_slice(key, config::standard())?.0,
                            bincode::decode_from_slice(value, config::standard())?.0,
                        )
                    };

                    data.insert(k, v);
                }
            }
        }

        Ok(GlobalKeyedView {
            table_name: self.table_name.to_string(),
            data,
            state_tx,
            version: V::VERSION,
        })
    }
}

#[async_trait::async_trait]
impl Table for GlobalKeyedTable {
    type Checkpointer = GlobalKeyedCheckpointer;

    type ConfigMessage = GlobalKeyedTableConfig;

    type TableSubtaskCheckpointMetadata = GlobalKeyedTableSubtaskCheckpointMetadata;

    type TableCheckpointMessage = GlobalKeyedTableTaskCheckpointMetadata;

    fn epoch_checkpointer(
        &self,
        epoch: u32,
        _previous_metadata: Option<Self::TableSubtaskCheckpointMetadata>,
    ) -> Result<Self::Checkpointer, StateError> {
        Ok(Self::Checkpointer {
            table_name: self.table_name.clone(),
            epoch,
            task_info: self.task_info.clone(),
            storage_provider: self.storage_provider.clone(),
            commit_data: None,
            latest_values: BTreeMap::new(),
            state_version: self.state_version,
        })
    }

    fn from_config(
        config: Self::ConfigMessage,
        task_info: Arc<TaskInfo>,
        storage_provider: StorageProviderRef,
        checkpoint_message: Option<Self::TableCheckpointMessage>,
        state_version: u32,
    ) -> Result<Self, StateError> {
        Ok(Self {
            table_name: config.table_name,
            task_info,
            storage_provider,
            files: checkpoint_message
                .map(|checkpoint| checkpoint.files)
                .unwrap_or_default(),
            state_version,
        })
    }

    fn merge_checkpoint_metadata(
        config: Self::ConfigMessage,
        subtask_metadata: HashMap<u32, Self::TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<Self::TableCheckpointMessage>, StateError> {
        if subtask_metadata.is_empty() {
            // TODO: maybe this should fail? These tables should emit on every epoch, and there should always be at least one value.
            Ok(None)
        } else if config.uses_two_phase_commit {
            let mut files = Vec::new();
            let mut commit_data_by_subtask = HashMap::new();
            for (subtask_index, subtask_meta) in subtask_metadata {
                if let Some(file) = subtask_meta.file {
                    files.push(file);
                }
                if let Some(commit_data) = subtask_meta.commit_data {
                    commit_data_by_subtask.insert(subtask_index, commit_data);
                }
            }
            Ok(Some(GlobalKeyedTableTaskCheckpointMetadata {
                files,
                commit_data_by_subtask,
            }))
        } else {
            Ok(Some(GlobalKeyedTableTaskCheckpointMetadata {
                files: subtask_metadata
                    .into_values()
                    .filter_map(|subtask_meta| subtask_meta.file)
                    .collect(),
                commit_data_by_subtask: HashMap::new(),
            }))
        }
    }

    fn subtask_metadata_from_table(
        &self,
        _table_metadata: Self::TableCheckpointMessage,
    ) -> Result<Option<Self::TableSubtaskCheckpointMetadata>, StateError> {
        // this method is to inherit data dependencies from previous epochs, but this table is regenerated every epoch.
        Ok(None)
    }

    fn table_type() -> TableEnum {
        TableEnum::GlobalKeyValue
    }

    fn task_info(&self) -> Arc<TaskInfo> {
        self.task_info.clone()
    }

    fn files_to_keep(
        _config: Self::ConfigMessage,
        checkpoint: Self::TableCheckpointMessage,
    ) -> Result<std::collections::HashSet<String>, StateError> {
        Ok(checkpoint.files.into_iter().collect())
    }

    fn committing_data(
        config: Self::ConfigMessage,
        table_metadata: Self::TableCheckpointMessage,
    ) -> Option<HashMap<u32, Vec<u8>>> {
        if config.uses_two_phase_commit {
            Some(table_metadata.commit_data_by_subtask)
        } else {
            None
        }
    }

    async fn compact_data(
        _config: Self::ConfigMessage,
        _compaction_config: &CompactionConfig,
        _operator_metadata: &OperatorMetadata,
        _current_metadata: Self::TableCheckpointMessage,
    ) -> Result<Option<Self::TableCheckpointMessage>, StateError> {
        Ok(None)
    }

    fn apply_compacted_checkpoint(
        &self,
        _epoch: u32,
        _compacted_checkpoint: Self::TableSubtaskCheckpointMetadata,
        subtask_metadata: Self::TableSubtaskCheckpointMetadata,
    ) -> Result<Self::TableSubtaskCheckpointMetadata, StateError> {
        Ok(subtask_metadata)
    }
}

pub struct GlobalKeyedCheckpointer {
    table_name: String,
    epoch: u32,
    task_info: Arc<TaskInfo>,
    storage_provider: StorageProviderRef,
    latest_values: BTreeMap<Vec<u8>, Vec<u8>>,
    commit_data: Option<Vec<u8>>,
    state_version: u32,
}

#[async_trait::async_trait]
impl TableEpochCheckpointer for GlobalKeyedCheckpointer {
    type SubTableCheckpointMessage = GlobalKeyedTableSubtaskCheckpointMetadata;

    async fn insert_data(&mut self, data: TableData) -> Result<(), StateError> {
        match data {
            TableData::RecordBatch(_) => {
                return Err(StateError::Other {
                    table: self.table_name.clone(),
                    error: "global keyed data expects KeyedData, not record batches".to_string(),
                });
            }
            TableData::CommitData { data } => {
                debug!("received commit data");
                // set commit data, failing if it was already set
                if self.commit_data.is_some() {
                    return Err(StateError::Other {
                        table: self.table_name.clone(),
                        error: "commit data already set for this epoch".to_string(),
                    });
                }
                self.commit_data = Some(data);
            }
            TableData::KeyedData { key, value } => {
                self.latest_values.insert(key, value);
            }
        }
        Ok(())
    }

    async fn finish(
        self,
        _checkpoint: &CheckpointMessage,
    ) -> Result<Option<(Self::SubTableCheckpointMessage, usize)>, StateError> {
        let _start_time = to_micros(SystemTime::now());
        let (keys, values): (Vec<_>, Vec<_>) = self
            .latest_values
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .unzip();
        let key_array = BinaryArray::from_vec(keys);
        let value_array = BinaryArray::from_vec(values);
        let batch = RecordBatch::try_new(
            GLOBAL_KEY_VALUE_SCHEMA.clone(),
            vec![Arc::new(key_array), Arc::new(value_array)],
        )?;

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::default()))
            .set_statistics_enabled(EnabledStatistics::None)
            .set_key_value_metadata(
                CheckpointParquetMetadata {
                    state_version: self.state_version,
                }
                .into(),
            )
            .build();

        let cursor = Vec::new();
        let mut writer = ArrowWriter::try_new(cursor, batch.schema(), Some(props))?;
        writer.write(&batch)?;

        writer.flush()?;

        let parquet_bytes = writer.into_inner().unwrap();
        let bytes = parquet_bytes.len() as u64;
        let path = table_checkpoint_path(
            &self.task_info.job_id,
            &self.task_info.operator_id,
            &self.table_name,
            self.task_info.task_index as usize,
            self.epoch,
            false,
        );
        self.storage_provider
            .put(path.as_str(), parquet_bytes)
            .await?;
        let _finish_time = to_micros(SystemTime::now());
        Ok(Some((
            GlobalKeyedTableSubtaskCheckpointMetadata {
                subtask_index: self.task_info.task_index,
                commit_data: self.commit_data,
                file: Some(path),
            },
            bytes as usize,
        )))
    }

    fn table_type() -> TableEnum {
        TableEnum::GlobalKeyValue
    }

    fn subtask_index(&self) -> u32 {
        self.task_info.task_index
    }
}

pub struct GlobalKeyedView<K: Key, V: Data> {
    table_name: String,
    data: HashMap<K, V>,
    state_tx: Sender<StateMessage>,
    version: u32,
}

impl<K: Key, V: Data> GlobalKeyedView<K, V> {
    /// Get the table name and version for sending to the checkpointer.
    /// This allows the caller to send the version message without
    /// holding a reference to self across an await point.
    pub fn version_info(&self) -> (String, u32, Sender<StateMessage>) {
        (self.table_name.clone(), self.version, self.state_tx.clone())
    }

    pub async fn insert(&mut self, key: K, value: V) {
        self.state_tx
            .send(StateMessage::TableData {
                table: self.table_name.clone(),
                data: TableData::KeyedData {
                    key: bincode::encode_to_vec(&key, config::standard()).unwrap(),
                    value: bincode::encode_to_vec(&value, config::standard()).unwrap(),
                },
            })
            .await
            .unwrap();
        self.data.insert(key, value);
    }

    pub fn get_all(&self) -> &HashMap<K, V> {
        &self.data
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.data.get(key)
    }

    pub fn take(&mut self) -> HashMap<K, V> {
        let mut data = HashMap::new();
        mem::swap(&mut data, &mut self.data);
        data
    }
}
