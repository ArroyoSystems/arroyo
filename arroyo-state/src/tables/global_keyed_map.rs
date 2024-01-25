use crate::metrics::TABLE_SIZE_GAUGE;
use crate::{BackingStore, CheckpointMessage, StateMessage, TableData};
use anyhow::{anyhow, bail, Result};
use arrow_array::{BinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arroyo_rpc::grpc::{
    GlobalKeyedTableSubtaskCheckpointMetadata, GlobalKeyedTableTaskCheckpointMetadata, TableEnum,
};
use arroyo_storage::StorageProviderRef;
use arroyo_types::{to_micros, Data, Key, TaskInfoRef};
use bincode::config;

use once_cell::sync::Lazy;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::{
    arrow::ArrowWriter,
    basic::ZstdLevel,
    file::properties::{EnabledStatistics, WriterProperties},
};

use std::iter::Zip;

use std::time::SystemTime;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::mpsc::Sender;

use super::{table_checkpoint_path, Table, TableEpochCheckpointer};
static GLOBAL_KEY_VALUE_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields = vec![
        Field::new("key", DataType::Binary, false), // non-nullable BinaryArray for 'key'
        Field::new("value", DataType::Binary, false), // non-nullable BinaryArray for 'value'
    ];
    Arc::new(Schema::new(fields))
});

pub struct GlobalKeyedState<'a, K: Key, V: Data, S: BackingStore> {
    table: char,
    parquet: &'a mut S,
    cache: &'a mut GlobalKeyedStateCache<K, V>,
}

impl<'a, K: Key, V: Data, S: BackingStore> GlobalKeyedState<'a, K, V, S> {
    pub fn new(
        table: char,
        backing_store: &'a mut S,
        cache: &'a mut GlobalKeyedStateCache<K, V>,
    ) -> Self {
        Self {
            table,
            parquet: backing_store,
            cache,
        }
    }
    pub async fn insert(&mut self, mut key: K, mut value: V) {
        self.parquet
            .write_key_value(self.table, &mut key, &mut value)
            .await;
        self.cache.values.insert(key, value);

        TABLE_SIZE_GAUGE
            .with_label_values(&[
                &self.parquet.task_info().operator_id,
                &self.parquet.task_info().task_index.to_string(),
                &self.table.to_string(),
            ])
            .set(self.cache.values.len() as f64);
    }

    pub fn get_all(&mut self) -> Vec<&V> {
        self.cache.values.values().collect()
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.cache.values.get(key)
    }

    pub fn get_key_values(&self) -> Vec<(&K, &V)> {
        self.cache.values.iter().collect()
    }
}

pub struct GlobalKeyedStateCache<K: Key, V: Data> {
    values: HashMap<K, V>,
}

impl<K: Key, V: Data> GlobalKeyedStateCache<K, V> {
    pub async fn from_checkpoint<S: BackingStore>(backing_store: &S, table: char) -> Self {
        let mut values = HashMap::new();
        for (key, value) in backing_store.get_global_key_values(table).await {
            values.insert(key, value);
        }
        Self { values }
    }
}

impl<K: Key, V: Data> Default for GlobalKeyedStateCache<K, V> {
    fn default() -> Self {
        Self {
            values: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GlobalKeyedTable {
    table_name: String,
    pub task_info: TaskInfoRef,
    storage_provider: StorageProviderRef,
    pub files: Vec<String>,
}

impl GlobalKeyedTable {
    fn get_key_value_iterator<'a>(
        &self,
        record_batch: &'a RecordBatch,
    ) -> Result<Zip<impl Iterator<Item = Option<&'a [u8]>>, impl Iterator<Item = Option<&'a [u8]>>>>
    {
        let key_column = record_batch
            .column_by_name("key")
            .ok_or_else(|| anyhow!("missing key column"))?;
        let value_column = record_batch
            .column_by_name("value")
            .ok_or_else(|| anyhow!("missing value column"))?;
        let cast_key_column = key_column
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| anyhow!("failed to downcast key column to BinaryArray"))?;
        let cast_value_column = value_column
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .ok_or_else(|| anyhow!("failed to downcast value column to BinaryArray"))?;
        Ok(cast_key_column
            .into_iter()
            .zip(cast_value_column.into_iter()))
    }
    pub async fn memory_view<K: Key, V: Data>(
        &self,
        state_tx: Sender<StateMessage>,
    ) -> anyhow::Result<GlobalKeyedView<K, V>> {
        let mut data = HashMap::new();
        for file in &self.files {
            let contents = self.storage_provider.get(file).await?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(contents)?.build()?;
            for batch in reader {
                for (key, value) in self.get_key_value_iterator(&batch?)?.into_iter() {
                    let key =
                        key.ok_or_else(|| anyhow!("unexpected null key from record batch"))?;
                    let value =
                        value.ok_or_else(|| anyhow!("unexpected null value from record batch"))?;
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
        })
    }
}

impl Table for GlobalKeyedTable {
    type Checkpointer = GlobalKeyedCheckpointer;

    type ConfigMessage = arroyo_rpc::grpc::GlobalKeyedTableConfig;

    type TableSubtaskCheckpointMetadata = GlobalKeyedTableSubtaskCheckpointMetadata;

    type TableCheckpointMessage = GlobalKeyedTableTaskCheckpointMetadata;

    fn epoch_checkpointer(
        &self,
        epoch: u32,
        _previous_metadata: Option<Self::TableSubtaskCheckpointMetadata>,
    ) -> Result<Self::Checkpointer> {
        Ok(Self::Checkpointer {
            table_name: self.table_name.clone(),
            epoch,
            task_info: self.task_info.clone(),
            storage_provider: self.storage_provider.clone(),
            latest_values: BTreeMap::new(),
        })
    }

    fn from_config(
        config: Self::ConfigMessage,
        task_info: TaskInfoRef,
        storage_provider: StorageProviderRef,
        checkpoint_message: Option<Self::TableCheckpointMessage>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            table_name: config.table_name,
            task_info,
            storage_provider,
            files: checkpoint_message
                .map(|checkpoint| checkpoint.files)
                .unwrap_or_default(),
        })
    }

    fn merge_checkpoint_metadata(
        _config: Self::ConfigMessage,
        subtask_metadata: HashMap<u32, Self::TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<Self::TableCheckpointMessage>> {
        if subtask_metadata.is_empty() {
            // TODO: maybe this should fail? These tables should emit on every epoch, and there should always be at least one value.
            Ok(None)
        } else {
            Ok(Some(GlobalKeyedTableTaskCheckpointMetadata {
                files: subtask_metadata
                    .into_values()
                    .filter_map(|subtask_meta| subtask_meta.file)
                    .collect(),
            }))
        }
    }

    fn subtask_metadata_from_table(
        &self,
        _table_metadata: Self::TableCheckpointMessage,
    ) -> Result<Option<Self::TableSubtaskCheckpointMetadata>> {
        // this method is to inherit data dependencies from previous epochs, but this table is regenerated every epoch.
        Ok(None)
    }

    fn table_type() -> TableEnum {
        TableEnum::GlobalKeyValue
    }

    fn task_info(&self) -> TaskInfoRef {
        self.task_info.clone()
    }

    fn files_to_keep(
        _config: Self::ConfigMessage,
        checkpoint: Self::TableCheckpointMessage,
    ) -> Result<std::collections::HashSet<String>> {
        Ok(checkpoint.files.into_iter().collect())
    }
}

pub struct GlobalKeyedCheckpointer {
    table_name: String,
    epoch: u32,
    task_info: TaskInfoRef,
    storage_provider: StorageProviderRef,
    latest_values: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[async_trait::async_trait]
impl TableEpochCheckpointer for GlobalKeyedCheckpointer {
    type SubTableCheckpointMessage = GlobalKeyedTableSubtaskCheckpointMetadata;

    async fn insert_data(&mut self, data: TableData) -> anyhow::Result<()> {
        match data {
            TableData::RecordBatch(_) => {
                bail!("global keyed data expects KeyedData, not record batches")
            }
            TableData::CommitData { data: _ } => {
                bail!("global keyed data expects keyed data, not commit data")
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
    ) -> Result<Option<Self::SubTableCheckpointMessage>> {
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
            .build();
        let cursor = Vec::new();
        let mut writer = ArrowWriter::try_new(cursor, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.flush()?;
        let parquet_bytes = writer.into_inner().unwrap();
        let _bytes = parquet_bytes.len() as u64;
        let path = table_checkpoint_path(&self.task_info, &self.table_name, self.epoch, false);
        self.storage_provider.put(&path, parquet_bytes).await?;
        let _finish_time = to_micros(SystemTime::now());
        Ok(Some(GlobalKeyedTableSubtaskCheckpointMetadata {
            subtask_index: self.task_info.task_index as u32,
            file: Some(path),
        }))
    }

    fn table_type() -> TableEnum {
        TableEnum::GlobalKeyValue
    }

    fn subtask_index(&self) -> u32 {
        self.task_info.task_index as u32
    }
}

pub struct GlobalKeyedView<K: Key, V: Data> {
    table_name: String,
    data: HashMap<K, V>,
    state_tx: Sender<StateMessage>,
}

impl<K: Key, V: Data> GlobalKeyedView<K, V> {
    pub fn new(table_name: String, data: HashMap<K, V>, state_tx: Sender<StateMessage>) -> Self {
        Self {
            table_name,
            data,
            state_tx,
        }
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
}
