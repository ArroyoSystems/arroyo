use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::tables::global_keyed_map::GlobalKeyedTable;
use crate::tables::{CompactionConfig, DataTuple, ErasedTable};
use crate::{
    hash_key, BackingStore, DataOperation, DeleteKeyOperation, DeleteTimeKeyOperation,
    DeleteTimeRangeOperation, DeleteValueOperation, BINCODE_CONFIG,
};
use anyhow::{bail, Context, Result};
use arrow_array::{Array, RecordBatch};
use arroyo_rpc::grpc::{
    CheckpointMetadata, OperatorCheckpointMetadata, ParquetStoreData, TableCheckpointMetadata,
    TableDescriptor, TableType,
};
use arroyo_rpc::{grpc, CompactionResult, ControlResp};
use arroyo_storage::StorageProvider;
use arroyo_types::{
    from_nanos, CheckpointBarrier, Data, Key, TaskInfo, CHECKPOINT_URL_ENV, S3_ENDPOINT_ENV,
    S3_REGION_ENV,
};
use bincode::config;
use bytes::Bytes;

use arroyo_rpc::df::ArroyoSchema;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use prost::Message;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

pub const FULL_KEY_RANGE: RangeInclusive<u64> = 0..=u64::MAX;
pub const GENERATIONS_TO_COMPACT: u32 = 1; // only compact generation 0 files

async fn get_storage_provider() -> anyhow::Result<StorageProvider> {
    // TODO: this should be encoded in the config so that the controller doesn't need
    // to be synchronized with the workers
    let storage_url =
        env::var(CHECKPOINT_URL_ENV).unwrap_or_else(|_| "file:///tmp/arroyo".to_string());

    StorageProvider::for_url(&storage_url)
        .await
        .context(format!(
            "failed to construct checkpoint backend for URL {}",
            storage_url
        ))
}

pub struct ParquetBackend {
    epoch: u32,
    min_epoch: u32,
    // ordered by table, then epoch.
    current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>>,
    writer: ParquetWriter,
    task_info: TaskInfo,
    tables: HashMap<char, TableDescriptor>,
    storage: StorageProvider,
}

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

    fn task_info(&self) -> &TaskInfo {
        &self.task_info
    }

    async fn load_checkpoint_metadata(job_id: &str, epoch: u32) -> Result<CheckpointMetadata> {
        let storage_client = get_storage_provider().await?;
        let data = storage_client
            .get(&metadata_path(&base_path(job_id, epoch)))
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
            .get_if_present(&metadata_path(&operator_path(job_id, epoch, operator_id)))
            .await?
            .map(|data| Ok(OperatorCheckpointMetadata::decode(&data[..])?))
            .transpose()
    }

    async fn write_operator_checkpoint_metadata(
        metadata: OperatorCheckpointMetadata,
    ) -> Result<()> {
        let storage_client = get_storage_provider().await?;
        let path = metadata_path(&operator_path(
            &metadata.job_id,
            metadata.epoch,
            &metadata.operator_id,
        ));
        // TODO: propagate error
        storage_client.put(&path, metadata.encode_to_vec()).await?;
        Ok(())
    }

    async fn write_checkpoint_metadata(metadata: CheckpointMetadata) -> Result<()> {
        debug!("writing checkpoint {:?}", metadata);
        let storage_client = get_storage_provider().await?;
        let path = metadata_path(&base_path(&metadata.job_id, metadata.epoch));
        storage_client.put(&path, metadata.encode_to_vec()).await?;
        Ok(())
    }

    async fn new(
        task_info: &TaskInfo,
        tables: Vec<TableDescriptor>,
        _tx: Sender<ControlResp>,
    ) -> Self {
        let storage = get_storage_provider().await.unwrap();
        Self {
            epoch: 1,
            min_epoch: 1,
            current_files: HashMap::new(),
            writer: ParquetWriter::new(),
            task_info: task_info.clone(),
            tables: tables
                .into_iter()
                .map(|table| (table.name.clone().chars().next().unwrap(), table))
                .collect(),
            storage,
        }
    }

    async fn from_checkpoint(
        _task_info: &TaskInfo,
        _metadata: CheckpointMetadata,
        _tables: Vec<TableDescriptor>,
        _control_tx: Sender<ControlResp>,
    ) -> Self {
        unimplemented!()
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

    async fn checkpoint(
        &mut self,
        barrier: CheckpointBarrier,
        watermark: Option<SystemTime>,
    ) -> u32 {
        assert_eq!(barrier.epoch, self.epoch);
        self.writer
            .checkpoint(self.epoch, barrier.timestamp, watermark, barrier.then_stop)
            .await;
        self.epoch += 1;
        self.min_epoch = barrier.min_epoch;
        self.epoch - 1
    }

    async fn get_data_tuples<K: Key, V: Data>(&self, table: char) -> Vec<DataTuple<K, V>> {
        let mut result = vec![];
        match self.tables.get(&table).unwrap().table_type() {
            TableType::Global => todo!(),
            TableType::TimeKeyMap | TableType::KeyTimeMultiMap => {
                // current_files is  table -> epoch -> file
                // so we look at all epoch's files for this table
                let Some(files) = self.current_files.get(&table) else {
                    return vec![];
                };
                for file in files.values().flatten() {
                    let bytes = self.storage.get(&file.file).await.unwrap_or_else(|_| {
                        panic!("unable to find file {} in checkpoint", file.file)
                    });
                    result.append(
                        &mut self
                            .tuples_from_parquet_bytes(bytes.into(), &self.task_info.key_range),
                    );
                }
            }
        }
        result
    }

    async fn write_data_tuple<K: Key, V: Data>(
        &mut self,
        table: char,
        _table_type: TableType,
        timestamp: SystemTime,
        key: &mut K,
        value: &mut V,
    ) {
        let (key_hash, key_bytes, value_bytes) = Self::get_hash_and_bytes(key, value);

        self.writer
            .write(
                table,
                key_hash,
                timestamp,
                key_bytes,
                value_bytes,
                DataOperation::Insert,
            )
            .await;
    }

    async fn delete_time_key<K: Key>(
        &mut self,
        table: char,
        _table_type: TableType,
        timestamp: SystemTime,
        key: &mut K,
    ) {
        let (key_hash, key_bytes) = {
            (
                hash_key(key),
                bincode::encode_to_vec(&*key, config::standard()).unwrap(),
            )
        };

        self.writer
            .write(
                table,
                key_hash,
                timestamp,
                key_bytes.clone(),
                vec![],
                DataOperation::DeleteTimeKey(DeleteTimeKeyOperation {
                    timestamp,
                    key: key_bytes,
                }),
            )
            .await;
    }

    async fn delete_data_value<K: Key, V: Data>(
        &mut self,
        table: char,
        timestamp: SystemTime,
        key: &mut K,
        value: &mut V,
    ) {
        let (key_hash, key_bytes, value_bytes) = Self::get_hash_and_bytes(key, value);

        self.writer
            .write(
                table,
                key_hash,
                timestamp,
                key_bytes.clone(),
                vec![],
                DataOperation::DeleteValue(DeleteValueOperation {
                    key: key_bytes,
                    timestamp,
                    value: value_bytes,
                }),
            )
            .await;
    }

    async fn delete_time_range<K: Key>(
        &mut self,
        table: char,
        key: &mut K,
        range: Range<SystemTime>,
    ) {
        let (key_hash, key_bytes) = {
            (
                hash_key(key),
                bincode::encode_to_vec(&*key, config::standard()).unwrap(),
            )
        };

        self.writer
            .write(
                table,
                key_hash,
                SystemTime::now(),
                key_bytes.clone(),
                vec![],
                DataOperation::DeleteTimeRange(DeleteTimeRangeOperation {
                    key: key_bytes,
                    start: range.start,
                    end: range.end,
                }),
            )
            .await;
    }

    async fn write_key_value<K: Key, V: Data>(
        &mut self,
        _table: char,
        _key: &mut K,
        _value: &mut V,
    ) {
        unimplemented!()
    }

    async fn delete_key<K: Key>(&mut self, table: char, key: &mut K) {
        let (key_hash, key_bytes) = {
            (
                hash_key(key),
                bincode::encode_to_vec(&*key, config::standard()).unwrap(),
            )
        };

        self.writer
            .write(
                table,
                key_hash,
                SystemTime::UNIX_EPOCH,
                key_bytes.clone(),
                vec![],
                DataOperation::DeleteKey(DeleteKeyOperation { key: key_bytes }),
            )
            .await;
    }

    async fn get_global_key_values<K: Key, V: Data>(&self, table: char) -> Vec<(K, V)> {
        self.get_key_values_for_key_range(table, &FULL_KEY_RANGE)
            .await
    }

    async fn get_key_values<K: Key, V: Data>(&self, table: char) -> Vec<(K, V)> {
        self.get_key_values_for_key_range(table, &self.task_info.key_range)
            .await
    }

    async fn load_compacted(&mut self, compaction: CompactionResult) {
        self.writer.load_compacted_data(compaction).await;
    }

    async fn insert_committing_data(&mut self, epoch: u32, table: char, committing_data: Vec<u8>) {
        self.writer
            .sender
            .send(ParquetQueueItem::CommitData {
                epoch,
                table,
                data: committing_data,
            })
            .await
            .unwrap();
    }
    async fn register_record_batch_table(&mut self, table: char, schema: ArroyoSchema) {
        self.writer
            .sender
            .send(ParquetQueueItem::RecordBatchInit { table, schema })
            .await
            .unwrap();
    }

    async fn write_record_batch_to_table(&mut self, table: char, record_batch: RecordBatch) {
        self.writer
            .sender
            .send(ParquetQueueItem::RecordBatch {
                record_batch,
                table,
            })
            .await
            .unwrap();
    }
}

impl ParquetBackend {
    /// Get all key-value pairs in the given table.
    /// Looks at the operation to determine if the key-value pair should be included.
    async fn get_key_values_for_key_range<K: Key, V: Data>(
        &self,
        table: char,
        key_range: &RangeInclusive<u64>,
    ) -> Vec<(K, V)> {
        let Some(files) = self.current_files.get(&table) else {
            return vec![];
        };
        let mut state_map = HashMap::new();
        for file in files.values().flatten() {
            let bytes = self
                .storage
                .get(&file.file)
                .await
                .unwrap_or_else(|_| panic!("unable to find file {} in checkpoint", file.file))
                .into();
            for tuple in self.tuples_from_parquet_bytes(bytes, key_range) {
                match tuple.operation {
                    DataOperation::Insert => {
                        state_map.insert(tuple.key, tuple.value.unwrap());
                    }
                    DataOperation::DeleteTimeKey(op) => {
                        let key = bincode::decode_from_slice(&op.key, BINCODE_CONFIG)
                            .unwrap()
                            .0;
                        state_map.remove(&key);
                    }
                    DataOperation::DeleteKey(_) => {
                        panic!("Not supported")
                    }
                    DataOperation::DeleteValue(_) => {
                        panic!("Not supported")
                    }
                    DataOperation::DeleteTimeRange(_) => {
                        panic!("Not supported")
                    }
                }
            }
        }
        state_map.into_iter().collect()
    }

    pub fn get_hash_and_bytes<K: Key, V: Data>(
        key: &mut K,
        value: &mut V,
    ) -> (u64, Vec<u8>, Vec<u8>) {
        (
            hash_key(key),
            bincode::encode_to_vec(&*key, config::standard()).unwrap(),
            bincode::encode_to_vec(&*value, config::standard()).unwrap(),
        )
    }

    /// Called after a checkpoint is committed
    pub async fn compact_operator(
        job_id: String,
        operator_id: String,
        epoch: u32,
    ) -> Result<HashMap<String, TableCheckpointMetadata>> {
        let min_files_to_compact = env::var("MIN_FILES_TO_COMPACT")
            .unwrap_or_else(|_| "4".to_string())
            .parse()?;

        let operator_checkpoint_metadata =
            Self::load_operator_metadata(&job_id, &operator_id, epoch)
                .await?
                .expect("expect operator metadata to still be present");
        let storage_provider = Arc::new(get_storage_provider().await?);
        let compaction_config = CompactionConfig {
            storage_provider,
            compact_generations: vec![0].into_iter().collect(),
            min_compaction_epochs: min_files_to_compact,
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
                grpc::TableEnum::MissingTableType => bail!("should have table type"),
                grpc::TableEnum::GlobalKeyValue => {
                    GlobalKeyedTable::compact_data(
                        table_config,
                        &compaction_config,
                        &operator_metadata,
                        table_metadata,
                    )
                    .await?
                }
                grpc::TableEnum::ExpiringKeyedTimeTable => {
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
                let files = match table_config.table_type() {
                    grpc::TableEnum::MissingTableType => todo!("should handle error"),
                    grpc::TableEnum::GlobalKeyValue => {
                        GlobalKeyedTable::files_to_keep(table_config, metadata.clone()).unwrap()
                    }
                    grpc::TableEnum::ExpiringKeyedTimeTable => {
                        ExpiringTimeKeyTable::files_to_keep(table_config, metadata.clone()).unwrap()
                    }
                };
                files
            })
            .collect();

        let mut deleted_paths = HashSet::new();
        let storage_client = get_storage_provider().await?;

        for epoch_to_remove in old_min_epoch..new_min_epoch {
            let Some(metadata) =
                Self::load_operator_metadata(&job_id, &operator_id, epoch_to_remove).await?
            else {
                continue;
            };

            // delete any files that are not in the new min epoch
            for file in metadata
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
                    let files = match table_config.table_type() {
                        grpc::TableEnum::MissingTableType => todo!("should handle error"),
                        grpc::TableEnum::GlobalKeyValue => {
                            GlobalKeyedTable::files_to_keep(table_config, metadata.clone()).unwrap()
                        }
                        grpc::TableEnum::ExpiringKeyedTimeTable => {
                            ExpiringTimeKeyTable::files_to_keep(table_config, metadata.clone())
                                .unwrap()
                        }
                    };
                    files
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

    /// Return rows from the given bytes that are in the given key range
    fn tuples_from_parquet_bytes<K: Key, V: Data>(
        &self,
        bytes: Vec<u8>,
        range: &RangeInclusive<u64>,
    ) -> Vec<DataTuple<K, V>> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes))
            .unwrap()
            .build()
            .unwrap();

        let mut result = vec![];

        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        for batch in batches {
            let num_rows = batch.num_rows();
            let key_hash_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .unwrap();
            let time_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                .expect("Column 1 is not a TimestampMicrosecondArray");
            let key_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap();
            let value_array = batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap();
            let operation_array = batch
                .column(4)
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap();
            for index in 0..num_rows {
                if !range.contains(&key_hash_array.value(index)) {
                    continue;
                }

                let timestamp = from_nanos(time_array.value(index) as u128);

                let key: K = bincode::decode_from_slice(key_array.value(index), BINCODE_CONFIG)
                    .unwrap()
                    .0;

                let value_option =
                    bincode::decode_from_slice(value_array.value(index), BINCODE_CONFIG);

                let value: Option<V> = match value_option {
                    Ok(v) => Some(v.0),
                    Err(_) => None,
                };

                let operation: DataOperation =
                    bincode::decode_from_slice(operation_array.value(index), BINCODE_CONFIG)
                        .unwrap()
                        .0;

                result.push(DataTuple {
                    timestamp,
                    key,
                    value,
                    operation,
                });
            }
        }
        result
    }
}

pub struct ParquetWriter {
    sender: Sender<ParquetQueueItem>,
    finish_rx: Option<oneshot::Receiver<()>>,
    load_compacted_tx: Sender<CompactionResult>,
}

impl ParquetWriter {
    fn new() -> Self {
        let (sender, _receiver) = mpsc::channel(100);
        let (_finish_tx, finish_rx) = oneshot::channel();
        let (load_compacted_tx, _load_compacted_rx) = mpsc::channel(100);
        ParquetWriter {
            sender,
            finish_rx: Some(finish_rx),
            load_compacted_tx,
        }
    }

    async fn write(
        &mut self,
        table: char,
        key_hash: u64,
        timestamp: SystemTime,
        key: Vec<u8>,
        data: Vec<u8>,
        operation: DataOperation,
    ) {
        self.sender
            .send(ParquetQueueItem::Write(ParquetWrite {
                table,
                key_hash,
                timestamp,
                key,
                data,
                operation,
            }))
            .await
            .unwrap();
    }

    async fn load_compacted_data(&mut self, compaction: CompactionResult) {
        self.load_compacted_tx.send(compaction).await.unwrap();
    }

    async fn checkpoint(
        &mut self,
        epoch: u32,
        time: SystemTime,
        watermark: Option<SystemTime>,
        then_stop: bool,
    ) {
        self.sender
            .send(ParquetQueueItem::Checkpoint(ParquetCheckpoint {
                epoch,
                time,
                watermark,
                then_stop,
            }))
            .await
            .unwrap();
        if then_stop {
            match self.finish_rx.take().unwrap().await {
                Ok(_) => info!("finished stopping checkpoint"),
                Err(err) => warn!("error waiting for stopping checkpoint {:?}", err),
            }
        }
    }
}

#[derive(Debug)]
enum ParquetQueueItem {
    Write(ParquetWrite),
    Checkpoint(ParquetCheckpoint),
    #[allow(unused)]
    CommitData {
        epoch: u32,
        table: char,
        data: Vec<u8>,
    },
    #[allow(unused)]
    RecordBatchInit {
        table: char,
        schema: ArroyoSchema,
    },
    #[allow(unused)]
    RecordBatch {
        record_batch: RecordBatch,
        table: char,
    },
}

#[allow(unused)]
#[derive(Debug)]
struct ParquetWrite {
    table: char,
    key_hash: u64,
    timestamp: SystemTime,
    key: Vec<u8>,
    data: Vec<u8>,
    operation: DataOperation,
}

#[allow(unused)]
#[derive(Debug)]
struct ParquetCheckpoint {
    epoch: u32,
    time: SystemTime,
    watermark: Option<SystemTime>,
    then_stop: bool,
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

pub fn get_storage_env_vars() -> HashMap<String, String> {
    [S3_REGION_ENV, S3_ENDPOINT_ENV, CHECKPOINT_URL_ENV]
        .iter()
        .filter_map(|&var| env::var(var).ok().map(|v| (var.to_string(), v)))
        .collect()
}
