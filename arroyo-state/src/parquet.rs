use crate::metrics::CURRENT_FILES_GAUGE;
use crate::tables::{BlindDataTuple, Compactor, DataTuple};
use crate::{
    hash_key, BackingStore, DataOperation, DeleteKeyOperation, DeleteTimeKeyOperation,
    DeleteTimeRangeOperation, DeleteValueOperation, StateStore, BINCODE_CONFIG,
};
use anyhow::{bail, Context, Result};
use arrow_array::RecordBatch;
use arroyo_rpc::grpc::backend_data::BackendData;
use arroyo_rpc::grpc::{
    backend_data, CheckpointMetadata, OperatorCheckpointMetadata, ParquetStoreData,
    SubtaskCheckpointMetadata, TableDeleteBehavior, TableDescriptor, TableType,
};
use arroyo_rpc::{grpc, CheckpointCompleted, CompactionResult, ControlResp};
use arroyo_storage::StorageProvider;
use arroyo_types::{
    from_nanos, range_for_server, to_micros, to_nanos, CheckpointBarrier, Data, Key, TaskInfo,
    CHECKPOINT_URL_ENV, S3_ENDPOINT_ENV, S3_REGION_ENV,
};
use bincode::config;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::ZstdLevel;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use prost::Message;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::ops::{Range, RangeInclusive};
use std::time::SystemTime;
use tokio::sync::mpsc::{self, channel, Receiver, Sender};
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

fn table_checkpoint_path(task_info: &TaskInfo, table: char, epoch: u32, compacted: bool) -> String {
    format!(
        "{}/table-{}-{:0>3}{}",
        operator_path(&task_info.job_id, epoch, &task_info.operator_id),
        table,
        task_info.task_index,
        if compacted { "-compacted" } else { "" }
    )
}

#[async_trait::async_trait]
impl BackingStore for ParquetBackend {
    fn name() -> &'static str {
        "parquet"
    }

    fn task_info(&self) -> &TaskInfo {
        &self.task_info
    }

    async fn load_latest_checkpoint_metadata(_job_id: &str) -> Option<CheckpointMetadata> {
        todo!()
    }

    // TODO: should this be a Result, rather than an option?
    async fn load_checkpoint_metadata(job_id: &str, epoch: u32) -> Option<CheckpointMetadata> {
        let storage_client = get_storage_provider().await.unwrap();
        let data = storage_client
            .get(&metadata_path(&base_path(job_id, epoch)))
            .await
            .ok()?;
        let metadata = CheckpointMetadata::decode(&data[..]).unwrap();
        Some(metadata)
    }

    async fn load_operator_metadata(
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Option<OperatorCheckpointMetadata> {
        let storage_client = get_storage_provider().await.unwrap();
        let data = storage_client
            .get(&metadata_path(&operator_path(job_id, epoch, operator_id)))
            .await
            .ok()?;
        Some(OperatorCheckpointMetadata::decode(&data[..]).unwrap())
    }

    async fn write_operator_checkpoint_metadata(metadata: OperatorCheckpointMetadata) {
        let storage_client = get_storage_provider().await.unwrap();
        let path = metadata_path(&operator_path(
            &metadata.job_id,
            metadata.epoch,
            &metadata.operator_id,
        ));
        // TODO: propagate error
        storage_client
            .put(&path, metadata.encode_to_vec())
            .await
            .unwrap();
    }

    async fn write_checkpoint_metadata(metadata: CheckpointMetadata) {
        debug!("writing checkpoint {:?}", metadata);
        let storage_client = get_storage_provider().await.unwrap();
        let path = metadata_path(&base_path(&metadata.job_id, metadata.epoch));
        // TODO: propagate error
        storage_client
            .put(&path, metadata.encode_to_vec())
            .await
            .unwrap();
    }

    async fn new(
        task_info: &TaskInfo,
        tables: Vec<TableDescriptor>,
        tx: Sender<ControlResp>,
    ) -> Self {
        let storage = get_storage_provider().await.unwrap();
        Self {
            epoch: 1,
            min_epoch: 1,
            current_files: HashMap::new(),
            writer: ParquetWriter::new(
                task_info.clone(),
                tx,
                tables.clone(),
                storage.clone(),
                HashMap::new(),
            ),
            task_info: task_info.clone(),
            tables: tables
                .into_iter()
                .map(|table| (table.name.clone().chars().next().unwrap(), table))
                .collect(),
            storage,
        }
    }

    async fn from_checkpoint(
        task_info: &TaskInfo,
        metadata: CheckpointMetadata,
        tables: Vec<TableDescriptor>,
        control_tx: Sender<ControlResp>,
    ) -> Self {
        let operator_metadata =
            Self::load_operator_metadata(&task_info.job_id, &task_info.operator_id, metadata.epoch)
                .await
                .unwrap_or_else(|| {
                    panic!(
                        "missing metadata for operator {}, epoch {}",
                        task_info.operator_id, metadata.epoch
                    )
                });
        let mut current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>> = HashMap::new();
        let tables: HashMap<char, TableDescriptor> = tables
            .into_iter()
            .map(|table| (table.name.clone().chars().next().unwrap(), table))
            .collect();
        for backend_data in operator_metadata.backend_data {
            let Some(backend_data::BackendData::ParquetStore(parquet_data)) =
                backend_data.backend_data
            else {
                panic!("expect parquet data")
            };
            let table_descriptor = tables
                .get(&parquet_data.table.chars().next().unwrap())
                .unwrap();
            if table_descriptor.table_type() != TableType::Global {
                // check if the file has data in the task's key range.
                if parquet_data.max_routing_key < *task_info.key_range.start()
                    || *task_info.key_range.end() < parquet_data.min_routing_key
                {
                    continue;
                }
            }

            let files = current_files
                .entry(parquet_data.table.chars().next().unwrap())
                .or_default()
                .entry(parquet_data.epoch)
                .or_default();
            files.push(parquet_data);
        }

        let writer_current_files = current_files.clone();

        let storage = get_storage_provider().await.unwrap();
        Self {
            epoch: metadata.epoch + 1,
            min_epoch: metadata.min_epoch,
            current_files,
            writer: ParquetWriter::new(
                task_info.clone(),
                control_tx,
                tables.values().cloned().collect(),
                storage.clone(),
                writer_current_files,
            ),
            task_info: task_info.clone(),
            tables,
            storage,
        }
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
        Self::write_checkpoint_metadata(metadata).await;
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

    async fn write_key_value<K: Key, V: Data>(&mut self, table: char, key: &mut K, value: &mut V) {
        self.write_data_tuple(table, TableType::Global, SystemTime::UNIX_EPOCH, key, value)
            .await
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

    async fn compact_table_partition(
        table_char: char,
        task: TaskInfo,
        generation: u32,
        generation_files: Vec<ParquetStoreData>,
        storage_client: StorageProvider,
        new_min_epoch: u32,
        table_descriptor: &TableDescriptor,
    ) -> Option<ParquetStoreData> {
        // accumulate this partition's tuples from all the table's files
        // (spread across multiple epochs and files)
        let mut tuples_in = vec![];
        for file in generation_files {
            let bytes = storage_client
                .get(&file.file)
                .await
                .unwrap_or_else(|_| panic!("unable to find file {} in checkpoint", file.file));
            let tuples =
                ParquetBackend::blind_tuples_from_parquet_bytes(bytes.into(), &task.key_range);
            tuples_in.extend(tuples);
        }

        // do the compaction
        let compactor: Compactor = Compactor::for_table_type(table_descriptor.table_type());
        let tuples_length = tuples_in.len();
        let tuples_out = compactor.compact_tuples(tuples_in);

        info!(
            message = "Compaction summary for operator",
            operator_id = task.operator_id,
            table_char = table_char.to_string(),
            table_type = table_descriptor.table_type().as_str_name(),
            task_index = task.task_index,
            tuples_in = tuples_length,
            tuples_out = tuples_out.len(),
            compacted = tuples_length - tuples_out.len()
        );

        let mut parquet_writer =
            ParquetCompactFileWriter::new(table_char, new_min_epoch, task.clone(), generation + 1);

        for tuple in tuples_out {
            parquet_writer.write(
                tuple.key_hash,
                tuple.timestamp,
                tuple.key,
                tuple.value,
                tuple.operation,
            );
        }

        let p = parquet_writer
            .flush(&storage_client)
            .await
            .expect("Failed to flush parquet writer");
        p
    }

    /// Called after a checkpoint is committed
    pub async fn compact_operator(
        parallelism: usize,
        job_id: String,
        operator_id: String,
        epoch: u32,
    ) -> Result<Option<CompactionResult>> {
        let min_files_to_compact = env::var("MIN_FILES_TO_COMPACT")
            .unwrap_or_else(|_| "4".to_string())
            .parse()
            .unwrap();

        let checkpoint_metadata = Self::load_checkpoint_metadata(&job_id, epoch)
            .await
            .unwrap_or_else(|| {
                panic!(
                    "missing checkpoint metadata for job {}, epoch {}",
                    job_id, epoch
                )
            });

        let operator_checkpoint_metadata =
            Self::load_operator_metadata(&job_id, &operator_id, epoch)
                .await
                .expect("expect operator metadata to still be present");

        let mut backend_data_to_drop = HashMap::new();
        let mut backend_data_to_load = vec![]; // one file per partition per table

        // we reduce the range of epochs to a set of compacted files
        for index in 0..parallelism {
            let key_range = range_for_server(index, parallelism);

            // construct a theoretical TaskInfo for the purpose of partitioning the data
            let task = TaskInfo {
                job_id: job_id.clone(),
                operator_name: "".to_string(), // TODO: this is not used
                operator_id: operator_id.clone(),
                task_index: index,
                parallelism,
                key_range: key_range.clone(),
            };
            let (tx, _) = channel(10);

            // we must access the data through the state store because
            // it keeps track of the table -> file relation
            let mut state_store = StateStore::<ParquetBackend>::from_checkpoint(
                &task,
                checkpoint_metadata.clone(),
                operator_checkpoint_metadata.tables.clone(),
                tx.clone(),
            )
            .await;

            // for each table this operator has, generate this partition's compacted file
            for (table_char, epoch_files) in state_store.backend.current_files.drain() {
                for generation in 0..GENERATIONS_TO_COMPACT {
                    // get just the files for this table
                    let generation_files: Vec<ParquetStoreData> = epoch_files
                        .values()
                        .flatten()
                        .filter(|file| file.generation == generation)
                        .cloned()
                        .collect();

                    if generation_files.len() < min_files_to_compact {
                        continue;
                    }

                    info!(
                        message = "Compacting table partition",
                        job_id,
                        operator_id,
                        table = table_char.to_string(),
                        epoch,
                        index,
                        files = generation_files.len(),
                    );

                    for file in &generation_files {
                        backend_data_to_drop.insert(
                            file.file.clone(),
                            grpc::BackendData {
                                backend_data: Some(BackendData::ParquetStore(file.clone())),
                            },
                        );
                    }

                    let compact_parquet_store_data = ParquetBackend::compact_table_partition(
                        table_char,
                        task.clone(),
                        generation,
                        generation_files,
                        get_storage_provider().await?,
                        epoch,
                        state_store.table_descriptors.get(&table_char).unwrap(),
                    )
                    .await;

                    if let Some(p) = compact_parquet_store_data {
                        backend_data_to_load.push(grpc::BackendData {
                            backend_data: Some(BackendData::ParquetStore(p)),
                        });
                    }
                }
            }
        }

        if !backend_data_to_drop.is_empty() {
            // CompactionResult is only sent if there is data to drop
            Ok(Some(CompactionResult {
                operator_id,
                backend_data_to_drop: backend_data_to_drop.values().cloned().collect(),
                backend_data_to_load,
            }))
        } else {
            Ok(None)
        }
    }

    /// Delete files no longer referenced by the new min epoch
    pub async fn cleanup_operator(
        job_id: String,
        operator_id: String,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<String> {
        let paths_to_keep: HashSet<String> =
            Self::load_operator_metadata(&job_id, &operator_id, new_min_epoch)
                .await
                .expect("expect new_min_epoch metadata to still be present")
                .backend_data
                .iter()
                .map(|backend_data| {
                    let Some(BackendData::ParquetStore(parquet_store)) = &backend_data.backend_data
                    else {
                        unreachable!("expect parquet backends")
                    };
                    parquet_store.file.clone()
                })
                .collect();

        let mut deleted_paths = HashSet::new();
        let storage_client = get_storage_provider().await?;

        for epoch_to_remove in old_min_epoch..new_min_epoch {
            let Some(metadata) =
                Self::load_operator_metadata(&job_id, &operator_id, epoch_to_remove).await
            else {
                continue;
            };

            // delete any files that are not in the new min epoch
            for backend_data in metadata.backend_data {
                let Some(BackendData::ParquetStore(parquet_store)) = &backend_data.backend_data
                else {
                    unreachable!("expect parquet backends")
                };
                let file = parquet_store.file.clone();
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

    /// Return rows from the given bytes that are in the given key range,
    /// but without deserializing the key and value.
    fn blind_tuples_from_parquet_bytes(
        bytes: Vec<u8>,
        range: &RangeInclusive<u64>,
    ) -> Vec<BlindDataTuple> {
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
                .expect("Column 1 is not a TimestampNanosecondArray");
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
                let key_hash = key_hash_array.value(index);
                if !range.contains(&key_hash) {
                    continue;
                }

                let timestamp = from_nanos(time_array.value(index) as u128);

                let key = key_array.value(index).to_owned();
                let value = value_array.value(index).to_owned();
                let operation: DataOperation =
                    bincode::decode_from_slice(operation_array.value(index), BINCODE_CONFIG)
                        .unwrap()
                        .0;

                result.push(BlindDataTuple {
                    key_hash,
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

pub struct ParquetCompactFileWriter {
    table_char: char,
    epoch: u32,
    task_info: TaskInfo,
    builder: RecordBatchBuilder,
    new_generation: u32,
}

impl ParquetCompactFileWriter {
    pub fn new(table_char: char, epoch: u32, task_info: TaskInfo, new_generation: u32) -> Self {
        ParquetCompactFileWriter {
            table_char,
            epoch,
            task_info,
            builder: RecordBatchBuilder::default(),
            new_generation,
        }
    }

    pub(crate) fn write(
        &mut self,
        key_hash: u64,
        timestamp: SystemTime,
        key: Vec<u8>,
        data: Vec<u8>,
        operation: DataOperation,
    ) {
        self.builder
            .insert(key_hash, timestamp, key, data, operation);
    }

    async fn upload_record_batch(
        key: &str,
        record_batch: arrow_array::RecordBatch,
        storage: &StorageProvider,
    ) -> Result<usize> {
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::default()))
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let cursor = Vec::new();
        let mut writer = ArrowWriter::try_new(cursor, record_batch.schema(), Some(props)).unwrap();
        writer.write(&record_batch)?;
        writer.flush().unwrap();
        let parquet_bytes = writer.into_inner().unwrap();
        let bytes = parquet_bytes.len();
        storage.put(key, parquet_bytes).await?;
        Ok(bytes)
    }

    pub async fn flush(self, storage: &StorageProvider) -> Result<Option<ParquetStoreData>> {
        let s3_key = table_checkpoint_path(&self.task_info, self.table_char, self.epoch, true);

        // write the file even if the builder is empty
        // so that we can delete the old files
        match self.builder.flush() {
            Some((record_batch, stats)) => {
                ParquetCompactFileWriter::upload_record_batch(&s3_key, record_batch, storage)
                    .await?;
                return Ok(Some(ParquetStoreData {
                    epoch: self.epoch,
                    file: s3_key,
                    table: self.table_char.to_string(),
                    min_routing_key: stats.min_routing_key,
                    max_routing_key: stats.max_routing_key,
                    max_timestamp_micros: arroyo_types::to_micros(stats.max_timestamp) + 1,
                    min_required_timestamp_micros: None,
                    generation: self.new_generation,
                }));
            }
            None => Ok(None),
        }
    }
}

pub struct ParquetWriter {
    sender: Sender<ParquetQueueItem>,
    finish_rx: Option<oneshot::Receiver<()>>,
    load_compacted_rx: Sender<CompactionResult>,
}

impl ParquetWriter {
    fn new(
        task_info: TaskInfo,
        control_tx: Sender<ControlResp>,
        tables: Vec<TableDescriptor>,
        storage: StorageProvider,
        current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1024 * 1024);
        let (finish_tx, finish_rx) = oneshot::channel();
        let (load_compacted_rx, load_compacted_tx) = mpsc::channel(1024 * 1024);

        (ParquetFlusher {
            queue: rx,
            storage,
            control_tx,
            finish_tx: Some(finish_tx),
            task_info,
            table_descriptors: tables
                .iter()
                .map(|table| (table.name.chars().next().unwrap(), table.clone()))
                .collect(),
            builders: HashMap::new(),
            commit_data: HashMap::new(),
            current_files,
            load_compacted_tx,
            new_compacted: vec![],
        })
        .start();

        ParquetWriter {
            sender: tx,
            finish_rx: Some(finish_rx),
            load_compacted_rx,
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
        self.load_compacted_rx.send(compaction).await.unwrap();
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
    CommitData {
        epoch: u32,
        table: char,
        data: Vec<u8>,
    },
}

#[derive(Debug)]
struct ParquetWrite {
    table: char,
    key_hash: u64,
    timestamp: SystemTime,
    key: Vec<u8>,
    data: Vec<u8>,
    operation: DataOperation,
}

#[derive(Debug)]
struct ParquetCheckpoint {
    epoch: u32,
    time: SystemTime,
    watermark: Option<SystemTime>,
    then_stop: bool,
}

struct RecordBatchBuilder {
    key_hash_builder: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
    start_time_array:
        arrow_array::builder::PrimitiveBuilder<arrow_array::types::TimestampNanosecondType>,
    key_bytes: arrow_array::builder::BinaryBuilder,
    data_bytes: arrow_array::builder::BinaryBuilder,
    parquet_stats: ParquetStats,
    operation_array: arrow_array::builder::BinaryBuilder,
}

struct ParquetStats {
    max_timestamp: SystemTime,
    min_routing_key: u64,
    max_routing_key: u64,
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

impl RecordBatchBuilder {
    fn insert(
        &mut self,
        key_hash: u64,
        timestamp: SystemTime,
        key: Vec<u8>,
        data: Vec<u8>,
        operation: DataOperation,
    ) {
        self.parquet_stats.min_routing_key = self.parquet_stats.min_routing_key.min(key_hash);
        self.parquet_stats.max_routing_key = self.parquet_stats.max_routing_key.max(key_hash);

        self.key_hash_builder.append_value(key_hash);
        self.start_time_array
            .append_value(to_nanos(timestamp) as i64);
        self.key_bytes.append_value(key);
        self.data_bytes.append_value(data);

        self.operation_array
            .append_value(bincode::encode_to_vec(operation, config::standard()).unwrap());
        self.parquet_stats.max_timestamp = self.parquet_stats.max_timestamp.max(timestamp);
    }

    fn flush(mut self) -> Option<(arrow_array::RecordBatch, ParquetStats)> {
        let key_hash_array: arrow_array::PrimitiveArray<arrow_array::types::UInt64Type> =
            self.key_hash_builder.finish();
        if key_hash_array.is_empty() {
            return None;
        }
        let start_time_array: arrow_array::PrimitiveArray<
            arrow_array::types::TimestampNanosecondType,
        > = self.start_time_array.finish();
        let key_array: arrow_array::BinaryArray = self.key_bytes.finish();
        let data_array: arrow_array::BinaryArray = self.data_bytes.finish();
        let operations_array = self.operation_array.finish();
        Some((
            arrow_array::RecordBatch::try_new(
                self.schema(),
                vec![
                    std::sync::Arc::new(key_hash_array),
                    std::sync::Arc::new(start_time_array),
                    std::sync::Arc::new(key_array),
                    std::sync::Arc::new(data_array),
                    std::sync::Arc::new(operations_array),
                ],
            )
            .unwrap(),
            self.parquet_stats,
        ))
    }

    fn schema(&self) -> std::sync::Arc<arrow_schema::Schema> {
        std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("key_hash", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new(
                "start_time",
                arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                false,
            ),
            arrow::datatypes::Field::new("key_bytes", arrow::datatypes::DataType::Binary, false),
            arrow::datatypes::Field::new(
                "aggregate_bytes",
                arrow::datatypes::DataType::Binary,
                false,
            ),
            arrow::datatypes::Field::new("operation", arrow::datatypes::DataType::Binary, false),
        ]))
    }
}
impl Default for RecordBatchBuilder {
    fn default() -> Self {
        Self {
            key_hash_builder: arrow_array::builder::PrimitiveBuilder::<
                arrow_array::types::UInt64Type,
            >::with_capacity(1024),
            start_time_array: arrow_array::builder::PrimitiveBuilder::<
                arrow_array::types::TimestampNanosecondType,
            >::with_capacity(1024),
            key_bytes: arrow_array::builder::BinaryBuilder::default(),
            data_bytes: arrow_array::builder::BinaryBuilder::default(),
            operation_array: arrow_array::builder::BinaryBuilder::default(),
            parquet_stats: ParquetStats::default(),
        }
    }
}

struct ParquetFlusher {
    queue: Receiver<ParquetQueueItem>,
    storage: StorageProvider,
    control_tx: Sender<ControlResp>,
    finish_tx: Option<oneshot::Sender<()>>,
    task_info: TaskInfo,
    table_descriptors: HashMap<char, TableDescriptor>,
    builders: HashMap<char, RecordBatchBuilder>,
    commit_data: HashMap<char, Vec<u8>>,
    current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>>, // table -> epoch -> file
    load_compacted_tx: Receiver<CompactionResult>,
    new_compacted: Vec<ParquetStoreData>,
}

impl ParquetFlusher {
    fn start(mut self) {
        tokio::spawn(async move {
            loop {
                if !self.flush_iteration().await.unwrap() {
                    return;
                }
            }
        });
    }
    async fn upload_record_batch(
        &self,
        key: &str,
        record_batch: arrow_array::RecordBatch,
    ) -> Result<usize> {
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::default()))
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let cursor = Vec::new();
        let mut writer = ArrowWriter::try_new(cursor, record_batch.schema(), Some(props)).unwrap();
        writer.write(&record_batch)?;
        writer.flush().unwrap();
        let parquet_bytes = writer.into_inner().unwrap();
        let bytes = parquet_bytes.len();
        self.storage.put(key, parquet_bytes).await?;
        Ok(bytes)
    }

    fn get_parquet_store_parts(backend_data: grpc::BackendData) -> (ParquetStoreData, char, u32) {
        let Some(BackendData::ParquetStore(parquet_store)) = backend_data.backend_data else {
            unreachable!("expect parquet backends")
        };
        let table_char = parquet_store.table.chars().next().unwrap();
        let epoch = parquet_store.epoch;
        (parquet_store, table_char, epoch)
    }

    async fn load_compacted(&mut self, compaction: &CompactionResult) {
        info!(
            "Loading compacted data for operator {}. Dropping {} files, loading {} files.",
            compaction.operator_id,
            compaction.backend_data_to_drop.len(),
            compaction.backend_data_to_load.len(),
        );

        // add all the new files
        for backend_data in compaction.backend_data_to_load.clone() {
            let (parquet_store, table_char, epoch) = Self::get_parquet_store_parts(backend_data);

            self.current_files
                .entry(table_char)
                .or_default()
                .entry(epoch)
                .or_default()
                .push(parquet_store.clone());

            // separately keep track of the new files so we can
            // force them to be included in the next checkpoint
            self.new_compacted.push(parquet_store);
        }

        // remove all the old files
        for backend_data in compaction.backend_data_to_drop.clone() {
            let (parquet_store, table_char, epoch) = Self::get_parquet_store_parts(backend_data);

            if let Some(x) = self.current_files.get_mut(&table_char) {
                if let Some(y) = x.get_mut(&epoch) {
                    if let Some(index) = y.iter().position(|f| f.file == parquet_store.file) {
                        y.remove(index);
                    }
                }
            }
        }
    }

    async fn flush_iteration(&mut self) -> Result<bool> {
        let mut checkpoint_epoch = None;
        let mut commit_epoch = None;

        // accumulate writes in the RecordBatchBuilders until we get a checkpoint
        while checkpoint_epoch.is_none() {
            tokio::select! {
                compaction = self.load_compacted_tx.recv() => {
                    match compaction {
                        Some(compaction) => {
                            self.load_compacted(&compaction).await;
                            return Ok(true)
                        }
                        None => { return Ok(false)}
                    }
                }
                op = self.queue.recv() => {
                    match op {
                        Some(ParquetQueueItem::Write( ParquetWrite{table, key_hash, timestamp, key, data, operation})) => {
                            self.builders.entry(table).or_default().insert(key_hash, timestamp, key, data, operation);
                        }
                        Some(ParquetQueueItem::Checkpoint(epoch)) => {
                            checkpoint_epoch = Some(epoch);
                        },
                        Some(ParquetQueueItem::CommitData{epoch, table, data}) => {
                            // confirm that epoch isn't different than prior ones
                            if let Some(commit_epoch) = commit_epoch {
                                if commit_epoch != epoch {
                                    bail!("prior commit epoch {} does not match new commit epoch {}", commit_epoch, epoch);
                                }
                            } else {
                                commit_epoch = Some(epoch);
                            }
                            self.commit_data.insert(table, data);
                        }
                        None => {
                            debug!("Parquet flusher closed");
                            return Ok(false);
                        }
                    }
                }
            }
        }
        let Some(cp) = checkpoint_epoch else {
            bail!("somehow exited loop without checkpoint_epoch being set");
        };
        if let Some(commit_epoch) = commit_epoch {
            if commit_epoch != cp.epoch {
                bail!(
                    "commit epoch {} does not match checkpoint epoch {}",
                    commit_epoch,
                    cp.epoch
                );
            }
        }

        let mut bytes = 0;
        let mut to_write = vec![];
        for (table, builder) in self.builders.drain() {
            let Some((record_batch, stats)) = builder.flush() else {
                continue;
            };
            let s3_key = table_checkpoint_path(&self.task_info, table, cp.epoch, false);
            to_write.push((record_batch, s3_key, table, stats));
        }

        // write the files and update current_files
        for (record_batch, s3_key, table, stats) in to_write {
            bytes += self.upload_record_batch(&s3_key, record_batch).await?;
            self.current_files
                .entry(table)
                .or_default()
                .entry(cp.epoch)
                .or_default()
                .push(ParquetStoreData {
                    epoch: cp.epoch,
                    file: s3_key,
                    table: table.to_string(),
                    min_routing_key: stats.min_routing_key,
                    max_routing_key: stats.max_routing_key,
                    max_timestamp_micros: arroyo_types::to_micros(stats.max_timestamp) + 1,
                    min_required_timestamp_micros: None,
                    generation: 0,
                });
        }

        // build backend_data (to send to controller in SubtaskCheckpointMetadata)
        // and new current_files
        let mut checkpoint_backend_data = vec![];
        let mut new_current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>> =
            HashMap::new();

        for (table, epoch_files) in self.current_files.drain() {
            let table_descriptor = self.table_descriptors.get(&table).unwrap();
            for (epoch, files) in epoch_files {
                if table_descriptor.table_type() == TableType::Global && epoch < cp.epoch {
                    continue;
                }
                for file in files {
                    if table_descriptor.delete_behavior()
                        == TableDeleteBehavior::NoReadsBeforeWatermark
                    {
                        if let Some(checkpoint_watermark) = cp.watermark {
                            if file.max_timestamp_micros
                                < to_micros(checkpoint_watermark)
                                    - table_descriptor.retention_micros
                                && !self.new_compacted.iter().any(|f| f.file == file.file)
                            {
                                // this file is not needed by the new checkpoint
                                // because its data is older than the watermark
                                continue;
                            }
                        }
                    }
                    checkpoint_backend_data.push(grpc::BackendData {
                        backend_data: Some(BackendData::ParquetStore(file.clone())),
                    });
                    new_current_files
                        .entry(table)
                        .or_default()
                        .entry(epoch)
                        .or_default()
                        .push(file);
                }
            }
        }
        self.current_files = new_current_files;
        self.new_compacted = vec![];

        // compute total number of files in this checkpoint
        let mut total_files = 0;
        let mut max_timestamp: u64 = 0;
        for (_table, epoch_files) in self.current_files.iter() {
            for (_epoch, files) in epoch_files.iter() {
                total_files += files.len();
                for file in files {
                    max_timestamp = max_timestamp.max(file.max_timestamp_micros);
                }
            }
        }

        let task_index = self.task_info.task_index.to_string();
        let label_values = [self.task_info.operator_id.as_str(), task_index.as_str()];
        CURRENT_FILES_GAUGE
            .with_label_values(&label_values)
            .set(total_files as f64);

        // send controller the subtask metadata
        let subtask_metadata = SubtaskCheckpointMetadata {
            subtask_index: self.task_info.task_index as u32,
            start_time: to_micros(cp.time),
            finish_time: to_micros(SystemTime::now()),
            has_state: !checkpoint_backend_data.is_empty(),
            tables: self.table_descriptors.values().cloned().collect(),
            watermark: cp.watermark.map(to_micros),
            backend_data: checkpoint_backend_data,
            bytes: bytes as u64,
            committing_data: self
                .commit_data
                .drain()
                .map(|(table, data)| (table.to_string(), data))
                .collect(),
        };
        self.control_tx
            .send(ControlResp::CheckpointCompleted(CheckpointCompleted {
                checkpoint_epoch: cp.epoch,
                operator_id: self.task_info.operator_id.clone(),
                subtask_metadata,
            }))
            .await
            .unwrap();
        if cp.then_stop {
            self.finish_tx.take().unwrap().send(()).unwrap();
            return Ok(false);
        }
        Ok(true)
    }
}

pub fn get_storage_env_vars() -> HashMap<String, String> {
    [S3_REGION_ENV, S3_ENDPOINT_ENV, CHECKPOINT_URL_ENV]
        .iter()
        .filter_map(|&var| env::var(var).ok().map(|v| (var.to_string(), v)))
        .collect()
}
