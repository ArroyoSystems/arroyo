use crate::judy::{self, BytesWithOffset, InMemoryJudyNode, JudyNode, JudyWriter};
use crate::tables::DataTuple;
use crate::{hash_key, BackingStore, BINCODE_CONFIG};
use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arroyo_rpc::grpc::backend_data::BackendData;
use arroyo_rpc::grpc::{
    backend_data, CheckpointMetadata, OperatorCheckpointMetadata, ParquetStoreData,
    SubtaskCheckpointMetadata, TableDeleteBehavior, TableDescriptor, TableType,
};
use arroyo_rpc::{grpc, CheckpointCompleted, CompactionResult, ControlResp};
use arroyo_storage::StorageProvider;
use arroyo_types::{
    from_micros, to_micros, CheckpointBarrier, Data, Key, TaskInfo, CHECKPOINT_URL_ENV,
    S3_ENDPOINT_ENV, S3_REGION_ENV,
};
use async_trait::async_trait;
use bincode::config;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use prost::Message;
use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::io::Cursor;
use std::ops::RangeInclusive;
use std::time::{Duration, SystemTime};
use tokio::io::BufWriter;
use tokio::io::{AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, info};
use tracing::{error, warn};

use super::tables::key_time_multimap::JudyKeyTimeMultiMap;
use super::tables::KeyTimeMultiMap;

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

pub struct JudyBackend {
    epoch: u32,
    min_epoch: u32,
    // ordered by table, then epoch.
    current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>>,
    judy_file_data:
        HashMap<char, BTreeMap<u32, BTreeMap<u64, (ParquetStoreData, BytesOrFutureBytes)>>>,
    writer: JudyCheckpointWriter,
    task_info: TaskInfo,
    tables: HashMap<char, TableDescriptor>,
    caches: HashMap<char, Box<dyn Any + Send + Sync>>,
    storage: StorageProvider,
}

pub struct BytesOrFutureBytes {
    bytes: Option<Bytes>,
    future_bytes: Option<oneshot::Receiver<Bytes>>,
}

impl BytesOrFutureBytes {
    fn from_oneshot(rx: oneshot::Receiver<Bytes>) -> Self {
        Self {
            bytes: None,
            future_bytes: Some(rx),
        }
    }
    fn from_bytes(bytes: Bytes) -> Self {
        Self {
            bytes: Some(bytes),
            future_bytes: None,
        }
    }

    pub async fn bytes(&mut self) -> Bytes {
        if self.bytes.is_some() {
            self.bytes.as_ref().unwrap().clone()
        } else {
            let bytes = self.future_bytes.take().unwrap().await.unwrap();
            self.bytes = Some(bytes.clone());
            bytes
        }
    }
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

fn table_checkpoint_path(task_info: &TaskInfo, table: char, epoch: u32) -> String {
    format!(
        "{}/table-{}-{:0>3}",
        operator_path(&task_info.job_id, epoch, &task_info.operator_id),
        table,
        task_info.task_index,
    )
}

#[async_trait::async_trait]
impl BackingStore for JudyBackend {
    fn name() -> &'static str {
        "judy"
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
            .put(&path, metadata.encode_to_vec().into())
            .await
            .unwrap();
    }

    async fn write_checkpoint_metadata(metadata: CheckpointMetadata) {
        debug!("writing checkpoint {:?}", metadata);
        let storage_client = get_storage_provider().await.unwrap();
        let path = metadata_path(&base_path(&metadata.job_id, metadata.epoch));
        // TODO: propagate error
        storage_client
            .put(&path, metadata.encode_to_vec().into())
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
            judy_file_data: HashMap::new(),
            writer: JudyCheckpointWriter::new(
                task_info.clone(),
                tx,
                tables.clone(),
                storage.clone(),
                HashMap::new(),
            ),
            caches: HashMap::new(),
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
        let mut judy_file_data: HashMap<
            char,
            BTreeMap<u32, BTreeMap<u64, (ParquetStoreData, BytesOrFutureBytes)>>,
        > = HashMap::new();
        let storage = get_storage_provider().await.unwrap();
        let tables: HashMap<char, TableDescriptor> = tables
            .into_iter()
            .map(|table| (table.name.clone().chars().next().unwrap(), table))
            .collect();
        for backend_data in operator_metadata.backend_data {
            let Some(backend_data::BackendData::ParquetStore(Judy_data)) =
                backend_data.backend_data
            else {
                panic!("expect Judy data")
            };
            let table_descriptor = tables
                .get(&Judy_data.table.chars().next().unwrap())
                .unwrap();
            if table_descriptor.table_type() != TableType::Global {
                // check if the file has data in the task's key range.
                if Judy_data.max_routing_key < *task_info.key_range.start()
                    || *task_info.key_range.end() < Judy_data.min_routing_key
                {
                    continue;
                }
            }

            let files = current_files
                .entry(Judy_data.table.chars().next().unwrap())
                .or_default()
                .entry(Judy_data.epoch)
                .or_default();
            files.push(Judy_data.clone());
            // create a oneshot channel to pass the data through
            let (tx, rx) = oneshot::channel();
            let path = Judy_data.file.clone();
            let storage_clone = storage.clone();
            tokio::spawn(async move {
                let bytes = storage_clone.get(&path).await.unwrap();
                tx.send(bytes).unwrap();
            });
            judy_file_data
                .entry(Judy_data.table.chars().next().unwrap())
                .or_default()
                .entry(Judy_data.epoch)
                .or_default()
                .insert(
                    Judy_data.min_routing_key,
                    (Judy_data.clone(), BytesOrFutureBytes::from_oneshot(rx)),
                );
        }

        let writer_current_files = current_files.clone();

        Self {
            epoch: metadata.epoch + 1,
            min_epoch: metadata.min_epoch,
            current_files,
            judy_file_data,
            writer: JudyCheckpointWriter::new(
                task_info.clone(),
                control_tx,
                tables.values().cloned().collect(),
                storage.clone(),
                writer_current_files,
            ),
            task_info: task_info.clone(),
            caches: HashMap::new(),
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
        unimplemented!()
    }

    async fn write_data_tuple<K: Key, V: Data>(
        &mut self,
        table: char,
        _table_type: TableType,
        timestamp: SystemTime,
        key: &mut K,
        value: &mut V,
    ) {
        unimplemented!()
    }

    async fn delete_data_tuple<K: Key>(
        &mut self,
        table: char,
        _table_type: TableType,
        timestamp: SystemTime,
        key: &mut K,
    ) {
        unimplemented!()
    }

    async fn write_key_value<K: Key, V: Data>(&mut self, table: char, key: &mut K, value: &mut V) {
        unimplemented!()
    }

    async fn delete_key_value<K: Key>(&mut self, table: char, key: &mut K) {
        unimplemented!()
    }

    async fn get_global_key_values<K: Key, V: Data>(&self, table: char) -> Vec<(K, V)> {
        unimplemented!()
    }

    async fn get_key_values<K: Key, V: Data>(&self, table: char) -> Vec<(K, V)> {
        unimplemented!()
    }

    async fn load_compacted(&mut self, compaction: CompactionResult) {
        unimplemented!()
    }

    async fn files_for_table(
        &mut self,
        table: char,
        watermark: Option<SystemTime>,
    ) -> Vec<(grpc::BackendData, Bytes)> {
        let table_descriptor = self.tables.get(&table).unwrap();
        let Some(files) = self.judy_file_data.get_mut(&table) else {
            return vec![];
        };
        if table_descriptor.table_type() == TableType::Global {
            let values: Vec<_> = files.values_mut().last().unwrap().values_mut().collect();
        }
        let mut values = vec![];
        if table_descriptor.table_type() == TableType::Global {
            values.extend(
                files
                    .values_mut()
                    .last()
                    .unwrap()
                    .values_mut()
                    .map(|(a, b)| (a, b)),
            );
        } else {
            values = files
                .values_mut()
                .flatten()
                .filter_map(|(_hash, (metadata, bytes))| {
                    if table_descriptor.delete_behavior()
                        == TableDeleteBehavior::NoReadsBeforeWatermark
                    {
                        if let Some(watermark) = watermark {
                            if from_micros(metadata.max_timestamp_micros)
                                < watermark
                                    - Duration::from_micros(table_descriptor.retention_micros)
                            {
                                return None;
                            }
                        }
                    }
                    if metadata.max_routing_key < *self.task_info.key_range.start()
                        || *self.task_info.key_range.end() < metadata.min_routing_key
                    {
                        return None;
                    }
                    Some((metadata, bytes))
                })
                .collect();
        };
        let mut result = vec![];
        for (metadata, bytes) in values.into_iter() {
            result.push((
                grpc::BackendData {
                    backend_data: Some(grpc::backend_data::BackendData::ParquetStore(
                        metadata.clone(),
                    )),
                },
                bytes.bytes().await,
            ));
        }
        result
    }
}

impl JudyBackend {
    async fn write_data_triple<K: Key, V: Data>(
        &mut self,
        table: char,
        _table_type: TableType,
        timestamp: SystemTime,
        key: &mut K,
        value: &mut V,
    ) {
        let (key_hash, key_bytes, value_bytes) = {
            (
                hash_key(key),
                bincode::encode_to_vec(&*key, config::standard()).unwrap(),
                bincode::encode_to_vec(&*value, config::standard()).unwrap(),
            )
        };
        self.writer
            .write(table, key_hash, timestamp, key_bytes, value_bytes)
            .await;
    }

    async fn write_key_value<K: Key, V: Data>(&mut self, table: char, key: &mut K, value: &mut V) {
        self.write_data_triple(table, TableType::Global, SystemTime::UNIX_EPOCH, key, value)
            .await
    }

    async fn get_global_key_values<K: Key, V: Data>(&self, table: char) -> Vec<(K, V)> {
        let Some(files) = self.current_files.get(&table) else {
            return vec![];
        };
        let mut state_map = HashMap::new();
        for file in files.values().flatten() {
            let bytes = self
                .storage
                .get(&file.file)
                .await
                .unwrap_or_else(|_| panic!("unable to find file {} in checkpoint", file.file));
            for (_timestamp, key, value) in self
                .triples_from_judy_bytes(bytes.into(), &(0..=u64::MAX))
                .await
                .unwrap()
            {
                state_map.insert(key, value);
            }
        }
        state_map.into_iter().collect()
    }

    async fn get_key_values<K: Key, V: Data>(&self, table: char) -> Vec<(K, V)> {
        let Some(files) = self.current_files.get(&table) else {
            return vec![];
        };
        let mut state_map = HashMap::new();
        for file in files.values().flatten() {
            let bytes = self
                .storage
                .get(&file.file)
                .await
                .unwrap_or_else(|_| panic!("unable to find file {} in checkpoint", file.file));
            for (_timestamp, key, value) in self
                .triples_from_judy_bytes(bytes.into(), &self.task_info.key_range)
                .await
                .unwrap()
            {
                state_map.insert(key, value);
            }
        }
        state_map.into_iter().collect()
    }

    pub async fn get_last_value_for_key<K: Key, V: Data>(
        &mut self,
        table: char,
        key: &mut K,
        timestamp: SystemTime,
    ) -> Option<V> {
        let key_hash = hash_key(key);
        let Some(files) = self.judy_file_data.get_mut(&table) else {
            return None;
        };
        for file in files.values_mut().rev() {
            let Some((_hash, (file_data, bytes))) = file.range_mut(0..=key_hash).rev().next()
            else {
                continue;
            };
            if file_data.max_routing_key < key_hash {
                continue;
            }
            if file_data.max_timestamp_micros < to_micros(timestamp) {
                continue;
            }
            let key_bytes = bincode::encode_to_vec(key.clone(), config::standard()).unwrap();

            let result = JudyNode::get_data(
                &mut Cursor::new(bytes.bytes().await),
                &mut BytesWithOffset::new(bytes.bytes().await),
                &key_bytes,
            )
            .await
            .unwrap();
            if let Some(value_bytes) = result {
                let Some(value) = JudyNode::get_data(
                    &mut Cursor::new(&value_bytes),
                    &mut BytesWithOffset::new(value_bytes.clone()),
                    &bincode::encode_to_vec(&timestamp, config::standard()).unwrap(),
                )
                .await
                .unwrap() else {
                    continue;
                };
                return Some(
                    bincode::decode_from_slice(&value, config::standard())
                        .unwrap()
                        .0,
                );
            }
        }
        None
    }

    pub(crate) async fn get_epoch_ordered_values_at_time(
        &mut self,
        table: char,
        timestamp: SystemTime,
    ) -> Vec<Bytes> {
        let Some(files_by_epoch) = self.judy_file_data.get_mut(&table) else {
            return vec![];
        };
        let mut result = vec![];
        let key = bincode::encode_to_vec(&timestamp, config::standard()).unwrap();
        for (_hash, (metadata, bytes)) in files_by_epoch.values_mut().flatten() {
            if metadata.max_timestamp_micros < to_micros(timestamp) {
                continue;
            }
            let bytes = bytes.bytes().await;
            if let Some(bytes) = JudyNode::get_data(
                &mut Cursor::new(bytes.clone()),
                &mut BytesWithOffset::new(bytes),
                &key,
            )
            .await
            .unwrap()
            {
                result.push(bytes);
            }
        }
        result
    }

    pub(crate) async fn get_earliest_timestamp(
        &mut self,
        table: char,
        min_live_timestamp: SystemTime,
    ) -> Option<SystemTime> {
        let candidates: Vec<_> = self
            .judy_file_data
            .get_mut(&table)?
            .values_mut()
            .flat_map(|data| {
                data.values_mut().filter(|(metadata, _bytes)| {
                    metadata.max_timestamp_micros >= to_micros(min_live_timestamp)
                })
            })
            .collect();
        let mut min_time = None;
        for (metadata, bytes) in candidates {
            let timestamps: Vec<SystemTime> =
                JudyNode::get_keys(bytes.bytes().await).await.unwrap();
            let min_time_for_file = timestamps
                .into_iter()
                .filter(|timestamp| *timestamp >= min_live_timestamp)
                .min();
            match (min_time, min_time_for_file) {
                (None, Some(time)) => min_time = Some(time),
                (Some(time), Some(time_for_file)) => {
                    if time_for_file < time {
                        min_time = Some(time_for_file);
                    }
                }
                _ => {}
            }
        }
        min_time
    }

    async fn get_key_time_multi_map<K: Key + Sync, V: Data + Sync>(
        &mut self,
        table: char,
        current_watermark: Option<SystemTime>,
    ) -> &mut JudyKeyTimeMultiMap<K, V> {
        if !self.caches.contains_key(&table) {
            let files = self.judy_file_data.get_mut(&table);
            let new_cache: JudyKeyTimeMultiMap<K, V> = match files {
                None => JudyKeyTimeMultiMap::new(),
                Some(files) => {
                    JudyKeyTimeMultiMap::from_files(
                        self.tables.get(&table).unwrap().clone(),
                        &self.task_info,
                        current_watermark,
                        files,
                    )
                    .await
                }
            };
            self.caches.insert(table, Box::new(new_cache));
        }
        return self
            .caches
            .get_mut(&table)
            .unwrap()
            .downcast_mut::<JudyKeyTimeMultiMap<K, V>>()
            .unwrap();
    }
}

impl JudyBackend {
    async fn triples_from_judy_bytes<K: Key, V: Data>(
        &self,
        bytes: Bytes,
        range: &RangeInclusive<u64>,
    ) -> Result<Vec<(SystemTime, K, V)>> {
        Ok(JudyNode::get_btree_map_from_bytes(bytes)
            .await?
            .into_iter()
            .filter_map(|(key, value)| {
                let key: K = bincode::decode_from_slice(&key, BINCODE_CONFIG).unwrap().0;
                let key_hash = hash_key(&key);
                if !range.contains(&key_hash) {
                    None
                } else {
                    Some((key, value))
                }
            })
            .flat_map(|(key, values)| {
                let time_value_pairs: Vec<(SystemTime, Vec<u8>)> =
                    bincode::decode_from_slice(&values, BINCODE_CONFIG)
                        .unwrap()
                        .0;
                time_value_pairs
                    .into_iter()
                    .map(move |(time, value_bytes)| {
                        (
                            time,
                            key.clone(),
                            bincode::decode_from_slice(&value_bytes, config::standard())
                                .unwrap()
                                .0,
                        )
                    })
            })
            .collect())
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
}

struct JudyCheckpointWriter {
    sender: Sender<JudyQueueItem>,
    finish_rx: Option<oneshot::Receiver<()>>,
}

impl JudyCheckpointWriter {
    fn new(
        task_info: TaskInfo,
        control_tx: Sender<ControlResp>,
        tables: Vec<TableDescriptor>,
        storage: StorageProvider,
        current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1024 * 1024);
        let (finish_tx, finish_rx) = oneshot::channel();

        (JudyFlusher {
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
            current_files,
        })
        .start();

        JudyCheckpointWriter {
            sender: tx,
            finish_rx: Some(finish_rx),
        }
    }

    async fn write(
        &mut self,
        table: char,
        key_hash: u64,
        timestamp: SystemTime,
        key: Vec<u8>,
        data: Vec<u8>,
    ) {
        self.sender
            .send(JudyQueueItem::Write(JudyWrite {
                table,
                key_hash,
                timestamp,
                key,
                data,
            }))
            .await
            .unwrap();
    }

    async fn checkpoint(
        &mut self,
        epoch: u32,
        time: SystemTime,
        watermark: Option<SystemTime>,
        then_stop: bool,
    ) {
        self.sender
            .send(JudyQueueItem::Checkpoint(JudyCheckpoint {
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
enum JudyQueueItem {
    Write(JudyWrite),
    Checkpoint(JudyCheckpoint),
    OneShot(oneshot::Receiver<JudyMessage>),
}
#[derive(Debug)]
struct JudyMessage {
    table: char,
    bytes: Bytes,
    metadata: ParquetStoreData,
}

#[derive(Debug)]
struct JudyWrite {
    table: char,
    key_hash: u64,
    timestamp: SystemTime,
    key: Vec<u8>,
    data: Vec<u8>,
}

#[derive(Debug)]
struct JudyCheckpoint {
    epoch: u32,
    time: SystemTime,
    watermark: Option<SystemTime>,
    then_stop: bool,
}
struct RecordBatchBuilder {
    key_hash_builder: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
    start_time_array:
        arrow_array::builder::PrimitiveBuilder<arrow_array::types::TimestampMicrosecondType>,
    key_bytes: arrow_array::builder::BinaryBuilder,
    data_bytes: arrow_array::builder::BinaryBuilder,
    judy_stats: JudyStats,
}

struct JudyStats {
    max_timestamp: SystemTime,
    min_routing_key: u64,
    max_routing_key: u64,
}

impl Default for JudyStats {
    fn default() -> Self {
        Self {
            max_timestamp: SystemTime::UNIX_EPOCH,
            min_routing_key: u64::MAX,
            max_routing_key: u64::MIN,
        }
    }
}

impl RecordBatchBuilder {
    fn insert(&mut self, key_hash: u64, timestamp: SystemTime, key: Vec<u8>, data: Vec<u8>) {
        self.judy_stats.min_routing_key = self.judy_stats.min_routing_key.min(key_hash);
        self.judy_stats.max_routing_key = self.judy_stats.max_routing_key.max(key_hash);

        self.key_hash_builder.append_value(key_hash);
        self.start_time_array
            .append_value(to_micros(timestamp) as i64);
        self.key_bytes.append_value(key);
        self.data_bytes.append_value(data);
        self.judy_stats.max_timestamp = self.judy_stats.max_timestamp.max(timestamp);
    }

    fn flush(mut self) -> Option<(arrow_array::RecordBatch, JudyStats)> {
        let key_hash_array: arrow_array::PrimitiveArray<arrow_array::types::UInt64Type> =
            self.key_hash_builder.finish();
        if key_hash_array.is_empty() {
            return None;
        }
        let start_time_array: arrow_array::PrimitiveArray<
            arrow_array::types::TimestampMicrosecondType,
        > = self.start_time_array.finish();
        let key_array: arrow_array::BinaryArray = self.key_bytes.finish();
        let data_array: arrow_array::BinaryArray = self.data_bytes.finish();
        Some((
            arrow_array::RecordBatch::try_new(
                self.schema(),
                vec![
                    std::sync::Arc::new(key_hash_array),
                    std::sync::Arc::new(start_time_array),
                    std::sync::Arc::new(key_array),
                    std::sync::Arc::new(data_array),
                ],
            )
            .unwrap(),
            self.judy_stats,
        ))
    }

    fn schema(&self) -> std::sync::Arc<arrow_schema::Schema> {
        std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("key_hash", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new(
                "start_time",
                arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                false,
            ),
            arrow::datatypes::Field::new("key_bytes", arrow::datatypes::DataType::Binary, false),
            arrow::datatypes::Field::new(
                "aggregate_bytes",
                arrow::datatypes::DataType::Binary,
                false,
            ),
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
                arrow_array::types::TimestampMicrosecondType,
            >::with_capacity(1024),
            key_bytes: arrow_array::builder::BinaryBuilder::default(),
            data_bytes: arrow_array::builder::BinaryBuilder::default(),
            judy_stats: JudyStats::default(),
        }
    }
}

struct JudyFlusher {
    queue: Receiver<JudyQueueItem>,
    storage: StorageProvider,
    control_tx: Sender<ControlResp>,
    finish_tx: Option<oneshot::Sender<()>>,
    task_info: TaskInfo,
    table_descriptors: HashMap<char, TableDescriptor>,
    builders: HashMap<char, JudyBuilder>,
    current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>>,
}

#[derive(Default)]
struct JudyBuilder {
    data: BTreeMap<Vec<u8>, Vec<(SystemTime, Vec<u8>)>>,
    stats: JudyStats,
}

impl JudyBuilder {
    fn insert(&mut self, key_hash: u64, timestamp: SystemTime, key: Vec<u8>, data: Vec<u8>) {
        self.stats.min_routing_key = self.stats.min_routing_key.min(key_hash);
        self.stats.max_routing_key = self.stats.max_routing_key.max(key_hash);
        self.stats.max_timestamp = self.stats.max_timestamp.max(timestamp);
        self.data.entry(key).or_default().push((timestamp, data));
    }
}

impl JudyFlusher {
    fn start(mut self) {
        tokio::spawn(async move {
            loop {
                if !self.flush_iteration().await.unwrap() {
                    return;
                }
            }
        });
    }

    async fn upload_judy_bytes(&self, key: &str, judy_bytes: Bytes) -> Result<usize> {
        let bytes = judy_bytes.len();
        self.storage.put(key, judy_bytes).await?;
        Ok(bytes)
    }

    async fn upload_judy_file(
        &self,
        key: &str,
        judy_data: BTreeMap<Vec<u8>, Vec<(SystemTime, Vec<u8>)>>,
    ) -> Result<usize> {
        let mut cursor = Vec::new();
        let mut writer = JudyWriter::new();
        for (key, values) in judy_data {
            writer
                .insert(&key, &bincode::encode_to_vec(values, config::standard())?)
                .await?;
        }
        writer.serialize(&mut cursor).await?;
        let bytes = cursor.len();
        self.storage.put(key, cursor.into()).await?;
        Ok(bytes)
    }

    async fn flush_iteration(&mut self) -> Result<bool> {
        let mut checkpoint_epoch = None;
        let mut futures = FuturesUnordered::new();

        let mut judy_files: HashMap<char, Vec<(ParquetStoreData, Bytes)>> = HashMap::new();

        while checkpoint_epoch.is_none() {
            tokio::select! {
                op = self.queue.recv() => {
                    match op {
                        Some(JudyQueueItem::Write( JudyWrite{table, key_hash, timestamp, key, data})) => {
                            self.builders.entry(table).or_default().insert(key_hash, timestamp, key, data);
                            //self.builders.entry(table).or_default().insert(key_hash, timestamp, key, data);
                        },
                        Some(JudyQueueItem::OneShot(receiver)) => {
                            futures.push(receiver);
                        },
                        Some(JudyQueueItem::Checkpoint(epoch)) => {
                            checkpoint_epoch = Some(epoch);
                        },
                        None => {
                            debug!("Judy flusher closed");
                            return Ok(false);
                        },
                }
            },
                Some(result) = futures.next() => {
                    let JudyMessage{table, bytes, metadata} = result?;
                    judy_files.entry(table).or_default().push((metadata, bytes));
                }
            }
        }
        // drain futures
        while let Some(result) = futures.next().await {
            let JudyMessage {
                table,
                bytes,
                metadata,
            } = result?;
            judy_files.entry(table).or_default().push((metadata, bytes));
        }

        let mut backend_data = vec![];

        if let Some(cp) = checkpoint_epoch {
            let mut bytes = 0;
            let mut to_write = vec![];
            for (table, builder) in self.builders.drain() {
                let s3_key = table_checkpoint_path(&self.task_info, table, cp.epoch);
                to_write.push((builder.data, s3_key, table, builder.stats));
            }

            for (data, s3_key, table, stats) in to_write {
                bytes += self.upload_judy_file(&s3_key, data).await?;
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
                        max_timestamp_micros: to_micros(stats.max_timestamp),
                        min_required_timestamp_micros: None,
                        generation: 0,
                    });
            }
            for (table, files) in judy_files.drain() {
                for (index, (metadata, judy_bytes)) in files.into_iter().enumerate() {
                    let s3_key = format!(
                        "{}-{}",
                        table_checkpoint_path(&self.task_info, table, cp.epoch),
                        index
                    );
                    bytes += self.upload_judy_bytes(&s3_key, judy_bytes).await?;
                    self.current_files
                        .entry(table)
                        .or_default()
                        .entry(cp.epoch)
                        .or_default()
                        .push(metadata);
                }
            }

            let mut new_file_map: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>> =
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
                                // this file is not needed by the new checkpoint.
                                if file.max_timestamp_micros
                                    < to_micros(checkpoint_watermark)
                                        - table_descriptor.retention_micros
                                {
                                    continue;
                                }
                            }
                        }
                        backend_data.push(arroyo_rpc::grpc::BackendData {
                            backend_data: Some(BackendData::ParquetStore(file.clone())),
                        });
                        new_file_map
                            .entry(table)
                            .or_default()
                            .entry(cp.epoch)
                            .or_default()
                            .push(file);
                    }
                }
            }

            self.current_files = new_file_map;

            // write checkpoint metadata
            let subtask_metadata = SubtaskCheckpointMetadata {
                subtask_index: self.task_info.task_index as u32,
                start_time: to_micros(cp.time),
                finish_time: to_micros(SystemTime::now()),
                has_state: !backend_data.is_empty(),
                tables: self.table_descriptors.values().cloned().collect(),
                watermark: cp.watermark.map(to_micros),
                backend_data,
                bytes: bytes as u64,
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
