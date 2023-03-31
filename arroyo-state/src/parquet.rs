use crate::{hash_key, BackingStore, BINCODE_CONFIG};
use anyhow::Result;
use arrow_array::RecordBatch;
use arroyo_rpc::grpc::backend_data::BackendData;
use arroyo_rpc::grpc::{
    backend_data, CheckpointMetadata, OperatorCheckpointMetadata, ParquetStoreData,
    SubtaskCheckpointMetadata, TableDeleteBehavior, TableDescriptor, TableType,
};
use arroyo_rpc::{CheckpointCompleted, ControlResp};
use arroyo_types::{from_micros, to_micros, CheckpointBarrier, Data, Key, TaskInfo};
use bincode::config;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use prost::Message;
use rusoto_core::{ByteStream, Region, RusotoError};
use rusoto_s3::{
    DeleteObjectRequest, GetObjectError, GetObjectRequest, PutObjectRequest, S3Client, S3,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::ErrorKind;
use std::ops::RangeInclusive;
use std::path::Path;
use std::str::FromStr;
use std::time::SystemTime;
use tokio::fs::{remove_file, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tracing::log::warn;
use tracing::{debug, info};

pub const S3_STORAGE_ENGINE_ENV: &str = "S3_BUCKET";
pub const S3_REGION_ENV: &str = "S3_REGION";

pub struct ParquetBackend {
    epoch: u32,
    min_epoch: u32,
    // ordered by table, then epoch.
    current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>>,
    writer: ParquetWriter,
    task_info: TaskInfo,
    tables: HashMap<char, TableDescriptor>,
    storage_client: StorageClient,
}

fn table_checkpoint_path(task_info: &TaskInfo, table: char, epoch: u32) -> String {
    format!(
        "job-id-{}/checkpoint-{:0>7}/data/operator-id-{}/table-{}/subtask-{}",
        task_info.job_id, epoch, task_info.operator_id, table, task_info.task_index,
    )
}

fn operator_metadata_path(job_id: &str, operator_id: &str, epoch: u32) -> String {
    format!(
        "job-id-{}/checkpoint-{:0>7}/metadata/operators/operator-id-{}",
        job_id, epoch, operator_id
    )
}

fn checkpoint_metadata_path(job_id: &str, epoch: u32) -> String {
    format!(
        "job-id-{}/checkpoint-{:0>7}/metadata/checkpoint-overall",
        job_id, epoch
    )
}

#[async_trait::async_trait]
impl BackingStore for ParquetBackend {
    fn name() -> &'static str {
        "parquet"
    }

    async fn load_latest_checkpoint_metadata(_job_id: &str) -> Option<CheckpointMetadata> {
        todo!()
    }

    // TODO: should this be a Result, rather than an option?
    async fn load_checkpoint_metadata(job_id: &str, epoch: u32) -> Option<CheckpointMetadata> {
        let storage_client = StorageClient::new().ok()?;
        let data = storage_client
            .get_bytes(checkpoint_metadata_path(job_id, epoch))
            .await?;
        let metadata = CheckpointMetadata::decode(&data[..]).unwrap();
        Some(metadata)
    }

    async fn load_operator_metadata(
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> OperatorCheckpointMetadata {
        let storage_client = StorageClient::new().unwrap();
        let data = storage_client
            .get_bytes(operator_metadata_path(job_id, operator_id, epoch))
            .await
            .unwrap();
        OperatorCheckpointMetadata::decode(&data[..]).unwrap()
    }

    async fn complete_operator_checkpoint(metadata: OperatorCheckpointMetadata) {
        let storage_client = StorageClient::new().unwrap();
        let path = operator_metadata_path(&metadata.job_id, &metadata.operator_id, metadata.epoch);
        storage_client.write(path, metadata.encode_to_vec()).await;
    }

    async fn complete_checkpoint(metadata: CheckpointMetadata) {
        debug!("writing checkpoint {:?}", metadata);
        let storage_client = StorageClient::new().unwrap();
        let path = checkpoint_metadata_path(&metadata.job_id, metadata.epoch);
        storage_client.write(path, metadata.encode_to_vec()).await;
    }

    async fn new(
        task_info: &TaskInfo,
        tables: Vec<TableDescriptor>,
        tx: Sender<ControlResp>,
    ) -> Self {
        Self {
            epoch: 1,
            min_epoch: 1,
            current_files: HashMap::new(),
            writer: ParquetWriter::new(
                task_info.clone(),
                tx,
                tables.clone(),
                StorageClient::new().unwrap(),
            ),
            task_info: task_info.clone(),
            tables: tables
                .into_iter()
                .map(|table| (table.name.clone().chars().next().unwrap(), table))
                .collect(),
            storage_client: StorageClient::new().unwrap(),
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
                .await;
        let mut current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>> = HashMap::new();
        let tables: HashMap<char, TableDescriptor> = tables
            .into_iter()
            .map(|table| (table.name.clone().chars().next().unwrap(), table))
            .collect();
        for backend_data in operator_metadata.backend_data {
            let Some(backend_data::BackendData::ParquetStore(parquet_data)) = backend_data.backend_data else {
                panic!("expect parquet data")
            };
            let table_descriptor = tables
                .get(&parquet_data.table.chars().next().unwrap())
                .unwrap();
            if table_descriptor.table_type() != TableType::Global {
                // check if the file has relevant data.
                if parquet_data.max_routing_key < *task_info.key_range.start()
                    || *task_info.key_range.end() < parquet_data.min_routing_key
                {
                    continue;
                }
                // don't process files older than min_epoch
            } else if parquet_data.epoch < metadata.min_epoch {
                continue;
            }
            if table_descriptor.delete_behavior() == TableDeleteBehavior::NoReadsBeforeWatermark {
                if let Some(watermark_micros) = operator_metadata.min_watermark {
                    if parquet_data.max_timestamp_micros
                        < watermark_micros - table_descriptor.retention_micros
                    {
                        continue;
                    }
                }
            }

            let files = current_files
                .entry(parquet_data.table.chars().next().unwrap())
                .or_default()
                .entry(parquet_data.epoch)
                .or_default();
            files.push(parquet_data);
        }

        Self {
            epoch: metadata.epoch + 1,
            min_epoch: metadata.min_epoch,
            current_files,
            writer: ParquetWriter::new(
                task_info.clone(),
                control_tx,
                tables.values().cloned().collect(),
                StorageClient::new().unwrap(),
            ),
            task_info: task_info.clone(),
            tables,
            storage_client: StorageClient::new().unwrap(),
        }
    }

    async fn prepare_checkpoint(_metadata: &CheckpointMetadata) -> anyhow::Result<()> {
        Ok(())
    }

    async fn compact_checkpoint(
        mut metadata: CheckpointMetadata,
        old_min_epoch: u32,
        min_epoch: u32,
    ) -> Result<()> {
        info!(message = "Compacting", min_epoch, job_id = metadata.job_id);

        let mut futures: FuturesUnordered<_> = metadata
            .operator_ids
            .iter()
            .map(|operator| {
                Self::compact_operator(
                    metadata.job_id.clone(),
                    operator.clone(),
                    metadata.epoch,
                    min_epoch,
                )
            })
            .collect();

        let storage_client = StorageClient::new()?;

        // wait for all of the futures to complete
        loop {
            match futures.next().await {
                Some(result) => {
                    let operator_id = result?;
                    for deletepoch in old_min_epoch..min_epoch {
                        let path =
                            operator_metadata_path(&metadata.job_id, &operator_id, deletepoch);
                        debug!("deleting {}", path);
                        storage_client.remove(path).await;
                    }
                    debug!(
                        message = "Finished compacting operator",
                        job_id = metadata.job_id,
                        operator_id,
                        min_epoch
                    );
                }
                None => {
                    break;
                }
            }
        }
        for deletepoch in old_min_epoch..min_epoch {
            StorageClient::new()?
                .remove(checkpoint_metadata_path(&metadata.job_id, deletepoch))
                .await;
        }
        metadata.min_epoch = min_epoch;
        Self::complete_checkpoint(metadata).await;
        Ok(())
    }

    async fn checkpoint(
        &mut self,
        barrier: CheckpointBarrier,
        watermark: Option<SystemTime>,
    ) -> u32 {
        assert_eq!(barrier.epoch, self.epoch);
        self.writer
            .checkpoint(
                self.epoch,
                barrier.min_epoch,
                barrier.timestamp,
                watermark,
                barrier.then_stop,
            )
            .await;
        self.epoch += 1;
        self.min_epoch = barrier.min_epoch;
        self.epoch - 1
    }

    async fn get_data_triples<K: Key, V: Data>(&self, table: char) -> Vec<(SystemTime, K, V)> {
        let mut result = vec![];
        match self.tables.get(&table).unwrap().table_type() {
            TableType::Global => todo!(),
            TableType::TimeKeyMap | TableType::KeyTimeMultiMap => {
                let Some(files) = self.current_files.get(&table) else {
                    return vec![];
                };
                for file in files.values().flatten() {
                    let bytes = self
                        .storage_client
                        .get_bytes(file.file.clone())
                        .await
                        .expect(&format!("unable to find file {} in checkpoint", file.file));
                    result.append(
                        &mut self.triples_from_parquet_bytes(bytes, &self.task_info.key_range),
                    );
                }
            }
        }
        result
    }

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

    async fn get_key_values<K: Key, V: Data>(&self, table: char) -> Vec<(K, V)> {
        let Some(files) = self.current_files.get(&table) else {
            return vec![];
        };
        let mut state_map = HashMap::new();
        for file in files.values().flatten() {
            let bytes = self
                .storage_client
                .get_bytes(file.file.clone())
                .await
                .expect(&format!("unable to find file {} in checkpoint", file.file));
            for (_timestamp, key, value) in self.triples_from_parquet_bytes(bytes, &(0..=u64::MAX))
            {
                state_map.insert(key, value);
            }
        }
        state_map.into_iter().collect()
    }
}

impl ParquetBackend {
    async fn compact_operator(
        job_id: String,
        operator: String,
        epoch: u32,
        min_epoch: u32,
    ) -> anyhow::Result<String> {
        let operator_metadata = Self::load_operator_metadata(&job_id, &operator, epoch).await;
        let global_tables: HashSet<_> = operator_metadata
            .tables
            .iter()
            .filter_map(|descriptor| {
                if descriptor.table_type() == TableType::Global {
                    Some(descriptor.name.clone())
                } else {
                    None
                }
            })
            .collect();
        let storage_client = StorageClient::new()?;
        for backend_data in operator_metadata.backend_data {
            let BackendData::ParquetStore(parquet_data) = backend_data.backend_data.unwrap();
            if global_tables.contains(&parquet_data.table) && parquet_data.epoch < min_epoch {
                storage_client.remove(parquet_data.file).await;
            }
            storage_client
                .remove(operator_metadata_path(
                    &job_id,
                    &operator,
                    parquet_data.epoch,
                ))
                .await;
        }
        Ok(operator)
    }
    fn triples_from_parquet_bytes<K: Key, V: Data>(
        &self,
        bytes: Vec<u8>,
        range: &RangeInclusive<u64>,
    ) -> Vec<(SystemTime, K, V)> {
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
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
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
            for index in 0..num_rows {
                if !range.contains(&key_hash_array.value(index)) {
                    continue;
                }

                //TODO: filter timestamp
                let timestamp = from_micros(time_array.value(index) as u64);

                let key: K = bincode::decode_from_slice(&key_array.value(index), BINCODE_CONFIG)
                    .unwrap()
                    .0;
                let value: V =
                    bincode::decode_from_slice(&value_array.value(index), BINCODE_CONFIG)
                        .unwrap()
                        .0;
                result.push((timestamp, key, value));
            }
        }
        result
    }
}

struct ParquetWriter {
    sender: Sender<ParquetQueueItem>,
    finish_rx: Option<oneshot::Receiver<()>>,
}

impl ParquetWriter {
    fn new(
        task_info: TaskInfo,
        control_tx: Sender<ControlResp>,
        tables: Vec<TableDescriptor>,
        storage_client: StorageClient,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1024 * 1024);
        let (finish_tx, finish_rx) = oneshot::channel();

        (ParquetFlusher {
            queue: rx,
            storage_client,
            control_tx,
            finish_tx: Some(finish_tx),
            task_info: task_info,
            table_descriptors: tables
                .iter()
                .map(|table| (table.name.chars().next().unwrap(), table.clone()))
                .collect(),
            builders: HashMap::new(),
            last_checkpoint_watermark: None,
            current_files: HashMap::new(),
        })
        .start();

        ParquetWriter {
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
            .send(ParquetQueueItem::Write(ParquetWrite {
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
        min_epoch: u32,
        time: SystemTime,
        watermark: Option<SystemTime>,
        then_stop: bool,
    ) {
        self.sender
            .send(ParquetQueueItem::Checkpoint(ParquetCheckpoint {
                epoch,
                min_epoch,
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
}

#[derive(Debug)]
struct ParquetWrite {
    table: char,
    key_hash: u64,
    timestamp: SystemTime,
    key: Vec<u8>,
    data: Vec<u8>,
}

#[derive(Debug)]
struct ParquetCheckpoint {
    epoch: u32,
    min_epoch: u32,
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
    parquet_stats: ParquetStats,
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
    fn insert(&mut self, key_hash: u64, timestamp: SystemTime, key: Vec<u8>, data: Vec<u8>) {
        self.parquet_stats.min_routing_key = self.parquet_stats.min_routing_key.min(key_hash);
        self.parquet_stats.max_routing_key = self.parquet_stats.max_routing_key.max(key_hash);

        self.key_hash_builder.append_value(key_hash);
        self.start_time_array
            .append_value(to_micros(timestamp) as i64);
        self.key_bytes.append_value(key);
        self.data_bytes.append_value(data);
        self.parquet_stats.max_timestamp = self.parquet_stats.max_timestamp.max(timestamp);
    }

    fn flush(mut self) -> Option<(arrow_array::RecordBatch, ParquetStats)> {
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
            self.parquet_stats,
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
            parquet_stats: ParquetStats::default(),
        }
    }
}

struct ParquetFlusher {
    queue: Receiver<ParquetQueueItem>,
    storage_client: StorageClient,
    control_tx: Sender<ControlResp>,
    finish_tx: Option<oneshot::Sender<()>>,
    task_info: TaskInfo,
    table_descriptors: HashMap<char, TableDescriptor>,
    builders: HashMap<char, RecordBatchBuilder>,
    last_checkpoint_watermark: Option<SystemTime>,
    current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>>,
}

#[derive(Eq, PartialEq, Clone, Debug)]
pub enum StorageClientConfig {
    LocalDirectory(String),
    S3 { region: String, bucket: String },
}

impl StorageClientConfig {
    pub fn bucket_var(&self) -> String {
        match self {
            StorageClientConfig::LocalDirectory(_) => "local".to_owned(),
            StorageClientConfig::S3 { region: _, bucket } => bucket.clone(),
        }
    }
    pub fn region_var(&self) -> String {
        match self {
            StorageClientConfig::LocalDirectory(_) => "local".to_owned(),
            StorageClientConfig::S3 { region, bucket: _ } => region.clone(),
        }
    }
}

impl Default for StorageClientConfig {
    fn default() -> Self {
        let bucket = std::env::var(S3_STORAGE_ENGINE_ENV).unwrap();
        match bucket.as_str() {
            "local" => Self::LocalDirectory("/tmp/arroyo-data".to_owned()),
            bucket => {
                let region = std::env::var(S3_REGION_ENV).unwrap();
                Region::from_str(&region).unwrap();
                Self::S3 {
                    region,
                    bucket: bucket.to_string(),
                }
            }
        }
    }
}

#[derive(Clone)]
enum StorageClient {
    LocalDirectory(String),
    S3 { client: S3Client, bucket: String },
}

// TODO: Better way to configure this.
impl StorageClient {
    fn new() -> Result<Self> {
        let bucket = std::env::var(S3_STORAGE_ENGINE_ENV);
        match bucket {
            Ok(bucket) => match bucket.as_str() {
                "local" => Ok(Self::LocalDirectory("/tmp/arroyo-data".to_owned())),
                bucket => {
                    let region = std::env::var(S3_REGION_ENV)?;
                    let region = Region::from_str(&region)?;
                    let client = S3Client::new(region);
                    Ok(Self::S3 {
                        client,
                        bucket: bucket.to_owned(),
                    })
                }
            },
            Err(_) => Ok(Self::LocalDirectory("/tmp/arroyo-data".to_owned())),
        }
    }

    async fn write(&self, key: String, parquet_bytes: Vec<u8>) {
        match self {
            StorageClient::LocalDirectory(directory) => {
                let file_path = Path::new(directory).join(Path::new(&key));

                if let Some(parent) = file_path.parent() {
                    tokio::fs::create_dir_all(parent).await.unwrap();
                }
                let mut file = File::create(file_path).await.unwrap();
                file.write_all(&parquet_bytes).await.unwrap();
            }
            StorageClient::S3 { client, bucket } => {
                let request = PutObjectRequest {
                    bucket: bucket.into(),
                    key,
                    body: Some(parquet_bytes.into()),
                    ..Default::default()
                };
                client.put_object(request).await.unwrap();
            }
        }
    }

    async fn remove(&self, key: String) {
        match self {
            StorageClient::LocalDirectory(directory) => {
                let file_path = Path::new(directory).join(Path::new(&key));
                if let Err(e) = remove_file(&file_path).await {
                    if e.kind() != ErrorKind::NotFound {
                        panic!("Error deleting file: {:?}", e);
                    }
                }
            }
            StorageClient::S3 { client, bucket } => {
                let delete_object_request = DeleteObjectRequest {
                    bucket: bucket.into(),
                    key,
                    ..Default::default()
                };
                client.delete_object(delete_object_request).await.unwrap();
            }
        }
    }

    async fn get_bytes(&self, key: String) -> Option<Vec<u8>> {
        match self {
            StorageClient::LocalDirectory(local_directory) => {
                let file_path = Path::new(local_directory).join(Path::new(&key));
                match tokio::fs::read(file_path).await {
                    Ok(bytes) => Some(bytes),
                    Err(e) => match e.kind() {
                        ErrorKind::NotFound => None,
                        _ => panic!("An unexpected error occurred: {}", e),
                    },
                }
            }
            StorageClient::S3 { client, bucket } => {
                let request = GetObjectRequest {
                    bucket: bucket.into(),
                    key: key.clone(),
                    ..Default::default()
                };
                let response = match client.get_object(request).await {
                    Ok(response) => response,
                    Err(RusotoError::Service(GetObjectError::NoSuchKey(_message))) => {
                        warn!("could not find state for {:?}, skipping", key);
                        return None;
                    }
                    Err(err) => panic!("errored on {:?}", err),
                };

                let data: ByteStream = response.body.unwrap();
                let mut buffer = Vec::new();
                data.into_async_read()
                    .read_to_end(&mut buffer)
                    .await
                    .unwrap();
                Some(buffer)
            }
        }
    }
}

impl From<StorageClientConfig> for StorageClient {
    fn from(value: StorageClientConfig) -> Self {
        match value {
            StorageClientConfig::LocalDirectory(directory) => Self::LocalDirectory(directory),
            StorageClientConfig::S3 { region, bucket } => {
                let region = Region::from_str(&region).unwrap();
                let client = S3Client::new(region);
                Self::S3 { client, bucket }
            }
        }
    }
}

impl ParquetFlusher {
    fn start(mut self) {
        tokio::spawn(async move {
            loop {
                if !self.flush_iteration().await {
                    return;
                }
            }
        });
    }
    async fn upload_record_batch(
        &self,
        key: String,
        record_batch: arrow_array::RecordBatch,
    ) -> usize {
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();
        let cursor = Vec::new();
        let mut writer = ArrowWriter::try_new(cursor, record_batch.schema(), Some(props)).unwrap();
        writer.write(&record_batch).expect("Writing batch");
        writer.flush().unwrap();
        let parquet_bytes = writer.into_inner().unwrap();
        let bytes = parquet_bytes.len();
        self.storage_client.write(key, parquet_bytes).await;
        bytes
    }

    async fn flush_iteration(&mut self) -> bool {
        let mut checkpoint_epoch = None;

        while checkpoint_epoch.is_none() {
            tokio::select! {
                op = self.queue.recv() => {
                    match op {
                        Some(ParquetQueueItem::Write( ParquetWrite{table, key_hash, timestamp, key, data})) => {
                            self.builders.entry(table).or_default().insert(key_hash, timestamp, key, data);
                        }
                        Some(ParquetQueueItem::Checkpoint(epoch)) => {
                            checkpoint_epoch = Some(epoch);
                        }
                        None => {
                            debug!("Parquet flusher closed");
                            return false;
                        }
                    }
                }
            }
        }

        let mut backend_data = vec![];

        if let Some(cp) = checkpoint_epoch {
            let mut bytes = 0;
            let mut to_write = vec![];
            for (table, builder) in self.builders.drain() {
                let Some((record_batch, stats)) = builder.flush() else {
                    continue;
                };
                let s3_key = table_checkpoint_path(&self.task_info, table, cp.epoch);
                to_write.push((record_batch, s3_key, table, stats));
            }

            for (record_batch, s3_key, table, stats) in to_write {
                bytes += self.upload_record_batch(s3_key.clone(), record_batch).await;
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
                    });
            }
            let mut files_to_delete = vec![];
            let mut new_file_map: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>> =
                HashMap::new();
            for (table, epoch_files) in self.current_files.drain() {
                let table_descriptor = self.table_descriptors.get(&table).unwrap();
                for (epoch, files) in epoch_files {
                    if table_descriptor.table_type() == TableType::Global && epoch < cp.min_epoch {
                        for file in files {
                            files_to_delete.push(file.file);
                        }
                        continue;
                    }
                    for file in files {
                        if table_descriptor.delete_behavior()
                            == TableDeleteBehavior::NoReadsBeforeWatermark
                        {
                            if let Some(last_checkpoint_watermark) = self.last_checkpoint_watermark
                            {
                                // this file wasn't in the prior checkpoint, fine to delete it.
                                if file.max_timestamp_micros
                                    < to_micros(last_checkpoint_watermark)
                                        - table_descriptor.retention_micros
                                {
                                    files_to_delete.push(file.file);
                                    continue;
                                }
                            }
                            if let Some(checkpoint_watermark) = cp.watermark {
                                // this file is not needed by the new checkpoint. Don't delete it as we that checkpoint hasn't finished.
                                if file.max_timestamp_micros
                                    < to_micros(checkpoint_watermark)
                                        - table_descriptor.retention_micros
                                {
                                    new_file_map
                                        .entry(table)
                                        .or_default()
                                        .entry(cp.epoch)
                                        .or_default()
                                        .push(file);
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
                has_state: bytes > 0,
                tables: self.table_descriptors.values().cloned().collect(),
                watermark: cp.watermark.map(to_micros),
                backend_data,
                bytes: bytes as u64,
            };
            for file in files_to_delete {
                self.storage_client.remove(file).await;
            }
            self.control_tx
                .send(ControlResp::CheckpointCompleted(CheckpointCompleted {
                    checkpoint_epoch: cp.epoch,
                    operator_id: self.task_info.operator_id.clone(),
                    subtask_metadata,
                }))
                .await
                .unwrap();
            self.last_checkpoint_watermark = cp.watermark;
            if cp.then_stop {
                self.finish_tx.take().unwrap().send(()).unwrap();
                return false;
            }
        }
        true
    }
}
