use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{bail, Result};
use arroyo_rpc::OperatorConfig;
use arroyo_storage::StorageProvider;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use chrono::{DateTime, Utc};
use futures::{stream::FuturesUnordered, Future};
use futures::{stream::StreamExt, TryStreamExt};
use object_store::{multipart::PartId, path::Path, MultipartId};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::warn;
use typify::import_types;
use uuid::Uuid;

import_types!(schema = "../connector-schemas/filesystem/table.json");

use arroyo_types::*;
mod delta;
pub mod json;
pub mod local;
pub mod parquet;
pub mod single_file;

use crate::SchemaData;

use self::{
    json::{JsonLocalWriter, JsonWriter, PassThrough},
    local::{LocalFileSystemWriter, LocalWriter},
    parquet::{FixedSizeRecordBatchBuilder, ParquetLocalWriter, RecordBatchBufferingWriter},
};

use super::two_phase_committer::{CommitStrategy, TwoPhaseCommitter, TwoPhaseCommitterOperator};

pub struct FileSystemSink<
    K: Key,
    T: Data + Sync,
    R: MultiPartWriter<InputType = T> + Send + 'static,
> {
    sender: Sender<FileSystemMessages<T>>,
    partitioner: Option<Box<dyn Fn(&Record<K, T>) -> String + Send>>,
    checkpoint_receiver: Receiver<CheckpointData<T>>,
    commit_strategy: CommitStrategy,
    _ts: PhantomData<(K, R)>,
}

pub type ParquetFileSystemSink<K, T, R> = FileSystemSink<
    K,
    T,
    BatchMultipartWriter<FixedSizeRecordBatchBuilder<R>, RecordBatchBufferingWriter<R>>,
>;

pub type JsonFileSystemSink<K, T> =
    FileSystemSink<K, T, BatchMultipartWriter<PassThrough<T>, JsonWriter<T>>>;

pub type LocalParquetFileSystemSink<K, T, R> = LocalFileSystemWriter<K, T, ParquetLocalWriter<R>>;

pub type LocalJsonFileSystemSink<K, T> = LocalFileSystemWriter<K, T, JsonLocalWriter>;

impl<K: Key, T: Data + Sync + SchemaData + Serialize, V: LocalWriter<T>>
    LocalFileSystemWriter<K, T, V>
{
    pub fn from_config(config_str: &str) -> TwoPhaseCommitterOperator<K, T, Self> {
        let config: OperatorConfig =
            serde_json::from_str(config_str).expect("Invalid config for FileSystemSink");
        let table: FileSystemTable =
            serde_json::from_value(config.table).expect("Invalid table config for FileSystemSink");
        let final_dir = table.write_target.path.clone();
        let writer = LocalFileSystemWriter::new(final_dir, table);
        TwoPhaseCommitterOperator::new(writer)
    }
}

impl<
        K: Key,
        T: Data + Sync + SchemaData + Serialize,
        R: MultiPartWriter<InputType = T> + Send + 'static,
    > FileSystemSink<K, T, R>
{
    pub fn create_and_start(table: FileSystemTable) -> TwoPhaseCommitterOperator<K, T, Self> {
        let (sender, receiver) = tokio::sync::mpsc::channel(10000);
        let (checkpoint_sender, checkpoint_receiver) = tokio::sync::mpsc::channel(10000);
        let partition_func = get_partitioner_from_table(&table);
        let commit_strategy = match table.file_settings.as_ref().unwrap().commit_style.unwrap() {
            CommitStyle::Direct => CommitStrategy::PerSubtask,
            CommitStyle::DeltaLake => CommitStrategy::PerOperator,
        };
        tokio::spawn(async move {
            let path: Path = StorageProvider::get_key(&table.write_target.path)
                .unwrap()
                .into();
            let provider = StorageProvider::for_url_with_options(
                &table.write_target.path,
                table.write_target.storage_options.clone(),
            )
            .await
            .unwrap();
            let mut writer = AsyncMultipartFileSystemWriter::<T, R>::new(
                path,
                Arc::new(provider),
                receiver,
                checkpoint_sender,
                table.clone(),
            );
            writer.run().await.unwrap();
        });

        TwoPhaseCommitterOperator::new(Self {
            sender,
            checkpoint_receiver,
            commit_strategy,
            partitioner: partition_func,
            _ts: PhantomData,
        })
    }

    pub fn from_config(config_str: &str) -> TwoPhaseCommitterOperator<K, T, Self> {
        let config: OperatorConfig =
            serde_json::from_str(config_str).expect("Invalid config for FileSystemSink");
        let table: FileSystemTable =
            serde_json::from_value(config.table).expect("Invalid table config for FileSystemSink");
        Self::create_and_start(table)
    }
}

fn get_partitioner_from_table<K: Key, T: Data + Serialize>(
    table: &FileSystemTable,
) -> Option<Box<dyn Fn(&Record<K, T>) -> String + Send>> {
    let Some(partitions) = table.file_settings.as_ref().unwrap().partitioning.clone() else {
        return None;
    };
    match (
        partitions.time_partition_pattern,
        partitions.partition_fields.is_empty(),
    ) {
        (None, false) => Some(Box::new(move |record: &Record<K, T>| {
            partition_string_for_fields(&record.value, &partitions.partition_fields).unwrap()
        })),
        (None, true) => None,
        (Some(pattern), false) => Some(Box::new(move |record: &Record<K, T>| {
            let time_partition = formatted_time_from_timestamp(record.timestamp, &pattern);
            let field_partition =
                partition_string_for_fields(&record.value, &partitions.partition_fields).unwrap();
            format!("{}/{}", time_partition, field_partition)
        })),
        (Some(pattern), true) => Some(Box::new(move |record: &Record<K, T>| {
            formatted_time_from_timestamp(record.timestamp, &pattern)
        })),
    }
}

fn formatted_time_from_timestamp(timestamp: SystemTime, pattern: &str) -> String {
    let datetime: DateTime<Utc> = DateTime::from(timestamp);
    datetime.format(pattern).to_string()
}
fn partition_string_for_fields<T: Data + Serialize>(
    value: &T,
    partition_fields: &Vec<String>,
) -> Result<String> {
    let value = serde_json::to_value(value)?;
    let fields: Vec<_> = partition_fields
        .iter()
        .map(|field| {
            let field_value = value
                .get(field)
                .ok_or_else(|| anyhow::anyhow!("field {} not found in value {:?}", field, value))?;
            Ok(format!("{}={}", field, field_value))
        })
        .collect::<Result<_>>()?;
    Ok(fields.join("/"))
}

#[derive(Debug)]
enum FileSystemMessages<T: Data> {
    Data {
        value: T,
        time: SystemTime,
        partition: Option<String>,
    },
    Init {
        max_file_index: usize,
        subtask_id: usize,
        recovered_files: Vec<InProgressFileCheckpoint<T>>,
    },
    Checkpoint {
        subtask_id: usize,
        watermark: Option<SystemTime>,
        then_stop: bool,
    },
    FilesToFinish(Vec<FileToFinish>),
}

#[derive(Debug)]
enum CheckpointData<T: Data> {
    InProgressFileCheckpoint(InProgressFileCheckpoint<T>),
    Finished {
        max_file_index: usize,
        delta_version: i64,
    },
}

#[derive(Decode, Encode, Clone, PartialEq, Eq)]
struct InProgressFileCheckpoint<T: Data> {
    filename: String,
    partition: Option<String>,
    data: FileCheckpointData,
    buffered_data: Vec<T>,
    representative_timestamp: SystemTime,
    pushed_size: usize,
}

impl<T: Data> std::fmt::Debug for InProgressFileCheckpoint<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProgressFileCheckpoint")
            .field("filename", &self.filename)
            .field("partition", &self.partition)
            .field("data", &self.data)
            .field("buffered_data_len", &self.buffered_data.len())
            .field("representative_timestamp", &self.representative_timestamp)
            .finish()
    }
}

#[derive(Decode, Encode, Clone, PartialEq, Eq)]
pub enum FileCheckpointData {
    Empty,
    MultiPartNotCreated {
        parts_to_add: Vec<Vec<u8>>,
        trailing_bytes: Option<Vec<u8>>,
    },
    MultiPartInFlight {
        multi_part_upload_id: String,
        in_flight_parts: Vec<InFlightPartCheckpoint>,
        trailing_bytes: Option<Vec<u8>>,
    },
    MultiPartWriterClosed {
        multi_part_upload_id: String,
        in_flight_parts: Vec<InFlightPartCheckpoint>,
    },
    MultiPartWriterUploadCompleted {
        multi_part_upload_id: String,
        completed_parts: Vec<String>,
    },
}

impl std::fmt::Debug for FileCheckpointData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileCheckpointData::Empty => write!(f, "Empty"),
            FileCheckpointData::MultiPartNotCreated {
                parts_to_add,
                trailing_bytes,
            } => {
                write!(f, "MultiPartNotCreated {{ parts_to_add: [")?;
                for part in parts_to_add {
                    write!(f, "{} bytes, ", part.len())?;
                }
                write!(f, "], trailing_bytes: ")?;
                if let Some(bytes) = trailing_bytes {
                    write!(f, "{} bytes", bytes.len())?;
                } else {
                    write!(f, "None")?;
                }
                write!(f, " }}")
            }
            FileCheckpointData::MultiPartInFlight {
                multi_part_upload_id,
                in_flight_parts,
                trailing_bytes,
            } => {
                write!(
                    f,
                    "MultiPartInFlight {{ multi_part_upload_id: {}, in_flight_parts: [",
                    multi_part_upload_id
                )?;
                for part in in_flight_parts {
                    match part {
                        InFlightPartCheckpoint::FinishedPart { content_id, .. } => {
                            write!(f, "FinishedPart {{ {} bytes }}, ", content_id.len())?
                        }
                        InFlightPartCheckpoint::InProgressPart { data, .. } => {
                            write!(f, "InProgressPart {{ {} bytes }}, ", data.len())?
                        }
                    }
                }
                write!(f, "], trailing_bytes: ")?;
                if let Some(bytes) = trailing_bytes {
                    write!(f, "{} bytes", bytes.len())?;
                } else {
                    write!(f, "None")?;
                }
                write!(f, " }}")
            }
            FileCheckpointData::MultiPartWriterClosed {
                multi_part_upload_id,
                in_flight_parts,
            } => {
                write!(
                    f,
                    "MultiPartWriterClosed {{ multi_part_upload_id: {}, in_flight_parts: [",
                    multi_part_upload_id
                )?;
                for part in in_flight_parts {
                    match part {
                        InFlightPartCheckpoint::FinishedPart { content_id, .. } => {
                            write!(f, "FinishedPart {{ {} bytes }}, ", content_id.len())?
                        }
                        InFlightPartCheckpoint::InProgressPart { data, .. } => {
                            write!(f, "InProgressPart {{ {} bytes }}, ", data.len())?
                        }
                    }
                }
                write!(f, "] }}")
            }
            FileCheckpointData::MultiPartWriterUploadCompleted {
                multi_part_upload_id,
                completed_parts,
            } => {
                write!(f, "MultiPartWriterUploadCompleted {{ multi_part_upload_id: {}, completed_parts: [", multi_part_upload_id)?;
                for part in completed_parts {
                    write!(f, "{} bytes, ", part.len())?;
                }
                write!(f, "] }}")
            }
        }
    }
}

#[derive(Debug, Decode, Encode, Clone, PartialEq, Eq)]
pub enum InFlightPartCheckpoint {
    FinishedPart { part: usize, content_id: String },
    InProgressPart { part: usize, data: Vec<u8> },
}

struct AsyncMultipartFileSystemWriter<T: Data + SchemaData + Sync, R: MultiPartWriter> {
    path: Path,
    active_writers: HashMap<Option<String>, String>,
    watermark: Option<SystemTime>,
    max_file_index: usize,
    subtask_id: usize,
    object_store: Arc<StorageProvider>,
    writers: HashMap<String, R>,
    receiver: Receiver<FileSystemMessages<T>>,
    checkpoint_sender: Sender<CheckpointData<T>>,
    futures: FuturesUnordered<BoxedTryFuture<MultipartCallbackWithName>>,
    files_to_finish: Vec<FileToFinish>,
    properties: FileSystemTable,
    rolling_policy: RollingPolicy,
    commit_state: CommitState,
    filenaming: Filenaming,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub enum CommitState {
    DeltaLake { last_version: i64 },
    VanillaParquet,
}

#[async_trait]
pub trait MultiPartWriter {
    type InputType: Data;
    fn new(
        object_store: Arc<StorageProvider>,
        path: Path,
        partition: Option<String>,
        config: &FileSystemTable,
    ) -> Self;

    fn name(&self) -> String;

    fn partition(&self) -> Option<String>;

    async fn insert_value(
        &mut self,
        value: Self::InputType,
        time: SystemTime,
    ) -> Result<Option<BoxedTryFuture<MultipartCallbackWithName>>>;

    fn handle_initialization(
        &mut self,
        multipart_id: String,
    ) -> Result<Vec<BoxedTryFuture<MultipartCallbackWithName>>>;

    fn handle_completed_part(
        &mut self,
        part_idx: usize,
        upload_part: PartId,
    ) -> Result<Option<FileToFinish>>;

    fn get_in_progress_checkpoint(&mut self) -> FileCheckpointData;

    fn currently_buffered_data(&mut self) -> Vec<Self::InputType>;

    fn close(&mut self) -> Result<Option<BoxedTryFuture<MultipartCallbackWithName>>>;

    fn stats(&self) -> Option<MultiPartWriterStats>;

    fn get_finished_file(&mut self) -> FileToFinish;
}

async fn from_checkpoint(
    path: &Path,
    partition: Option<String>,
    checkpoint_data: FileCheckpointData,
    mut pushed_size: usize,
    object_store: Arc<StorageProvider>,
) -> Result<Option<FileToFinish>> {
    let mut parts = vec![];
    let multipart_id = match checkpoint_data {
        FileCheckpointData::Empty => {
            return Ok(None);
        }
        FileCheckpointData::MultiPartNotCreated {
            parts_to_add,
            trailing_bytes,
        } => {
            let multipart_id = object_store
                .start_multipart(path)
                .await
                .expect("failed to create multipart upload");
            let mut parts = vec![];
            for (part_index, data) in parts_to_add.into_iter().enumerate() {
                pushed_size += data.len();
                let upload_part = object_store
                    .add_multipart(path, &multipart_id, part_index, data.into())
                    .await
                    .unwrap();
                parts.push(upload_part);
            }
            if let Some(trailing_bytes) = trailing_bytes {
                pushed_size += trailing_bytes.len();
                let upload_part = object_store
                    .add_multipart(path, &multipart_id, parts.len(), trailing_bytes.into())
                    .await?;
                parts.push(upload_part);
            }
            multipart_id
        }
        FileCheckpointData::MultiPartInFlight {
            multi_part_upload_id,
            in_flight_parts,
            trailing_bytes,
        } => {
            for data in in_flight_parts.into_iter() {
                match data {
                    InFlightPartCheckpoint::FinishedPart {
                        part: _,
                        content_id,
                    } => parts.push(PartId { content_id }),
                    InFlightPartCheckpoint::InProgressPart { part, data } => {
                        let upload_part = object_store
                            .add_multipart(path, &multi_part_upload_id, part, data.into())
                            .await
                            .unwrap();
                        parts.push(upload_part);
                    }
                }
            }
            if let Some(trailing_bytes) = trailing_bytes {
                pushed_size += trailing_bytes.len();
                let upload_part = object_store
                    .add_multipart(
                        path,
                        &multi_part_upload_id,
                        parts.len(),
                        trailing_bytes.into(),
                    )
                    .await?;
                parts.push(upload_part);
            }
            multi_part_upload_id
        }
        FileCheckpointData::MultiPartWriterClosed {
            multi_part_upload_id,
            in_flight_parts,
        } => {
            for (part_index, data) in in_flight_parts.into_iter().enumerate() {
                match data {
                    InFlightPartCheckpoint::FinishedPart {
                        part: _,
                        content_id,
                    } => parts.push(PartId { content_id }),
                    InFlightPartCheckpoint::InProgressPart { part: _, data } => {
                        let upload_part = object_store
                            .add_multipart(path, &multi_part_upload_id, part_index, data.into())
                            .await
                            .unwrap();
                        parts.push(upload_part);
                    }
                }
            }
            multi_part_upload_id
        }
        FileCheckpointData::MultiPartWriterUploadCompleted {
            multi_part_upload_id,
            completed_parts,
        } => {
            for content_id in completed_parts {
                parts.push(PartId { content_id })
            }
            multi_part_upload_id
        }
    };
    Ok(Some(FileToFinish {
        filename: path.to_string(),
        partition,
        multi_part_upload_id: multipart_id,
        completed_parts: parts.into_iter().map(|p| p.content_id).collect(),
        size: pushed_size,
    }))
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct FileToFinish {
    filename: String,
    partition: Option<String>,
    multi_part_upload_id: String,
    completed_parts: Vec<String>,
    size: usize,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct FinishedFile {
    filename: String,
    partition: Option<String>,
    size: usize,
}

enum RollingPolicy {
    PartLimit(usize),
    SizeLimit(usize),
    InactivityDuration(Duration),
    RolloverDuration(Duration),
    WatermarkExpiration { pattern: String },
    AnyPolicy(Vec<RollingPolicy>),
}

impl RollingPolicy {
    fn should_roll(&self, stats: &MultiPartWriterStats, watermark: Option<SystemTime>) -> bool {
        match self {
            RollingPolicy::PartLimit(part_limit) => stats.parts_written >= *part_limit,
            RollingPolicy::SizeLimit(size_limit) => stats.bytes_written >= *size_limit,
            RollingPolicy::InactivityDuration(duration) => {
                stats.last_write_at.elapsed() >= *duration
            }
            RollingPolicy::RolloverDuration(duration) => {
                stats.first_write_at.elapsed() >= *duration
            }
            RollingPolicy::AnyPolicy(policies) => policies
                .iter()
                .any(|policy| policy.should_roll(stats, watermark)),
            RollingPolicy::WatermarkExpiration { pattern } => {
                let Some(watermark) = watermark else {
                    return false;
                };
                if watermark <= stats.representative_timestamp {
                    return false;
                }
                // if the watermark is greater than the representative and has a different string format,
                // then this partition is closed and should be rolled over.
                let datetime: DateTime<Utc> = DateTime::from(watermark);
                let formatted_time = datetime.format(&pattern).to_string();
                let representative_datetime: DateTime<Utc> =
                    DateTime::from(stats.representative_timestamp);
                let representative_formatted_time =
                    representative_datetime.format(&pattern).to_string();
                formatted_time != representative_formatted_time
            }
        }
    }

    fn from_file_settings(file_settings: &FileSettings) -> RollingPolicy {
        let mut policies = vec![];
        let part_size_limit = file_settings.max_parts.unwrap_or(1000) as usize;
        // this is a hard limit, so will always be present.
        policies.push(RollingPolicy::PartLimit(part_size_limit));
        if let Some(file_size_target) = file_settings.target_file_size {
            policies.push(RollingPolicy::SizeLimit(file_size_target as usize))
        }
        if let Some(inactivity_timeout) = file_settings
            .inactivity_rollover_seconds
            .map(|seconds| Duration::from_secs(seconds as u64))
        {
            policies.push(RollingPolicy::InactivityDuration(inactivity_timeout))
        }
        let rollover_timeout =
            Duration::from_secs(file_settings.rollover_seconds.unwrap_or(30) as u64);
        policies.push(RollingPolicy::RolloverDuration(rollover_timeout));
        if let Some(partitioning) = &file_settings.partitioning {
            if let Some(pattern) = &partitioning.time_partition_pattern {
                policies.push(RollingPolicy::WatermarkExpiration {
                    pattern: pattern.clone(),
                });
            }
        }
        RollingPolicy::AnyPolicy(policies)
    }
}

#[derive(Debug, Clone)]
pub struct MultiPartWriterStats {
    bytes_written: usize,
    parts_written: usize,
    last_write_at: Instant,
    first_write_at: Instant,
    representative_timestamp: SystemTime,
}

impl<T, R> AsyncMultipartFileSystemWriter<T, R>
where
    T: Data + SchemaData + std::marker::Sync,
    R: MultiPartWriter<InputType = T>,
{
    fn new(
        path: Path,
        object_store: Arc<StorageProvider>,
        receiver: Receiver<FileSystemMessages<T>>,
        checkpoint_sender: Sender<CheckpointData<T>>,
        writer_properties: FileSystemTable,
    ) -> Self {
        let commit_state = match writer_properties
            .file_settings
            .as_ref()
            .unwrap()
            .commit_style
            .unwrap()
        {
            CommitStyle::DeltaLake => CommitState::DeltaLake { last_version: -1 },
            CommitStyle::Direct => CommitState::VanillaParquet,
        };
        let filenaming = writer_properties
            .file_settings
            .as_ref()
            .unwrap()
            .filenaming
            .clone()
            .unwrap();
        Self {
            path,
            active_writers: HashMap::new(),
            watermark: None,
            max_file_index: 0,
            subtask_id: 0,
            object_store,
            writers: HashMap::new(),
            receiver,
            checkpoint_sender,
            futures: FuturesUnordered::new(),
            files_to_finish: Vec::new(),
            rolling_policy: RollingPolicy::from_file_settings(
                writer_properties.file_settings.as_ref().unwrap(),
            ),
            properties: writer_properties,
            commit_state,
            filenaming,
        }
    }

    fn add_part_to_finish(&mut self, file_to_finish: FileToFinish) {
        self.files_to_finish.push(file_to_finish);
    }

    async fn run(&mut self) -> Result<()> {
        let mut next_policy_check = tokio::time::Instant::now();
        loop {
            tokio::select! {
                Some(message) = self.receiver.recv() => {
                    match message {
                        FileSystemMessages::Data{value, time, partition} => {
                            if let Some(future) = self.get_or_insert_writer(&partition).insert_value(value, time).await? {
                                self.futures.push(future);
                            }
                        },
                        FileSystemMessages::Init {max_file_index, subtask_id, recovered_files } => {
                            self.max_file_index = max_file_index;
                            self.subtask_id = subtask_id;
                            for recovered_file in recovered_files {
                                if let Some(file_to_finish) = from_checkpoint(
                                     &Path::parse(&recovered_file.filename)?, recovered_file.partition.clone(), recovered_file.data, recovered_file.pushed_size, self.object_store.clone()).await? {
                                        self.add_part_to_finish(file_to_finish);
                                     }

                                for value in recovered_file.buffered_data {
                                    self.get_or_insert_writer(&recovered_file.partition).insert_value(value, SystemTime::now()).await?;
                                }
                            }
                        },
                        FileSystemMessages::Checkpoint { subtask_id, watermark, then_stop } => {
                            self.watermark = watermark;
                            self.flush_futures().await?;
                            if then_stop {
                                self.stop().await?;
                            }
                            self.take_checkpoint( subtask_id).await?;
                            let delta_version = self.delta_version();
                            self.checkpoint_sender.send({CheckpointData::Finished {  max_file_index: self.max_file_index,
                            delta_version}}).await?;
                        },
                        FileSystemMessages::FilesToFinish(files_to_finish) =>{
                            self.finish_files(files_to_finish).await?;
                        }
                    }
                }
                Some(result) = self.futures.next() => {
                    let MultipartCallbackWithName { callback, name } = result?;
                    self.process_callback(name, callback)?;
                }
                _ = tokio::time::sleep_until(next_policy_check) => {
                    next_policy_check = tokio::time::Instant::now() + Duration::from_millis(100);
                    let mut removed_partitions = vec![];
                    for (partition, filename) in &self.active_writers {
                            let writer = self.writers.get_mut(filename).unwrap();
                            if let Some(stats) = writer.stats() {
                            if self.rolling_policy.should_roll(&stats, self.watermark) {

                                if let Some(future) = writer.close()? {
                                self.futures.push(future);
                                    removed_partitions.push((partition.clone(), false));
                                } else {
                                    removed_partitions.push((partition.clone(), true));
                                }
                            }
                        }
                    }
                    if removed_partitions.len() > 0 {
                        self.max_file_index += 1;
                    }
                    for (partition, remove_writer) in removed_partitions {
                        let file = self.active_writers.remove(&partition).ok_or_else(|| anyhow::anyhow!("can't find writer {:?}", partition))?;
                        if remove_writer {
                            self.writers.remove(&file);
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }
        Ok(())
    }

    fn get_or_insert_writer(&mut self, partition: &Option<String>) -> &mut R {
        if !self.active_writers.contains_key(partition) {
            let new_writer = self.new_writer(partition);
            self.active_writers
                .insert(partition.clone(), new_writer.name());
            self.writers.insert(new_writer.name(), new_writer);
        }
        self.writers
            .get_mut(self.active_writers.get(partition).unwrap())
            .unwrap()
    }

    fn new_writer(&mut self, partition: &Option<String>) -> R {
        let filename_strategy = match self.filenaming.strategy {
            Some(FilenameStrategy::Uuid) => FilenameStrategy::Uuid,
            Some(FilenameStrategy::Serial) => FilenameStrategy::Serial,
            None => FilenameStrategy::Serial,
        };

        // TODO: File suffix

        // This forms the base for naming files depending on strategy
        let filename_base = if filename_strategy == FilenameStrategy::Uuid {
            Uuid::new_v4().to_string()
        } else {
            format!("{:>05}-{:>03}", self.max_file_index, self.subtask_id)
        };

        // This allows us to manipulate the filename_base
        let filename_core = if self.filenaming.prefix.is_some() {
            format!(
                "{}-{}",
                self.filenaming.prefix.as_ref().unwrap(),
                filename_base
            )
        } else {
            filename_base
        };

        let path = match partition {
            Some(sub_bucket) => format!("{}/{}/{}", self.path, sub_bucket, filename_core),
            None => format!("{}/{}", self.path, filename_core),
        };
        R::new(
            self.object_store.clone(),
            path.into(),
            partition.clone(),
            &self.properties,
        )
    }

    async fn flush_futures(&mut self) -> Result<()> {
        while let Some(MultipartCallbackWithName { callback, name }) =
            self.futures.try_next().await?
        {
            self.process_callback(name, callback)?;
        }
        Ok(())
    }

    fn process_callback(&mut self, name: String, callback: MultipartCallback) -> Result<()> {
        let writer = self.writers.get_mut(&name).ok_or_else(|| {
            anyhow::anyhow!("missing writer {} for callback {:?}", name, callback)
        })?;
        match callback {
            MultipartCallback::InitializedMultipart { multipart_id } => {
                self.futures
                    .extend(writer.handle_initialization(multipart_id)?);
                Ok(())
            }
            MultipartCallback::CompletedPart {
                part_idx,
                upload_part,
            } => {
                if let Some(file_to_write) = writer.handle_completed_part(part_idx, upload_part)? {
                    // need the file to finish to be checkpointed first.
                    self.add_part_to_finish(file_to_write);
                    self.writers.remove(&name);
                }
                Ok(())
            }
            MultipartCallback::UploadsFinished => {
                let file_to_write = writer.get_finished_file();
                self.add_part_to_finish(file_to_write);
                self.writers.remove(&name);
                Ok(())
            }
        }
    }

    async fn finish_files(&mut self, files_to_finish: Vec<FileToFinish>) -> Result<()> {
        let mut finished_files: Vec<FinishedFile> = vec![];
        for file_to_finish in files_to_finish {
            if let Some(file) = self.finish_file(file_to_finish).await? {
                finished_files.push(file);
            }
        }
        if let CommitState::DeltaLake { last_version } = self.commit_state {
            if let Some(new_version) = delta::commit_files_to_delta(
                finished_files,
                self.path.clone(),
                self.object_store.clone(),
                last_version,
                T::schema(),
            )
            .await?
            {
                self.commit_state = CommitState::DeltaLake {
                    last_version: new_version,
                };
            }
        }
        let finished_message = CheckpointData::Finished {
            max_file_index: self.max_file_index,
            delta_version: self.delta_version(),
        };
        self.checkpoint_sender.send(finished_message).await?;
        Ok(())
    }

    fn delta_version(&mut self) -> i64 {
        match self.commit_state {
            CommitState::DeltaLake { last_version } => last_version,
            CommitState::VanillaParquet => 0,
        }
    }

    async fn finish_file(&mut self, file_to_finish: FileToFinish) -> Result<Option<FinishedFile>> {
        let FileToFinish {
            filename,
            partition,
            multi_part_upload_id,
            completed_parts,
            size,
        } = file_to_finish;
        if completed_parts.len() == 0 {
            warn!("no parts to finish for file {}", filename);
            return Ok(None);
        }

        let parts: Vec<_> = completed_parts
            .into_iter()
            .map(|content_id| PartId {
                content_id: content_id.clone(),
            })
            .collect();
        let location = Path::parse(&filename)?;
        match self
            .object_store
            .close_multipart(&location, &multi_part_upload_id, parts)
            .await
        {
            Ok(_) => Ok(Some(FinishedFile {
                filename,
                partition,
                size,
            })),
            Err(err) => {
                warn!(
                    "when attempting to complete {}, received an error: {}",
                    filename, err
                );
                // check if the file is already there with the correct size.
                let contents = self.object_store.get(&filename).await?;
                if contents.len() == size {
                    Ok(Some(FinishedFile {
                        filename,
                        partition,
                        size,
                    }))
                } else {
                    bail!(
                        "file written to {} should have length of {}, not {}",
                        filename,
                        size,
                        contents.len()
                    );
                }
            }
        }
    }

    async fn stop(&mut self) -> Result<()> {
        for (_partition, filename) in &self.active_writers {
            let writer = self.writers.get_mut(filename).unwrap();
            let close_future: Option<BoxedTryFuture<MultipartCallbackWithName>> = writer.close()?;
            if let Some(future) = close_future {
                self.futures.push(future);
            } else {
                self.writers.remove(filename);
            }
        }
        self.active_writers.clear();
        while let Some(result) = self.futures.next().await {
            let MultipartCallbackWithName { callback, name } = result?;
            self.process_callback(name, callback)?;
        }
        Ok(())
    }

    async fn take_checkpoint(&mut self, _subtask_id: usize) -> Result<()> {
        for (filename, writer) in self.writers.iter_mut() {
            let data = writer.get_in_progress_checkpoint();
            let buffered_data = writer.currently_buffered_data();
            let in_progress_checkpoint =
                CheckpointData::InProgressFileCheckpoint(InProgressFileCheckpoint {
                    filename: filename.clone(),
                    partition: writer.partition(),
                    data,
                    buffered_data,
                    representative_timestamp: writer
                        .stats()
                        .as_ref()
                        .unwrap()
                        .representative_timestamp,
                    pushed_size: writer.stats().as_ref().unwrap().bytes_written,
                });
            self.checkpoint_sender.send(in_progress_checkpoint).await?;
        }
        for file_to_finish in &self.files_to_finish {
            self.checkpoint_sender
                .send(CheckpointData::InProgressFileCheckpoint(
                    InProgressFileCheckpoint {
                        filename: file_to_finish.filename.clone(),
                        partition: file_to_finish.partition.clone(),
                        data: FileCheckpointData::MultiPartWriterUploadCompleted {
                            multi_part_upload_id: file_to_finish.multi_part_upload_id.clone(),
                            completed_parts: file_to_finish.completed_parts.clone(),
                        },
                        buffered_data: vec![],
                        // TODO: this is only needed if there is buffered data, so this is a dummy value
                        representative_timestamp: SystemTime::now(),
                        pushed_size: file_to_finish.size,
                    },
                ))
                .await?;
        }
        self.files_to_finish.clear();
        Ok(())
    }
}

type BoxedTryFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

struct MultipartManager {
    object_store: Arc<StorageProvider>,
    location: Path,
    partition: Option<String>,
    multipart_id: Option<MultipartId>,
    pushed_parts: Vec<PartIdOrBufferedData>,
    uploaded_parts: usize,
    pushed_size: usize,
    parts_to_add: Vec<PartToUpload>,
    closed: bool,
}

impl MultipartManager {
    fn new(object_store: Arc<StorageProvider>, location: Path, partition: Option<String>) -> Self {
        Self {
            object_store,
            location,
            partition,
            multipart_id: None,
            pushed_parts: vec![],
            uploaded_parts: 0,
            pushed_size: 0,
            parts_to_add: vec![],
            closed: false,
        }
    }

    fn name(&self) -> String {
        self.location.to_string()
    }

    fn write_next_part(
        &mut self,
        data: Vec<u8>,
    ) -> Result<Option<BoxedTryFuture<MultipartCallbackWithName>>> {
        match &self.multipart_id {
            Some(_multipart_id) => Ok(Some(self.get_part_upload_future(PartToUpload {
                part_index: self.pushed_parts.len(),
                byte_data: data,
            })?)),
            None => {
                let is_first_part = self.parts_to_add.is_empty();
                self.parts_to_add.push(PartToUpload {
                    byte_data: data,
                    part_index: self.parts_to_add.len(),
                });
                if is_first_part {
                    // start a new multipart upload
                    Ok(Some(self.get_initialize_multipart_future()?))
                } else {
                    Ok(None)
                }
            }
        }
    }
    // Future for uploading a part of a multipart upload.
    // Argument is either from a newly flushed part or from parts_to_add.
    fn get_part_upload_future(
        &mut self,
        part_to_upload: PartToUpload,
    ) -> Result<BoxedTryFuture<MultipartCallbackWithName>> {
        self.pushed_parts.push(PartIdOrBufferedData::BufferedData {
            // TODO: use Bytes to avoid clone
            data: part_to_upload.byte_data.clone(),
        });
        self.pushed_size += part_to_upload.byte_data.len();
        let location = self.location.clone();
        let multipart_id = self
            .multipart_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("missing multipart id"))?;
        let object_store = self.object_store.clone();
        Ok(Box::pin(async move {
            let upload_part = object_store
                .add_multipart(
                    &location,
                    &multipart_id,
                    part_to_upload.part_index,
                    part_to_upload.byte_data.into(),
                )
                .await?;
            Ok(MultipartCallbackWithName {
                name: location.to_string(),
                callback: MultipartCallback::CompletedPart {
                    part_idx: part_to_upload.part_index,
                    upload_part,
                },
            })
        }))
    }

    fn get_initialize_multipart_future(
        &mut self,
    ) -> Result<BoxedTryFuture<MultipartCallbackWithName>> {
        let object_store = self.object_store.clone();
        let location = self.location.clone();
        Ok(Box::pin(async move {
            let multipart_id = object_store.start_multipart(&location).await?;
            Ok(MultipartCallbackWithName {
                name: location.to_string(),
                callback: MultipartCallback::InitializedMultipart { multipart_id },
            })
        }))
    }

    fn handle_initialization(
        &mut self,
        multipart_id: String,
    ) -> Result<Vec<BoxedTryFuture<MultipartCallbackWithName>>> {
        // for each part in parts_to_add, start a new part upload
        self.multipart_id = Some(multipart_id);
        std::mem::take(&mut self.parts_to_add)
            .into_iter()
            .map(|part_to_upload| self.get_part_upload_future(part_to_upload))
            .collect::<Result<Vec<_>>>()
    }

    fn handle_completed_part(
        &mut self,
        part_idx: usize,
        upload_part: PartId,
    ) -> Result<Option<FileToFinish>> {
        self.pushed_parts[part_idx] = PartIdOrBufferedData::PartId(upload_part);
        self.uploaded_parts += 1;

        if !self.all_uploads_finished() {
            Ok(None)
        } else {
            Ok(Some(FileToFinish {
                filename: self.name(),
                partition: self.partition.clone(),
                multi_part_upload_id: self
                    .multipart_id
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("need multipart id to complete"))?
                    .clone(),
                completed_parts: self
                    .pushed_parts
                    .iter()
                    .map(|part| match part {
                        PartIdOrBufferedData::PartId(upload_part) => {
                            Ok(upload_part.content_id.clone())
                        }
                        PartIdOrBufferedData::BufferedData { .. } => {
                            bail!("unfinished part in get_complete_multipart_future")
                        }
                    })
                    .collect::<Result<Vec<_>>>()?,
                size: self.pushed_size,
            }))
        }
    }
    fn all_uploads_finished(&self) -> bool {
        self.closed && self.uploaded_parts == self.pushed_parts.len()
    }

    fn get_closed_file_checkpoint_data(&mut self) -> FileCheckpointData {
        if !self.closed {
            unreachable!("get_closed_file_checkpoint_data called on open file");
        }
        let Some(ref multipart_id) = self.multipart_id else {
            if self.pushed_size == 0 {
                return FileCheckpointData::Empty;
            } else {
                return FileCheckpointData::MultiPartNotCreated {
                    parts_to_add: self
                        .parts_to_add
                        .iter()
                        .map(|val| val.byte_data.clone())
                        .collect(),
                    trailing_bytes: None,
                };
            }
        };
        if self.all_uploads_finished() {
            return FileCheckpointData::MultiPartWriterUploadCompleted {
                multi_part_upload_id: multipart_id.clone(),
                completed_parts: self
                    .pushed_parts
                    .iter()
                    .map(|val| match val {
                        PartIdOrBufferedData::PartId(upload_part) => upload_part.content_id.clone(),
                        PartIdOrBufferedData::BufferedData { .. } => {
                            unreachable!("unfinished part in get_closed_file_checkpoint_data")
                        }
                    })
                    .collect(),
            };
        } else {
            let in_flight_parts = self
                .pushed_parts
                .iter()
                .enumerate()
                .map(|(part_index, part)| match part {
                    PartIdOrBufferedData::PartId(upload_part) => {
                        InFlightPartCheckpoint::FinishedPart {
                            part: part_index,
                            content_id: upload_part.content_id.clone(),
                        }
                    }
                    PartIdOrBufferedData::BufferedData { data } => {
                        InFlightPartCheckpoint::InProgressPart {
                            part: part_index,
                            data: data.clone(),
                        }
                    }
                })
                .collect();
            FileCheckpointData::MultiPartWriterClosed {
                multi_part_upload_id: multipart_id.clone(),
                in_flight_parts,
            }
        }
    }

    fn get_in_progress_checkpoint(
        &mut self,
        trailing_bytes: Option<Vec<u8>>,
    ) -> FileCheckpointData {
        if self.closed {
            unreachable!("get_in_progress_checkpoint called on closed file");
        }
        if self.multipart_id.is_none() {
            return FileCheckpointData::MultiPartNotCreated {
                parts_to_add: self
                    .parts_to_add
                    .iter()
                    .map(|val| val.byte_data.clone())
                    .collect(),
                trailing_bytes,
            };
        }
        let multi_part_id = self.multipart_id.as_ref().unwrap().clone();
        let in_flight_parts = self
            .pushed_parts
            .iter()
            .enumerate()
            .map(|(part_index, part)| match part {
                PartIdOrBufferedData::PartId(upload_part) => InFlightPartCheckpoint::FinishedPart {
                    part: part_index,
                    content_id: upload_part.content_id.clone(),
                },
                PartIdOrBufferedData::BufferedData { data } => {
                    InFlightPartCheckpoint::InProgressPart {
                        part: part_index,
                        data: data.clone(),
                    }
                }
            })
            .collect();
        FileCheckpointData::MultiPartInFlight {
            multi_part_upload_id: multi_part_id,
            in_flight_parts,
            trailing_bytes,
        }
    }

    fn get_finished_file(&mut self) -> FileToFinish {
        if !self.closed {
            unreachable!("get_finished_file called on open file");
        }
        FileToFinish {
            filename: self.name(),
            partition: self.partition.clone(),
            multi_part_upload_id: self
                .multipart_id
                .as_ref()
                .expect("finished files should have a multipart ID")
                .clone(),
            completed_parts: self
                .pushed_parts
                .iter()
                .map(|part| match part {
                    PartIdOrBufferedData::PartId(upload_part) => upload_part.content_id.clone(),
                    PartIdOrBufferedData::BufferedData { .. } => {
                        unreachable!("unfinished part in get_finished_file")
                    }
                })
                .collect(),
            size: self.pushed_size,
        }
    }
}

pub trait BatchBuilder: Send {
    type InputType: Data;
    type BatchData;
    fn new(config: &FileSystemTable) -> Self;
    fn insert(&mut self, value: Self::InputType) -> Option<Self::BatchData>;
    fn buffered_inputs(&self) -> Vec<Self::InputType>;
    fn flush_buffer(&mut self) -> Self::BatchData;
}

pub trait BatchBufferingWriter: Send {
    type BatchData;
    fn new(config: &FileSystemTable) -> Self;
    fn suffix() -> String;
    fn add_batch_data(&mut self, data: Self::BatchData) -> Option<Vec<u8>>;
    fn buffer_length(&self) -> usize;
    fn evict_current_buffer(&mut self) -> Vec<u8>;
    fn get_trailing_bytes_for_checkpoint(&mut self) -> Option<Vec<u8>>;
    fn close(&mut self, final_batch: Option<Self::BatchData>) -> Option<Vec<u8>>;
}
pub struct BatchMultipartWriter<
    BB: BatchBuilder,
    BBW: BatchBufferingWriter<BatchData = BB::BatchData>,
> {
    batch_builder: BB,
    batch_buffering_writer: BBW,
    multipart_manager: MultipartManager,
    stats: Option<MultiPartWriterStats>,
}
#[async_trait]
impl<BB: BatchBuilder, BBW: BatchBufferingWriter<BatchData = BB::BatchData>> MultiPartWriter
    for BatchMultipartWriter<BB, BBW>
{
    type InputType = BB::InputType;

    fn new(
        object_store: Arc<StorageProvider>,
        path: Path,
        partition: Option<String>,
        config: &FileSystemTable,
    ) -> Self {
        let batch_builder = BB::new(config);
        let batch_buffering_writer = BBW::new(config);
        let path = format!("{}.{}", path, BBW::suffix()).into();
        Self {
            batch_builder,
            batch_buffering_writer,
            multipart_manager: MultipartManager::new(object_store, path, partition),
            stats: None,
        }
    }

    fn name(&self) -> String {
        self.multipart_manager.name()
    }

    fn partition(&self) -> Option<String> {
        self.multipart_manager.partition.clone()
    }

    async fn insert_value(
        &mut self,
        value: Self::InputType,
        time: SystemTime,
    ) -> Result<Option<BoxedTryFuture<MultipartCallbackWithName>>> {
        if self.stats.is_none() {
            self.stats = Some(MultiPartWriterStats {
                bytes_written: 0,
                parts_written: 0,
                last_write_at: Instant::now(),
                first_write_at: Instant::now(),
                representative_timestamp: time,
            });
        }
        let stats = self.stats.as_mut().unwrap();
        stats.last_write_at = Instant::now();

        if let Some(batch) = self.batch_builder.insert(value.clone()) {
            let prev_size = self.batch_buffering_writer.buffer_length();
            if let Some(bytes) = self.batch_buffering_writer.add_batch_data(batch) {
                stats.bytes_written += bytes.len() - prev_size;
                stats.parts_written += 1;
                self.multipart_manager.write_next_part(bytes)
            } else {
                stats.bytes_written += self.batch_buffering_writer.buffer_length() - prev_size;
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn handle_initialization(
        &mut self,
        multipart_id: String,
    ) -> Result<Vec<BoxedTryFuture<MultipartCallbackWithName>>> {
        self.multipart_manager.handle_initialization(multipart_id)
    }

    fn handle_completed_part(
        &mut self,
        part_idx: usize,
        upload_part: PartId,
    ) -> Result<Option<FileToFinish>> {
        self.multipart_manager
            .handle_completed_part(part_idx, upload_part)
    }

    fn get_in_progress_checkpoint(&mut self) -> FileCheckpointData {
        if self.multipart_manager.closed {
            self.multipart_manager.get_closed_file_checkpoint_data()
        } else {
            self.multipart_manager.get_in_progress_checkpoint(
                self.batch_buffering_writer
                    .get_trailing_bytes_for_checkpoint(),
            )
        }
    }

    fn currently_buffered_data(&mut self) -> Vec<Self::InputType> {
        self.batch_builder.buffered_inputs()
    }

    fn close(&mut self) -> Result<Option<BoxedTryFuture<MultipartCallbackWithName>>> {
        self.multipart_manager.closed = true;
        self.write_closing_multipart()
    }

    fn stats(&self) -> Option<MultiPartWriterStats> {
        self.stats.clone()
    }

    fn get_finished_file(&mut self) -> FileToFinish {
        self.multipart_manager.get_finished_file()
    }
}

impl<BB: BatchBuilder, BBW: BatchBufferingWriter<BatchData = BB::BatchData>>
    BatchMultipartWriter<BB, BBW>
{
    fn write_closing_multipart(
        &mut self,
    ) -> Result<Option<BoxedTryFuture<MultipartCallbackWithName>>> {
        self.multipart_manager.closed = true;
        let final_batch = if !self.batch_builder.buffered_inputs().is_empty() {
            Some(self.batch_builder.flush_buffer())
        } else {
            None
        };
        if let Some(bytes) = self.batch_buffering_writer.close(final_batch) {
            self.multipart_manager.write_next_part(bytes)
        } else if self.multipart_manager.all_uploads_finished() {
            // Return a finished file future
            let name = self.multipart_manager.name();
            Ok(Some(Box::pin(async move {
                Ok(MultipartCallbackWithName {
                    name,
                    callback: MultipartCallback::UploadsFinished,
                })
            })))
        } else {
            Ok(None)
        }
    }
}

struct PartToUpload {
    part_index: usize,
    byte_data: Vec<u8>,
}

#[derive(Debug)]
enum PartIdOrBufferedData {
    PartId(PartId),
    BufferedData { data: Vec<u8> },
}

pub struct MultipartCallbackWithName {
    callback: MultipartCallback,
    name: String,
}

pub enum MultipartCallback {
    InitializedMultipart {
        multipart_id: MultipartId,
    },
    CompletedPart {
        part_idx: usize,
        upload_part: PartId,
    },
    UploadsFinished,
}

impl Debug for MultipartCallback {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MultipartCallback::InitializedMultipart { .. } => {
                write!(f, "MultipartCallback::InitializedMultipart")
            }
            MultipartCallback::CompletedPart { part_idx, .. } => {
                write!(f, "MultipartCallback::CompletedPart({})", part_idx)
            }
            MultipartCallback::UploadsFinished => write!(f, "MultipartCallback::UploadsFinished"),
        }
    }
}

#[derive(Debug, Decode, Encode, Clone, PartialEq, Eq)]
pub struct FileSystemDataRecovery<T: Data> {
    next_file_index: usize,
    active_files: Vec<InProgressFileCheckpoint<T>>,
    delta_version: i64,
}

#[async_trait]
impl<K: Key, T: Data + Sync, R: MultiPartWriter<InputType = T> + Send + 'static>
    TwoPhaseCommitter<K, T> for FileSystemSink<K, T, R>
{
    type DataRecovery = FileSystemDataRecovery<T>;

    type PreCommit = FileToFinish;

    fn name(&self) -> String {
        "filesystem_sink".to_string()
    }

    fn commit_strategy(&self) -> CommitStrategy {
        self.commit_strategy
    }

    async fn init(
        &mut self,
        task_info: &TaskInfo,
        data_recovery: Vec<Self::DataRecovery>,
    ) -> Result<()> {
        let mut max_file_index = 0;
        let mut recovered_files = Vec::new();
        for file_system_data_recovery in data_recovery {
            max_file_index = max_file_index.max(file_system_data_recovery.next_file_index);
            // task 0 is responsible for recovering all files.
            // This is because the number of subtasks may have changed.
            // Recovering should be reasonably fast since it is just finishing in-flight uploads.
            if task_info.task_index == 0 {
                recovered_files.extend(file_system_data_recovery.active_files.into_iter());
            }
        }
        self.sender
            .send(FileSystemMessages::Init {
                max_file_index,
                subtask_id: task_info.task_index,
                recovered_files,
            })
            .await?;
        Ok(())
    }

    async fn insert_record(&mut self, record: &Record<K, T>) -> Result<()> {
        let partition = self
            .partitioner
            .as_ref()
            .map(|partition_fn| partition_fn(record));
        let value = record.value.clone();

        self.sender
            .send(FileSystemMessages::Data {
                value,
                time: record.timestamp,
                partition,
            })
            .await?;
        Ok(())
    }

    async fn commit(
        &mut self,
        _task_info: &TaskInfo,
        pre_commit: Vec<Self::PreCommit>,
    ) -> Result<()> {
        self.sender
            .send(FileSystemMessages::FilesToFinish(pre_commit))
            .await?;
        // loop over checkpoint receiver until finished received
        while let Some(checkpoint_message) = self.checkpoint_receiver.recv().await {
            match checkpoint_message {
                CheckpointData::Finished {
                    max_file_index: _,
                    delta_version: _,
                } => return Ok(()),
                _ => {
                    bail!("unexpected checkpoint message")
                }
            }
        }
        bail!("checkpoint receiver closed unexpectedly")
    }

    async fn checkpoint(
        &mut self,
        task_info: &TaskInfo,
        watermark: Option<SystemTime>,
        stopping: bool,
    ) -> Result<(Self::DataRecovery, HashMap<String, Self::PreCommit>)> {
        self.sender
            .send(FileSystemMessages::Checkpoint {
                subtask_id: task_info.task_index,
                watermark,
                then_stop: stopping,
            })
            .await?;
        let mut pre_commit_messages = HashMap::new();
        let mut active_files = Vec::new();
        while let Some(checkpoint_message) = self.checkpoint_receiver.recv().await {
            match checkpoint_message {
                CheckpointData::Finished {
                    max_file_index,
                    delta_version,
                } => {
                    return Ok((
                        FileSystemDataRecovery {
                            next_file_index: max_file_index + 1,
                            active_files,
                            delta_version,
                        },
                        pre_commit_messages,
                    ))
                }
                CheckpointData::InProgressFileCheckpoint(InProgressFileCheckpoint {
                    filename,
                    partition,
                    data,
                    buffered_data,
                    representative_timestamp,
                    pushed_size,
                }) => {
                    if let FileCheckpointData::MultiPartWriterUploadCompleted {
                        multi_part_upload_id,
                        completed_parts,
                    } = data
                    {
                        pre_commit_messages.insert(
                            filename.clone(),
                            FileToFinish {
                                filename,
                                partition,
                                multi_part_upload_id,
                                completed_parts,
                                size: pushed_size,
                            },
                        );
                    } else {
                        active_files.push(InProgressFileCheckpoint {
                            filename,
                            partition,
                            data,
                            buffered_data,
                            representative_timestamp,
                            pushed_size,
                        })
                    }
                }
            }
        }
        bail!("checkpoint receiver closed unexpectedly")
    }
}
