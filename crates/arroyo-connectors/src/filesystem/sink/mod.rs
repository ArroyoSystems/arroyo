use std::{
    any::Any,
    collections::HashMap,
    fmt::{Debug, Formatter},
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use ::arrow::{
    array::StringBuilder,
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
    util::display::{ArrayFormatter, FormatOptions},
};
use anyhow::{bail, Result};
use arroyo_operator::context::OperatorContext;
use arroyo_rpc::{df::ArroyoSchemaRef, formats::Format, OperatorConfig, TIMESTAMP_FIELD};
use arroyo_storage::StorageProvider;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::concat;
use datafusion::{
    common::{Column, Result as DFResult},
    logical_expr::{
        expr::ScalarFunction, Expr, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
    },
    physical_plan::{ColumnarValue, PhysicalExpr},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    scalar::ScalarValue,
};
use deltalake::DeltaTable;
use futures::{stream::FuturesUnordered, Future};
use futures::{stream::StreamExt, TryStreamExt};
use object_store::{multipart::PartId, path::Path, MultipartId};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info, warn};
use uuid::Uuid;

use arroyo_types::*;
pub mod arrow;
mod delta;
pub mod json;
pub mod local;
pub mod parquet;
mod two_phase_committer;

use self::{
    json::{JsonLocalWriter, JsonWriter},
    local::LocalFileSystemWriter,
    parquet::{
        batches_by_partition, representitive_timestamp, ParquetLocalWriter,
        RecordBatchBufferingWriter,
    },
};

use crate::filesystem::sink::delta::load_or_create_table;
use crate::filesystem::{
    CommitStyle, FileNaming, FileSettings, FileSystemTable, FilenameStrategy, TableType,
};
use two_phase_committer::{CommitStrategy, TwoPhaseCommitter, TwoPhaseCommitterOperator};

const DEFAULT_TARGET_PART_SIZE: usize = 32 * 1024 * 1024;

pub struct FileSystemSink<R: MultiPartWriter + Send + 'static> {
    sender: Option<Sender<FileSystemMessages>>,
    partitioner: Option<Arc<dyn PhysicalExpr>>,
    checkpoint_receiver: Option<Receiver<CheckpointData>>,
    table: FileSystemTable,
    format: Option<Format>,
    commit_strategy: CommitStrategy,
    _ts: PhantomData<R>,
}

pub type ParquetFileSystemSink = FileSystemSink<BatchMultipartWriter<RecordBatchBufferingWriter>>;

pub type JsonFileSystemSink = FileSystemSink<BatchMultipartWriter<JsonWriter>>;

pub type LocalParquetFileSystemSink = LocalFileSystemWriter<ParquetLocalWriter>;

pub type LocalJsonFileSystemSink = LocalFileSystemWriter<JsonLocalWriter>;

impl<R: MultiPartWriter + Send + 'static> FileSystemSink<R> {
    pub fn create_and_start(
        table: FileSystemTable,
        format: Option<Format>,
    ) -> TwoPhaseCommitterOperator<Self> {
        let TableType::Sink { file_settings, .. } = table.clone().table_type else {
            unreachable!("multi-part writer can only be used as sink");
        };
        let commit_strategy = match file_settings.as_ref().unwrap().commit_style.unwrap() {
            CommitStyle::Direct => CommitStrategy::PerSubtask,
            CommitStyle::DeltaLake => CommitStrategy::PerOperator,
        };

        TwoPhaseCommitterOperator::new(Self {
            sender: None,
            checkpoint_receiver: None,
            table,
            format,
            commit_strategy,
            partitioner: None,
            _ts: PhantomData,
        })
    }
    pub fn new(
        table_properties: FileSystemTable,
        config: OperatorConfig,
    ) -> TwoPhaseCommitterOperator<Self> {
        Self::create_and_start(table_properties, config.format)
    }

    pub async fn start(&mut self, schema: ArroyoSchemaRef) -> Result<()> {
        let TableType::Sink {
            write_path,
            file_settings,
            format_settings: _,
            storage_options,
            ..
        } = self.table.clone().table_type
        else {
            unreachable!("multi-part writer can only be used as sink");
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(10000);
        let (checkpoint_sender, checkpoint_receiver) = tokio::sync::mpsc::channel(10000);

        self.sender = Some(sender);
        self.checkpoint_receiver = Some(checkpoint_receiver);

        let partition_func = get_partitioner_from_file_settings(
            file_settings.as_ref().unwrap().clone(),
            schema.clone(),
        );
        self.partitioner = partition_func;
        let table = self.table.clone();
        let format = self.format.clone();

        let provider = StorageProvider::for_url_with_options(&write_path, storage_options.clone())
            .await
            .unwrap();

        let mut writer = AsyncMultipartFileSystemWriter::<R>::new(
            Arc::new(provider),
            receiver,
            checkpoint_sender,
            table,
            format,
            schema,
        )
        .await?;

        tokio::spawn(async move {
            writer.run().await.unwrap();
        });
        Ok(())
    }
}

fn get_partitioner_from_file_settings(
    file_settings: FileSettings,
    schema: ArroyoSchemaRef,
) -> Option<Arc<dyn PhysicalExpr>> {
    let partitions = file_settings.partitioning?;
    match (
        partitions.time_partition_pattern,
        partitions.partition_fields.is_empty(),
    ) {
        (None, false) => {
            Some(partition_string_for_fields(schema, &partitions.partition_fields).unwrap())
        }
        (None, true) => None,
        (Some(pattern), false) => Some(
            partition_string_for_fields_and_time(schema, &partitions.partition_fields, pattern)
                .unwrap(),
        ),
        (Some(pattern), true) => Some(partition_string_for_time(schema, pattern).unwrap()),
    }
}

fn partition_string_for_fields(
    schema: ArroyoSchemaRef,
    partition_fields: &[String],
) -> Result<Arc<dyn PhysicalExpr>> {
    let function = field_logical_expression(schema.clone(), partition_fields)?;
    compile_expression(&function, schema)
}

fn partition_string_for_time(
    schema: ArroyoSchemaRef,
    time_partition_pattern: String,
) -> Result<Arc<dyn PhysicalExpr>> {
    let function = timestamp_logical_expression(time_partition_pattern)?;
    compile_expression(&function, schema)
}

fn partition_string_for_fields_and_time(
    schema: ArroyoSchemaRef,
    partition_fields: &[String],
    time_partition_pattern: String,
) -> Result<Arc<dyn PhysicalExpr>> {
    let field_function = field_logical_expression(schema.clone(), partition_fields)?;
    let time_function = timestamp_logical_expression(time_partition_pattern)?;
    let function = concat(vec![
        time_function,
        Expr::Literal(ScalarValue::Utf8(Some("/".to_string()))),
        field_function,
    ]);
    compile_expression(&function, schema)
}

fn compile_expression(expr: &Expr, schema: ArroyoSchemaRef) -> Result<Arc<dyn PhysicalExpr>> {
    let physical_planner = DefaultPhysicalPlanner::default();
    let session_state = SessionStateBuilder::new().build();

    let plan = physical_planner.create_physical_expr(
        expr,
        &(schema.schema.as_ref().clone()).try_into()?,
        &session_state,
    )?;
    Ok(plan)
}

fn field_logical_expression(schema: ArroyoSchemaRef, partition_fields: &[String]) -> Result<Expr> {
    let columns_as_string = partition_fields
        .iter()
        .map(|field| {
            let field = schema.schema.field_with_name(field)?;
            let column_expr = Expr::Column(Column::from_name(field.name().to_string()));
            let expr = match field.data_type() {
                DataType::Utf8 => column_expr,
                _ => Expr::Cast(datafusion::logical_expr::Cast {
                    expr: Box::new(column_expr),
                    data_type: DataType::Utf8,
                }),
            };
            Ok((field.name(), expr))
        })
        .collect::<Result<Vec<_>>>()?;
    let function = concat(
        columns_as_string
            .into_iter()
            .enumerate()
            .flat_map(|(i, (name, expr))| {
                let preamble = if i == 0 {
                    format!("{}=", name)
                } else {
                    format!("/{}=", name)
                };
                vec![Expr::Literal(ScalarValue::Utf8(Some(preamble))), expr]
            })
            .collect(),
    );
    Ok(function)
}

fn timestamp_logical_expression(time_partition_pattern: String) -> Result<Expr> {
    let udf = TimestampFormattingUDF::new(time_partition_pattern);
    let scalar_function = ScalarFunction::new_udf(
        Arc::new(ScalarUDF::new_from_impl(udf)),
        vec![Expr::Column(Column::from_name(TIMESTAMP_FIELD))],
    );
    let function = Expr::ScalarFunction(scalar_function);
    Ok(function)
}

#[derive(Debug)]
enum FileSystemMessages {
    Data {
        value: RecordBatch,
        partition: Option<String>,
    },
    Init {
        max_file_index: usize,
        subtask_id: usize,
        recovered_files: Vec<InProgressFileCheckpoint>,
    },
    Checkpoint {
        subtask_id: usize,
        watermark: Option<SystemTime>,
        then_stop: bool,
    },
    FilesToFinish(Vec<FileToFinish>),
}

#[derive(Debug)]
enum CheckpointData {
    InProgressFileCheckpoint(InProgressFileCheckpoint),
    Finished {
        max_file_index: usize,
        delta_version: i64,
    },
}

#[derive(Decode, Encode, Clone, PartialEq, Eq)]
struct InProgressFileCheckpoint {
    filename: String,
    partition: Option<String>,
    data: FileCheckpointData,
    pushed_size: usize,
}

impl std::fmt::Debug for InProgressFileCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProgressFileCheckpoint")
            .field("filename", &self.filename)
            .field("partition", &self.partition)
            .field("data", &self.data)
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

struct AsyncMultipartFileSystemWriter<R: MultiPartWriter> {
    active_writers: HashMap<Option<String>, String>,
    watermark: Option<SystemTime>,
    max_file_index: usize,
    subtask_id: usize,
    object_store: Arc<StorageProvider>,
    writers: HashMap<String, R>,
    receiver: Receiver<FileSystemMessages>,
    checkpoint_sender: Sender<CheckpointData>,
    futures: FuturesUnordered<BoxedTryFuture<MultipartCallbackWithName>>,
    files_to_finish: Vec<FileToFinish>,
    properties: FileSystemTable,
    rolling_policies: Vec<RollingPolicy>,
    commit_state: CommitState,
    file_naming: FileNaming,
    format: Option<Format>,
    schema: ArroyoSchemaRef,
}

#[derive(Debug)]
pub enum CommitState {
    DeltaLake {
        last_version: i64,
        table: Box<DeltaTable>,
    },
    VanillaParquet,
}

#[async_trait]
pub trait MultiPartWriter {
    fn new(
        object_store: Arc<StorageProvider>,
        path: Path,
        partition: Option<String>,
        config: &FileSystemTable,
        format: Option<Format>,
        schema: ArroyoSchemaRef,
    ) -> Self;

    fn name(&self) -> String;

    fn suffix() -> String;

    fn partition(&self) -> Option<String>;

    async fn insert_batch(
        &mut self,
        value: RecordBatch,
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

    fn close(&mut self) -> Result<Vec<BoxedTryFuture<MultipartCallbackWithName>>>;

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
            debug!("finishing file for path {:?}", path);
            let multipart_id = object_store
                .start_multipart(path)
                .await
                .expect("failed to create multipart upload");
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
            debug!(
                "parts: {:?}, pushed_size: {:?}, multipart id: {:?}",
                parts, pushed_size, multipart_id
            );
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

#[derive(Debug)]
enum RollingPolicy {
    PartLimit(usize),
    SizeLimit(usize),
    InactivityDuration(Duration),
    RolloverDuration(Duration),
    WatermarkExpiration { pattern: String },
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
                let formatted_time = datetime.format(pattern).to_string();
                let representative_datetime: DateTime<Utc> =
                    DateTime::from(stats.representative_timestamp);
                let representative_formatted_time =
                    representative_datetime.format(pattern).to_string();
                formatted_time != representative_formatted_time
            }
        }
    }

    fn from_file_settings(file_settings: &FileSettings) -> Vec<RollingPolicy> {
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
        policies
    }
}

#[derive(Debug, Clone)]
pub struct MultiPartWriterStats {
    bytes_written: usize,
    parts_written: usize,
    part_size: Option<usize>,
    last_write_at: Instant,
    first_write_at: Instant,
    representative_timestamp: SystemTime,
}

impl<R> AsyncMultipartFileSystemWriter<R>
where
    R: MultiPartWriter,
{
    async fn new(
        object_store: Arc<StorageProvider>,
        receiver: Receiver<FileSystemMessages>,
        checkpoint_sender: Sender<CheckpointData>,
        writer_properties: FileSystemTable,
        format: Option<Format>,
        schema: ArroyoSchemaRef,
    ) -> Result<Self> {
        let file_settings = if let TableType::Sink {
            ref file_settings, ..
        } = writer_properties.table_type
        {
            file_settings.as_ref().unwrap()
        } else {
            unreachable!("AsyncMultipartFileSystemWriter can only be used as a sink");
        };

        let commit_state = match file_settings.commit_style.unwrap() {
            CommitStyle::DeltaLake => CommitState::DeltaLake {
                last_version: -1,
                table: Box::new(
                    load_or_create_table(&object_store, &schema.schema_without_timestamp()).await?,
                ),
            },
            CommitStyle::Direct => CommitState::VanillaParquet,
        };
        let mut file_naming = file_settings.file_naming.clone().unwrap_or(FileNaming {
            strategy: Some(FilenameStrategy::Serial),
            prefix: None,
            suffix: None,
        });
        if file_naming.suffix.is_none() {
            file_naming.suffix = Some(R::suffix());
        }

        Ok(Self {
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
            rolling_policies: RollingPolicy::from_file_settings(file_settings),
            properties: writer_properties,
            commit_state,
            file_naming,
            format,
            schema,
        })
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
                        FileSystemMessages::Data{value, partition} => {
                            if let Some(future) = self.get_or_insert_writer(&partition).insert_batch(value).await? {
                                self.futures.push(future);
                            }
                        },
                        FileSystemMessages::Init {max_file_index, subtask_id, recovered_files } => {
                            self.max_file_index = max_file_index;
                            self.subtask_id = subtask_id;
                            info!("recovered files: {:?}", recovered_files);
                            for recovered_file in recovered_files {
                                if let Some(file_to_finish) = from_checkpoint(
                                     &Path::parse(&recovered_file.filename)?, recovered_file.partition.clone(), recovered_file.data, recovered_file.pushed_size, self.object_store.clone()).await? {
                                        info!("adding file to finish: {:?}", file_to_finish);
                                        self.add_part_to_finish(file_to_finish);
                                     }
                            }
                        },
                        FileSystemMessages::Checkpoint { subtask_id, watermark, then_stop } => {
                            self.watermark = watermark;
                            self.flush_futures().await?;
                            if then_stop {
                                self.stop().await?;
                            }
                            self.take_checkpoint(subtask_id).await?;
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

                            let should_roll = self.rolling_policies.iter()
                            .find(|p| {
                                p.should_roll(&stats, self.watermark)
                            });

                            if let Some(policy) = should_roll {
                                debug!("rolling file {} due to policy {:?}", filename, policy);
                                let futures = writer.close()?;
                                if !futures.is_empty() {
                                    self.futures.extend(futures.into_iter());
                                    removed_partitions.push((partition.clone(), false));
                                } else {
                                    removed_partitions.push((partition.clone(), true));
                                }
                            }
                        }
                    }
                    if !removed_partitions.is_empty() {
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
        let filename_strategy = match self.file_naming.strategy {
            Some(FilenameStrategy::Uuid) => FilenameStrategy::Uuid,
            Some(FilenameStrategy::Serial) => FilenameStrategy::Serial,
            None => FilenameStrategy::Serial,
        };

        // This forms the base for naming files depending on strategy
        let filename_base = if filename_strategy == FilenameStrategy::Uuid {
            Uuid::new_v4().to_string()
        } else {
            format!("{:>05}-{:>03}", self.max_file_index, self.subtask_id)
        };
        let filename = add_suffix_prefix(
            filename_base,
            self.file_naming.prefix.as_ref(),
            self.file_naming.suffix.as_ref().unwrap(),
        );

        let path = match partition {
            Some(sub_bucket) => format!("{}/{}", sub_bucket, filename),
            None => filename.clone(),
        };

        R::new(
            self.object_store.clone(),
            path.into(),
            partition.clone(),
            &self.properties,
            self.format.clone(),
            self.schema.clone(),
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
        let Some(writer) = self.writers.get_mut(&name) else {
            warn!("missing writer {} for callback {:?}", name, callback);
            return Ok(());
        };

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
        if let CommitState::DeltaLake {
            last_version,
            table,
        } = &mut self.commit_state
        {
            if let Some(new_version) =
                delta::commit_files_to_delta(&finished_files, table, *last_version).await?
            {
                *last_version = new_version;
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
        match &self.commit_state {
            CommitState::DeltaLake { last_version, .. } => *last_version,
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
        if completed_parts.is_empty() {
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
                let contents = self.object_store.get(location).await?;
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
        for filename in self.active_writers.values() {
            let writer = self.writers.get_mut(filename).unwrap();
            let close_futures = writer.close()?;
            if !close_futures.is_empty() {
                self.futures.extend(close_futures);
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
            let in_progress_checkpoint =
                CheckpointData::InProgressFileCheckpoint(InProgressFileCheckpoint {
                    filename: filename.clone(),
                    partition: writer.partition(),
                    data,
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
    storage_provider: Arc<StorageProvider>,
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
            storage_provider: object_store,
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
        data: Bytes,
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
        let object_store = self.storage_provider.clone();
        Ok(Box::pin(async move {
            let upload_part = object_store
                .add_multipart(
                    &location,
                    &multipart_id,
                    part_to_upload.part_index,
                    part_to_upload.byte_data,
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
        let object_store = self.storage_provider.clone();
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
                        .map(|val| val.byte_data.to_vec())
                        .collect(),
                    trailing_bytes: None,
                };
            }
        };
        if self.all_uploads_finished() {
            FileCheckpointData::MultiPartWriterUploadCompleted {
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
            }
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
                            data: data.to_vec(),
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
                    .map(|val| val.byte_data.to_vec())
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
                        data: data.to_vec(),
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

pub trait BatchBufferingWriter: Send {
    fn new(config: &FileSystemTable, format: Option<Format>, schema: ArroyoSchemaRef) -> Self;
    fn suffix() -> String;
    fn add_batch_data(&mut self, data: RecordBatch);
    fn buffered_bytes(&self) -> usize;
    /// Splits currently written data from the buffer (up to pos) and returns it. Note there may
    /// still be additional interally-buffered data that has not been written out into the
    /// actual buffer, for example in-progress row groups. This method does not flush those
    /// in progress writes.
    ///
    /// Panics if pos > `self.buffered_bytes()`
    fn split_to(&mut self, pos: usize) -> Bytes;
    fn get_trailing_bytes_for_checkpoint(&mut self) -> Option<Vec<u8>>;
    fn close(&mut self, final_batch: Option<RecordBatch>) -> Option<Bytes>;
}
pub struct BatchMultipartWriter<BBW: BatchBufferingWriter> {
    batch_buffering_writer: BBW,
    multipart_manager: MultipartManager,
    stats: Option<MultiPartWriterStats>,
    schema: ArroyoSchemaRef,
    target_part_size_bytes: usize,
}
#[async_trait]
impl<BBW: BatchBufferingWriter> MultiPartWriter for BatchMultipartWriter<BBW> {
    fn new(
        object_store: Arc<StorageProvider>,
        path: Path,
        partition: Option<String>,
        config: &FileSystemTable,
        format: Option<Format>,
        schema: ArroyoSchemaRef,
    ) -> Self {
        let batch_buffering_writer = BBW::new(config, format, schema.clone());

        let FileSystemTable {
            table_type:
                TableType::Sink {
                    file_settings:
                        Some(FileSettings {
                            target_part_size, ..
                        }),
                    ..
                },
        } = config
        else {
            panic!("FileSystem source configuration in sink");
        };

        Self {
            batch_buffering_writer,
            multipart_manager: MultipartManager::new(object_store, path, partition),
            stats: None,
            schema,
            target_part_size_bytes: target_part_size
                .map(|t| t as usize)
                .unwrap_or(DEFAULT_TARGET_PART_SIZE),
        }
    }

    fn name(&self) -> String {
        self.multipart_manager.name()
    }

    fn suffix() -> String {
        BBW::suffix()
    }

    fn partition(&self) -> Option<String> {
        self.multipart_manager.partition.clone()
    }

    async fn insert_batch(
        &mut self,
        batch: RecordBatch,
    ) -> Result<Option<BoxedTryFuture<MultipartCallbackWithName>>> {
        if self.stats.is_none() {
            let representative_timestamp =
                representitive_timestamp(batch.column(self.schema.timestamp_index))?;
            self.stats = Some(MultiPartWriterStats {
                bytes_written: 0,
                parts_written: 0,
                part_size: None,
                last_write_at: Instant::now(),
                first_write_at: Instant::now(),
                representative_timestamp,
            });
        }

        let stats = self.stats.as_mut().unwrap();
        stats.last_write_at = Instant::now();

        self.batch_buffering_writer.add_batch_data(batch);

        let bytes = match stats.part_size {
            None => {
                if self.batch_buffering_writer.buffered_bytes() >= self.target_part_size_bytes {
                    let buf = self
                        .batch_buffering_writer
                        .split_to(self.target_part_size_bytes);
                    assert!(!buf.is_empty(), "Trying to write empty part file");
                    stats.part_size = Some(buf.len());
                    Some(buf)
                } else {
                    None
                }
            }
            Some(part_size) => (self.batch_buffering_writer.buffered_bytes() >= part_size)
                .then(|| self.batch_buffering_writer.split_to(part_size)),
        };

        if let Some(bytes) = bytes {
            stats.bytes_written += bytes.len();
            stats.parts_written += 1;
            self.multipart_manager.write_next_part(bytes)
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

    fn close(&mut self) -> Result<Vec<BoxedTryFuture<MultipartCallbackWithName>>> {
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

impl<BBW: BatchBufferingWriter> BatchMultipartWriter<BBW> {
    fn write_closing_multipart(
        &mut self,
    ) -> Result<Vec<BoxedTryFuture<MultipartCallbackWithName>>> {
        self.multipart_manager.closed = true;

        if let Some(bytes) = self.batch_buffering_writer.close(None) {
            let existing_part_size = self.stats.as_ref().and_then(|s| s.part_size);

            if self
                .multipart_manager
                .storage_provider
                .requires_same_part_sizes()
                && existing_part_size.is_some()
                && bytes.len() > existing_part_size.unwrap()
            {
                // our last part is bigger than our part size, which isn't allowed by some object stores
                // so we need to split it up
                let part_size = existing_part_size.unwrap();
                debug!("final multipart upload ({}) is bigger than part size ({}) so splitting into two",
                    bytes.len(), part_size);
                let mut part1 = bytes;
                let part2 = part1.split_off(part_size);
                // these can't be None, so safe to unwrap
                let f1 = self.multipart_manager.write_next_part(part1)?.unwrap();
                let f2 = self.multipart_manager.write_next_part(part2)?.unwrap();
                Ok(vec![f1, f2])
            } else {
                Ok(self
                    .multipart_manager
                    .write_next_part(bytes)?
                    .into_iter()
                    .collect())
            }
        } else if self.multipart_manager.all_uploads_finished() {
            // Return a finished file future
            let name = self.multipart_manager.name();
            Ok(vec![Box::pin(async move {
                Ok(MultipartCallbackWithName {
                    name,
                    callback: MultipartCallback::UploadsFinished,
                })
            })])
        } else {
            Ok(vec![])
        }
    }
}

struct PartToUpload {
    part_index: usize,
    byte_data: Bytes,
}

#[derive(Debug)]
enum PartIdOrBufferedData {
    PartId(PartId),
    BufferedData { data: Bytes },
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
pub struct FileSystemDataRecovery {
    next_file_index: usize,
    active_files: Vec<InProgressFileCheckpoint>,
    delta_version: i64,
}

#[async_trait]
impl<R: MultiPartWriter + Send + 'static> TwoPhaseCommitter for FileSystemSink<R> {
    type DataRecovery = FileSystemDataRecovery;

    type PreCommit = FileToFinish;

    fn name(&self) -> String {
        "filesystem_sink".to_string()
    }

    fn commit_strategy(&self) -> CommitStrategy {
        self.commit_strategy
    }

    async fn init(
        &mut self,
        ctx: &mut OperatorContext,
        data_recovery: Vec<Self::DataRecovery>,
    ) -> Result<()> {
        self.start(ctx.in_schemas.first().unwrap().clone()).await?;

        let mut max_file_index = 0;
        let mut recovered_files = Vec::new();

        for file_system_data_recovery in data_recovery {
            max_file_index = max_file_index.max(file_system_data_recovery.next_file_index);
            // task 0 is responsible for recovering all files.
            // This is because the number of subtasks may have changed.
            // Recovering should be reasonably fast since it is just finishing in-flight uploads.
            if ctx.task_info.task_index == 0 {
                recovered_files.extend(file_system_data_recovery.active_files.into_iter());
            }
        }
        self.sender
            .as_ref()
            .unwrap()
            .send(FileSystemMessages::Init {
                max_file_index,
                subtask_id: ctx.task_info.task_index as usize,
                recovered_files,
            })
            .await?;
        Ok(())
    }

    async fn insert_batch(&mut self, record: RecordBatch) -> Result<()> {
        // TODO: implement partitioning
        match &self.partitioner {
            None => {
                self.sender
                    .as_ref()
                    .unwrap()
                    .send(FileSystemMessages::Data {
                        value: record,
                        partition: None,
                    })
                    .await?;
            }
            Some(partitioner) => {
                for (batch, partition) in batches_by_partition(record, partitioner.clone())? {
                    self.sender
                        .as_ref()
                        .unwrap()
                        .send(FileSystemMessages::Data {
                            value: batch,
                            partition,
                        })
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn commit(
        &mut self,
        _task_info: &TaskInfo,
        pre_commit: Vec<Self::PreCommit>,
    ) -> Result<()> {
        self.sender
            .as_ref()
            .unwrap()
            .send(FileSystemMessages::FilesToFinish(pre_commit))
            .await?;
        // loop over checkpoint receiver until finished received
        if let Some(checkpoint_message) = self.checkpoint_receiver.as_mut().unwrap().recv().await {
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
            .as_ref()
            .unwrap()
            .send(FileSystemMessages::Checkpoint {
                subtask_id: task_info.task_index as usize,
                watermark,
                then_stop: stopping,
            })
            .await?;
        let mut pre_commit_messages = HashMap::new();
        let mut active_files = Vec::new();
        while let Some(checkpoint_message) = self.checkpoint_receiver.as_mut().unwrap().recv().await
        {
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
                            pushed_size,
                        })
                    }
                }
            }
        }
        bail!("checkpoint receiver closed unexpectedly")
    }
}

pub(crate) fn add_suffix_prefix(
    filename: String,
    prefix: Option<&String>,
    suffix: &String,
) -> String {
    match prefix {
        None => format!("{}.{}", filename, suffix),
        Some(prefix) => format!("{}-{}.{}", prefix, filename, suffix),
    }
}

#[derive(Debug)]
pub struct TimestampFormattingUDF {
    // TODO: figure out how to manage this with lifetimes.
    pattern: String,
    signature: Signature,
}

impl TimestampFormattingUDF {
    // Initialization method
    pub fn new(pattern: String) -> Self {
        TimestampFormattingUDF {
            pattern,
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None)]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TimestampFormattingUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "partitioning_formatter"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let (array, scalar) = match &args[0] {
            ColumnarValue::Array(array) => (array.clone(), false),
            ColumnarValue::Scalar(scalar) => (scalar.to_array_of_size(1)?, true),
        };
        let items = array.len();
        let mut result = StringBuilder::with_capacity(items, 20 * items);
        let format_options = FormatOptions::new().with_timestamp_format(Some(&self.pattern));
        let formatter = ArrayFormatter::try_new(array.as_ref(), &format_options)?;
        for i in 0..items {
            result.append_value(format!("{}", formatter.value(i)));
        }
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                result.finish().value(0).to_string(),
            ))))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }
    }
}
