use ::arrow::{datatypes::DataType, record_batch::RecordBatch};
use anyhow::{bail, Result};
use arroyo_operator::context::OperatorContext;
use arroyo_rpc::{df::ArroyoSchemaRef, formats::Format, log_trace_event, TIMESTAMP_FIELD};
use arroyo_storage::StorageProvider;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{col, concat, lit, to_char};
use datafusion::{
    common::Column,
    logical_expr::Expr,
    physical_plan::PhysicalExpr,
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    scalar::ScalarValue,
};
use deltalake::DeltaTable;
use futures::{stream::FuturesUnordered, Future};
use futures::{stream::StreamExt, TryStreamExt};
use object_store::{multipart::PartId, path::Path, MultipartId};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info, warn};
use ulid::Ulid;
use uuid::Uuid;

use arroyo_types::*;
pub mod arrow;
mod delta;
pub(crate) mod iceberg;
pub mod json;
pub mod local;
pub mod parquet;
mod two_phase_committer;
use self::{
    json::{JsonLocalWriter, JsonWriter},
    local::LocalFileSystemWriter,
    parquet::{
        batches_by_partition, representitive_timestamp, ParquetBatchBufferingWriter,
        ParquetLocalWriter,
    },
};

use self::iceberg::metadata::IcebergFileMetadata;
use crate::filesystem::config::{NamingConfig, PartitioningConfig};
use crate::filesystem::sink::delta::load_or_create_table;
use crate::filesystem::sink::iceberg::IcebergTable;
use crate::filesystem::{config, FilenameStrategy, TableFormat};
use two_phase_committer::{CommitStrategy, TwoPhaseCommitter, TwoPhaseCommitterOperator};

const DEFAULT_TARGET_PART_SIZE: usize = 32 * 1024 * 1024;

pub struct FileSystemSink<BBW: BatchBufferingWriter> {
    sender: Option<Sender<FileSystemMessages>>,
    partitioner: Option<Arc<dyn PhysicalExpr>>,
    checkpoint_receiver: Option<Receiver<CheckpointData>>,
    table: config::FileSystemSink,
    format: Format,
    table_format: Option<TableFormat>,
    commit_strategy: CommitStrategy,
    event_logger: FsEventLogger,
    _ts: PhantomData<BBW>,
}

pub type ParquetFileSystemSink = FileSystemSink<ParquetBatchBufferingWriter>;

pub type JsonFileSystemSink = FileSystemSink<JsonWriter>;

pub type LocalParquetFileSystemSink = LocalFileSystemWriter<ParquetLocalWriter>;

pub type LocalJsonFileSystemSink = LocalFileSystemWriter<JsonLocalWriter>;

impl<R: BatchBufferingWriter + Send + 'static> FileSystemSink<R> {
    pub fn create_and_start(
        config: config::FileSystemSink,
        table_format: TableFormat,
        format: Format,
        connection_id: Option<String>,
    ) -> TwoPhaseCommitterOperator<Self> {
        TwoPhaseCommitterOperator::new(Self {
            sender: None,
            checkpoint_receiver: None,
            table: config,
            format,
            commit_strategy: match &table_format {
                TableFormat::None => CommitStrategy::PerSubtask,
                TableFormat::Delta | TableFormat::Iceberg(_) => CommitStrategy::PerOperator,
            },
            table_format: Some(table_format),

            partitioner: None,
            event_logger: FsEventLogger {
                task_info: None,
                connection_id: connection_id.unwrap_or_default().into(),
            },
            _ts: Default::default(),
        })
    }

    pub async fn start(&mut self, task_info: Arc<TaskInfo>, schema: ArroyoSchemaRef) -> Result<()> {
        let (sender, receiver) = tokio::sync::mpsc::channel(10000);
        let (checkpoint_sender, checkpoint_receiver) = tokio::sync::mpsc::channel(10000);

        self.event_logger.task_info = Some(task_info.clone());
        self.sender = Some(sender);
        self.checkpoint_receiver = Some(checkpoint_receiver);

        let partition_func =
            get_partitioner_from_file_settings(&self.table.partitioning, schema.clone());
        self.partitioner = partition_func;
        let table = self.table.clone();
        let format = self.format.clone();

        let mut table_format = self.table_format.take().expect("table format must be set");

        let provider = table_format
            .get_storage_provider(
                task_info.clone(),
                &self.table,
                &schema.schema_without_timestamp(),
            )
            .await?;

        let mut writer = AsyncMultipartFileSystemWriter::<R>::new(
            task_info,
            Arc::new(provider),
            receiver,
            checkpoint_sender,
            table,
            table_format,
            format,
            schema,
            self.event_logger.clone(),
        )
        .await?;

        tokio::spawn(async move {
            writer.run().await.unwrap();
        });
        Ok(())
    }
}

fn get_partitioner_from_file_settings(
    partitioning: &PartitioningConfig,
    schema: ArroyoSchemaRef,
) -> Option<Arc<dyn PhysicalExpr>> {
    match (&partitioning.time_pattern, &partitioning.fields) {
        (None, fields) if !fields.is_empty() => {
            Some(partition_string_for_fields(schema, fields).unwrap())
        }
        (None, _) => None,
        (Some(pattern), fields) if !fields.is_empty() => {
            Some(partition_string_for_fields_and_time(schema, fields, pattern).unwrap())
        }
        (Some(pattern), _) => Some(partition_string_for_time(schema, pattern).unwrap()),
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
    time_partition_pattern: &str,
) -> Result<Arc<dyn PhysicalExpr>> {
    let function = timestamp_logical_expression(time_partition_pattern);
    compile_expression(&function, schema)
}

fn partition_string_for_fields_and_time(
    schema: ArroyoSchemaRef,
    partition_fields: &[String],
    time_partition_pattern: &str,
) -> Result<Arc<dyn PhysicalExpr>> {
    let field_function = field_logical_expression(schema.clone(), partition_fields)?;
    let time_function = timestamp_logical_expression(time_partition_pattern);
    let function = concat(vec![
        time_function,
        Expr::Literal(ScalarValue::Utf8(Some("/".to_string())), None),
        field_function,
    ]);
    compile_expression(&function, schema)
}

pub(crate) fn compile_expression(
    expr: &Expr,
    schema: ArroyoSchemaRef,
) -> Result<Arc<dyn PhysicalExpr>> {
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
                    format!("{name}=")
                } else {
                    format!("/{name}=")
                };
                vec![Expr::Literal(ScalarValue::Utf8(Some(preamble)), None), expr]
            })
            .collect(),
    );
    Ok(function)
}

pub(crate) fn timestamp_logical_expression(time_partition_pattern: &str) -> Expr {
    to_char(col(TIMESTAMP_FIELD), lit(time_partition_pattern))
}

#[derive(Clone)]
pub(crate) struct FsEventLogger {
    task_info: Option<Arc<TaskInfo>>,
    connection_id: Arc<String>,
}

impl FsEventLogger {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn log_fs_event(
        &self,
        bytes: usize,
        files: u64,
        write_time: Duration,
        failures: u64,
        failure_message: Option<String>,
        row_groups_written: u64,
        uncompressed_bytes_written: u64,
        rows_written: u64,
    ) {
        let task_info = self.task_info.as_ref().unwrap();
        log_trace_event!("filesystem_write",
        {
            "operator_id": task_info.operator_id,
            "operator_name": task_info.operator_name,
            "connection_id": self.connection_id.as_str(),
            "write_error_reason": failure_message.as_deref().unwrap_or(""),
            "subtask_idx": task_info.task_index,
        }, [
            "bytes_written" => bytes as f64,
            "files_written" => files as f64,
            "write_time_ms" => write_time.as_millis() as f64,
            "write_failures" => failures as f64,
            "row_groups_written" => row_groups_written as f64,
            "uncompressed_bytes_written" => uncompressed_bytes_written as f64,
            "rows_written" => rows_written as f64,
        ]);
    }
}

#[derive(Debug)]
enum FileSystemMessages {
    Data {
        value: RecordBatch,
        partition: Option<String>,
    },
    Init {
        max_file_index: usize,
        recovered_files: Vec<InProgressFileCheckpoint>,
        task_info: Arc<TaskInfo>,
    },
    Checkpoint {
        subtask_id: usize,
        watermark: Option<SystemTime>,
        then_stop: bool,
    },
    FilesToFinish {
        files: Vec<FileToFinish>,
        epoch: u32,
    },
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
        metadata: Option<IcebergFileMetadata>,
    },
    MultiPartInFlight {
        multi_part_upload_id: String,
        in_flight_parts: Vec<InFlightPartCheckpoint>,
        trailing_bytes: Option<Vec<u8>>,
        metadata: Option<IcebergFileMetadata>,
    },
    MultiPartWriterClosed {
        multi_part_upload_id: String,
        in_flight_parts: Vec<InFlightPartCheckpoint>,
        metadata: Option<IcebergFileMetadata>,
    },
    MultiPartWriterUploadCompleted {
        multi_part_upload_id: String,
        completed_parts: Vec<String>,
        metadata: Option<IcebergFileMetadata>,
    },
}

impl std::fmt::Debug for FileCheckpointData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileCheckpointData::Empty => write!(f, "Empty"),
            FileCheckpointData::MultiPartNotCreated {
                parts_to_add,
                trailing_bytes,
                ..
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
                ..
            } => {
                write!(
                    f,
                    "MultiPartInFlight {{ multi_part_upload_id: {multi_part_upload_id}, in_flight_parts: ["
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
                ..
            } => {
                write!(
                    f,
                    "MultiPartWriterClosed {{ multi_part_upload_id: {multi_part_upload_id}, in_flight_parts: ["
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
                ..
            } => {
                write!(f, "MultiPartWriterUploadCompleted {{ multi_part_upload_id: {multi_part_upload_id}, completed_parts: [")?;
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

struct AsyncMultipartFileSystemWriter<BBW: BatchBufferingWriter> {
    active_writers: HashMap<Option<String>, String>,
    watermark: Option<SystemTime>,
    max_file_index: usize,
    task_info: Option<Arc<TaskInfo>>,
    object_store: Arc<StorageProvider>,
    writers: HashMap<String, BatchMultipartWriter<BBW>>,
    receiver: Receiver<FileSystemMessages>,
    checkpoint_sender: Sender<CheckpointData>,
    futures: FuturesUnordered<BoxedTryFuture<MultipartCallbackWithName>>,
    files_to_finish: Vec<FileToFinish>,
    properties: config::FileSystemSink,
    rolling_policies: Vec<RollingPolicy>,
    commit_state: CommitState,
    file_naming: NamingConfig,
    format: Format,
    schema: ArroyoSchemaRef,
    iceberg_schema: Option<::iceberg::spec::SchemaRef>,
    event_logger: FsEventLogger,
}

#[derive(Debug)]
pub enum CommitState {
    DeltaLake {
        last_version: i64,
        table: Box<DeltaTable>,
    },
    Iceberg(Box<IcebergTable>),
    VanillaParquet,
}

impl CommitState {
    fn name(&self) -> &'static str {
        match self {
            CommitState::DeltaLake { .. } => "delta",
            CommitState::Iceberg(_) => "iceberg",
            CommitState::VanillaParquet => "none",
        }
    }
}

async fn from_checkpoint(
    path: &Path,
    partition: Option<String>,
    checkpoint_data: FileCheckpointData,
    mut pushed_size: usize,
    object_store: Arc<StorageProvider>,
) -> Result<Option<FileToFinish>> {
    let mut parts = vec![];
    let (multipart_id, metadata) = match checkpoint_data {
        FileCheckpointData::Empty => {
            return Ok(None);
        }
        FileCheckpointData::MultiPartNotCreated {
            parts_to_add,
            trailing_bytes,
            metadata,
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
                    .await?;
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
            (multipart_id, metadata)
        }
        FileCheckpointData::MultiPartInFlight {
            multi_part_upload_id,
            in_flight_parts,
            trailing_bytes,
            metadata,
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
                            .await?;
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
            (multi_part_upload_id, metadata)
        }
        FileCheckpointData::MultiPartWriterClosed {
            multi_part_upload_id,
            in_flight_parts,
            metadata,
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
            (multi_part_upload_id, metadata)
        }
        FileCheckpointData::MultiPartWriterUploadCompleted {
            multi_part_upload_id,
            completed_parts,
            metadata,
        } => {
            for content_id in completed_parts {
                parts.push(PartId { content_id })
            }
            (multi_part_upload_id, metadata)
        }
    };
    Ok(Some(FileToFinish {
        filename: path.to_string(),
        partition,
        multi_part_upload_id: multipart_id,
        completed_parts: parts.into_iter().map(|p| p.content_id).collect(),
        size: pushed_size,
        metadata,
    }))
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct FileToFinish {
    filename: String,
    partition: Option<String>,
    multi_part_upload_id: String,
    completed_parts: Vec<String>,
    size: usize,
    metadata: Option<IcebergFileMetadata>,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct FinishedFile {
    filename: String,
    partition: Option<String>,
    size: usize,
    metadata: Option<IcebergFileMetadata>,
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

    fn from_file_settings(table: &config::FileSystemSink) -> Vec<RollingPolicy> {
        let mut policies = vec![];
        let part_size_limit = table.multipart.max_parts.map(|d| d.get()).unwrap_or(1000) as usize;

        // this is a hard limit, so will always be present.
        policies.push(RollingPolicy::PartLimit(part_size_limit));
        if let Some(file_size_target) = table.rolling_policy.file_size_bytes {
            policies.push(RollingPolicy::SizeLimit(file_size_target as usize))
        }

        if let Some(s) = table.rolling_policy.inactivity_seconds {
            policies.push(RollingPolicy::InactivityDuration(Duration::from_secs(s)))
        }

        if let Some(s) = table.rolling_policy.interval_seconds {
            policies.push(RollingPolicy::RolloverDuration(Duration::from_secs(s)));
        }

        if let Some(pattern) = &table.partitioning.time_pattern {
            policies.push(RollingPolicy::WatermarkExpiration {
                pattern: pattern.clone(),
            });
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

impl<BBW> AsyncMultipartFileSystemWriter<BBW>
where
    BBW: BatchBufferingWriter,
{
    #[allow(clippy::too_many_arguments)]
    async fn new(
        task_info: Arc<TaskInfo>,
        object_store: Arc<StorageProvider>,
        receiver: Receiver<FileSystemMessages>,
        checkpoint_sender: Sender<CheckpointData>,
        sink_config: config::FileSystemSink,
        table_format: TableFormat,
        format: Format,
        schema: ArroyoSchemaRef,
        event_logger: FsEventLogger,
    ) -> Result<Self> {
        let mut iceberg_schema = None;
        let commit_state = match table_format {
            TableFormat::Delta => CommitState::DeltaLake {
                last_version: -1,
                table: Box::new(
                    load_or_create_table(&object_store, &schema.schema_without_timestamp()).await?,
                ),
            },
            TableFormat::None => CommitState::VanillaParquet,
            TableFormat::Iceberg(mut table) => {
                let t = table.load_or_create(task_info, &schema.schema).await?;
                iceberg_schema = Some(t.metadata().current_schema().clone());
                CommitState::Iceberg(table)
            }
        };
        let mut file_naming = sink_config.file_naming.clone();

        if file_naming.suffix.is_none() {
            file_naming.suffix = Some(BBW::suffix());
        }

        Ok(Self {
            active_writers: HashMap::new(),
            watermark: None,
            max_file_index: 0,
            task_info: None,
            object_store,
            writers: HashMap::new(),
            receiver,
            checkpoint_sender,
            futures: FuturesUnordered::new(),
            files_to_finish: Vec::new(),
            rolling_policies: RollingPolicy::from_file_settings(&sink_config),
            properties: sink_config.clone(),
            commit_state,
            file_naming,
            format,
            schema,
            iceberg_schema,
            event_logger,
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
                        FileSystemMessages::Init {max_file_index, task_info, recovered_files } => {
                            self.max_file_index = max_file_index;
                            info!("recovered files: {:?}", recovered_files);
                            self.task_info = Some(task_info);
                            for recovered_file in recovered_files {
                                let file_to_finish = from_checkpoint(
                                    &Path::parse(&recovered_file.filename)?,
                                    recovered_file.partition.clone(),
                                    recovered_file.data,
                                    recovered_file.pushed_size,
                                    self.object_store.clone()).await?;

                                if let Some(file_to_finish) = file_to_finish {
                                    debug!("adding file to finish: {:?}", file_to_finish);
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
                        FileSystemMessages::FilesToFinish{ files, epoch }  =>{
                            self.finish_files(epoch, files).await?;
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

    fn get_or_insert_writer(
        &mut self,
        partition: &Option<String>,
    ) -> &mut BatchMultipartWriter<BBW> {
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

    fn new_writer(&mut self, partition: &Option<String>) -> BatchMultipartWriter<BBW> {
        let filename_strategy = self.file_naming.strategy.unwrap_or_default();

        // This forms the base for naming files depending on strategy
        let filename_base = match filename_strategy {
            FilenameStrategy::Serial => {
                format!(
                    "{:>05}-{:>03}",
                    self.max_file_index,
                    self.task_info.as_ref().unwrap().task_index
                )
            }
            FilenameStrategy::Ulid => Ulid::new().to_string(),
            FilenameStrategy::Uuid => Uuid::new_v4().to_string(),
            FilenameStrategy::UuidV7 => Uuid::now_v7().to_string(),
        };

        let filename = add_suffix_prefix(
            filename_base,
            self.file_naming.prefix.as_ref(),
            self.file_naming.suffix.as_ref().unwrap(),
        );

        let path = if self.iceberg_schema.is_none() {
            match partition {
                Some(sub_bucket) => format!("{sub_bucket}/{filename}"),
                None => filename,
            }
        } else {
            filename
        };

        BatchMultipartWriter::new(
            self.object_store.clone(),
            path.into(),
            partition.clone(),
            &self.properties,
            self.format.clone(),
            self.schema.clone(),
            self.iceberg_schema.clone(),
            self.event_logger.clone(),
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

    async fn finish_files(&mut self, epoch: u32, files_to_finish: Vec<FileToFinish>) -> Result<()> {
        debug!(
            message = "finishing files",
            number_of_files = files_to_finish.len()
        );
        let mut finished_files: Vec<FinishedFile> = vec![];
        for file_to_finish in files_to_finish {
            if let Some(file) = self.finish_file(file_to_finish).await? {
                finished_files.push(file);
            }
        }

        debug!(
            message = "starting commit",
            commit_type = self.commit_state.name()
        );

        match &mut self.commit_state {
            CommitState::DeltaLake {
                last_version,
                table,
            } => {
                if let Some(new_version) =
                    delta::commit_files_to_delta(&finished_files, table, *last_version).await?
                {
                    *last_version = new_version;
                }
            }
            CommitState::Iceberg(table) => {
                table.commit(epoch, &finished_files).await?;
            }
            CommitState::VanillaParquet => {
                // nothing to do
            }
        }

        debug!(
            message = "finished commit",
            commit_type = self.commit_state.name()
        );

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
            CommitState::Iceberg { .. } => 0,
        }
    }

    async fn finish_file(&mut self, file: FileToFinish) -> Result<Option<FinishedFile>> {
        if file.completed_parts.is_empty() {
            warn!("no parts to finish for file {}", file.filename);
            return Ok(None);
        }

        let parts: Vec<_> = file
            .completed_parts
            .into_iter()
            .map(|content_id| PartId {
                content_id: content_id.clone(),
            })
            .collect();
        let location = Path::parse(&file.filename)?;

        let start = Instant::now();
        match self
            .object_store
            .close_multipart(&location, &file.multi_part_upload_id, parts)
            .await
        {
            Ok(_) => {
                self.event_logger
                    .log_fs_event(0, 1, start.elapsed(), 0, None, 0, 0, 0);

                Ok(Some(FinishedFile {
                    filename: file.filename,
                    partition: file.partition,
                    size: file.size,
                    metadata: file.metadata,
                }))
            }
            Err(err) => {
                self.event_logger.log_fs_event(
                    0,
                    0,
                    start.elapsed(),
                    1,
                    Some(err.to_string()),
                    0,
                    0,
                    0,
                );

                // check if the file is already there with the correct size.
                if let Some(contents) = self.object_store.get_if_present(location).await? {
                    if contents.len() == file.size {
                        Ok(Some(FinishedFile {
                            filename: file.filename,
                            partition: file.partition,
                            size: file.size,
                            metadata: file.metadata,
                        }))
                    } else {
                        bail!(
                            "file written to {} should have length of {}, not {}",
                            file.filename,
                            file.size,
                            contents.len()
                        );
                    }
                } else {
                    bail!(
                        "failed to complete file {}, received an error: {}",
                        file.filename,
                        err
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
        for file_to_finish in self.files_to_finish.drain(..) {
            self.checkpoint_sender
                .send(CheckpointData::InProgressFileCheckpoint(
                    InProgressFileCheckpoint {
                        filename: file_to_finish.filename,
                        partition: file_to_finish.partition,
                        data: FileCheckpointData::MultiPartWriterUploadCompleted {
                            multi_part_upload_id: file_to_finish.multi_part_upload_id,
                            completed_parts: file_to_finish.completed_parts,
                            metadata: file_to_finish.metadata,
                        },
                        pushed_size: file_to_finish.size,
                    },
                ))
                .await?;
        }

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
    event_logger: FsEventLogger,
}

impl MultipartManager {
    fn new(
        object_store: Arc<StorageProvider>,
        location: Path,
        partition: Option<String>,
        event_logger: FsEventLogger,
    ) -> Self {
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
            event_logger,
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

        let event_logger = self.event_logger.clone();
        Ok(Box::pin(async move {
            let start = Instant::now();
            let bytes = part_to_upload.byte_data.len();
            let upload_part = object_store
                .add_multipart(
                    &location,
                    &multipart_id,
                    part_to_upload.part_index,
                    part_to_upload.byte_data,
                )
                .await;
            let elapsed = start.elapsed();

            match &upload_part {
                Ok(_) => event_logger.log_fs_event(bytes, 0, elapsed, 0, None, 0, 0, 0),
                Err(e) => event_logger.log_fs_event(0, 0, elapsed, 1, Some(e.to_string()), 0, 0, 0),
            };

            Ok(MultipartCallbackWithName {
                name: location.to_string(),
                callback: MultipartCallback::CompletedPart {
                    part_idx: part_to_upload.part_index,
                    upload_part: upload_part?,
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
        metadata: &Option<IcebergFileMetadata>,
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
                metadata: metadata.clone(),
            }))
        }
    }
    fn all_uploads_finished(&self) -> bool {
        self.closed && self.uploaded_parts == self.pushed_parts.len()
    }

    fn get_closed_file_checkpoint_data(
        &mut self,
        metadata: &Option<IcebergFileMetadata>,
    ) -> FileCheckpointData {
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
                    metadata: None,
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
                metadata: metadata.clone(),
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
                metadata: metadata.clone(),
            }
        }
    }

    fn get_in_progress_checkpoint(
        &mut self,
        trailing_bytes: Option<Vec<u8>>,
        metadata: Option<IcebergFileMetadata>,
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
                metadata,
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
            metadata,
        }
    }

    fn get_finished_file(&mut self, metadata: &Option<IcebergFileMetadata>) -> FileToFinish {
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
            metadata: metadata.clone(),
        }
    }
}

pub trait BatchBufferingWriter: Send {
    fn new(
        config: &config::FileSystemSink,
        format: Format,
        schema: ArroyoSchemaRef,
        iceberg_schema: Option<::iceberg::spec::SchemaRef>,
        event_logger: FsEventLogger,
    ) -> Self;
    fn suffix() -> String;
    fn add_batch_data(&mut self, data: &RecordBatch);
    fn buffered_bytes(&self) -> usize;
    /// Splits currently written data from the buffer (up to pos) and returns it. Note there may
    /// still be additional interally-buffered data that has not been written out into the
    /// actual buffer, for example in-progress row groups. This method does not flush those
    /// in progress writes.
    ///
    /// Panics if pos > `self.buffered_bytes()`
    fn split_to(&mut self, pos: usize) -> Bytes;
    fn get_trailing_bytes_for_checkpoint(
        &mut self,
    ) -> (Option<Vec<u8>>, Option<IcebergFileMetadata>);
    fn close(
        &mut self,
        final_batch: Option<RecordBatch>,
    ) -> Option<(Bytes, Option<IcebergFileMetadata>)>;
}

pub struct BatchMultipartWriter<BBW: BatchBufferingWriter> {
    batch_buffering_writer: BBW,
    multipart_manager: MultipartManager,
    stats: Option<MultiPartWriterStats>,
    schema: ArroyoSchemaRef,
    target_part_size_bytes: usize,
    pub metadata: Option<IcebergFileMetadata>,
}
impl<BBW: BatchBufferingWriter> BatchMultipartWriter<BBW> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        object_store: Arc<StorageProvider>,
        path: Path,
        partition: Option<String>,
        config: &config::FileSystemSink,
        format: Format,
        schema: ArroyoSchemaRef,
        iceberg_schema: Option<::iceberg::spec::SchemaRef>,
        event_logger: FsEventLogger,
    ) -> Self {
        let batch_buffering_writer = BBW::new(
            config,
            format,
            schema.clone(),
            iceberg_schema,
            event_logger.clone(),
        );

        Self {
            batch_buffering_writer,
            multipart_manager: MultipartManager::new(object_store, path, partition, event_logger),
            stats: None,
            schema,
            target_part_size_bytes: config
                .multipart
                .target_part_size_bytes
                .map(|t| t as usize)
                .unwrap_or(DEFAULT_TARGET_PART_SIZE),
            metadata: None,
        }
    }

    fn name(&self) -> String {
        self.multipart_manager.name()
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

        self.batch_buffering_writer.add_batch_data(&batch);

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
            .handle_completed_part(part_idx, upload_part, &self.metadata)
    }

    fn get_in_progress_checkpoint(&mut self) -> FileCheckpointData {
        if self.multipart_manager.closed {
            self.multipart_manager
                .get_closed_file_checkpoint_data(&self.metadata)
        } else {
            let (bytes, metadata) = self
                .batch_buffering_writer
                .get_trailing_bytes_for_checkpoint();

            self.multipart_manager
                .get_in_progress_checkpoint(bytes, metadata)
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
        self.multipart_manager.get_finished_file(&self.metadata)
    }
}

impl<BBW: BatchBufferingWriter> BatchMultipartWriter<BBW> {
    fn write_closing_multipart(
        &mut self,
    ) -> Result<Vec<BoxedTryFuture<MultipartCallbackWithName>>> {
        self.multipart_manager.closed = true;

        if let Some((bytes, metadata)) = self.batch_buffering_writer.close(None) {
            self.metadata = metadata;
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
                write!(f, "MultipartCallback::CompletedPart({part_idx})")
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
impl<R: BatchBufferingWriter + Send + 'static> TwoPhaseCommitter for FileSystemSink<R> {
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
        self.start(
            ctx.task_info.clone(),
            ctx.in_schemas.first().unwrap().clone(),
        )
        .await?;

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
                task_info: ctx.task_info.clone(),
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
        epoch: u32,
        _task_info: &TaskInfo,
        pre_commit: Vec<Self::PreCommit>,
    ) -> Result<()> {
        self.sender
            .as_ref()
            .unwrap()
            .send(FileSystemMessages::FilesToFinish {
                files: pre_commit,
                epoch,
            })
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
                        metadata,
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
                                metadata,
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
        None => format!("{filename}.{suffix}"),
        Some(prefix) => format!("{prefix}-{filename}.{suffix}"),
    }
}

#[cfg(test)]
mod tests {
    use crate::filesystem::sink::{compile_expression, timestamp_logical_expression};
    use arrow::array::{AsArray, RecordBatch, TimestampNanosecondArray};
    use arroyo_rpc::df::ArroyoSchema;
    use datafusion::logical_expr::ColumnarValue;
    use std::sync::Arc;

    fn test_pattern(pattern: &str, expected: &str) -> anyhow::Result<()> {
        let pattern = timestamp_logical_expression(pattern);

        let test_schema = Arc::new(ArroyoSchema::from_fields(vec![]));

        let expr = compile_expression(&pattern, test_schema.clone()).unwrap();

        let data = RecordBatch::try_new(
            test_schema.schema.clone(),
            vec![Arc::new(TimestampNanosecondArray::from_value(
                1759871368595325952,
                1,
            ))],
        )
        .unwrap();

        match expr.evaluate(&data)? {
            ColumnarValue::Array(a) => {
                assert_eq!(a.as_string::<i32>().value(0), expected)
            }
            ColumnarValue::Scalar(_) => {
                panic!("should be array");
            }
        }

        Ok(())
    }

    #[test]
    fn test_timestamp_udf() {
        test_pattern("%Y", "2025").unwrap();
        test_pattern("%Y-%m-%d", "2025-10-07").unwrap();
        test_pattern("%Y%m%d", "20251007").unwrap();
        test_pattern("%H", "21").unwrap();
        test_pattern("%H:%M:%S", "21:09:28").unwrap();
        test_pattern("%Y/%m/%d/%H", "2025/10/07/21").unwrap();
        test_pattern("year=%Y/month=%m/day=%d", "year=2025/month=10/day=07").unwrap();
        test_pattern("%Y-%m-%dT%H:%M:%S%.3fZ", "2025-10-07T21:09:28.595Z").unwrap();
        test_pattern("literal_text_%Y%m%d", "literal_text_20251007").unwrap();

        // invalid pattern
        test_pattern("%F/%H%M%S%L", "").unwrap_err();
    }
}
