use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::future::ready;
use std::hash::{Hash, Hasher};
use std::time::SystemTime;

use anyhow::Result;
use arrow::array::RecordBatch;

use arrow::datatypes::SchemaRef;
use arroyo_state::global_table_config;
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use datafusion::common::ScalarValue;
use futures::StreamExt;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

use arroyo_operator::context::ArrowContext;
use regex::Regex;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::select;
use tokio_stream::wrappers::LinesStream;
use tokio_stream::Stream;
use tracing::info;

use crate::filesystem::{CompressionFormat, TableType};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::{grpc::rpc::StopMode, ControlMessage};
use arroyo_storage::StorageProvider;
use arroyo_types::{to_nanos, UserError};

#[allow(unused)]
pub struct FileSystemSourceFunc {
    pub table: TableType,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub file_states: HashMap<String, FileReadState>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, PartialOrd)]
pub enum FileReadState {
    Finished,
    RecordsRead(usize),
}

#[async_trait]
impl SourceOperator for FileSystemSourceFunc {
    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("a", "fs")
    }

    fn name(&self) -> String {
        "FileSystem".to_string()
    }

    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType {
        match self.run_int(ctx).await {
            Ok(s) => s,
            Err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;

                panic!("{}: {}", e.name, e.details);
            }
        }
    }
}

impl FileSystemSourceFunc {
    #[allow(unused)]
    fn get_compression_format(&self) -> CompressionFormat {
        match &self.table {
            TableType::Source {
                compression_format, ..
            } => (*compression_format).unwrap_or(CompressionFormat::None),
            TableType::Sink { .. } => unreachable!(),
        }
    }

    async fn run_int(&mut self, ctx: &mut ArrowContext) -> Result<SourceFinishType, UserError> {
        let (storage_provider, regex_pattern) = match &self.table {
            TableType::Source {
                path,
                storage_options,
                compression_format: _,
                regex_pattern,
            } => {
                let storage_provider =
                    StorageProvider::for_url_with_options(path, storage_options.clone())
                        .await
                        .map_err(|err| {
                            UserError::new("failed to create storage provider", err.to_string())
                        })?;
                let matcher = regex_pattern
                    .as_ref()
                    .map(|pattern| Regex::new(pattern))
                    .transpose()
                    .map_err(|err| {
                        UserError::new(
                            format!("invalid regex pattern {}", regex_pattern.as_ref().unwrap()),
                            err.to_string(),
                        )
                    })?;
                (storage_provider, matcher)
            }
            TableType::Sink { .. } => {
                return Err(UserError::new(
                    "invalid table config",
                    "filesystem source cannot be used as a sink".to_string(),
                ))
            }
        };
        ctx.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
        );
        let parallelism = ctx.task_info.parallelism;
        let task_index = ctx.task_info.task_index;

        // TODO: sort by creation time
        let mut file_paths = storage_provider
            .list(regex_pattern.is_some())
            .await
            .map_err(|err| UserError::new("could not list files", err.to_string()))?
            .filter(|path| {
                let Ok(path) = path else {
                    return ready(true);
                };
                // hash the path and modulo by the number of tasks
                let mut hasher = DefaultHasher::new();
                path.hash(&mut hasher);
                if (hasher.finish() as usize) % parallelism != task_index {
                    return ready(false);
                }

                if let Some(matcher) = &regex_pattern {
                    ready(matcher.is_match(path.as_ref()))
                } else {
                    ready(true)
                }
            });

        let state: &mut GlobalKeyedView<String, (String, FileReadState)> = ctx
            .table_manager
            .get_global_keyed_state("a")
            .await
            .expect("should have table");
        self.file_states = state.get_all().clone().into_values().collect();

        while let Some(path) = file_paths.next().await {
            let obj_key = path
                .map_err(|err| UserError::new("could not get next path", err.to_string()))?
                .to_string();

            if let Some(FileReadState::Finished) = self.file_states.get(&obj_key) {
                // already finished
                continue;
            }

            if let Some(finish_type) = self.read_file(ctx, &storage_provider, &obj_key).await? {
                return Ok(finish_type);
            }
        }
        info!("FileSystem source finished");
        Ok(SourceFinishType::Final)
    }

    async fn get_newline_separated_stream(
        &mut self,
        storage_provider: &StorageProvider,
        path: String,
    ) -> Result<Box<dyn Stream<Item = Result<String, UserError>> + Unpin + Send>, UserError> {
        match &self.format {
            Format::Json(_) => {
                let stream_reader = storage_provider.get_as_stream(path).await.unwrap();

                let compression_reader: Box<dyn AsyncRead + Unpin + Send> =
                    match self.get_compression_format() {
                        CompressionFormat::Zstd => {
                            Box::new(ZstdDecoder::new(BufReader::new(stream_reader)))
                        }
                        CompressionFormat::Gzip => {
                            Box::new(GzipDecoder::new(BufReader::new(stream_reader)))
                        }
                        CompressionFormat::None => Box::new(BufReader::new(stream_reader)),
                    };
                // use line iterators
                let lines = LinesStream::new(BufReader::new(compression_reader).lines());
                Ok(Box::new(lines.map(|string_result| {
                    string_result.map_err(|err| {
                        UserError::new("could not read line from stream", err.to_string())
                    })
                })))
            }
            other => Err(UserError::new(
                "bad format",
                format!("newline separated stream not supported for {:?}", other),
            )),
        }
    }

    async fn get_record_batch_stream(
        &mut self,
        storage_provider: &StorageProvider,
        path: &str,
        out_schema: SchemaRef,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatch, UserError>> + Unpin + Send>, UserError>
    {
        match &self.format {
            Format::Parquet(_) => {
                let object_meta = storage_provider
                    .get_backing_store()
                    .head(&(path.into()))
                    .await
                    .map_err(|err| {
                        UserError::new("could not get object metadata", err.to_string())
                    })?;
                let object_reader =
                    ParquetObjectReader::new(storage_provider.get_backing_store(), object_meta);
                let reader_builder = ParquetRecordBatchStreamBuilder::new(object_reader)
                    .await
                    .map_err(|err| {
                        UserError::new(
                            "could not create parquet record batch stream builder",
                            format!("path:{}, err:{}", path, err),
                        )
                    })?
                    .with_batch_size(8192);
                let stream = reader_builder.build().map_err(|err| {
                    UserError::new(
                        "could not build parquet record batch stream",
                        err.to_string(),
                    )
                })?;
                let result = Box::new(stream.map(move |res| match res {
                    Ok(record_batch) => {
                        // add timestamp
                        let mut columns = record_batch.columns().to_vec();
                            let current_time = to_nanos(SystemTime::now());
                            let current_time_scalar =
                                ScalarValue::TimestampNanosecond(Some(current_time as i64), None);

                            let time_column = current_time_scalar
                                .to_array_of_size(record_batch.num_rows())
                                .unwrap();

                            columns.push(time_column);

                            let out_batch = RecordBatch::try_new(
                                out_schema.clone(),
                                columns
                            ).map_err(|e| UserError::new("data does not match schema",
                                format!("The parquet file has a schema that does not match the table schema: {:?}", e)))?;
                                Ok(out_batch)
                    },
                    Err(err) => Err(UserError::new(
                        "could not read record batch from stream",
                        err.to_string(),
                    )),
                }))
                    as Box<dyn Stream<Item = Result<RecordBatch, UserError>> + Send + Unpin>;
                Ok(result)
            }
            _ => unreachable!("code path only for Parquet"),
        }
    }

    async fn read_file(
        &mut self,
        ctx: &mut ArrowContext,
        storage_provider: &StorageProvider,
        obj_key: &String,
    ) -> Result<Option<SourceFinishType>, UserError> {
        let read_state = self
            .file_states
            .entry(obj_key.to_string())
            .or_insert(FileReadState::RecordsRead(0));
        let records_read = match read_state {
            FileReadState::RecordsRead(records_read) => *records_read,
            FileReadState::Finished => {
                return Err(UserError::new(
                    "reading finished file",
                    format!("{} has already been read", obj_key),
                ));
            }
        };

        match self.format {
            Format::Json(_) => {
                let line_reader = self
                    .get_newline_separated_stream(storage_provider, obj_key.to_string())
                    .await?
                    .skip(records_read);
                self.read_line_file(ctx, line_reader, obj_key, records_read)
                    .await
            }
            Format::Avro(_) => todo!(),
            Format::Parquet(_) => {
                let record_batch_stream = self
                    .get_record_batch_stream(
                        storage_provider,
                        obj_key,
                        ctx.out_schema.as_ref().unwrap().schema.clone(),
                    )
                    .await?
                    .skip(records_read);

                self.read_parquet_file(ctx, record_batch_stream, obj_key, records_read)
                    .await
            }
            Format::RawString(_) => todo!(),
            Format::RawBytes(_) => todo!(),
            Format::Protobuf(_) => todo!("Protobuf not supported"),
        }
    }

    async fn read_parquet_file(
        &mut self,
        ctx: &mut ArrowContext,
        mut record_batch_stream: impl Stream<Item = Result<RecordBatch, UserError>> + Unpin + Send,
        obj_key: &String,
        mut records_read: usize,
    ) -> Result<Option<SourceFinishType>, UserError> {
        loop {
            select! {
                item = record_batch_stream.next() => {
                    match item.transpose()? {
                        Some(batch) => {
                            ctx.collect(batch).await;
                            records_read += 1;
                        }
                        None => {
                            info!("finished reading file {}", obj_key);
                            self.file_states.insert(obj_key.to_string(), FileReadState::Finished);
                            return Ok(None);
                        }
                    }
                },
                msg_res = ctx.control_rx.recv() => {
                    if let Some(control_message) = msg_res {
                        self.file_states.insert(obj_key.to_string(), FileReadState::RecordsRead(records_read));
                        if let Some(finish_type) = self.process_control_message(ctx, control_message).await {
                             return Ok(Some(finish_type))
                        }
                    }
                }
            }
        }
    }

    async fn read_line_file(
        &mut self,
        ctx: &mut ArrowContext,
        mut line_reader: impl Stream<Item = Result<String, UserError>> + Unpin + Send,
        obj_key: &String,
        mut records_read: usize,
    ) -> Result<Option<SourceFinishType>, UserError> {
        loop {
            select! {
                line = line_reader.next() => {
                    match line.transpose()? {
                        Some(line) => {
                            ctx.deserialize_slice(line.as_bytes(), SystemTime::now()).await?;
                            records_read += 1;
                            if ctx.should_flush() {
                                ctx.flush_buffer().await?;
                            }
                        }
                        None => {
                            info!("finished reading file {}", obj_key);
                            ctx.flush_buffer().await?;
                            self.file_states.insert(obj_key.to_string(), FileReadState::Finished);
                            return Ok(None);
                        }
                    }
                },
                msg_res = ctx.control_rx.recv() => {
                    if let Some(control_message) = msg_res {
                        self.file_states.insert(obj_key.to_string(), FileReadState::RecordsRead(records_read));
                        if let Some(finish_type) = self.process_control_message(ctx, control_message).await {
                            return Ok(Some(finish_type))
                        }
                    }
                }
            }
        }
    }

    async fn process_control_message(
        &mut self,
        ctx: &mut ArrowContext,
        control_message: ControlMessage,
    ) -> Option<SourceFinishType> {
        match control_message {
            ControlMessage::Checkpoint(c) => {
                for (file, read_state) in &self.file_states {
                    ctx.table_manager
                        .get_global_keyed_state("a")
                        .await
                        .unwrap()
                        .insert(file.clone(), (file.clone(), read_state.clone()))
                        .await;
                }
                // checkpoint our state
                if self.start_checkpoint(c, ctx).await {
                    Some(SourceFinishType::Immediate)
                } else {
                    None
                }
            }
            ControlMessage::Stop { mode } => {
                info!("Stopping FileSystem source {:?}", mode);
                match mode {
                    StopMode::Graceful => Some(SourceFinishType::Graceful),
                    StopMode::Immediate => Some(SourceFinishType::Immediate),
                }
            }
            ControlMessage::Commit { .. } => {
                unreachable!("sources shouldn't receive commit messages");
            }
            _ => None,
        }
    }
}
