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

use arroyo_operator::context::{SourceCollector, SourceContext};
use regex::Regex;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::select;
use tokio_stream::wrappers::LinesStream;
use tokio_stream::Stream;
use tracing::info;

use crate::filesystem::config;
use crate::filesystem::config::SourceFileCompressionFormat;
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::errors::{DataflowError};
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::{connector_err, grpc::rpc::StopMode, ControlMessage};
use arroyo_storage::StorageProvider;
use arroyo_types::to_nanos;

#[allow(unused)]
pub struct FileSystemSourceFunc {
    pub source: config::FileSystemSource,
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

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, DataflowError> {
        let storage_provider = StorageProvider::for_url_with_options(
            &self.source.path,
            self.source.storage_options.clone(),
        )
            .await
            .map_err(|err| connector_err!(User, NoRetry, source: err.into(), "failed to construct storage provider"))?;

        let regex_pattern = self
            .source
            .regex_pattern
            .as_ref()
            .map(|pattern| Regex::new(pattern))
            .transpose()
            .map_err(|err| {
                connector_err!(User, NoRetry, source: err.into(),
                        "invalid regex pattern {}",
                        self.source.regex_pattern.as_ref().unwrap())
            })?;

        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &[],
        );
        let parallelism = ctx.task_info.parallelism;
        let task_index = ctx.task_info.task_index;

        // TODO: sort by creation time
        let mut file_paths = storage_provider
            .list(regex_pattern.is_some())
            .await
            .map_err(|err| connector_err!(External, WithBackoff, source: err.into(), "could not list files"))?
            .filter(|path| {
                let Ok(path) = path else {
                    return ready(true);
                };
                // hash the path and modulo by the number of tasks
                let mut hasher = DefaultHasher::new();
                path.hash(&mut hasher);
                if (hasher.finish() as usize) % parallelism as usize != task_index as usize {
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
                .map_err(|err| connector_err!(External, WithBackoff, source: err.into(), "could not get next path"))?
                .to_string();

            if let Some(FileReadState::Finished) = self.file_states.get(&obj_key) {
                // already finished
                continue;
            }

            if let Some(finish_type) = self
                .read_file(ctx, collector, &storage_provider, &obj_key)
                .await?
            {
                return Ok(finish_type);
            }
        }
        info!("FileSystem source finished");
        Ok(SourceFinishType::Final)
    }}

impl FileSystemSourceFunc {
    async fn get_newline_separated_stream(
        &mut self,
        storage_provider: &StorageProvider,
        path: String,
    ) -> Result<Box<dyn Stream<Item = Result<String, DataflowError>> + Unpin + Send>, DataflowError> {
        match &self.format {
            Format::Json(_) => {
                let stream_reader = storage_provider.get_as_stream(path).await.unwrap();

                let compression_reader: Box<dyn AsyncRead + Unpin + Send> = match self
                    .source
                    .compression_format
                {
                    SourceFileCompressionFormat::Zstd => {
                        Box::new(ZstdDecoder::new(BufReader::new(stream_reader)))
                    }
                    SourceFileCompressionFormat::Gzip => {
                        Box::new(GzipDecoder::new(BufReader::new(stream_reader)))
                    }
                    SourceFileCompressionFormat::None => Box::new(BufReader::new(stream_reader)),
                };
                // use line iterators
                let lines = LinesStream::new(BufReader::new(compression_reader).lines());
                Ok(Box::new(lines.map(|string_result| {
                    string_result.map_err(|err| connector_err!(External, WithBackoff, source: err.into(), "could not get next path"))
                })))
            }
            other => Err(connector_err!(User, NoRetry, "newline separated stream not supported for {other:?}")),
        }
    }

    async fn get_record_batch_stream(
        &mut self,
        storage_provider: &StorageProvider,
        path: &str,
        out_schema: SchemaRef,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatch, DataflowError>> + Unpin + Send>, DataflowError>
    {
        match &self.format {
            Format::Parquet(_) => {
                let object_meta = storage_provider
                    .get_backing_store()
                    .head(&(path.into()))
                    .await
                    .map_err(|err| connector_err!(External, WithBackoff, source: err.into(), "could not get object metadata"))?;
                let object_reader = ParquetObjectReader::new(
                    storage_provider.get_backing_store(),
                    object_meta.location,
                )
                .with_file_size(object_meta.size);
                let reader_builder = ParquetRecordBatchStreamBuilder::new(object_reader)
                    .await
                    .map_err(|err| connector_err!(External, WithBackoff, source: err.into(), "could not construct parquet reader for file {path}"))?
                    .with_batch_size(8192);

                let stream = reader_builder.build().map_err(|err| {
                    connector_err!(External, WithBackoff, source: err.into(), "could not construct parquet stream for file {path}")
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

                            RecordBatch::try_new(
                                out_schema.clone(),
                                columns
                            ).map_err(|e| connector_err!(User, NoRetry, source: e.into(), "The parquet file has a schema that does not match the table schema"))
                    },
                    Err(err) => Err(connector_err!(
                        User, NoRetry, source: err.into(),
                        "could not read record batch from stream",
                    )),
                }))
                    as Box<dyn Stream<Item = Result<RecordBatch, DataflowError>> + Send + Unpin>;
                Ok(result)
            }
            _ => unreachable!("code path only for Parquet"),
        }
    }

    async fn read_file(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
        storage_provider: &StorageProvider,
        obj_key: &String,
    ) -> Result<Option<SourceFinishType>, DataflowError> {
        let read_state = self
            .file_states
            .entry(obj_key.to_string())
            .or_insert(FileReadState::RecordsRead(0));
        let records_read = match read_state {
            FileReadState::RecordsRead(records_read) => *records_read,
            FileReadState::Finished => {
                return Err(connector_err!(
                    User,
                    NoRetry,
                    "{obj_key} has already been read",
                ));
            }
        };

        match self.format {
            Format::Json(_) => {
                let line_reader = self
                    .get_newline_separated_stream(storage_provider, obj_key.to_string())
                    .await?
                    .skip(records_read);
                self.read_line_file(ctx, collector, line_reader, obj_key, records_read)
                    .await
            }
            Format::Avro(_) => todo!(),
            Format::Parquet(_) => {
                let record_batch_stream = self
                    .get_record_batch_stream(
                        storage_provider,
                        obj_key,
                        ctx.out_schema.schema.clone(),
                    )
                    .await?
                    .skip(records_read);

                self.read_parquet_file(ctx, collector, record_batch_stream, obj_key, records_read)
                    .await
            }
            Format::RawString(_) => todo!(),
            Format::RawBytes(_) => todo!(),
            Format::Protobuf(_) => todo!("Protobuf not supported"),
        }
    }

    async fn read_parquet_file(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
        mut record_batch_stream: impl Stream<Item = Result<RecordBatch, DataflowError>> + Unpin + Send,
        obj_key: &String,
        mut records_read: usize,
    ) -> Result<Option<SourceFinishType>, DataflowError> {
        loop {
            select! {
                item = record_batch_stream.next() => {
                    match item.transpose()? {
                        Some(batch) => {
                            collector.collect(batch).await;
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
                        if let Some(finish_type) = self.process_control_message(ctx, collector, control_message).await {
                             return Ok(Some(finish_type))
                        }
                    }
                }
            }
        }
    }

    async fn read_line_file(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
        mut line_reader: impl Stream<Item = Result<String, DataflowError>> + Unpin + Send,
        obj_key: &String,
        mut records_read: usize,
    ) -> Result<Option<SourceFinishType>, DataflowError> {
        loop {
            select! {
                line = line_reader.next() => {
                    match line.transpose()? {
                        Some(line) => {
                            collector.deserialize_slice(line.as_bytes(), SystemTime::now(), None).await?;
                            records_read += 1;
                            if collector.should_flush() {
                                collector.flush_buffer().await?;
                            }
                        }
                        None => {
                            info!("finished reading file {}", obj_key);
                            collector.flush_buffer().await?;
                            self.file_states.insert(obj_key.to_string(), FileReadState::Finished);
                            return Ok(None);
                        }
                    }
                },
                msg_res = ctx.control_rx.recv() => {
                    if let Some(control_message) = msg_res {
                        self.file_states.insert(obj_key.to_string(), FileReadState::RecordsRead(records_read));
                        if let Some(finish_type) = self.process_control_message(ctx, collector, control_message).await {
                            return Ok(Some(finish_type))
                        }
                    }
                }
            }
        }
    }

    async fn process_control_message(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
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
                if self.start_checkpoint(c, ctx, collector).await {
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
