use core::panic;
use std::future::ready;
use std::pin::Pin;
use std::{collections::HashMap, marker::PhantomData};

use anyhow::Result;
use arrow_array::RecordBatch;
use arroyo_state::tables::global_keyed_map::GlobalKeyedState;
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use futures::StreamExt;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncRead;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    select,
};
use tokio::sync::mpsc::Receiver;
use tokio_stream::wrappers::LinesStream;
use tokio_stream::Stream;
use tracing::{info, warn};

use arroyo_formats::{DataDeserializer, SchemaData};
use arroyo_macro::{source_fn, StreamNode};
use arroyo_rpc::formats::BadData;
use arroyo_rpc::{ControlMessage, grpc::StopMode, OperatorConfig};
use arroyo_storage::StorageProvider;
use arroyo_types::{ArrowMessage, Data, SourceError, UserError};
use typify::import_types;
use arroyo_rpc::grpc::api;
use arroyo_rpc::grpc::api::ConnectorOp;

use crate::{RateLimiter, SourceFinishType};
use crate::engine::ArrowContext;
use crate::old::Context;
use crate::operator::{ArrowOperator, ArrowOperatorConstructor, BaseOperator};

import_types!(schema = "../connector-schemas/filesystem/table.json");

pub struct FileSystemSourceFunc {
    table: TableType,
    deserializer: DataDeserializer<()>,
    bad_data: Option<BadData>,
    rate_limiter: RateLimiter,
    file_states: HashMap<String, FileReadState>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, PartialOrd)]
enum FileReadState {
    Finished,
    RecordsRead(usize),
}

impl ArrowOperatorConstructor<api::ConnectorOp, Self> for FileSystemSourceFunc {
    fn from_config(config: ConnectorOp) -> Result<Self> {
        let config: OperatorConfig =
            serde_json::from_str(&config.config).expect("Invalid config for FileSystemSourceFunc");
        let table: FileSystemTable = serde_json::from_value(config.table)
            .expect("should be able to deserialize to FileSystemTable");
        let format = config
            .format
            .expect("Format must be set for filesystem source");

        Ok(Self {
            table: table.table_type,
            deserializer: DataDeserializer::new(format, config.framing),
            bad_data: config.bad_data,
            rate_limiter: RateLimiter::new(),
            file_states: HashMap::new(),
        })
    }
}

#[async_trait]
impl BaseOperator for FileSystemSourceFunc {
    fn tables(&self) -> Vec<arroyo_rpc::grpc::TableDescriptor> {
        vec![arroyo_state::global_table('a', "fs")]
    }

    fn name(&self) -> String {
        "FileSystem".to_string()
    }

    async fn run_behavior(mut self: Box<Self>, ctx: &mut ArrowContext, _: Vec<Receiver<ArrowMessage>>) -> Option<ArrowMessage> {
        match self.run_int(ctx).await {
            Ok(s) => s.into(),
            Err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;

                panic!("{}: {}", e.name, e.details);
            }
        }

    }

    async fn on_start(&mut self, _: &mut ArrowContext) {
    }

    async fn on_close(&mut self, _: &mut ArrowContext) {
    }
}

impl FileSystemSourceFunc {
    fn get_compression_format(&self) -> CompressionFormat {
        match &self.table {
            TableType::Source {
                compression_format, ..
            } => compression_format
                .clone()
                .unwrap_or(CompressionFormat::None),
            TableType::Sink { .. } => unreachable!(),
        }
    }

    async fn run_int(&mut self, ctx: &mut ArrowContext) -> Result<SourceFinishType, UserError> {
        if ctx.task_info.task_index != 0 {
            return Ok(SourceFinishType::Final);
        }

        let (storage_provider, regex_pattern) = match &self.table {
            TableType::Source {
                path,
                storage_options,
                compression_format: _,
                regex_pattern,
            } => {
                let storage_provider =
                    StorageProvider::for_url_with_options(&path, storage_options.clone())
                        .await
                        .map_err(|err| {
                            UserError::new("failed to create storage provider", err.to_string())
                        })?;
                let matcher = regex_pattern
                    .as_ref()
                    .map(|pattern| Regex::new(&pattern))
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

        // TODO: sort by creation time
        let mut file_paths = storage_provider
            .list(regex_pattern.is_some())
            .await
            .map_err(|err| UserError::new("could not list files", err.to_string()))?
            .filter(|path| {
                let Ok(path) = path else {
                    return ready(true);
                };
                if let Some(matcher) = &regex_pattern {
                    ready(matcher.is_match(&path.to_string()))
                } else {
                    ready(true)
                }
            });

        let mut state: GlobalKeyedState<String, (String, FileReadState), _> =
            ctx.state.get_global_keyed_state('a').await;
        self.file_states = state
            .get_all()
            .into_iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        while let Some(path) = file_paths.next().await {
            let obj_key = path
                .map_err(|err| UserError::new("could not get next path", err.to_string()))?
                .to_string();

            if let Some(FileReadState::Finished) = self.file_states.get(&obj_key) {
                // already finished
                continue;
            }

            match self.read_file(ctx, &storage_provider, &obj_key).await? {
                Some(finish_type) => return Ok(finish_type),
                None => (),
            }
        }
        info!("FileSystem source finished");
        Ok(SourceFinishType::Final)
    }

    async fn get_record_batch_stream(
        &mut self,
        storage_provider: &StorageProvider,
        path: String,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatch, UserError>> + Unpin + Send>, UserError>
    {
        let format = self.deserializer.get_format().clone();
        match *format {
            arroyo_rpc::formats::Format::Json(_) => {
                Err(UserError::new("unsupported format:", "json"))
            }
            arroyo_rpc::formats::Format::Avro(_) => {
                Err(UserError::new("unsupported format:", "avro"))
            }
            arroyo_rpc::formats::Format::Parquet(_) => {
                let object_meta = storage_provider
                    .get_backing_store()
                    .head(&(path.clone().into()))
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
                    })?;
                let stream = reader_builder.build().map_err(|err| {
                    UserError::new(
                        "could not build parquet record batch stream",
                        err.to_string(),
                    )
                })?;
                let result = Box::new(stream.map(|res| match res {
                    Ok(record_batch) => Ok(record_batch),
                    Err(err) => Err(UserError::new(
                        "could not read record batch from stream",
                        err.to_string(),
                    )),
                }))
                    as Box<dyn Stream<Item = Result<RecordBatch, UserError>> + Send + Unpin>;
                Ok(result)
            }
            arroyo_rpc::formats::Format::RawString(_) => todo!(),
        }
    }

    // async fn get_item_stream(
    //     &mut self,
    //     storage_provider: &StorageProvider,
    //     path: String,
    // ) -> Result<Box<dyn Stream<Item = Result<T, SourceError>> + Unpin + Send>, UserError> {
    //     let format = self.deserializer.get_format().clone();
    //     match *format {
    //         arroyo_rpc::formats::Format::Json(_) => {
    //             let deserializer = self.deserializer.clone();
    //             let stream_reader = storage_provider.get_as_stream(path).await.unwrap();
    //
    //             let compression_reader: Box<dyn AsyncRead + Unpin + Send> =
    //                 match self.get_compression_format() {
    //                     CompressionFormat::Zstd => {
    //                         Box::new(ZstdDecoder::new(BufReader::new(stream_reader)))
    //                     }
    //                     CompressionFormat::Gzip => {
    //                         Box::new(GzipDecoder::new(BufReader::new(stream_reader)))
    //                     }
    //                     CompressionFormat::None => Box::new(BufReader::new(stream_reader)),
    //                 };
    //             // use line iterators
    //             let lines = LinesStream::new(BufReader::new(compression_reader).lines());
    //             let x = Box::new(lines.map(move |res| match res {
    //                 Ok(line) => deserializer.deserialize_single(line.as_bytes()),
    //                 Err(err) => Err(SourceError::other(
    //                     "could not read line from stream",
    //                     err.to_string(),
    //                 )),
    //             }))
    //                 as Box<dyn Stream<Item = Result<T, SourceError>> + Unpin + Send>;
    //             Ok(x as Box<dyn Stream<Item = Result<T, SourceError>> + Unpin + Send>)
    //         }
    //         arroyo_rpc::formats::Format::Avro(_) => todo!(),
    //         arroyo_rpc::formats::Format::Parquet(_) => {
    //             let object_meta = storage_provider
    //                 .get_backing_store()
    //                 .head(&(path.clone().into()))
    //                 .await
    //                 .map_err(|err| {
    //                     UserError::new("could not get object metadata", err.to_string())
    //                 })?;
    //             let object_reader =
    //                 ParquetObjectReader::new(storage_provider.get_backing_store(), object_meta);
    //             let reader_builder = ParquetRecordBatchStreamBuilder::new(object_reader)
    //                 .await
    //                 .map_err(|err| {
    //                     UserError::new(
    //                         "could not create parquet record batch stream builder",
    //                         format!("path:{}, err:{}", path, err),
    //                     )
    //                 })?;
    //             let stream = reader_builder.build().map_err(|err| {
    //                 UserError::new(
    //                     "could not build parquet record batch stream",
    //                     err.to_string(),
    //                 )
    //             })?;
    //             let result = Box::new(stream.flat_map(|res| match res {
    //                 Ok(record_batch) => {
    //                     let iterator = match T::iterator_from_record_batch(record_batch) {
    //                         Ok(iterator) => iterator.map(|item| Ok(item)),
    //                         Err(err) => {
    //                             return Box::pin(tokio_stream::once(Err(SourceError::other(
    //                                 "could not get iterator from parquet record batch",
    //                                 err.to_string(),
    //                             ))))
    //                                 as Pin<Box<dyn Stream<Item = Result<T, SourceError>> + Send>>
    //                         }
    //                     };
    //
    //                     let stream = futures::stream::iter(iterator);
    //                     Box::pin(stream)
    //                         as Pin<Box<dyn Stream<Item = Result<T, SourceError>> + Send>>
    //                 }
    //                 Err(err) => Box::pin(tokio_stream::once(Err(SourceError::other(
    //                     "could not read record batch from stream",
    //                     err.to_string(),
    //                 ))))
    //                     as Pin<Box<dyn Stream<Item = Result<T, SourceError>> + Send>>,
    //             }))
    //                 as Box<dyn Stream<Item = Result<T, SourceError>> + Send + Unpin>;
    //             Ok(result)
    //         }
    //         arroyo_rpc::formats::Format::RawString(_) => todo!(),
    //     }
    // }

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
        let mut records_read = match read_state {
            FileReadState::RecordsRead(records_read) => *records_read,
            FileReadState::Finished => {
                return Err(UserError::new(
                    "reading finished file",
                    format!("{} has already been read", obj_key),
                ));
            }
        };

        let mut reader = self
            .get_record_batch_stream(storage_provider, obj_key.to_string())
            .await?;

        if records_read > 0 {
            warn!("skipping {} items", records_read);
            for _ in 0..records_read {
                let _ = reader.next().await.ok_or_else(|| {
                    UserError::new(
                        "could not skip item",
                        format!(
                            "based on checkpoint expected {} to have at least {} lines",
                            obj_key, records_read
                        ),
                    )
                })?;
            }
        }

        let out_schema = ctx.out_schema.as_ref().unwrap().clone();

        loop {
            select! {
                item = reader.next() => {
                    match item {
                        Some(value) => {
                            let batch = RecordBatch::try_new(
                                out_schema.schema.clone(),
                                value?.columns().to_vec()
                            ).map_err(|e| UserError::new("data does not match schema",
                                format!("The parquet file has a schema that does not match the table schema: {:?}", e)))?;

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
                        match self.process_control_message(ctx, control_message).await {
                            Some(finish_type) => return Ok(Some(finish_type)),
                            None => ()
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
                    ctx.state
                        .get_global_keyed_state('a')
                        .await
                        .insert(file.clone(), (file.clone(), read_state.clone()))
                        .await;
                }
                // checkpoint our state
                if self.checkpoint(c, ctx).await {
                    Some(SourceFinishType::Immediate)
                } else {
                    None
                }
            }
            ControlMessage::Stop { mode } => {
                info!("Stopping FileSystem source {:?}", mode);
                match mode {
                    StopMode::Graceful => {
                        return Some(SourceFinishType::Graceful);
                    }
                    StopMode::Immediate => {
                        return Some(SourceFinishType::Immediate);
                    }
                }
            }
            ControlMessage::Commit { .. } => {
                unreachable!("sources shouldn't receive commit messages");
            }
            _ => None,
        }
    }
}