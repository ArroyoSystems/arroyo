use std::marker::PhantomData;
use std::time::SystemTime;

use anyhow::Result;
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, BufReader, Lines},
    select,
};
use tracing::info;

use arroyo_macro::{source_fn, StreamNode};
use arroyo_rpc::{grpc::StopMode, ControlMessage, ControlResp, OperatorConfig};
use arroyo_storage::StorageProvider;
use arroyo_types::{Data, Record, UserError};
use futures::stream::StreamExt;
use typify::import_types;

use crate::{engine::Context, formats::DataDeserializer, SchemaData, SourceFinishType};

import_types!(schema = "../connector-schemas/filesystem/table.json");

#[derive(StreamNode)]
pub struct FileSystemSourceFunc<K: Data, T: SchemaData + Data> {
    table: TableType,
    deserializer: DataDeserializer<T>,
    total_lines_read: usize,
    _t: PhantomData<(K, T)>,
}

#[source_fn(out_t = T)]
impl<K: Data, T: SchemaData + Data> FileSystemSourceFunc<K, T> {
    pub fn from_config(config_str: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config_str).expect("Invalid config for FileSystemSourceFunc");
        let table: TableType = serde_json::from_value(config.table)
            .expect("Invalid table config for FileSystemSourceFunc");
        let format = config
            .format
            .expect("Format must be set for filesystem source");

        Self {
            table,
            deserializer: DataDeserializer::new(format, config.framing),
            total_lines_read: 0,
            _t: PhantomData,
        }
    }

    pub fn tables(&self) -> Vec<arroyo_rpc::grpc::TableDescriptor> {
        vec![arroyo_state::global_table('a', "fs")]
    }

    fn name(&self) -> String {
        "FileSystem".to_string()
    }

    async fn run(&mut self, ctx: &mut Context<(), T>) -> SourceFinishType {
        match self.run_int(ctx).await {
            Ok(r) => r,
            Err(e) => {
                ctx.control_tx
                    .send(ControlResp::Error {
                        operator_id: ctx.task_info.operator_id.clone(),
                        task_index: ctx.task_info.task_index,
                        message: e.name.clone(),
                        details: e.details.clone(),
                    })
                    .await
                    .unwrap();

                panic!("{}: {}", e.name, e.details);
            }
        }
    }

    async fn run_int(&mut self, ctx: &mut Context<(), T>) -> Result<SourceFinishType, UserError> {
        if ctx.task_info.task_index != 0 {
            return Ok(SourceFinishType::Final);
        }

        let (storage_provider, compression_format, prefix) = match &self.table {
            TableType::Source { read_source } => match read_source {
                Some(S3 {
                    bucket,
                    compression_format,
                    ref prefix,
                    region,
                }) => {
                    let storage_provider = StorageProvider::for_url(&format!(
                        "https://{}.amazonaws.com/{}/{}",
                        region.as_ref().ok_or(UserError::new(
                            "invalid table config",
                            "region must be set for S3 source".to_string()
                        ))?,
                        bucket.as_ref().ok_or(UserError::new(
                            "invalid table config",
                            "bucket must be set for S3 source"
                        ))?,
                        prefix.as_ref().ok_or(UserError::new(
                            "invalid table config",
                            "prefix must be set for S3 source"
                        ))?
                    ))
                    .await
                    .map_err(|err| {
                        UserError::new("failed to create storage provider", err.to_string())
                    })?;
                    (
                        storage_provider,
                        compression_format.ok_or(UserError::new(
                            "invalid table config",
                            "compression_format must be set for S3 source".to_string(),
                        ))?,
                        prefix.clone().ok_or(UserError::new(
                            "invalid table config",
                            "prefix must be set for S3 source".to_string(),
                        ))?,
                    )
                }
                None => {
                    return Err(UserError::new(
                        "invalid table config",
                        "no read source specified for filesystem source".to_string(),
                    ))
                }
            },
            TableType::Sink { .. } => {
                return Err(UserError::new(
                    "invalid table config",
                    "filesystem source cannot be used as a sink".to_string(),
                ))
            }
        };

        // TODO: sort by creation time
        let mut file_paths = storage_provider
            .list_stream(prefix.clone())
            .await
            .map_err(|err| UserError::new("could not list files", err.to_string()))?;

        let (prev_s3_key, prev_lines_read) = ctx
            .state
            .get_global_keyed_state('a')
            .await
            .get(&prefix)
            .map(|v: &(String, usize)| v.clone())
            .unwrap_or_default();

        let mut prev_file_found = prev_s3_key == "";

        // let mut contents = list_res.contents.unwrap();

        while let Some(path) = file_paths.next().await {
            let obj_key = path
                .map(|p| p.to_string())
                .map_err(|err| UserError::new("could not get next path", err.to_string()))?;
            if obj_key.ends_with('/') {
                continue;
            }

            // if restoring from checkpoint, skip until we find the file we were on
            if !prev_file_found && obj_key != prev_s3_key {
                continue;
            }
            prev_file_found = true;

            let stream_reader = storage_provider.get_stream(&obj_key).await.unwrap();

            match compression_format {
                CompressionFormat::Zstd => {
                    let r = ZstdDecoder::new(BufReader::new(stream_reader));
                    match self
                        .read_file(
                            ctx,
                            BufReader::new(r).lines(),
                            &prefix,
                            &obj_key,
                            prev_lines_read,
                        )
                        .await?
                    {
                        Some(finish_type) => return Ok(finish_type),
                        None => (),
                    }
                }
                CompressionFormat::Gzip => {
                    let r = GzipDecoder::new(BufReader::new(stream_reader));
                    match self
                        .read_file(
                            ctx,
                            BufReader::new(r).lines(),
                            &prefix,
                            &obj_key,
                            prev_lines_read,
                        )
                        .await?
                    {
                        Some(finish_type) => return Ok(finish_type),
                        None => (),
                    }
                }
                CompressionFormat::None => {
                    match self
                        .read_file(
                            ctx,
                            BufReader::new(stream_reader).lines(),
                            &prefix,
                            &obj_key,
                            prev_lines_read,
                        )
                        .await?
                    {
                        Some(finish_type) => return Ok(finish_type),
                        None => (),
                    }
                }
            }
        }
        info!("S3 source finished");
        Ok(SourceFinishType::Final)
    }

    async fn read_file<R: AsyncBufRead + Unpin>(
        &mut self,
        ctx: &mut Context<(), T>,
        mut reader: Lines<R>,
        prefix: &str,
        obj_key: &str,
        prev_lines_read: usize,
    ) -> Result<Option<SourceFinishType>, UserError> {
        let mut lines_read = 0;
        loop {
            select! {
                line_res = reader.next_line() => {
                    let line_res = line_res.map_err(|err| UserError::new(
                        "could not read next line from S3 file", err.to_string()))?;
                    match line_res {
                        Some(line) => {
                            // if we're restoring from checkpoint, skip until we find the line we were on
                            if lines_read < prev_lines_read {
                                lines_read += 1;
                                continue;
                            }
                            let iter = self.deserializer.deserialize_slice(line.as_bytes());
                            for value in iter {
                                ctx.collector
                                    .collect(Record{
                                        timestamp: SystemTime::now(),
                                        key: None,
                                        value: value?,
                                    })
                                    .await;
                                self.total_lines_read += 1;
                                lines_read += 1;
                            }
                        }
                        None => return Ok(Some(SourceFinishType::Final))
                    }
                },
                msg_res = ctx.control_rx.recv() => {
                    if let Some(control_message) = msg_res {
                        match self.process_control_message(ctx, control_message, &prefix, &obj_key, lines_read).await {
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
        ctx: &mut Context<(), T>,
        control_message: ControlMessage,
        prefix: &str,
        key: &str,
        lines_read: usize,
    ) -> Option<SourceFinishType> {
        match control_message {
            ControlMessage::Checkpoint(c) => {
                ctx.state
                    .get_global_keyed_state('a')
                    .await
                    .insert(prefix.to_string(), (key.to_string(), lines_read))
                    .await;
                // checkpoint our state
                if self.checkpoint(c, ctx).await {
                    Some(SourceFinishType::Immediate)
                } else {
                    None
                }
            }
            ControlMessage::Stop { mode } => {
                info!("Stopping S3 source {:?}", mode);
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
