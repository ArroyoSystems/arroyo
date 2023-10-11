use std::marker::PhantomData;
use std::time::SystemTime;

use anyhow::Result;
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use aws_sdk_s3::Region;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, BufReader, Lines},
    select,
};
use tracing::info;

use arroyo_macro::{source_fn, StreamNode};
use arroyo_rpc::{grpc::StopMode, ControlMessage, ControlResp, OperatorConfig};
use arroyo_types::{Data, Record, UserError};

use crate::{
    connectors::filesystem::s3::CompressionFormat, engine::Context, formats::DataDeserializer,
    SchemaData, SourceFinishType,
};

use super::S3Table;

#[derive(StreamNode)]
pub struct S3SourceFunc<K: Data, T: SchemaData + Data> {
    bucket: String,
    prefix: String,
    region: String,
    deserializer: DataDeserializer<T>,
    compression: CompressionFormat,
    total_lines_read: usize,
    _t: PhantomData<(K, T)>,
}

#[source_fn(out_t = T)]
impl<K: Data, T: SchemaData + Data> S3SourceFunc<K, T> {
    pub fn from_config(config_str: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config_str).expect("Invalid config for S3SourceFunc");
        let table: S3Table =
            serde_json::from_value(config.table).expect("Invalid table config for S3SourceFunc");
        let format = config.format.expect("Format must be set for S3 source");
        Self {
            bucket: table.bucket,
            prefix: table.prefix,
            region: table.region,
            deserializer: DataDeserializer::new(format, config.framing),
            compression: table.compression_format,
            total_lines_read: 0,
            _t: PhantomData,
        }
    }

    pub fn tables(&self) -> Vec<arroyo_rpc::grpc::TableDescriptor> {
        vec![arroyo_state::global_table('a', "s3")]
    }

    fn name(&self) -> String {
        "S3".to_string()
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
        let (prev_s3_key, prev_lines_read) = ctx
            .state
            .get_global_keyed_state('a')
            .await
            .get(&self.prefix)
            .map(|v: &(String, usize)| v.clone())
            .unwrap_or_default();

        let config = aws_config::from_env()
            .region(Region::new(self.region.clone()))
            .load()
            .await;

        let client = aws_sdk_s3::Client::new(&config);

        // TODO: sort by creation date
        let list_res = client
            .list_objects_v2()
            .bucket(self.bucket.clone())
            .set_prefix(Some(self.prefix.clone()))
            .send()
            .await
            .unwrap();
        let mut prev_file_found = prev_s3_key == "";

        let mut contents = list_res.contents.unwrap();

        while let Some(obj) = contents.pop() {
            let obj_key = obj.key.unwrap();
            if obj_key.ends_with('/') {
                continue;
            }

            // if restoring from checkpoint, skip until we find the file we were on
            if !prev_file_found && obj_key != prev_s3_key {
                continue;
            }
            prev_file_found = true;

            let get_res = client
                .get_object()
                .bucket(self.bucket.clone())
                .key(obj_key.clone())
                .send()
                .await
                .unwrap();
            let body = get_res.body;

            match self.compression {
                CompressionFormat::Zstd => {
                    let r = ZstdDecoder::new(BufReader::new(body.into_async_read()));
                    match self
                        .read_file(ctx, BufReader::new(r).lines(), &obj_key, prev_lines_read)
                        .await?
                    {
                        Some(finish_type) => return Ok(finish_type),
                        None => (),
                    }
                }
                CompressionFormat::Gzip => {
                    let r = GzipDecoder::new(BufReader::new(body.into_async_read()));
                    match self
                        .read_file(ctx, BufReader::new(r).lines(), &obj_key, prev_lines_read)
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
                            BufReader::new(body.into_async_read()).lines(),
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
                        match self.process_control_message(ctx, control_message, &obj_key, lines_read).await {
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
        s3_key: &str,
        lines_read: usize,
    ) -> Option<SourceFinishType> {
        match control_message {
            ControlMessage::Checkpoint(c) => {
                ctx.state
                    .get_global_keyed_state('a')
                    .await
                    .insert(self.prefix.clone(), (s3_key.to_string(), lines_read))
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
