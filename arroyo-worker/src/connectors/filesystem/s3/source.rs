use std::marker::PhantomData;
use std::time::SystemTime;

use async_compression::tokio::bufread::ZstdDecoder;
use aws_sdk_s3::Region;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::info;

use arroyo_macro::{source_fn, StreamNode};
use arroyo_rpc::{grpc::StopMode, ControlMessage, OperatorConfig};
use arroyo_types::{Data, Record};

use crate::{engine::Context, SourceFinishType};

use super::S3Table;

#[derive(StreamNode)]
pub struct S3SourceFunc<K: Data, T: DeserializeOwned + Data> {
    bucket: String,
    prefix: String,
    region: String,
    compression: String,
    check_frequency: usize,
    total_lines_read: usize,
    _t: PhantomData<(K, T)>,
}

#[source_fn(out_t = T)]
impl<K: Data, T: DeserializeOwned + Data> S3SourceFunc<K, T> {
    pub fn from_config(config_str: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config_str).expect("Invalid config for S3SourceFunc");
        let table: S3Table =
            serde_json::from_value(config.table).expect("Invalid table config for S3SourceFunc");
        Self {
            bucket: table.bucket,
            prefix: table.prefix,
            region: table.region,
            compression: table.compression_format.to_string(),
            check_frequency: 1000,
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
        if ctx.task_info.task_index != 0 {
            return SourceFinishType::Final;
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
            // fixme: unify branches, the only difference is the zstd decoder
            if self.compression == "zstd" {
                let r = BufReader::new(body.into_async_read());
                let r = ZstdDecoder::new(r);
                let mut lines = BufReader::new(r).lines();
                let mut lines_read = 0;
                while let Ok(Some(s)) = lines.next_line().await {
                    // if we're restoring from checkpoint, skip until we find the line we were on
                    if lines_read < prev_lines_read {
                        lines_read += 1;
                        continue;
                    }
                    // json! will correctly handle escaping
                    let value = serde_json::from_value(serde_json::json!({
                        "value": s
                    }))
                    .unwrap();
                    ctx.collector
                        .collect(Record::<(), T>::from_value(SystemTime::now(), value).unwrap())
                        .await;
                    self.total_lines_read += 1;
                    lines_read += 1;
                    match self.check_control_message(ctx, &obj_key, lines_read).await {
                        Some(finish_type) => return finish_type,
                        None => {}
                    }
                }
            } else {
                // assume uncompressed
                let mut lines = BufReader::new(body.into_async_read()).lines();
                let mut lines_read = 0;
                while let Ok(Some(s)) = lines.next_line().await {
                    if lines_read < prev_lines_read {
                        lines_read += 1;
                        continue;
                    }
                    // json! will correctly handle escaping
                    let value = serde_json::from_value(serde_json::json!({
                        "value": s
                    }))
                    .unwrap();
                    ctx.collector
                        .collect(Record::<(), T>::from_value(SystemTime::now(), value).unwrap())
                        .await;
                    self.total_lines_read += 1;
                    lines_read += 1;
                    match self.check_control_message(ctx, &obj_key, lines_read).await {
                        Some(finish_type) => return finish_type,
                        None => {}
                    }
                }
            }
        }
        info!("S3 source finished");
        SourceFinishType::Final
    }

    async fn check_control_message(
        &mut self,
        ctx: &mut Context<(), T>,
        s3_key: &str,
        lines_read: usize,
    ) -> Option<SourceFinishType> {
        if self.total_lines_read % self.check_frequency == 0 {
            match ctx.control_rx.try_recv().ok() {
                Some(ControlMessage::Checkpoint(c)) => {
                    ctx.state
                        .get_global_keyed_state('a')
                        .await
                        .insert(self.prefix.clone(), (s3_key.to_string(), lines_read))
                        .await;
                    // checkpoint our state
                    if self.checkpoint(c, ctx).await {
                        return Some(SourceFinishType::Immediate);
                    }
                }
                Some(ControlMessage::Stop { mode }) => {
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
                Some(ControlMessage::Commit { .. }) => {
                    unreachable!("sources shouldn't receive commit messages");
                }
                _ => {}
            }
        }
        None
    }
}
