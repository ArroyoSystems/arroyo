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
    _t: PhantomData<(K, T)>,
}

#[source_fn(out_t = T)]
impl<K: Data, T: DeserializeOwned + Data> S3SourceFunc<K, T> {
    pub fn fom_config(config_str: &str) -> Self {
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
        let (mut s3_key, mut lines_read) = ctx
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

        // let mut sort_criteria = HashMap::new();

        // // sorting by creation date ensures that we always read in the same
        // // order when restarting from a checkpoint, even if new files have
        // // been added.
        // sort_criteria.insert("key", "CreationDate");
        // list_req.sort = Some(sort_criteria);

        let list_res = client
            .list_objects_v2()
            .bucket(self.bucket.clone())
            .set_prefix(Some(self.prefix.clone()))
            .send()
            .await
            .unwrap();
        let mut prev_file_found = s3_key == "";

        let mut contents = list_res.contents.unwrap();

        while let Some(obj) = contents.pop() {
            let obj_key = obj.key.unwrap();
            if obj_key.ends_with('/') {
                continue;
            }

            if !prev_file_found && obj_key != s3_key {
                continue;
            }

            prev_file_found = true;
            s3_key = obj_key.clone();
            let get_res = client
                .get_object()
                .bucket(self.bucket.clone())
                .key(s3_key.clone())
                .send()
                .await
                .unwrap();
            let body = get_res.body;
            if self.compression == "zstd" {
                let r = BufReader::new(body.into_async_read());
                let r = ZstdDecoder::new(r);
                let mut lines = BufReader::new(Box::new(r)).lines();
                let mut i = 0;
                while let Some(s) = lines.next_line().await.unwrap() {
                    if i < lines_read {
                        i += 1;
                        continue;
                    }
                    let value = serde_json::from_str(&format!("\"{}\"", s)).unwrap();
                    ctx.collector
                        .collect(Record::<(), T>::from_value(SystemTime::now(), value).unwrap())
                        .await;
                    lines_read += 1;
                    i += 1;
                    if lines_read % self.check_frequency == 0 {
                        match ctx.control_rx.try_recv().ok() {
                            Some(ControlMessage::Checkpoint(c)) => {
                                ctx.state
                                    .get_global_keyed_state('a')
                                    .await
                                    .insert(self.prefix.clone(), (s3_key.clone(), lines_read))
                                    .await;
                                // checkpoint our state
                                if self.checkpoint(c, ctx).await {
                                    return SourceFinishType::Immediate;
                                }
                            }
                            Some(ControlMessage::Stop { mode }) => {
                                info!("Stopping S3 source {:?}", mode);

                                match mode {
                                    StopMode::Graceful => {
                                        return SourceFinishType::Graceful;
                                    }
                                    StopMode::Immediate => {
                                        return SourceFinishType::Immediate;
                                    }
                                }
                            }
                            Some(ControlMessage::Commit { .. }) => {
                                unreachable!("sources shouldn't receive commit messages");
                            }
                            _ => {}
                        }
                    }
                    lines_read = 0;
                }
            } else {
                // assume uncompressed
                let mut lines = BufReader::new(Box::new(body.into_async_read())).lines();
                let mut i = 0;
                while let Some(s) = lines.next_line().await.unwrap() {
                    if i < lines_read {
                        i += 1;
                        continue;
                    }
                    let value = serde_json::from_str(&format!("\"{}\"", s)).unwrap();
                    ctx.collector
                        .collect(Record::<(), T>::from_value(SystemTime::now(), value).unwrap())
                        .await;
                    lines_read += 1;
                    i += 1;
                    if lines_read % self.check_frequency == 0 {
                        match ctx.control_rx.try_recv().ok() {
                            Some(ControlMessage::Checkpoint(c)) => {
                                ctx.state
                                    .get_global_keyed_state('a')
                                    .await
                                    .insert(self.prefix.clone(), (s3_key.clone(), lines_read))
                                    .await;
                                // checkpoint our state
                                if self.checkpoint(c, ctx).await {
                                    return SourceFinishType::Immediate;
                                }
                            }
                            Some(ControlMessage::Stop { mode }) => {
                                info!("Stopping S3 source {:?}", mode);

                                match mode {
                                    StopMode::Graceful => {
                                        return SourceFinishType::Graceful;
                                    }
                                    StopMode::Immediate => {
                                        return SourceFinishType::Immediate;
                                    }
                                }
                            }
                            Some(ControlMessage::Commit { .. }) => {
                                unreachable!("sources shouldn't receive commit messages");
                            }
                            _ => {}
                        }
                    }
                    lines_read = 0;
                }
            }
        }
        info!("S3 source finished");
        SourceFinishType::Final
    }
}
