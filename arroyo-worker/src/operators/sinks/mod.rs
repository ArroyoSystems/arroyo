use std::io::Write;
use std::{
    fs::{self, File},
    marker::PhantomData,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::connectors::OperatorConfig;
use crate::engine::{Context, StreamNode};
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::controller_grpc_client::ControllerGrpcClient;
use arroyo_rpc::grpc::SinkDataReq;
use arroyo_types::*;
use chrono::{DateTime, Utc};
use serde::Serialize;
use tonic::transport::Channel;
use tracing::info;

#[derive(StreamNode)]
pub struct FileSink<K: Key, T: Data> {
    output_dir: PathBuf,
    file: Option<File>,
    _ts: PhantomData<(K, T)>,
}

#[process_fn(in_k = K, in_t = T)]
impl<K: Key, T: Data> FileSink<K, T> {
    pub fn new(directory: PathBuf) -> FileSink<K, T> {
        FileSink {
            output_dir: directory,
            file: None,
            _ts: PhantomData,
        }
    }

    fn name(&self) -> String {
        "file_sink".to_string()
    }

    async fn on_start(&mut self, ctx: &mut Context<(), ()>) {
        let path = self
            .output_dir
            .join(format!("output-{}", ctx.task_info.task_index));
        fs::create_dir_all(&self.output_dir).unwrap();
        self.file = Some(File::create(path).unwrap());
    }

    async fn handle_watermark(&mut self, _: SystemTime, _: &mut Context<(), ()>) {
        self.file.as_mut().unwrap().flush().unwrap();
    }

    async fn process_element(&mut self, record: &Record<K, T>, _ctx: &mut Context<(), ()>) {
        let unix = record
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        if let Some(key) = record.key.as_ref() {
            writeln!(
                self.file.as_mut().unwrap(),
                "{:?}\t{:?}\t{:?}",
                unix,
                key,
                record.value
            )
            .unwrap();
        } else {
            writeln!(
                self.file.as_mut().unwrap(),
                "{:?}\t{:?}",
                unix,
                record.value
            )
            .unwrap();
        }
    }
}

#[derive(StreamNode)]
pub struct NullSink<K: Key, T: Data> {
    _phantom: PhantomData<(K, T)>,
}

#[process_fn(in_k = K, in_t = T)]
impl<K: Key, T: Data> NullSink<K, T> {
    pub fn new() -> NullSink<K, T> {
        NullSink {
            _phantom: PhantomData,
        }
    }

    fn name(&self) -> String {
        "NullSink".to_string()
    }

    async fn process_element(&mut self, _record: &Record<K, T>, _ctx: &mut Context<(), ()>) {
        // no-op
    }
}

#[derive(StreamNode)]
pub struct ConsoleSink<K: Key, T: Data> {
    _ts: PhantomData<(K, T)>,
}

#[process_fn(in_k=K, in_t=T)]
impl<K: Key, T: Data> ConsoleSink<K, T> {
    pub fn new() -> ConsoleSink<K, T> {
        ConsoleSink { _ts: PhantomData }
    }

    fn name(&self) -> String {
        "ConsoleSink".to_string()
    }

    async fn process_element(&mut self, record: &Record<K, T>, _: &mut Context<(), ()>) {
        let ts = DateTime::<Utc>::from(record.timestamp);
        let s = ts.format("%D %H:%M:%S%.3f");
        match &record.key {
            Some(k) => {
                info!("{}\t{:?}\t{:?}", s, k, record.value);
            }
            None => {
                info!("{}\t{:?}", s, record.value);
            }
        }
    }
}

#[derive(StreamNode)]
pub struct GrpcSink<K: Key, T: Data + Serialize> {
    _ts: PhantomData<(K, T)>,
    client: Option<ControllerGrpcClient<Channel>>,
}

#[process_fn(in_k=K, in_t=T)]
impl<K: Key, T: Data + Serialize> GrpcSink<K, T> {
    pub fn new() -> GrpcSink<K, T> {
        GrpcSink {
            _ts: PhantomData,
            client: None,
        }
    }

    pub fn from_config(_config: &str) -> Self {
        Self::new()
    }

    fn name(&self) -> String {
        "GrpcSink".to_string()
    }

    async fn on_start(&mut self, _: &mut Context<(), ()>) {
        let controller_addr = std::env::var(arroyo_types::CONTROLLER_ADDR_ENV)
            .unwrap_or_else(|_| crate::LOCAL_CONTROLLER_ADDR.to_string());

        self.client = Some(
            ControllerGrpcClient::connect(controller_addr)
                .await
                .unwrap(),
        );
    }

    async fn process_element(&mut self, record: &Record<K, T>, ctx: &mut Context<(), ()>) {
        let value = serde_json::to_string(&record.value).unwrap();
        self.client
            .as_mut()
            .unwrap()
            .send_sink_data(SinkDataReq {
                job_id: ctx.task_info.job_id.clone(),
                operator_id: ctx.task_info.operator_id.clone(),
                subtask_index: ctx.task_info.task_index as u32,
                timestamp: to_micros(record.timestamp),
                key: format!("{:?}", record.key),
                value,
                done: false,
            })
            .await
            .unwrap();
    }

    async fn on_close(&mut self, ctx: &mut Context<(), ()>) {
        self.client
            .as_mut()
            .unwrap()
            .send_sink_data(SinkDataReq {
                job_id: ctx.task_info.job_id.clone(),
                operator_id: ctx.task_info.operator_id.clone(),
                subtask_index: ctx.task_info.task_index as u32,
                timestamp: to_micros(SystemTime::now()),
                key: "".to_string(),
                value: "".to_string(),
                done: true,
            })
            .await
            .unwrap();
    }
}
