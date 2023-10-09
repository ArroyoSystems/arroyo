use std::{marker::PhantomData, time::SystemTime};

use crate::engine::{Context, StreamNode};
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::controller_grpc_client::ControllerGrpcClient;
use arroyo_rpc::grpc::SinkDataReq;
use arroyo_types::*;
use serde::Serialize;
use tonic::transport::Channel;

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

    async fn on_close(&mut self, ctx: &mut Context<(), ()>, _final_message: &Option<Message<(),()>>) {
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
