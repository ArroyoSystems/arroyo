use std::{fs, time::SystemTime};

use crate::grpc::SubtaskCheckpointMetadata;
use arroyo_types::{CheckpointBarrier, API_ADDR_ENV};
use grpc::{api::api_grpc_client::ApiGrpcClient, StopMode, TaskCheckpointEventType};
use tonic::{
    codegen::InterceptedService,
    metadata::{Ascii, MetadataValue},
    service::Interceptor,
    transport::Channel,
};

pub mod grpc {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("arroyo_rpc");

    pub mod api {
        #![allow(clippy::derive_partial_eq_without_eq)]
        tonic::include_proto!("arroyo_api");
    }

    pub const API_FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("api_descriptor");
}

#[derive(Debug)]
pub enum ControlMessage {
    Checkpoint(CheckpointBarrier),
    Stop { mode: StopMode },
}

#[derive(Debug, Clone)]
pub struct CheckpointCompleted {
    pub checkpoint_epoch: u32,
    pub operator_id: String,
    pub subtask_metadata: SubtaskCheckpointMetadata,
}

#[derive(Debug, Clone)]
pub struct CheckpointEvent {
    pub checkpoint_epoch: u32,
    pub operator_id: String,
    pub subtask_index: u32,
    pub time: SystemTime,
    pub event_type: TaskCheckpointEventType,
}

#[derive(Debug, Clone)]
pub enum ControlResp {
    CheckpointEvent(CheckpointEvent),
    CheckpointCompleted(CheckpointCompleted),
    TaskFinished {
        operator_id: String,
        task_index: usize,
    },
    TaskFailed {
        operator_id: String,
        task_index: usize,
        error: String,
    },
    Error {
        operator_id: String,
        task_index: usize,
        message: String,
        details: String,
    },
}

pub struct FileAuthInterceptor {
    token: MetadataValue<Ascii>,
}

impl FileAuthInterceptor {
    pub fn load() -> Self {
        let path = format!("{}/.arroyo-token", std::env::var("HOME").unwrap());
        let token = fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("Expected auth token to be in {}", path));

        Self {
            token: token.trim().parse().unwrap(),
        }
    }
}

impl Interceptor for FileAuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("authorization", self.token.clone());

        Ok(request)
    }
}

pub async fn api_client() -> ApiGrpcClient<InterceptedService<Channel, FileAuthInterceptor>> {
    let host = std::env::var(API_ADDR_ENV).unwrap_or_else(|_| "http://localhost:8001".to_string());
    let channel = Channel::from_shared(host).unwrap().connect().await.unwrap();

    ApiGrpcClient::with_interceptor(channel, FileAuthInterceptor::load())
}
