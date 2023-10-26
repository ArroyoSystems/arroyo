pub mod api_types;
pub mod formats;
pub mod public_ids;
pub mod schema_resolver;

use std::{fs, time::SystemTime};

use crate::api_types::connections::PrimitiveType;
use crate::formats::{Format, Framing};
use crate::grpc::{LoadCompactedDataReq, SubtaskCheckpointMetadata};
use arroyo_types::CheckpointBarrier;
use grpc::{StopMode, TaskCheckpointEventType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tonic::{
    metadata::{Ascii, MetadataValue},
    service::Interceptor,
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
    Commit { epoch: u32 },
    LoadCompacted { compacted: CompactionResult },
    NoOp,
}

#[derive(Debug, Clone)]
pub struct CompactionResult {
    pub operator_id: String,
    pub backend_data_to_drop: Vec<grpc::BackendData>,
    pub backend_data_to_load: Vec<grpc::BackendData>,
}

impl From<LoadCompactedDataReq> for CompactionResult {
    fn from(req: LoadCompactedDataReq) -> Self {
        Self {
            operator_id: req.operator_id,
            backend_data_to_drop: req.backend_data_to_drop,
            backend_data_to_load: req.backend_data_to_load,
        }
    }
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
    TaskStarted {
        operator_id: String,
        task_index: usize,
        start_time: SystemTime,
    },
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

pub fn primitive_to_sql(primitive_type: PrimitiveType) -> &'static str {
    match primitive_type {
        PrimitiveType::Int32 => "INTEGER",
        PrimitiveType::Int64 => "BIGINT",
        PrimitiveType::UInt32 => "INTEGER UNSIGNED",
        PrimitiveType::UInt64 => "BIGINT UNSIGNED",
        PrimitiveType::F32 => "FLOAT",
        PrimitiveType::F64 => "DOUBLE",
        PrimitiveType::Bool => "BOOLEAN",
        PrimitiveType::String => "TEXT",
        PrimitiveType::Bytes => "BINARY",
        PrimitiveType::UnixMillis
        | PrimitiveType::UnixMicros
        | PrimitiveType::UnixNanos
        | PrimitiveType::DateTime => "TIMESTAMP",
        PrimitiveType::Json => "JSONB",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RateLimit {
    pub messages_per_second: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorConfig {
    pub connection: Value,
    pub table: Value,
    pub format: Option<Format>,
    pub framing: Option<Framing>,
    pub rate_limit: Option<RateLimit>,
}
