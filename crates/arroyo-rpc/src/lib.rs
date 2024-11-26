pub mod api_types;
pub mod formats;
pub mod public_ids;
pub mod schema_resolver;
pub mod var_str;

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::{fs, time::SystemTime};

use crate::api_types::connections::PrimitiveType;
use crate::formats::{BadData, Format, Framing};
use crate::grpc::rpc::{LoadCompactedDataReq, SubtaskCheckpointMetadata};
use anyhow::Result;
use arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_schema::{DataType, Field, Fields};
use arroyo_types::{CheckpointBarrier, HASH_SEEDS};
use grpc::rpc::{StopMode, TableCheckpointMetadata, TaskCheckpointEventType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tonic::{
    metadata::{Ascii, MetadataValue},
    service::Interceptor,
};

pub mod config;
pub mod df;

pub mod grpc {
    pub mod rpc {
        #![allow(clippy::derive_partial_eq_without_eq, deprecated)]
        tonic::include_proto!("arroyo_rpc");
    }

    pub mod api {
        #![allow(clippy::derive_partial_eq_without_eq, deprecated)]
        tonic::include_proto!("api");
    }

    pub const API_FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("api_descriptor");
}

static DB_BACKUP_NOTIFIER: OnceLock<Sender<bool>> = OnceLock::new();

pub fn init_db_notifier() -> Receiver<bool> {
    let (tx, rx) = channel(1);
    DB_BACKUP_NOTIFIER
        .set(tx)
        .expect("DB notifier was initialized multiple times!");
    rx
}

pub fn notify_db() -> Option<()> {
    DB_BACKUP_NOTIFIER.get()?.try_send(true).ok()
}

#[derive(Debug)]
pub enum ControlMessage {
    Checkpoint(CheckpointBarrier),
    Stop {
        mode: StopMode,
    },
    Commit {
        epoch: u32,
        commit_data: HashMap<String, HashMap<u32, Vec<u8>>>,
    },
    LoadCompacted {
        compacted: CompactionResult,
    },
    NoOp,
}

#[derive(Debug, Clone)]
pub struct CompactionResult {
    pub operator_id: String,
    pub compacted_tables: HashMap<String, TableCheckpointMetadata>,
}

impl From<LoadCompactedDataReq> for CompactionResult {
    fn from(req: LoadCompactedDataReq) -> Self {
        Self {
            operator_id: req.operator_id,
            compacted_tables: req.compacted_metadata,
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
        PrimitiveType::Json => "JSON",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RateLimit {
    pub messages_per_second: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetadataField {
    pub field_name: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorConfig {
    pub connection: Value,
    pub table: Value,
    pub format: Option<Format>,
    pub bad_data: Option<BadData>,
    pub framing: Option<Framing>,
    pub rate_limit: Option<RateLimit>,
    #[serde(default)]
    pub metadata_fields: Vec<MetadataField>,
}

impl Default for OperatorConfig {
    fn default() -> Self {
        Self {
            connection: serde_json::from_str("{}").unwrap(),
            table: serde_json::from_str("{}").unwrap(),
            format: None,
            bad_data: None,
            framing: None,
            rate_limit: None,
            metadata_fields: vec![],
        }
    }
}

pub fn error_chain(e: anyhow::Error) -> String {
    e.chain()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(": ")
}

pub const TIMESTAMP_FIELD: &str = "_timestamp";
pub const UPDATING_META_FIELD: &str = "_updating_meta";

pub fn updating_meta_fields() -> Fields {
    static UPDATING_META_FIELDS: OnceLock<Fields> = OnceLock::new();

    UPDATING_META_FIELDS
        .get_or_init(|| {
            Fields::from(vec![
                Field::new("is_retract", DataType::Boolean, true),
                Field::new("id", DataType::FixedSizeBinary(16), true),
            ])
        })
        .clone()
}

pub fn updating_meta_field() -> Arc<Field> {
    static UPDATING_META_DATATYPE: OnceLock<Arc<Field>> = OnceLock::new();

    UPDATING_META_DATATYPE
        .get_or_init(|| {
            Arc::new(Field::new(
                UPDATING_META_FIELD,
                DataType::Struct(updating_meta_fields()),
                false,
            ))
        })
        .clone()
}
// need to handle the empty case as a row converter without sort fields emits empty Rows.
#[derive(Debug)]
pub enum Converter {
    RowConverter(RowConverter),
    Empty(RowConverter, Arc<dyn Array>),
}

impl Converter {
    pub fn new(sort_fields: Vec<SortField>) -> Result<Self> {
        if sort_fields.is_empty() {
            let array = Arc::new(BooleanArray::from(vec![false]));
            Ok(Self::Empty(
                RowConverter::new(vec![SortField::new(DataType::Boolean)])?,
                array,
            ))
        } else {
            Ok(Self::RowConverter(RowConverter::new(sort_fields)?))
        }
    }

    pub fn convert_columns(&self, columns: &[Arc<dyn Array>]) -> anyhow::Result<OwnedRow> {
        match self {
            Converter::RowConverter(row_converter) => {
                Ok(row_converter.convert_columns(columns)?.row(0).owned())
            }
            Converter::Empty(row_converter, array) => Ok(row_converter
                .convert_columns(&[array.clone()])?
                .row(0)
                .owned()),
        }
    }

    pub fn convert_all_columns(
        &self,
        columns: &[Arc<dyn Array>],
        num_rows: usize,
    ) -> anyhow::Result<Rows> {
        match self {
            Converter::RowConverter(row_converter) => Ok(row_converter.convert_columns(columns)?),
            Converter::Empty(row_converter, _array) => {
                let array = Arc::new(BooleanArray::from(vec![false; num_rows]));
                Ok(row_converter.convert_columns(&[array])?)
            }
        }
    }

    pub fn convert_rows(&self, rows: Vec<arrow::row::Row<'_>>) -> anyhow::Result<Vec<ArrayRef>> {
        match self {
            Converter::RowConverter(row_converter) => Ok(row_converter.convert_rows(rows)?),
            Converter::Empty(_row_converter, _array) => Ok(vec![]),
        }
    }

    pub fn convert_raw_rows(&self, row_bytes: Vec<&[u8]>) -> anyhow::Result<Vec<ArrayRef>> {
        match self {
            Converter::RowConverter(row_converter) => {
                let parser = row_converter.parser();
                let mut row_list = vec![];
                for bytes in row_bytes {
                    let row = parser.parse(bytes);
                    row_list.push(row);
                }
                Ok(row_converter.convert_rows(row_list)?)
            }
            Converter::Empty(_row_converter, _array) => Ok(vec![]),
        }
    }
}

pub fn get_hasher() -> ahash::RandomState {
    ahash::RandomState::with_seeds(HASH_SEEDS[0], HASH_SEEDS[1], HASH_SEEDS[2], HASH_SEEDS[3])
}

#[macro_export]
macro_rules! retry {
    ($e:expr, $max_retries:expr, $base:expr, $max_delay:expr, |$err_var: ident| $error_handler:expr) => {{
        use std::time::Duration;
        use tracing::error;
        let mut retries: u32 = 0;
        use rand::Rng;
        loop {
            match $e {
                Ok(value) => break Ok(value),
                Err(e) if retries < $max_retries => {
                    retries += 1;
                    {
                        let $err_var = e;
                        $error_handler;
                    }
                    let tmp = $max_delay.min($base * (2u32.pow(retries)));
                    let backoff = tmp / 2
                        + Duration::from_micros(
                            rand::thread_rng().gen_range(0..tmp.as_micros() as u64 / 2),
                        );

                    tokio::time::sleep(backoff).await;
                }
                Err(e) => break Err(e),
            }
        }
    }};
}
