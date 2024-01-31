pub mod api_types;
pub mod formats;
pub mod public_ids;
pub mod schema_resolver;
pub mod var_str;

use std::collections::HashMap;
use std::ops::Range;
use std::{fs, time::SystemTime};

use crate::api_types::connections::PrimitiveType;
use crate::formats::{BadData, Format, Framing};
use crate::grpc::{LoadCompactedDataReq, SubtaskCheckpointMetadata};
use anyhow::{anyhow, Result};
use arrow::compute::take;
use arrow::row::{OwnedRow, RowConverter, SortField};
use arrow_array::builder::{make_builder, ArrayBuilder};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_ord::partition::partition;
use arrow_ord::sort::{lexsort_to_indices, SortColumn};
use arrow_schema::{DataType, Field, Schema, SchemaBuilder, TimeUnit};
use arroyo_types::{CheckpointBarrier, HASH_SEEDS};
use grpc::{api, StopMode, TaskCheckpointEventType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
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
    Stop {
        mode: StopMode,
    },
    Commit {
        epoch: u32,
        commit_data: HashMap<char, HashMap<u32, Vec<u8>>>,
    },
    LoadCompacted {
        compacted: CompactionResult,
    },
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
    pub bad_data: Option<BadData>,
    pub framing: Option<Framing>,
    pub rate_limit: Option<RateLimit>,
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
        }
    }
}

pub fn error_chain(e: anyhow::Error) -> String {
    e.chain()
        .into_iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(": ")
}

pub const TIMESTAMP_FIELD: &str = "_timestamp";

pub type ArroyoSchemaRef = Arc<ArroyoSchema>;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ArroyoSchema {
    pub schema: Arc<Schema>,
    pub timestamp_index: usize,
    pub key_indices: Vec<usize>,
}

impl TryFrom<grpc::ArroyoSchema> for ArroyoSchema {
    type Error = anyhow::Error;
    fn try_from(schema_proto: grpc::ArroyoSchema) -> anyhow::Result<Self> {
        let schema: Schema = serde_json::from_str(&schema_proto.arrow_schema)?;
        let timestamp_index = schema_proto.timestamp_index as usize;
        let key_indices = schema_proto
            .key_indices
            .iter()
            .map(|index| (*index) as usize)
            .collect();
        Ok(Self {
            schema: Arc::new(schema),
            timestamp_index,
            key_indices,
        })
    }
}

impl TryFrom<ArroyoSchema> for grpc::ArroyoSchema {
    type Error = anyhow::Error;

    fn try_from(schema: ArroyoSchema) -> anyhow::Result<Self> {
        let arrow_schema = serde_json::to_string(schema.schema.as_ref())?;
        let timestamp_index = schema.timestamp_index as u32;
        let key_indices = schema
            .key_indices
            .iter()
            .map(|index| (*index) as u32)
            .collect();
        Ok(Self {
            arrow_schema,
            timestamp_index,
            key_indices,
        })
    }
}

impl TryFrom<api::ArroyoSchema> for ArroyoSchema {
    type Error = anyhow::Error;
    fn try_from(schema_proto: api::ArroyoSchema) -> anyhow::Result<Self> {
        let schema: Schema = serde_json::from_str(&schema_proto.arrow_schema)?;
        let timestamp_index = schema_proto.timestamp_index as usize;
        let key_indices = schema_proto
            .key_indices
            .iter()
            .map(|index| (*index) as usize)
            .collect();
        Ok(Self {
            schema: Arc::new(schema),
            timestamp_index,
            key_indices,
        })
    }
}

impl TryFrom<ArroyoSchema> for api::ArroyoSchema {
    type Error = anyhow::Error;

    fn try_from(schema: ArroyoSchema) -> anyhow::Result<Self> {
        let arrow_schema = serde_json::to_string(schema.schema.as_ref())?;
        let timestamp_index = schema.timestamp_index as u32;
        let key_indices = schema
            .key_indices
            .iter()
            .map(|index| (*index) as u32)
            .collect();
        Ok(Self {
            arrow_schema,
            timestamp_index,
            key_indices,
        })
    }
}

impl ArroyoSchema {
    pub fn new(schema: Arc<Schema>, timestamp_index: usize, key_indices: Vec<usize>) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices,
        }
    }

    pub fn from_fields(mut fields: Vec<Field>) -> Self {
        if !fields.iter().any(|f| f.name() == TIMESTAMP_FIELD) {
            fields.push(Field::new(
                TIMESTAMP_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ));
        }

        Self::from_schema_keys(Arc::new(Schema::new(fields)), vec![]).unwrap()
    }

    pub fn from_schema_keys(schema: Arc<Schema>, key_indices: Vec<usize>) -> anyhow::Result<Self> {
        let timestamp_index = schema
            .column_with_name(TIMESTAMP_FIELD)
            .ok_or_else(|| anyhow!("no {} field in schema", TIMESTAMP_FIELD))?
            .0;

        Ok(Self {
            schema,
            timestamp_index,
            key_indices,
        })
    }

    pub fn schema_without_timestamp(&self) -> Schema {
        let mut builder = SchemaBuilder::from(self.schema.fields());
        builder.remove(self.timestamp_index);
        builder.finish()
    }

    pub fn builders(&self) -> Vec<Box<dyn ArrayBuilder>> {
        self.schema
            .fields
            .iter()
            .map(|f| make_builder(f.data_type(), 8))
            .collect()
    }

    pub fn sort_columns(&self, batch: &RecordBatch, with_timestamp: bool) -> Vec<SortColumn> {
        let mut columns: Vec<_> = self
            .key_indices
            .iter()
            .map(|index| SortColumn {
                values: batch.column(*index).clone(),
                options: None,
            })
            .collect();
        if with_timestamp {
            columns.push(SortColumn {
                values: batch.column(self.timestamp_index).clone(),
                options: None,
            });
        }
        columns
    }

    pub fn sort_fields(&self, with_timestamp: bool) -> Vec<SortField> {
        let mut sort_fields = vec![];
        sort_fields.extend(
            self.key_indices
                .iter()
                .map(|index| SortField::new(self.schema.field(*index).data_type().clone())),
        );
        if with_timestamp {
            sort_fields.push(SortField::new(DataType::Timestamp(
                TimeUnit::Nanosecond,
                None,
            )));
        }
        sort_fields
    }

    pub fn converter(&self, with_timestamp: bool) -> Result<Converter> {
        Converter::new(self.sort_fields(with_timestamp))
    }

    pub fn sort(&self, batch: RecordBatch, with_timestamp: bool) -> Result<RecordBatch> {
        if self.key_indices.is_empty() && !with_timestamp {
            return Ok(batch);
        }
        let sort_columns = self.sort_columns(&batch, with_timestamp);
        let sort_indices = lexsort_to_indices(&sort_columns, None).expect("should be able to sort");
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &sort_indices, None).unwrap())
            .collect();

        Ok(RecordBatch::try_new(batch.schema(), columns)?)
    }

    pub fn partition(
        &self,
        batch: &RecordBatch,
        with_timestamp: bool,
    ) -> Result<Vec<Range<usize>>> {
        if self.key_indices.is_empty() && !with_timestamp {
            return Ok(vec![0..batch.num_rows()]);
        }
        let mut partition_columns: Vec<_> = self
            .key_indices
            .iter()
            .map(|index| batch.column(*index).clone())
            .collect();
        if with_timestamp {
            partition_columns.push(batch.column(self.timestamp_index).clone());
        }
        Ok(partition(&partition_columns)?.ranges())
    }

    pub fn unkeyed_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let columns: Vec<_> = (0..batch.num_columns())
            .filter(|index| !self.key_indices.contains(index))
            .collect();
        Ok(batch.project(&columns)?)
    }

    pub fn schema_without_keys(&self) -> Result<Self> {
        let unkeyed_schema = Schema::new(
            self.schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(index, _field)| !self.key_indices.contains(index))
                .map(|(_, field)| field.as_ref().clone())
                .collect::<Vec<_>>(),
        );
        let timestamp_index = unkeyed_schema.index_of(TIMESTAMP_FIELD)?;
        Ok(Self {
            schema: Arc::new(unkeyed_schema),
            timestamp_index: timestamp_index,
            key_indices: vec![],
        })
    }
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
                .convert_columns(&vec![array.clone()])?
                .row(0)
                .owned()),
        }
    }

    pub fn convert_rows(&self, rows: Vec<arrow::row::Row<'_>>) -> anyhow::Result<Vec<ArrayRef>> {
        match self {
            Converter::RowConverter(row_converter) => Ok(row_converter.convert_rows(rows)?),
            Converter::Empty(_row_converter, _array) => Ok(vec![]),
        }
    }
}

fn default_async_timeout_seconds() -> u64 {
    10
}

fn default_async_max_concurrency() -> u64 {
    100
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UdfOpts {
    #[serde(default = "bool::default")]
    pub async_results_ordered: bool,
    #[serde(default = "default_async_timeout_seconds")]
    pub async_timeout_seconds: u64,
    #[serde(default = "default_async_max_concurrency")]
    pub async_max_concurrency: u64,
}

pub fn get_hasher() -> ahash::RandomState {
    ahash::RandomState::with_seeds(HASH_SEEDS[0], HASH_SEEDS[1], HASH_SEEDS[2], HASH_SEEDS[3])
}
