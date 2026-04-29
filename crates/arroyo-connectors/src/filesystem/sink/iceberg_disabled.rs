#![allow(dead_code)]

pub mod metadata {
    use anyhow::anyhow;
    use arroyo_rpc::errors::{DataflowError, DataflowResult, ErrorDomain, RetryHint};
    use bincode::{Decode, Encode};
    use std::collections::HashMap;

    #[derive(Debug, Decode, Encode, Clone, PartialEq, Eq, Default)]
    pub struct BoundsEncoded {
        pub min: Option<Vec<u8>>,
        pub max: Option<Vec<u8>>,
    }

    #[derive(Debug, Decode, Encode, Clone, PartialEq, Eq, Default)]
    pub struct ColumnAccum {
        pub compressed_size: u64,
        pub num_values: u64,
        pub null_values: u64,
        pub bounds: BoundsEncoded,
    }

    #[derive(Debug, Decode, Encode, Clone, PartialEq, Eq, Default)]
    pub struct IcebergFileMetadata {
        pub row_count: u64,
        pub split_offsets: Vec<i64>,
        pub columns: HashMap<i32, ColumnAccum>,
    }

    impl IcebergFileMetadata {
        pub fn from_parquet<T>(
            _: T,
            _: &crate::filesystem::sink::iceberg::schema::SchemaRef,
        ) -> Self {
            Self::default()
        }
    }

    pub fn iceberg_disabled_error() -> DataflowError {
        DataflowError::ConnectorError {
            domain: ErrorDomain::User,
            retry: RetryHint::NoRetry,
            error: "iceberg support has been removed".to_string(),
            source: Some(anyhow!("iceberg support removed")),
        }
    }

    pub fn build_datafile_from_meta(
        _: &(),
        _: &IcebergFileMetadata,
        _: &crate::filesystem::config::IcebergPartitioning,
        _: String,
        _: u64,
        _: i32,
    ) -> DataflowResult<()> {
        Err(iceberg_disabled_error())
    }
}

pub mod schema {
    use anyhow::Result;
    use arrow::array::RecordBatch;
    use arrow::datatypes::Schema;
    use std::sync::Arc;

    pub type SchemaRef = Arc<Schema>;

    pub fn add_parquet_field_ids(schema: &Schema) -> Schema {
        schema.clone()
    }

    pub fn update_field_ids_to_iceberg(schema: &Schema, _: &Schema) -> Result<Schema> {
        Ok(schema.clone())
    }

    pub fn normalize_batch_to_schema(batch: &RecordBatch, _: &Schema) -> Result<RecordBatch> {
        Ok(batch.clone())
    }
}

use crate::filesystem::config::{IcebergCatalog, IcebergSink};
use crate::filesystem::sink::FinishedFile;
use arrow::datatypes::Schema;
use arroyo_rpc::errors::{DataflowError, DataflowResult};
use arroyo_storage::StorageProvider;
use arroyo_types::TaskInfo;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct IcebergTable;

#[derive(Debug, Default)]
pub struct LoadedIcebergTable {
    metadata: LoadedIcebergMetadata,
}

#[derive(Debug)]
pub struct LoadedIcebergMetadata {
    current_schema: schema::SchemaRef,
}

impl Default for LoadedIcebergMetadata {
    fn default() -> Self {
        Self {
            current_schema: Arc::new(Schema::empty()),
        }
    }
}

impl LoadedIcebergTable {
    pub fn metadata(&self) -> &LoadedIcebergMetadata {
        &self.metadata
    }
}

impl LoadedIcebergMetadata {
    pub fn current_schema(&self) -> &schema::SchemaRef {
        &self.current_schema
    }
}

impl IcebergTable {
    pub fn new(_: &IcebergCatalog, _: &IcebergSink) -> anyhow::Result<Self> {
        Ok(Self)
    }

    pub async fn load_or_create(
        &mut self,
        _: Arc<TaskInfo>,
        _: &Schema,
    ) -> Result<&LoadedIcebergTable, DataflowError> {
        Err(metadata::iceberg_disabled_error())
    }

    pub async fn get_storage_provider(
        &mut self,
        _: Arc<TaskInfo>,
        _: &Schema,
    ) -> Result<StorageProvider, DataflowError> {
        Err(metadata::iceberg_disabled_error())
    }

    pub async fn commit(&mut self, _: u32, _: &[FinishedFile]) -> DataflowResult<()> {
        Err(metadata::iceberg_disabled_error())
    }
}
