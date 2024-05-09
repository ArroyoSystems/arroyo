use std::{ops::RangeInclusive, sync::Arc};

use anyhow::{anyhow, bail, Result};
use arrow::compute::{and, filter, kernels::aggregate, max, min};
use arrow_array::{
    cast::AsArray,
    types::{TimestampNanosecondType, UInt64Type},
    PrimitiveArray, RecordBatch, TimestampNanosecondArray, UInt64Array,
};
use arrow_ord::cmp::{gt_eq, lt_eq};
use arrow_schema::{DataType, Field, Schema};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use arroyo_rpc::get_hasher;
use arroyo_types::from_nanos;
use bincode::config;
use datafusion::common::{hash_utils::create_hashes, ScalarValue};
use tracing::warn;

use crate::{parquet::ParquetStats, DataOperation};

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct SchemaWithHashAndOperation {
    state_schema: ArroyoSchemaRef,
    memory_schema: ArroyoSchemaRef,
    generation_index: Option<usize>,
    hash_index: usize,
    operation_index: usize,
}

#[allow(unused)]
impl SchemaWithHashAndOperation {
    pub(crate) fn new(mut memory_schema: ArroyoSchemaRef, with_generation: bool) -> Self {
        let mut fields = memory_schema.schema.fields().to_vec();
        let generation_index = if with_generation {
            fields.push(Arc::new(Field::new("_generation", DataType::UInt64, false)));
            memory_schema = Arc::new(ArroyoSchema::new(
                Arc::new(Schema::new_with_metadata(
                    fields.clone(),
                    memory_schema.schema.metadata().clone(),
                )),
                memory_schema.timestamp_index,
                memory_schema.key_indices.clone(),
            ));
            Some(fields.len() - 1)
        } else {
            None
        };

        fields.push(Arc::new(Field::new("_key_hash", DataType::UInt64, false)));
        let hash_index = fields.len() - 1;
        fields.push(Arc::new(Field::new(
            "_operation",
            arrow::datatypes::DataType::Binary,
            false,
        )));
        let operation_index = fields.len() - 1;
        let state_schema = Arc::new(Schema::new_with_metadata(
            fields,
            memory_schema.schema.metadata.clone(),
        ));
        let state_schema = Arc::new(ArroyoSchema::new(
            state_schema,
            memory_schema.timestamp_index,
            memory_schema.key_indices.clone(),
        ));
        Self {
            state_schema,
            memory_schema,
            generation_index,
            hash_index,
            operation_index,
        }
    }

    pub(crate) fn state_schema(&self) -> ArroyoSchemaRef {
        self.state_schema.clone()
    }

    pub fn memory_schema(&self) -> ArroyoSchemaRef {
        self.memory_schema.clone()
    }

    fn project_indices(&self) -> Vec<usize> {
        (0..self.memory_schema.schema.fields().len()).collect()
    }

    fn project_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        Ok(batch.project(&self.project_indices())?)
    }

    pub(crate) fn timestamp_index(&self) -> usize {
        self.memory_schema.timestamp_index
    }

    pub(crate) fn hash_index(&self) -> usize {
        self.hash_index
    }

    pub(crate) fn generation_index(&self) -> Option<usize> {
        self.generation_index
    }

    pub(crate) fn filter_by_hash_index(
        &self,
        batch: RecordBatch,
        range: &RangeInclusive<u64>,
    ) -> Result<Option<RecordBatch>> {
        let hash_array: &PrimitiveArray<UInt64Type> = batch
            .column(self.hash_index)
            .as_primitive_opt()
            .ok_or_else(|| anyhow!("failed to find key column"))?;
        let min_hash = aggregate::min(hash_array).ok_or_else(|| anyhow!("should have min hash"))?;
        let max_hash = aggregate::max(hash_array).ok_or_else(|| anyhow!("should have max hash"))?;
        if *range.end() < min_hash || *range.start() > max_hash {
            warn!("filtering out a record batch");
            return Ok(None);
        }
        // filter batch using arrow kernels
        let filtered_indices = and(
            &gt_eq(&hash_array, &UInt64Array::new_scalar(*range.start()))?,
            &lt_eq(&hash_array, &UInt64Array::new_scalar(*range.end()))?,
        )?;
        let columns = batch
            .columns()
            .iter()
            .map(|column| Ok(filter(column, &filtered_indices)?))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(RecordBatch::try_new(
            self.state_schema.schema.clone(),
            columns,
        )?))
    }

    pub(crate) fn batch_stats_from_state_batch(&self, batch: &RecordBatch) -> Result<ParquetStats> {
        if batch.num_rows() == 0 {
            bail!("unexpected empty batch");
        }
        let hash_array = batch
            .column(self.hash_index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("should be able to convert hash array to UInt64Array"))?;
        let hash_min = min(hash_array).expect("should have min hash value");
        let hash_max = max(hash_array).expect("should have max hash value");
        let timestamp_array = batch
            .column(self.state_schema.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| anyhow!("should be able to extract timestamp array"))?;
        let max_timestamp_nanos = max(timestamp_array).expect("should have max timestamp");
        Ok(ParquetStats {
            max_timestamp: from_nanos(max_timestamp_nanos as u128),
            min_routing_key: hash_min,
            max_routing_key: hash_max,
        })
    }

    pub(crate) fn annotate_record_batch(
        &mut self,
        record_batch: &RecordBatch,
    ) -> Result<(RecordBatch, ParquetStats)> {
        let key_batch = self
            .memory_schema
            .key_indices
            .as_ref()
            .map(|key_indices| record_batch.project(key_indices))
            .transpose()?
            .unwrap_or_else(|| record_batch.project(&[]).unwrap());

        let mut hash_buffer = vec![0u64; key_batch.num_rows()];
        let _hashes = create_hashes(key_batch.columns(), &get_hasher(), &mut hash_buffer)?;
        let hash_array = PrimitiveArray::<UInt64Type>::from(hash_buffer);

        let hash_min = min(&hash_array).unwrap();
        let hash_max = max(&hash_array).unwrap();
        let max_timestamp_nanos: i64 = max(record_batch
            .column(self.memory_schema.timestamp_index)
            .as_primitive::<TimestampNanosecondType>())
        .unwrap();

        let batch_stats = ParquetStats {
            max_timestamp: from_nanos(max_timestamp_nanos as u128),
            min_routing_key: hash_min,
            max_routing_key: hash_max,
        };

        let mut columns = record_batch.columns().to_vec();
        columns.push(Arc::new(hash_array));

        // TODO: move off of bincode for this
        let insert_op = ScalarValue::Binary(Some(bincode::encode_to_vec(
            DataOperation::Insert,
            config::standard(),
        )?));

        // TODO: handle other types of updates
        let op_array = insert_op.to_array_of_size(record_batch.num_rows())?;
        columns.push(op_array);

        let annotated_record_batch =
            RecordBatch::try_new(self.state_schema.schema.clone(), columns)?;

        Ok((annotated_record_batch, batch_stats))
    }
}
