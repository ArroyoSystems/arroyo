use std::{ops::RangeInclusive, sync::Arc};

use anyhow::{anyhow, Result};
use arrow::compute::{and, filter, kernels::aggregate, max, min, take};
use arrow_array::{
    cast::AsArray,
    types::{TimestampNanosecondType, UInt64Type},
    PrimitiveArray, RecordBatch, UInt64Array,
};
use arrow_ord::cmp::{gt_eq, lt_eq};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arroyo_rpc::{get_hasher, ArroyoSchemaRef};
use arroyo_types::from_nanos;
use bincode::config;
use datafusion_common::{hash_utils::create_hashes, ScalarValue};
use tracing::warn;

use crate::{parquet::ParquetStats, DataOperation};

#[allow(unused)]
#[derive(Debug, Clone)]
pub struct SchemaWithHashAndOperation {
    state_schema: SchemaRef,
    memory_schema: ArroyoSchemaRef,
    hash_index: usize,
    operation_index: usize,
}

#[allow(unused)]
impl SchemaWithHashAndOperation {
    pub(crate) fn new(memory_schema: ArroyoSchemaRef) -> Self {
        let mut fields = memory_schema.schema.fields().to_vec();
        //TODO: we could have the additional columns at the start, rather than the end
        fields.push(Arc::new(Field::new("_key_hash", DataType::UInt64, false)));
        fields.push(Arc::new(Field::new(
            "_operation",
            arrow::datatypes::DataType::Binary,
            false,
        )));
        let state_schema = Arc::new(Schema::new_with_metadata(
            fields,
            memory_schema.schema.metadata.clone(),
        ));
        let hash_index = memory_schema.schema.fields().len();
        let operation_index = hash_index + 1;
        Self {
            state_schema,
            memory_schema,
            hash_index,
            operation_index,
        }
    }

    pub(crate) fn state_schema(&self) -> SchemaRef {
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
        let filtered_indices = filter(
            &hash_array,
            &and(
                &gt_eq(&hash_array, &UInt64Array::new_scalar(*range.start()))?,
                &lt_eq(&hash_array, &UInt64Array::new_scalar(*range.end()))?,
            )?,
        )?;
        let columns = batch
            .columns()
            .iter()
            .map(|column| Ok(take(column, &filtered_indices, None)?))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(RecordBatch::try_new(
            self.state_schema.clone(),
            columns,
        )?))
    }

    pub(crate) fn annotate_record_batch(
        &mut self,
        record_batch: &RecordBatch,
    ) -> Result<(RecordBatch, ParquetStats)> {
        let key_batch = record_batch
            .project(&self.memory_schema.key_indices)
            .unwrap();

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

        let annotated_record_batch = RecordBatch::try_new(self.state_schema.clone(), columns)?;

        Ok((annotated_record_batch, batch_stats))
    }
}
