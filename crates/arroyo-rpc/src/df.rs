use crate::grpc::api;
use crate::{grpc, Converter, TIMESTAMP_FIELD};
use anyhow::{anyhow, Result};
use arrow::compute::kernels::numeric::div;
use arrow::compute::{filter_record_batch, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, TimeUnit};
use arrow::row::SortField;
use arrow_array::builder::{make_builder, ArrayBuilder};
use arrow_array::types::UInt64Type;
use arrow_array::{Array, PrimitiveArray, RecordBatch, TimestampNanosecondArray, UInt64Array};
use arrow_ord::cmp::gt_eq;
use arrow_ord::partition::partition;
use arrow_ord::sort::{lexsort_to_indices, SortColumn};
use arroyo_types::to_nanos;
use std::ops::Range;
use std::sync::Arc;
use std::time::SystemTime;

pub type ArroyoSchemaRef = Arc<ArroyoSchema>;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ArroyoSchema {
    pub schema: Arc<Schema>,
    pub timestamp_index: usize,
    pub key_indices: Option<Vec<usize>>,
}

impl TryFrom<grpc::ArroyoSchema> for ArroyoSchema {
    type Error = anyhow::Error;
    fn try_from(schema_proto: grpc::ArroyoSchema) -> anyhow::Result<Self> {
        let schema: Schema = serde_json::from_str(&schema_proto.arrow_schema)?;
        let timestamp_index = schema_proto.timestamp_index as usize;
        let key_indices = if schema_proto.has_keys {
            Some(
                schema_proto
                    .key_indices
                    .iter()
                    .map(|index| (*index) as usize)
                    .collect(),
            )
        } else {
            None
        };
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
        let has_keys = schema.key_indices.is_some();
        let key_indices = match schema.key_indices {
            Some(indices) => indices.iter().map(|index| (*index) as u32).collect(),
            None => vec![],
        };
        Ok(Self {
            arrow_schema,
            timestamp_index,
            key_indices,
            has_keys,
        })
    }
}

impl TryFrom<api::ArroyoSchema> for ArroyoSchema {
    type Error = anyhow::Error;
    fn try_from(schema_proto: api::ArroyoSchema) -> anyhow::Result<Self> {
        let schema: Schema = serde_json::from_str(&schema_proto.arrow_schema)?;
        let timestamp_index = schema_proto.timestamp_index as usize;
        let key_indices = if schema_proto.has_keys {
            Some(
                schema_proto
                    .key_indices
                    .iter()
                    .map(|index| (*index) as usize)
                    .collect(),
            )
        } else {
            None
        };
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
        let has_keys = schema.key_indices.is_some();
        let key_indices = match schema.key_indices {
            Some(indices) => indices.iter().map(|index| (*index) as u32).collect(),
            None => vec![],
        };
        Ok(Self {
            arrow_schema,
            timestamp_index,
            key_indices,
            has_keys,
        })
    }
}

impl ArroyoSchema {
    pub fn new(
        schema: Arc<Schema>,
        timestamp_index: usize,
        key_indices: Option<Vec<usize>>,
    ) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices,
        }
    }
    pub fn new_unkeyed(schema: Arc<Schema>, timestamp_index: usize) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices: None,
        }
    }
    pub fn new_keyed(schema: Arc<Schema>, timestamp_index: usize, key_indices: Vec<usize>) -> Self {
        Self {
            schema,
            timestamp_index,
            key_indices: Some(key_indices),
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

    pub fn from_schema_unkeyed(schema: Arc<Schema>) -> anyhow::Result<Self> {
        let timestamp_index = schema
            .column_with_name(TIMESTAMP_FIELD)
            .ok_or_else(|| {
                anyhow!(
                    "no {} field in schema, schema is {:?}",
                    TIMESTAMP_FIELD,
                    schema
                )
            })?
            .0;

        Ok(Self {
            schema,
            timestamp_index,
            key_indices: None,
        })
    }

    pub fn from_schema_keys(schema: Arc<Schema>, key_indices: Vec<usize>) -> anyhow::Result<Self> {
        let timestamp_index = schema
            .column_with_name(TIMESTAMP_FIELD)
            .ok_or_else(|| {
                anyhow!(
                    "no {} field in schema, schema is {:?}",
                    TIMESTAMP_FIELD,
                    schema
                )
            })?
            .0;

        Ok(Self {
            schema,
            timestamp_index,
            key_indices: Some(key_indices),
        })
    }

    pub fn schema_without_timestamp(&self) -> Schema {
        let mut builder = SchemaBuilder::from(self.schema.fields());
        builder.remove(self.timestamp_index);
        builder.finish()
    }

    pub fn remove_timestamp_column(&self, batch: &mut RecordBatch) {
        batch.remove_column(self.timestamp_index);
    }

    pub fn builders(&self) -> Vec<Box<dyn ArrayBuilder>> {
        self.schema
            .fields
            .iter()
            .map(|f| make_builder(f.data_type(), 8))
            .collect()
    }

    pub fn timestamp_column<'a>(&self, batch: &'a RecordBatch) -> &'a TimestampNanosecondArray {
        batch
            .column(self.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
    }

    pub fn filter_by_time(
        &self,
        batch: RecordBatch,
        cutoff: Option<SystemTime>,
    ) -> anyhow::Result<RecordBatch> {
        let Some(cutoff) = cutoff else {
            // no watermark, so we just return the same batch.
            return Ok(batch);
        };
        // filter out late data
        let timestamp_column = batch
            .column(self.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| anyhow!("failed to downcast column {} of {:?} to timestamp. Schema is supposed to be {:?}", self.timestamp_index, batch, self.schema))?;
        let cutoff_scalar = TimestampNanosecondArray::new_scalar(to_nanos(cutoff) as i64);
        let on_time = gt_eq(timestamp_column, &cutoff_scalar).unwrap();
        Ok(filter_record_batch(&batch, &on_time)?)
    }

    pub fn sort_columns(&self, batch: &RecordBatch, with_timestamp: bool) -> Vec<SortColumn> {
        let mut columns = vec![];
        if let Some(keys) = &self.key_indices {
            columns.extend(keys.iter().map(|index| SortColumn {
                values: batch.column(*index).clone(),
                options: None,
            }));
        }
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
        if let Some(keys) = &self.key_indices {
            sort_fields.extend(keys.iter());
        }
        if with_timestamp {
            sort_fields.push(self.timestamp_index);
        }
        self.sort_fields_by_indices(&sort_fields)
    }

    fn sort_fields_by_indices(&self, indices: &[usize]) -> Vec<SortField> {
        indices
            .iter()
            .map(|index| SortField::new(self.schema.field(*index).data_type().clone()))
            .collect()
    }

    pub fn converter(&self, with_timestamp: bool) -> Result<Converter> {
        Converter::new(self.sort_fields(with_timestamp))
    }

    pub fn value_converter(
        &self,
        with_timestamp: bool,
        generation_index: usize,
    ) -> Result<Converter> {
        match &self.key_indices {
            None => {
                let mut indices = (0..self.schema.fields().len()).collect::<Vec<_>>();
                indices.remove(generation_index);
                if !with_timestamp {
                    indices.remove(self.timestamp_index);
                }
                Converter::new(self.sort_fields_by_indices(&indices))
            }
            Some(keys) => {
                let indices = (0..self.schema.fields().len())
                    .filter(|index| {
                        !keys.contains(index)
                            && (with_timestamp || *index != self.timestamp_index)
                            && *index != generation_index
                    })
                    .collect::<Vec<_>>();
                Converter::new(self.sort_fields_by_indices(&indices))
            }
        }
    }
    pub fn value_indices(&self, with_timestamp: bool) -> Vec<usize> {
        let field_count = self.schema.fields().len();
        match &self.key_indices {
            None => {
                let mut indices = (0..field_count).collect::<Vec<_>>();

                if !with_timestamp {
                    indices.remove(self.timestamp_index);
                }
                indices
            }
            Some(keys) => (0..field_count)
                .filter(|index| {
                    !keys.contains(index) && (with_timestamp || *index != self.timestamp_index)
                })
                .collect::<Vec<_>>(),
        }
    }

    pub fn sort(&self, batch: RecordBatch, with_timestamp: bool) -> Result<RecordBatch> {
        if self.key_indices.is_none() && !with_timestamp {
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
        if self.key_indices.is_none() && !with_timestamp {
            #[allow(clippy::single_range_in_vec_init)]
            return Ok(vec![0..batch.num_rows()]);
        }
        let mut partition_columns = vec![];
        if let Some(keys) = &self.key_indices {
            partition_columns.extend(keys.iter().map(|index| batch.column(*index).clone()));
        }
        if with_timestamp {
            partition_columns.push(batch.column(self.timestamp_index).clone());
        }
        Ok(partition(&partition_columns)?.ranges())
    }

    pub fn unkeyed_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if self.key_indices.is_none() {
            return Ok(batch.clone());
        }
        let columns: Vec<_> = (0..batch.num_columns())
            .filter(|index| !self.key_indices.as_ref().unwrap().contains(index))
            .collect();
        Ok(batch.project(&columns)?)
    }

    pub fn schema_without_keys(&self) -> Result<Self> {
        if self.key_indices.is_none() {
            return Ok(self.clone());
        }
        let key_indices = self.key_indices.as_ref().unwrap();
        let unkeyed_schema = Schema::new(
            self.schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(index, _field)| !key_indices.contains(index))
                .map(|(_, field)| field.as_ref().clone())
                .collect::<Vec<_>>(),
        );
        let timestamp_index = unkeyed_schema.index_of(TIMESTAMP_FIELD)?;
        Ok(Self {
            schema: Arc::new(unkeyed_schema),
            timestamp_index,
            key_indices: None,
        })
    }
}

pub fn server_for_hash_array(
    hash: &PrimitiveArray<UInt64Type>,
    n: usize,
) -> anyhow::Result<PrimitiveArray<UInt64Type>> {
    let range_size = u64::MAX / (n as u64) + 1;
    let range_scalar = UInt64Array::new_scalar(range_size);
    let division = div(hash, &range_scalar)?;
    let result: &PrimitiveArray<UInt64Type> = division.as_any().downcast_ref().unwrap();
    Ok(result.clone())
}
