use crate::filesystem::config::{IcebergPartitioning, PartitioningConfig};
use arrow::array::{ArrayRef, AsArray, RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Schema};
use arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::Expr;
use itertools::Itertools;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum PartitionerMode {
    FileConfig(PartitioningConfig),
    Iceberg(IcebergPartitioning),
}

impl PartitionerMode {
    pub fn hive_style_paths(&self) -> bool {
        match self {
            PartitionerMode::FileConfig(_) => true,
            PartitionerMode::Iceberg(_) => false,
        }
    }
}

pub struct Partitioner {
    mode: PartitionerMode,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    row_converter: RowConverter,
}

fn compile_expression(expr: &Expr, schema: &Schema) -> anyhow::Result<Arc<dyn PhysicalExpr>> {
    let physical_planner = DefaultPhysicalPlanner::default();
    let session_state = SessionStateBuilder::new().build();

    let plan =
        physical_planner.create_physical_expr(expr, &schema.clone().try_into()?, &session_state)?;
    Ok(plan)
}

impl Partitioner {
    pub fn new(mode: PartitionerMode, schema: &Schema) -> anyhow::Result<Self> {
        let exprs = match &mode {
            PartitionerMode::FileConfig(fc) => fc
                .partition_expr(schema)?
                .map(|f| vec![f])
                .unwrap_or_default(),
            PartitionerMode::Iceberg(ic) => ic.partition_expr(schema)?.unwrap_or_default(),
        };

        let (exprs, row_converter) = Self::compile(exprs, schema)?;

        Ok(Self {
            mode,
            row_converter,
            exprs,
        })
    }

    fn compile(
        exprs: Vec<Expr>,
        schema: &Schema,
    ) -> anyhow::Result<(Vec<Arc<dyn PhysicalExpr>>, RowConverter)> {
        let exprs: Vec<_> = exprs
            .iter()
            .map(|e| compile_expression(e, schema))
            .try_collect()?;

        let fields: Result<_, anyhow::Error> = exprs
            .iter()
            .map(|e| Ok(SortField::new(e.data_type(schema)?)))
            .collect();

        Ok((exprs, RowConverter::new(fields?)?))
    }

    pub fn is_partitioned(&self) -> bool {
        !self.exprs.is_empty()
    }

    /// Partition the batch by this partitioner
    pub fn partition(&self, batch: &RecordBatch) -> anyhow::Result<Vec<(OwnedRow, RecordBatch)>> {
        let key_arrays: anyhow::Result<Vec<_>> = self
            .exprs
            .iter()
            .map(|e| Ok(e.evaluate(batch)?.into_array(batch.num_rows())?))
            .collect();

        let rows = self.row_converter.convert_columns(&key_arrays?)?;

        let mut groups: HashMap<OwnedRow, Vec<u32>> = HashMap::default();
        let mut order: Vec<OwnedRow> = Vec::new();

        let n = batch.num_rows();
        for i in 0..n {
            let k = rows.row(i).owned();
            let entry = groups.entry(k.clone()).or_insert_with(|| {
                order.push(k);
                Vec::new()
            });
            entry.push(i as u32);
        }

        let mut perm: Vec<u32> = Vec::with_capacity(n);
        let mut lens: Vec<usize> = Vec::with_capacity(order.len());
        for k in &order {
            let idxs = &groups[k];
            perm.extend_from_slice(idxs);
            lens.push(idxs.len());
        }
        let perm = UInt32Array::from(perm);

        let permuted_cols: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|c| Ok(take(c.as_ref(), &perm, None)?))
            .collect::<anyhow::Result<_>>()?;

        let permuted = RecordBatch::try_new(batch.schema(), permuted_cols)?;

        let mut out = Vec::with_capacity(order.len());
        let mut offset = 0;
        for (k, len) in order.into_iter().zip(lens.into_iter()) {
            let rb = permuted.slice(offset, len);
            out.push((k, rb));
            offset += len;
        }
        Ok(out)
    }

    pub fn hive_path(&self, partition: &OwnedRow) -> Option<String> {
        if !self.mode.hive_style_paths() {
            return None;
        }

        let array = self
            .row_converter
            .convert_rows(vec![partition.row()])
            .unwrap();

        assert_eq!(array.len(), 1);
        assert_eq!(array[0].data_type(), &DataType::Utf8);

        Some(array[0].as_string::<i32>().value(0).to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::filesystem::config::PartitioningConfig;
    use crate::filesystem::sink::partitioning::{Partitioner, PartitionerMode};
    use arrow::array::{
        ArrayRef, AsArray, Int32Array, RecordBatch, StringArray, TimestampNanosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arroyo_rpc::df::ArroyoSchema;
    use datafusion::logical_expr::{col, ColumnarValue};
    use datafusion::prelude::{lit, Expr};
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        // a:  ["x", "y", null, "x", "y", "y", null, "x"]
        // b:  [ 10,  11,  12,   13,  14,  15,   16,   17]
        let a = Arc::new(StringArray::from(vec![
            Some("x"),
            Some("y"),
            None,
            Some("x"),
            Some("y"),
            Some("y"),
            None,
            Some("x"),
        ])) as ArrayRef;
        let b = Arc::new(Int32Array::from(vec![10, 11, 12, 13, 14, 15, 16, 17])) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, false),
        ]));
        RecordBatch::try_new(schema, vec![a, b]).unwrap()
    }

    fn partitioner(exprs: Vec<Expr>, schema: &Schema) -> Partitioner {
        let mut partitioner = Partitioner::new(
            PartitionerMode::FileConfig(PartitioningConfig::default()),
            schema,
        )
        .unwrap();
        let (exprs, row_converter) = Partitioner::compile(exprs, schema).unwrap();
        partitioner.exprs = exprs;
        partitioner.row_converter = row_converter;
        partitioner
    }

    #[test]
    fn partitions_by_column_value_groups_rows_correctly() -> anyhow::Result<()> {
        let batch = make_batch();
        let partitioner = partitioner(vec![col("a")], &batch.schema());

        let parts = partitioner.partition(&batch)?;
        assert_eq!(parts.len(), 3);

        let expected_b_seq = vec![10, 13, 17, 11, 14, 15, 12, 16];

        let mut actual_b_seq = Vec::new();
        for (_, rb) in &parts {
            let b = rb.column(1).as_primitive::<Int32Type>();
            for i in 0..b.len() {
                actual_b_seq.push(b.value(i));
            }
        }
        assert_eq!(actual_b_seq, expected_b_seq);
        Ok(())
    }

    #[test]
    fn scalar_key_collapses_to_single_partition() -> anyhow::Result<()> {
        let batch = make_batch();
        let partitioner = partitioner(vec![lit(1)], &batch.schema());

        let parts = partitioner.partition(&batch)?;
        assert_eq!(parts.len(), 1);
        let (_, only) = &parts[0];
        assert_eq!(only.num_rows(), batch.num_rows());
        Ok(())
    }

    fn test_pattern(pattern: &str, expected: &str) -> anyhow::Result<()> {
        let config = PartitioningConfig {
            time_pattern: Some(pattern.to_string()),
            ..Default::default()
        };

        let test_schema = Arc::new(ArroyoSchema::from_fields(vec![]));

        let partitioner =
            Partitioner::new(PartitionerMode::FileConfig(config), &test_schema.schema).unwrap();
        let expr = partitioner.exprs.first().unwrap();

        let data = RecordBatch::try_new(
            test_schema.schema.clone(),
            vec![Arc::new(TimestampNanosecondArray::from_value(
                1759871368595325952,
                1,
            ))],
        )
        .unwrap();

        match expr.evaluate(&data)? {
            ColumnarValue::Array(a) => {
                assert_eq!(a.as_string::<i32>().value(0), expected)
            }
            ColumnarValue::Scalar(_) => {
                panic!("should be array");
            }
        }

        Ok(())
    }

    #[test]
    fn test_timestamp_udf() {
        test_pattern("%Y", "2025").unwrap();
        test_pattern("%Y-%m-%d", "2025-10-07").unwrap();
        test_pattern("%Y%m%d", "20251007").unwrap();
        test_pattern("%H", "21").unwrap();
        test_pattern("%H:%M:%S", "21:09:28").unwrap();
        test_pattern("%Y/%m/%d/%H", "2025/10/07/21").unwrap();
        test_pattern("year=%Y/month=%m/day=%d", "year=2025/month=10/day=07").unwrap();
        test_pattern("%Y-%m-%dT%H:%M:%S%.3fZ", "2025-10-07T21:09:28.595Z").unwrap();
        test_pattern("literal_text_%Y%m%d", "literal_text_20251007").unwrap();

        // invalid pattern
        test_pattern("%F/%H%M%S%L", "").unwrap_err();
    }
}
