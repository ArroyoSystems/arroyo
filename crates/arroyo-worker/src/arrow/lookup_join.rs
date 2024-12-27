use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{RecordBatch};
use arrow::row::{OwnedRow, RowConverter};
use async_trait::async_trait;
use datafusion::physical_expr::PhysicalExpr;

use arroyo_connectors::LookupConnector;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_types::JoinType;

/// A simple in-operator cache storing the entire “right side” row batch keyed by a string.
pub struct LookupJoin {
    connector: Box<dyn LookupConnector + Send>,
    key_exprs: Vec<Arc<dyn PhysicalExpr>>,
    cache: HashMap<Vec<u8>, OwnedRow>,
    key_row_converter: RowConverter,
    result_row_converter: RowConverter,
    join_type: JoinType,
}

#[async_trait]
impl ArrowOperator for LookupJoin {
    fn name(&self) -> String {
        format!("LookupJoin<{}>", self.connector.name())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        let num_rows = batch.num_rows();

        let key_arrays: Vec<_> = self
            .key_exprs
            .iter()
            .map(|expr| {
                expr.evaluate(&batch)
                    .unwrap()
                    .into_array(num_rows)
                    .unwrap()
            })
            .collect();

        let rows = self.key_row_converter.convert_columns(&key_arrays).unwrap();

        let mut key_map: HashMap<_, Vec<usize>> = HashMap::new();
        for (i, row) in rows.iter().enumerate() {
            key_map.entry(row.owned()).or_default().push(i);
        }

        let mut uncached_keys = Vec::new();
        for k in key_map.keys() {
            if !self.cache.contains_key(k.row().as_ref()) {
                uncached_keys.push(k.clone());
            }
        }

        if !uncached_keys.is_empty() {
            let cols = self.key_row_converter.convert_rows(uncached_keys.iter().map(|r| r.row())).unwrap();

            let result_batch = self
                .connector
                .lookup(&cols)
                .await;

            if let Some(result_batch) = result_batch {
                let result_rows = self.result_row_converter.convert_columns(result_batch.unwrap().columns())
                    .unwrap();

                assert_eq!(result_rows.num_rows(), uncached_keys.len());

                for (k, v) in uncached_keys.iter().zip(result_rows.iter()) {
                    self.cache.insert(k.as_ref().to_vec(), v.owned());
                }
            }
        }

        let mut output_rows = self.result_row_converter.empty_rows(batch.num_rows(), batch.num_rows() * 10);

        for row in rows.iter() {
            output_rows.push(self.cache.get(row.data()).expect("row should be cached").row());
        }
        
        let right_side = self.result_row_converter.convert_rows(output_rows.iter()).unwrap();
        let mut result = batch.columns().to_vec();
        result.extend(right_side);
        
        collector.collect(RecordBatch::try_new(ctx.out_schema.as_ref().unwrap().schema.clone(), result).unwrap())
            .await;
    }
}
