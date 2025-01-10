use std::collections::HashMap;
use std::ops::Index;
use std::sync::Arc;

use arrow::row::{OwnedRow, RowConverter, SortField};
use arrow_array::{Array, RecordBatch};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_schema::DataType;
use arroyo_connectors::connectors;
use arroyo_operator::connector::LookupConnector;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{ArrowOperator, ConstructedOperator, DisplayableOperator, OperatorConstructor, Registry};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::api;
use arroyo_types::{JoinType, LOOKUP_KEY_INDEX_FIELD};
use async_trait::async_trait;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;

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
        format!("LookupJoin({})", self.connector.name())
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
            .map(|expr| expr.evaluate(&batch).unwrap().into_array(num_rows).unwrap())
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
            let cols = self
                .key_row_converter
                .convert_rows(uncached_keys.iter().map(|r| r.row()))
                .unwrap();

            let result_batch = self.connector.lookup(&cols).await;

            println!("Batch = {:?}", result_batch);

            if let Some(result_batch) = result_batch {
                let mut result_batch = result_batch.unwrap();
                let key_idx_col = result_batch.schema().index_of(LOOKUP_KEY_INDEX_FIELD).unwrap();

                let keys = result_batch.remove_column(key_idx_col);
                let keys = keys.as_primitive::<UInt64Type>();

                let result_rows = self
                    .result_row_converter
                    .convert_columns(result_batch.columns())
                    .unwrap();

                for (v, idx) in result_rows.iter().zip(keys) {
                    self.cache.insert(uncached_keys[idx.unwrap() as usize].as_ref().to_vec(), v.owned());
                }
            }
        }

        let mut output_rows = self
            .result_row_converter
            .empty_rows(batch.num_rows(), batch.num_rows() * 10);

        println!("Cache {:?}", self.cache);

        for row in rows.iter() {
            output_rows.push(
                self.cache
                    .get(row.data())
                    .expect("row should be cached")
                    .row(),
            );
        }

        let right_side = self
            .result_row_converter
            .convert_rows(output_rows.iter())
            .unwrap();

        let mut result = batch.columns().to_vec();
        result.extend(right_side);

        println!("SCHEMA = {:?}", ctx.out_schema.as_ref().unwrap().schema);
        println!("RESULT COLS = {:?}", result.iter().map(|s| s.data_type()));

        collector
            .collect(
                RecordBatch::try_new(ctx.out_schema.as_ref().unwrap().schema.clone(), result)
                    .unwrap(),
            )
            .await;
    }
}

pub struct LookupJoinConstructor;
impl OperatorConstructor for LookupJoinConstructor {
    type ConfigT = api::LookupJoinOperator;
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let join_type = config.join_type();
        let input_schema: ArroyoSchema = config.input_schema.unwrap().try_into()?;
        let lookup_schema: ArroyoSchema = config.lookup_schema.unwrap().try_into()?;

        let exprs = config
            .key_exprs
            .iter()
            .map(|e| {
                let expr = PhysicalExprNode::decode(&mut e.left_expr.as_slice())?;
                Ok(parse_physical_expr(
                    &expr,
                    registry.as_ref(),
                    &input_schema.schema,
                    &DefaultPhysicalExtensionCodec {},
                )?)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let op = config.connector.unwrap();
        let operator_config = serde_json::from_str(&op.config)?;

        let result_row_converter = RowConverter::new(
            lookup_schema
                .schema_without_timestamp()
                .fields
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        let lookup_schema = lookup_schema
            .with_field(LOOKUP_KEY_INDEX_FIELD, DataType::UInt64, false)?
            .schema_without_timestamp();

        let connector = connectors()
            .get(op.connector.as_str())
            .unwrap_or_else(|| panic!("No connector with name '{}'", op.connector))
            .make_lookup(operator_config, Arc::new(lookup_schema))?;

        Ok(ConstructedOperator::from_operator(Box::new(LookupJoin {
            connector,
            cache: Default::default(),
            key_row_converter: RowConverter::new(
                exprs
                    .iter()
                    .map(|e| Ok(SortField::new(e.data_type(&input_schema.schema)?)))
                    .collect::<anyhow::Result<_>>()?,
            )?,
            key_exprs: exprs,
            result_row_converter,
            join_type: join_type.into(),
        })))
    }
}
