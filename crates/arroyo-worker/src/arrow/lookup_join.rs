use arrow::compute::filter_record_batch;
use arrow::row::{OwnedRow, RowConverter, SortField};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arroyo_connectors::connectors;
use arroyo_operator::connector::LookupConnector;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::{
    ArrowOperator, ConstructedOperator, OperatorConstructor, Registry,
};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::api;
use arroyo_rpc::{MetadataField, OperatorConfig};
use arroyo_types::LOOKUP_KEY_INDEX_FIELD;
use async_trait::async_trait;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::protobuf::PhysicalExprNode;
use mini_moka::sync::Cache;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Copy, Clone, PartialEq)]
pub(crate) enum LookupJoinType {
    Left,
    Inner,
}

/// A simple in-operator cache storing the entire “right side” row batch keyed by a string.
pub struct LookupJoin {
    connector: Box<dyn LookupConnector + Send>,
    key_exprs: Vec<Arc<dyn PhysicalExpr>>,
    cache: Option<Cache<OwnedRow, OwnedRow>>,
    key_row_converter: RowConverter,
    result_row_converter: RowConverter,
    join_type: LookupJoinType,
    lookup_schema: Arc<Schema>,
    metadata_fields: Vec<MetadataField>,
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
    ) -> DataflowResult<()> {
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

        let uncached_keys = if let Some(cache) = &mut self.cache {
            let mut uncached_keys = Vec::new();
            for k in key_map.keys() {
                if !cache.contains_key(k) {
                    uncached_keys.push(k);
                }
            }
            uncached_keys
        } else {
            key_map.keys().collect()
        };

        let mut results = HashMap::new();

        #[allow(unused_assignments)]
        let mut result_rows = None;

        if !uncached_keys.is_empty() {
            let cols = self
                .key_row_converter
                .convert_rows(uncached_keys.iter().map(|r| r.row()))
                .unwrap();

            let result_batch = self.connector.lookup(&cols).await;

            if let Some(result_batch) = result_batch {
                let mut result_batch = result_batch.unwrap();
                let key_idx_col = result_batch
                    .schema()
                    .index_of(LOOKUP_KEY_INDEX_FIELD)
                    .unwrap();

                let keys = result_batch.remove_column(key_idx_col);
                let keys = keys.as_primitive::<UInt64Type>();

                result_rows = Some(
                    self.result_row_converter
                        .convert_columns(result_batch.columns())
                        .unwrap(),
                );

                for (v, idx) in result_rows.as_ref().unwrap().iter().zip(keys) {
                    results.insert(uncached_keys[idx.unwrap() as usize].as_ref(), v);
                    if let Some(cache) = &mut self.cache {
                        cache.insert(uncached_keys[idx.unwrap() as usize].clone(), v.owned());
                    }
                }
            }
        }

        let mut output_rows = self
            .result_row_converter
            .empty_rows(batch.num_rows(), batch.num_rows() * 10);

        for row in rows.iter() {
            let row = self
                .cache
                .as_mut()
                .and_then(|c| c.get(&row.owned()))
                .unwrap_or_else(|| results.get(row.as_ref()).unwrap().owned());

            output_rows.push(row.row());
        }

        let right_side = self
            .result_row_converter
            .convert_rows(output_rows.iter())
            .unwrap();

        let nonnull = (self.join_type == LookupJoinType::Inner).then(|| {
            let mut nonnull = vec![false; batch.num_rows()];

            for (_, a) in self
                .lookup_schema
                .fields
                .iter()
                .zip(right_side.iter())
                .filter(|(f, _)| {
                    !self
                        .metadata_fields
                        .iter()
                        .any(|m| &m.field_name == f.name())
                })
            {
                if let Some(nulls) = a.logical_nulls() {
                    for (a, b) in nulls.iter().zip(nonnull.iter_mut()) {
                        *b |= a;
                    }
                } else {
                    nonnull.fill(true);
                    break;
                }
            }

            BooleanArray::from(nonnull)
        });

        let in_schema = ctx.in_schemas.first().unwrap();
        let key_indices = in_schema.routing_keys().unwrap();
        let non_keys: Vec<_> = (0..batch.num_columns())
            .filter(|i| !key_indices.contains(i) && *i != in_schema.timestamp_index)
            .collect();

        let mut result = batch.project(&non_keys).unwrap().columns().to_vec();
        result.extend(right_side);
        result.push(batch.column(in_schema.timestamp_index).clone());

        let mut batch =
            RecordBatch::try_new(ctx.out_schema.as_ref().unwrap().schema.clone(), result).unwrap();

        if let Some(nonnull) = nonnull {
            batch = filter_record_batch(&batch, &nonnull).unwrap();
        }

        collector.collect(batch).await;
        Ok(())
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
        let operator_config: OperatorConfig = serde_json::from_str(&op.config)?;

        let result_row_converter = RowConverter::new(
            lookup_schema
                .schema_without_timestamp()
                .fields
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        let lookup_schema = Arc::new(
            lookup_schema
                .with_additional_fields(
                    [Field::new(LOOKUP_KEY_INDEX_FIELD, DataType::UInt64, false)].into_iter(),
                )?
                .schema_without_timestamp(),
        );

        let connector = connectors()
            .get(op.connector.as_str())
            .unwrap_or_else(|| panic!("No connector with name '{}'", op.connector))
            .make_lookup(operator_config.clone(), lookup_schema.clone())?;

        let max_capacity_bytes = config.max_capacity_bytes.unwrap_or(8 * 1024 * 1024);
        let cache = (max_capacity_bytes > 0).then(|| {
            let mut c = Cache::builder()
                .weigher(|k: &OwnedRow, v: &OwnedRow| (k.as_ref().len() + v.as_ref().len()) as u32)
                .max_capacity(max_capacity_bytes);

            if let Some(ttl) = config.ttl_micros {
                c = c.time_to_live(Duration::from_micros(ttl));
            }

            c.build()
        });

        Ok(ConstructedOperator::from_operator(Box::new(LookupJoin {
            connector,
            cache,
            key_row_converter: RowConverter::new(
                exprs
                    .iter()
                    .map(|e| Ok(SortField::new(e.data_type(&input_schema.schema)?)))
                    .collect::<anyhow::Result<_>>()?,
            )?,
            key_exprs: exprs,
            result_row_converter,
            join_type: match join_type {
                api::JoinType::Inner => LookupJoinType::Inner,
                api::JoinType::Left => LookupJoinType::Left,
                jt => unreachable!("invalid lookup join type {:?}", jt),
            },
            lookup_schema,
            metadata_fields: operator_config.metadata_fields,
        })))
    }
}
