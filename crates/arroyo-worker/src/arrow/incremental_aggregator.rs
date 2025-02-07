use crate::arrow::updating_cache::{Key, UpdatingCache};
use anyhow::{anyhow, bail, Result};
use arrow::row::{RowConverter, SortField};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StructArray};
use arrow_schema::{Field, Schema, SchemaBuilder};
use arroyo_operator::context::Collector;
use arroyo_operator::{
    context::OperatorContext,
    operator::{
        ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator,
        OperatorConstructor, Registry,
    },
};
use arroyo_planner::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::{api::UpdatingAggregateOperator, rpc::TableConfig};
use arroyo_rpc::{updating_meta_fields, TIMESTAMP_FIELD, UPDATING_META_FIELD};
use arroyo_state::timestamp_table_config;
use arroyo_types::{CheckpointBarrier, SignalMessage, Watermark};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::{Accumulator, PhysicalExpr};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use prost::Message;
use std::borrow::Cow;
use std::time::{Duration, Instant};
use std::{
    collections::HashMap,
    mem,
    sync::{Arc, RwLock},
};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use datafusion_proto::protobuf::PhysicalExprNode;
use itertools::Itertools;
use tracing::debug;
use tracing::log::warn;
use crate::arrow::decode_aggregate;

/// Abstract over aggregations that support retracts (sliding accumulators), which we can use
/// directly, and those that don't in which case we just need to store the raw values and aggregate
/// them on demand
enum IncrementalState {
    Sliding {
        accumulator: Box<dyn Accumulator>,
    },
    Batch {
        expr: Arc<AggregateFunctionExpr>,
        data: HashMap<Vec<u8>, usize>,
        row_converter: Arc<RowConverter>,
    },
}

impl IncrementalState {
    fn update_batch(&mut self, batch: &[ArrayRef]) -> DFResult<()> {
        match self {
            IncrementalState::Sliding { accumulator, .. } => {
                accumulator.update_batch(batch)?;
            }
            IncrementalState::Batch {
                data,
                row_converter,
                ..
            } => {
                for r in row_converter.convert_columns(batch)?.iter() {
                    if data.contains_key(r.as_ref()) {
                        *data.get_mut(r.as_ref()).unwrap() += 1;
                    } else {
                        data.insert(r.as_ref().to_vec(), 1);
                    }
                }
            }
        }

        Ok(())
    }

    fn retract_batch(&mut self, batch: &[ArrayRef]) -> DFResult<()> {
        match self {
            IncrementalState::Sliding { accumulator, .. } => accumulator.retract_batch(batch),
            IncrementalState::Batch {
                data,
                row_converter,
                ..
            } => {
                for r in row_converter.convert_columns(batch)?.iter() {
                    if data.contains_key(r.as_ref()) {
                        let v = data.get_mut(r.as_ref()).unwrap();
                        if *v == 1 {
                            data.remove(r.as_ref());
                        } else {
                            *v -= 1;
                        }
                    } else {
                        debug!(
                            "tried to retract value for missing key: {:?}; this implies an append \
                        was lost (possibly from source)",
                            batch
                        )
                    }
                }
                Ok(())
            }
        }
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        match self {
            IncrementalState::Sliding { accumulator, .. } => accumulator.evaluate(),
            IncrementalState::Batch {
                expr,
                data,
                row_converter,
                ..
            } => {
                if data.is_empty() {
                    Ok(ScalarValue::Null)
                } else {
                    let parser = row_converter.parser();
                    let input = row_converter.convert_rows(data.keys().map(|v| parser.parse(v)))?;
                    let mut acc = expr.create_accumulator()?;
                    acc.update_batch(&input)?;
                    acc.evaluate_mut()
                }
            }
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum AccumulatorType {
    Sliding,
    Batch,
}

pub struct IncrementalAggregatingFunc {
    flush_interval: Duration,
    metadata_expr: Arc<dyn PhysicalExpr>,
    aggregates: Vec<(Arc<AggregateFunctionExpr>, AccumulatorType)>,
    accumulators: UpdatingCache<Vec<IncrementalState>>,
    agg_row_converters: Vec<Arc<RowConverter>>,
    updated_keys: HashMap<Key, Option<Vec<ScalarValue>>>,
    state_schema: Arc<ArroyoSchema>,
    schema_without_metadata: Arc<Schema>,
    ttl: Duration,
    key_converter: RowConverter,
}

const GLOBAL_KEY: Vec<u8> = vec![];

impl IncrementalAggregatingFunc {
    fn update_batch(
        &mut self,
        key: &[u8],
        batch: &[Vec<ArrayRef>],
        idx: Option<usize>,
    ) -> DFResult<()> {
        self.accumulators
            .modify_and_update(key, Instant::now(), |values| {
                for (inputs, accs) in batch.iter().zip(values.iter_mut()) {
                    let values = if let Some(idx) = idx {
                        &inputs.iter().map(|c| c.slice(idx, 1)).collect()
                    } else {
                        inputs
                    };

                    accs.update_batch(values)?;
                }
                Ok(())
            })
            .expect("tried to update for non-existent key")
    }

    fn retract_batch(
        &mut self,
        key: &[u8],
        batch: &[Vec<ArrayRef>],
        idx: Option<usize>,
    ) -> DFResult<()> {
        self.accumulators
            .modify(key, |values| {
                for (inputs, accs) in batch.iter().zip(values.iter_mut()) {
                    let values = if let Some(idx) = idx {
                        &inputs.iter().map(|c| c.slice(idx, 1)).collect()
                    } else {
                        inputs
                    };

                    accs.retract_batch(values)?;
                }
                Ok::<(), datafusion::common::DataFusionError>(())
            })
            .expect("tried to retract state for non-existent key")?;

        Ok(())
    }

    fn evaluate(&mut self, key: &[u8]) -> DFResult<Vec<ScalarValue>> {
        self.accumulators
            .get_mut(key)
            .expect("tried to evaluate non-existent key")
            .iter_mut()
            .map(|s| s.evaluate())
            .collect::<DFResult<_>>()
    }

    fn flush(&mut self, ctx: &mut OperatorContext) -> Result<Option<RecordBatch>> {
        let mut output_keys = Vec::with_capacity(self.updated_keys.len() * 2);
        let mut output_values =
            vec![Vec::with_capacity(self.updated_keys.len() * 2); self.aggregates.len()];
        let mut is_retracts = Vec::with_capacity(self.updated_keys.len() * 2);

        let (updated_keys, updated_values): (Vec<_>, Vec<_>) =
            mem::take(&mut self.updated_keys).into_iter().unzip();

        let mut deleted_keys = vec![];

        for (k, retract) in updated_keys.iter().zip(updated_values.into_iter()) {
            let append = self.evaluate(&k.0)?;

            if let Some(v) = retract {
                // don't bother emitting updates that just retract / append the same values (excluding
                // the last, timestamp field)
                if v.iter()
                    .zip(append.iter())
                    .take(v.len() - 1)
                    .all(|(a, b)| a == b)
                {
                    continue;
                }

                is_retracts.push(true);
                output_keys.push(k);
                for (out, v) in output_values.iter_mut().zip(v) {
                    out.push(v);
                }
            }

            if !append.last().unwrap().is_null() {
                // if the timestamp is null, that means we've removed all of the data from
                // this key, and we shouldn't emit an append
                is_retracts.push(false);
                output_keys.push(k);
                for (out, v) in output_values.iter_mut().zip(append) {
                    out.push(v);
                }
            } else {
                deleted_keys.push(k);
            }
        }

        for k in deleted_keys {
            self.accumulators.remove(&k.0);
        }

        let mut ttld_keys = vec![];

        for (k, mut v) in self.accumulators.time_out(Instant::now()) {
            // retract items that are being ttl'd
            is_retracts.push(true);
            ttld_keys.push(k);

            for (out, v) in output_values
                .iter_mut()
                .zip(v.iter_mut().map(|s| s.evaluate()))
            {
                out.push(v?);
            }
        }

        if output_keys.is_empty() {
            return Ok(None);
        }

        let row_parser = self.key_converter.parser();
        let mut result_cols = self.key_converter.convert_rows(
            output_keys
                .iter()
                .map(|k| row_parser.parse(k.0.as_slice()))
                .chain(ttld_keys.iter().map(|k| row_parser.parse(k.as_slice()))),
        )?;

        for acc in output_values {
            result_cols.push(ScalarValue::iter_to_array(acc).unwrap());
        }

        // push the metadata column
        let record_batch =
            RecordBatch::try_new(self.schema_without_metadata.clone(), result_cols).unwrap();

        let metadata = self
            .metadata_expr
            .evaluate(&record_batch)
            .unwrap()
            .into_array(record_batch.num_rows())
            .unwrap();
        let metadata = set_retract_metadata(metadata, Arc::new(BooleanArray::from(is_retracts)));

        let mut final_batch = record_batch.columns().to_vec();
        final_batch.push(metadata);

        Ok(Some(RecordBatch::try_new(
            ctx.out_schema.as_ref().unwrap().schema.clone(),
            final_batch,
        )?))
    }

    fn get_retracts(batch: &RecordBatch) -> Option<&BooleanArray> {
        let retracts = if let Some(meta_col) = batch.column_by_name(UPDATING_META_FIELD) {
            let meta_struct = meta_col
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("_updating_meta must be StructArray");

            let is_retract_array = meta_struct
                .column_by_name("is_retract")
                .expect("meta struct must have is_retract");
            let is_retract = is_retract_array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("is_retract must be BooleanArray");

            Some(is_retract)
        } else {
            None
        };
        retracts
    }

    fn make_accumulators(&self) -> Vec<IncrementalState> {
        self.aggregates
            .iter()
            .zip(self.agg_row_converters.iter())
            .map(|((agg, accumulator_type), row_converter)| {
                match accumulator_type {
                    AccumulatorType::Sliding => IncrementalState::Sliding { 
                        accumulator: agg.create_sliding_accumulator().unwrap() 
                    },
                    AccumulatorType::Batch => {
                        IncrementalState::Batch {
                            expr: agg.clone(),
                            data: Default::default(),
                            row_converter: row_converter.clone(),
                        }
                    }
                }
            })
            .collect()
    }

    fn global_aggregate(&mut self, batch: &RecordBatch) -> Result<()> {
        let retracts = Self::get_retracts(batch);

        let aggregate_input_cols = self.compute_inputs(&batch);

        let mut first = false;

        #[allow(clippy::map_entry)]
        // workaround for https://github.com/rust-lang/rust-clippy/issues/13934
        if !self.accumulators.contains_key(&GLOBAL_KEY) {
            first = true;
            self.accumulators.insert(
                Arc::new(GLOBAL_KEY),
                Instant::now(),
                self.make_accumulators(),
            );
        }

        if self.updated_keys.contains_key(GLOBAL_KEY.as_slice()) {
            if first {
                self.updated_keys.insert(Key(Arc::new(GLOBAL_KEY)), None);
            } else {
                let v = Some(self.evaluate(&GLOBAL_KEY)?);
                self.updated_keys.insert(Key(Arc::new(GLOBAL_KEY)), v);
            }
        }

        // update / retract the values against the accumulators

        if let Some(retracts) = retracts {
            for (i, r) in retracts.iter().enumerate() {
                if r.unwrap_or_default() {
                    self.retract_batch(&GLOBAL_KEY, &aggregate_input_cols, Some(i))?;
                } else {
                    self.update_batch(&GLOBAL_KEY, &aggregate_input_cols, Some(i))?;
                }
            }
        } else {
            // if these are all appends, we can much more efficiently do all of the updates at once
            self.update_batch(&GLOBAL_KEY, &aggregate_input_cols, None)
                .unwrap();
        }

        Ok(())
    }

    fn keyed_aggregate(&mut self, batch: &RecordBatch, ctx: &OperatorContext) -> Result<()> {
        let retracts = Self::get_retracts(batch);

        let sort_columns = &ctx.in_schemas[0]
            .sort_columns(batch, false)
            .into_iter()
            .map(|e| e.values)
            .collect::<Vec<_>>();

        let keys = self.key_converter.convert_columns(sort_columns).unwrap();

        // store the initial values for keys which we are updating for the first time for the current
        // flush, so that we can retract them
        for k in &keys {
            if !self.updated_keys.contains_key(k.as_ref()) {
                if let Some((key, accs)) = self.accumulators.get_mut_key_value(k.as_ref()) {
                    self.updated_keys.insert(
                        key,
                        Some(
                            accs.iter_mut()
                                .map(|s| s.evaluate())
                                .collect::<DFResult<_>>()?,
                        ),
                    );
                } else {
                    self.updated_keys
                        .insert(Key(Arc::new(k.as_ref().to_vec())), None);
                }
            }
        }

        // then update the states with the new data
        let aggregate_input_cols = self.compute_inputs(&batch);

        for (i, key) in keys.iter().enumerate() {
            if self.accumulators.contains_key(key.as_ref()) {
                self.accumulators.get_mut(key.as_ref()).unwrap()
            } else {
                let new_accumulators = self.make_accumulators();
                self.accumulators.insert(
                    Arc::new(key.as_ref().to_vec()),
                    Instant::now(),
                    new_accumulators,
                );
                self.accumulators.get_mut(key.as_ref()).unwrap()
            };

            let retract = retracts.map(|r| r.value(i)).unwrap_or_default();
            if retract {
                self.retract_batch(key.as_ref(), &aggregate_input_cols, Some(i))?;
            } else {
                self.update_batch(key.as_ref(), &aggregate_input_cols, Some(i))?;
            }
        }

        Ok(())
    }

    fn compute_inputs(&self, batch: &&RecordBatch) -> Vec<Vec<ArrayRef>> {
        let aggregate_input_cols = self
            .aggregates
            .iter()
            .map(|(agg, _)| {
                agg.expressions()
                    .iter()
                    .map(|ex| {
                        ex.evaluate(batch)
                            .unwrap()
                            .into_array(batch.num_rows())
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        aggregate_input_cols
    }
}

#[async_trait::async_trait]
impl ArrowOperator for IncrementalAggregatingFunc {
    fn name(&self) -> String {
        "UpdatingAggregatingFunc".to_string()
    }

    fn display(&self) -> DisplayableOperator {
        let aggregates = self
            .aggregates
            .iter()
            .map(|(f, t)| {
                format!(
                    "{} ({:?})",
                    f.name(),
                    t
                )
            })
            .collect::<Vec<_>>();

        println!("aggregates = {:?}", self.aggregates);

        DisplayableOperator {
            name: Cow::Borrowed("UpdatingAggregatingFunc"),
            fields: vec![
                ("flush_interval", AsDisplayable::Debug(&self.flush_interval)),
                ("ttl", AsDisplayable::Debug(&self.ttl)),
                (
                    "state_schema",
                    AsDisplayable::Schema(&self.state_schema.schema),
                ),
                ("aggregates", AsDisplayable::List(aggregates)),
            ],
        }
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        let input_schema = &ctx.in_schemas[0];

        if input_schema
            .key_indices
            .as_ref()
            .map(|k| !k.is_empty())
            .unwrap_or_default()
        {
            self.keyed_aggregate(&batch, ctx).unwrap()
        } else {
            self.global_aggregate(&batch).unwrap()
        };
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        if let Some(batch) = self.flush(ctx).unwrap() {
            collector.collect(batch).await;
        }
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        vec![(
            "a".to_string(),
            timestamp_table_config(
                "a",
                "accumulator_state",
                self.ttl,
                true,
                self.state_schema.as_ref().clone(),
            ),
        )]
        .into_iter()
        .collect()
    }

    fn tick_interval(&self) -> Option<Duration> {
        Some(self.flush_interval)
    }

    async fn handle_tick(
        &mut self,
        _tick: u64,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        if let Some(batch) = self.flush(ctx).unwrap() {
            collector.collect(batch).await;
        }
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> Option<Watermark> {
        // let last_watermark = ctx.last_present_watermark();
        // let partial_table = ctx
        //     .table_manager
        //     .get_last_key_value_table("p", last_watermark)
        //     .await
        //     .expect("should have partial table");
        // if partial_table.would_expire(last_watermark) {
        //     self.flush(ctx, collector).await.unwrap();
        // }
        // let partial_table = ctx
        //     .table_manager
        //     .get_last_key_value_table("p", last_watermark)
        //     .await
        //     .expect("should have partial table");
        // partial_table
        //     .expire(last_watermark)
        //     .expect("should expire partial table");

        Some(watermark)
    }

    async fn on_close(
        &mut self,
        final_message: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        if let Some(SignalMessage::EndOfData) = final_message {
            if let Some(batch) = self.flush(ctx).unwrap() {
                collector.collect(batch).await;
            }
        }
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) {
        let table = ctx.table_manager
            .get_uncached_kv_table("a", ctx.last_present_watermark())
            .await
            .unwrap();
        
        // initialize the accumualtor cache
        table.
    }
}

fn set_retract_metadata(metadata: ArrayRef, is_retract: Arc<BooleanArray>) -> ArrayRef {
    let metadata = metadata.as_struct();

    let arrays: Vec<Arc<dyn Array>> = vec![is_retract, metadata.column(1).clone()];
    Arc::new(StructArray::new(updating_meta_fields(), arrays, None))
}

pub struct IncrementalAggregatingConstructor;

impl OperatorConstructor for IncrementalAggregatingConstructor {
    type ConfigT = UpdatingAggregateOperator;

    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let ttl = Duration::from_micros(if config.ttl_micros == 0 {
            warn!("ttl was not set for updating aggregate");
            24 * 60 * 60 * 1000 * 1000
        } else {
            config.ttl_micros
        });

        let input_schema: ArroyoSchema = config.input_schema.unwrap().try_into()?;
        let final_schema: ArroyoSchema = config.final_schema.unwrap().try_into()?;
        let mut schema_without_metadata = SchemaBuilder::from((*final_schema.schema).clone());
        schema_without_metadata.remove(final_schema.schema.index_of(UPDATING_META_FIELD).unwrap());

        let metadata_expr =
            parse_physical_expr(&PhysicalExprNode::decode(&mut config.metadata_expr.as_slice())?,
                registry.as_ref(),
                &input_schema.schema,
                &DefaultPhysicalExtensionCodec{})?;
        
        let aggregate_exec = PhysicalPlanNode::decode(&mut config.aggregate_exec.as_ref())?;
        let PhysicalPlanType::Aggregate(aggregate_exec) = aggregate_exec.physical_plan_type.unwrap() else {
            bail!("invalid proto -- expected aggregate exec");
        };

        let aggregates: Vec<_> = aggregate_exec.aggr_expr
            .iter()
            .zip(aggregate_exec.aggr_expr_name.iter())
            .map(|(expr, name)| {
                Ok(decode_aggregate(&input_schema.schema, &name, expr, registry.as_ref())?)
            })
            .map_ok((|agg| {
                let retract = match agg.create_sliding_accumulator() {
                    Ok(s) => s.supports_retract_batch(),
                    _ => false,
                };

                (agg, if retract { AccumulatorType::Sliding } else { AccumulatorType::Batch })
            }))
            .collect::<Result<_>>()?;


        let mut agg_row_converters = vec![];
        for (agg, _) in &aggregates {
            agg_row_converters.push(Arc::new(RowConverter::new(
                agg
                    .expressions()
                    .iter()
                    .map(|ex| Ok(SortField::new(ex.data_type(&input_schema.schema)?)))
                    .collect::<DFResult<_>>()?,
            )?))
        }

        // the state schema is made up of the key fields + the state fields for each aggregator
        // (if it supports retraction) otherwise, the input data + a count
        let mut state_fields = input_schema.key_indices
            .as_ref()
            .map(|v| v.iter()
                .map(|idx| input_schema.schema.field(*idx).clone())
                .collect_vec())
            .unwrap_or_default();
        
        let key_fields = (0..state_fields.len()).collect_vec();

        for (agg, accumulator_type) in &aggregates {
            match accumulator_type {
                AccumulatorType::Sliding => {
                    state_fields.extend(agg.state_fields()?.into_iter());
                }
                AccumulatorType::Batch => {
                    for (i, expr) in agg.expressions().iter().enumerate() {
                        state_fields.push(Field::new(
                            format!("{}_{}", agg.name(), i),
                            expr.data_type(&input_schema.schema)?, expr.nullable(&input_schema.schema)?));
                    }
                }
            }
        }
        
        // ensure the last field (timestamp) has the expected name 
        let timestamp_field = state_fields.pop().unwrap();
        state_fields.push(timestamp_field.with_name(TIMESTAMP_FIELD));
        
        let state_schema = Arc::new(ArroyoSchema::from_schema_keys(
            Schema::new(state_fields).into(), key_fields)?);

        Ok(ConstructedOperator::from_operator(Box::new(
            IncrementalAggregatingFunc {
                flush_interval: Duration::from_micros(config.flush_interval_micros),
                metadata_expr,
                ttl,
                aggregates,
                accumulators: UpdatingCache::with_time_to_idle(ttl),
                schema_without_metadata: Arc::new(schema_without_metadata.finish()),
                updated_keys: Default::default(),
                key_converter: RowConverter::new(input_schema.sort_fields(false))?,
                agg_row_converters,
                state_schema,
            },
        )))
    }
}
