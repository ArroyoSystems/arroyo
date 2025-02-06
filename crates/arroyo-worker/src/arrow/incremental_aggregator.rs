use anyhow::{anyhow, Result};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StructArray};
use std::borrow::{Borrow, Cow};
use std::{
    collections::HashMap,
    mem,
    sync::{Arc, RwLock},
};
use std::collections::LinkedList;
use std::ptr::null_mut;
use arrow::row::{RowConverter, SortField};
use arrow_array::cast::AsArray;
use arrow_schema::{Schema, SchemaBuilder};
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
use arroyo_rpc::{updating_meta_fields, UPDATING_META_FIELD};
use arroyo_state::timestamp_table_config;
use arroyo_types::{CheckpointBarrier, SignalMessage, Watermark};
use datafusion::common::{exec_datafusion_err, Result as DFResult, ScalarValue};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::aggregates::{AggregateExec};
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::{execute_input_stream, Accumulator, PhysicalExpr};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use prost::Message;
use std::time::{Duration, Instant};
use rand::{thread_rng, Rng};
use tracing::debug;
use tracing::log::warn;



pub struct IncrementalAggregatingFunc {
    flush_interval: Duration,
    metadata_expr: Arc<dyn PhysicalExpr>,
    aggregates: Vec<Arc<AggregateFunctionExpr>>,
    accumulators: HashMap<Vec<u8>, CacheEntry>,
    agg_row_converters: Vec<Arc<RowConverter>>,
    updated_keys: HashMap<Vec<u8>, Option<Vec<ScalarValue>>>,
    partial_schema: Arc<ArroyoSchema>,
    schema_without_metadata: Arc<Schema>,
    ttl: Duration,
    key_converter: RowConverter,
    last_timed_out: Instant,
}

const GLOBAL_KEY: Vec<u8> = vec![];

impl IncrementalAggregatingFunc {
    fn flush(&mut self, ctx: &mut OperatorContext) -> Result<Option<RecordBatch>> {
        let mut output_keys = Vec::with_capacity(self.updated_keys.len() * 2);
        let mut output_values =
            vec![Vec::with_capacity(self.updated_keys.len() * 2); self.aggregates.len()];
        let mut is_retracts = Vec::with_capacity(self.updated_keys.len() * 2);

        let (updated_keys, updated_values): (Vec<_>, Vec<_>) =
            mem::take(&mut self.updated_keys).into_iter().unzip();

        let mut deleted_keys = vec![];

        for (k, retract) in updated_keys.iter().zip(updated_values.into_iter()) {
            let append = self
                .accumulators
                .get_mut(k)
                .unwrap()
                .evaluate()?;

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
            self.accumulators.remove(k);
        }

        if output_keys.is_empty() {
            return Ok(None);
        }

        let row_parser = self.key_converter.parser();
        let mut result_cols = self.key_converter
            .convert_rows(output_keys.iter().map(|k| row_parser.parse(k.as_slice())))?;

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

    fn time_out_keys(&mut self) {
        let now = Instant::now();
        self.accumulators
            .retain(|_, v| (now - v.updated) < self.ttl);
        self.last_timed_out = now;
    }

    fn get_aggregate_state_cols(&self, batch: &RecordBatch) -> Vec<Vec<Arc<dyn Array>>> {
        self.aggregates
            .iter()
            .map(|agg| {
                agg.state_fields()
                    .unwrap()
                    .into_iter()
                    .map(|f| {
                        let idx = self.partial_schema.schema.index_of(f.name()).unwrap();
                        batch.column(idx).clone()
                    })
                    .collect()
            })
            .collect()
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

    fn make_accumulators(&self) -> CacheEntry {
        let accumulators = self.aggregates
            .iter()
            .zip(self.agg_row_converters.iter())
            .map(|(agg, row_converter)| {
                let accumulator = agg.create_accumulator().unwrap();
                if accumulator.supports_retract_batch() {
                    IncrementalState::Sliding {
                        accumulator
                    }
                } else {
                    IncrementalState::Batch {
                        expr: agg.clone(),
                        data: Default::default(),
                        row_converter: row_converter.clone(),
                    }
                }
            })
            .collect();

        CacheEntry {
            updated: Instant::now(),
            data: accumulators,
        }
    }

    fn global_aggregate(&mut self, batch: &RecordBatch) -> Result<()> {
        let retracts = Self::get_retracts(batch);

        let aggregate_input_cols = self.compute_inputs(&batch);

        let mut first = false;
        
        #[allow(clippy::map_entry)] // workaround for https://github.com/rust-lang/rust-clippy/issues/13934
        if !self.accumulators.contains_key(&GLOBAL_KEY) {
            first = true;
            self.accumulators
                .insert(GLOBAL_KEY, self.make_accumulators());
        }

        let accumulators = self.accumulators.get_mut(&GLOBAL_KEY).unwrap();

        if let std::collections::hash_map::Entry::Vacant(e) = self.updated_keys.entry(GLOBAL_KEY) {
            if first {
                e.insert(None);
            } else {
                e.insert(Some(
                        accumulators
                            .data
                            .iter_mut()
                            .map(|acc| acc.evaluate())
                            .collect::<DFResult<_>>()?,
                    ));
            }
        }

        // update / retract the values against the accumulators

        if let Some(retracts) = retracts {
            for (i, r) in retracts.iter().enumerate() {
                if r.unwrap_or_default() {
                    accumulators.retract_batch(&aggregate_input_cols, Some(i))?;
                } else {
                    accumulators.update_batch(&aggregate_input_cols, Some(i))?;
                }

            }
        } else {
            // if these are all appends, we can much more efficiently do all of the updates at once
            accumulators.update_batch(&aggregate_input_cols, None).unwrap();
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
                if let Some(accs) = self.accumulators.get_mut(k.as_ref()) {
                    self.updated_keys.insert(
                        k.as_ref().to_vec(),
                        Some(
                            accs.evaluate()?
                        ),
                    );
                } else {
                    self.updated_keys.insert(k.as_ref().to_vec(), None);
                }
            }
        }

        // then update the states with the new data
        let aggregate_input_cols = self.compute_inputs(&batch);

        for (i, key) in keys.iter().enumerate() {
            let row_accumulators = if self.accumulators.contains_key(key.as_ref()) {
                self.accumulators.get_mut(key.as_ref()).unwrap()
            } else {
                let new_accumulators = self.make_accumulators();
                self.accumulators
                    .insert(key.as_ref().to_vec(), new_accumulators);
                self.accumulators.get_mut(key.as_ref()).unwrap()
            };

            let retract = retracts.map(|r| r.value(i)).unwrap_or_default();
            if retract {
                row_accumulators.retract_batch(&aggregate_input_cols, Some(i))?;
            } else {
                row_accumulators.update_batch(&aggregate_input_cols, Some(i))?;
            }
        }

        Ok(())
    }

    fn compute_inputs(&self, batch: &&RecordBatch) -> Vec<Vec<ArrayRef>> {
        let aggregate_input_cols = self
            .aggregates
            .iter()
            .map(|agg| {
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
            .map(|f| {
                format!(
                    "{}({})",
                    f.name(),
                    f.expressions()
                        .iter()
                        .map(|ex| ex.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
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
                    "partial_schema",
                    AsDisplayable::Schema(&self.partial_schema.schema),
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
        let output_schema = ctx.out_schema.as_ref().unwrap();

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
            "p".to_string(),
            timestamp_table_config(
                "p",
                "partial_table",
                self.ttl,
                true,
                self.partial_schema.as_ref().clone(),
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
        if self.last_timed_out.elapsed() > Duration::from_secs(60 + thread_rng().gen_range(0..60)) {
            self.time_out_keys();
        }


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
        // ctx.table_manager
        //     .get_last_key_value_table("p", ctx.last_present_watermark())
        //     .await
        //     .unwrap();
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
        let receiver = Arc::new(RwLock::new(None));

        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::UnboundedBatchStream(receiver.clone()),
        };

        let partial_aggregation_plan =
            PhysicalPlanNode::decode(&mut config.partial_aggregation_plan.as_slice())?;

        // deserialize partial aggregation into execution plan with an UnboundedBatchStream source.
        let partial_aggregation_plan = partial_aggregation_plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::try_new(RuntimeConfig::new()).unwrap(),
            &codec,
        )?;

        let aggregate_exec: &AggregateExec =
            partial_aggregation_plan.as_any().downcast_ref().unwrap();

        let partial_schema: ArroyoSchema = config
            .partial_schema
            .ok_or_else(|| anyhow!("requires partial schema"))?
            .try_into()?;

        let combine_plan = PhysicalPlanNode::decode(&mut config.combine_plan.as_slice())?;
        let combine_execution_plan = combine_plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::try_new(RuntimeConfig::new()).unwrap(),
            &codec,
        )?;

        let finish_plan = PhysicalPlanNode::decode(&mut config.final_aggregation_plan.as_slice())?;

        let finish_execution_plan = finish_plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnv::try_new(RuntimeConfig::new()).unwrap(),
            &codec,
        )?;

        let ttl = if config.ttl_micros == 0 {
            warn!("ttl was not set for updating aggregate");
            24 * 60 * 60 * 1000 * 1000
        } else {
            config.ttl_micros
        };

        let input_schema: ArroyoSchema = config.input_schema.unwrap().try_into()?;
        let finish_schema = finish_execution_plan.schema();
        let mut schema_without_metadata = SchemaBuilder::from((*finish_schema).clone());
        schema_without_metadata.remove(finish_schema.index_of(UPDATING_META_FIELD).unwrap());
        
        let mut agg_row_converters = vec![];
        for agg in aggregate_exec.aggr_expr() {
            agg_row_converters.push(Arc::new(RowConverter::new(agg.expressions().iter()
                                  .map(|ex| Ok(SortField::new(ex.data_type(&input_schema.schema)?)))
                                  .collect::<DFResult<_>>()?)?))
        }

        Ok(ConstructedOperator::from_operator(Box::new(
            IncrementalAggregatingFunc {
                partial_schema: partial_schema.into(),
                flush_interval: Duration::from_micros(config.flush_interval_micros),
                metadata_expr: aggregate_exec.group_expr().expr().last().unwrap().0.clone(),
                ttl: Duration::from_micros(ttl),
                aggregates: aggregate_exec.aggr_expr().to_vec(),
                accumulators: Default::default(),
                schema_without_metadata: Arc::new(schema_without_metadata.finish()),
                updated_keys: Default::default(),
                key_converter: RowConverter::new(input_schema.sort_fields(false))?,
                agg_row_converters,
                last_timed_out: Instant::now(),
            },
        )))
    }
}
