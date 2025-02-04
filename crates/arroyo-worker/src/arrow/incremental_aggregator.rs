use anyhow::{anyhow, Result};
use arrow::compute::{concat, concat_batches, filter_record_batch, not, sort_to_indices};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StructArray};
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Mutex;
use std::{
    any::Any,
    collections::HashMap,
    pin::Pin,
    sync::{Arc, RwLock},
};

use arrow::row::{Row, RowConverter, SortField};
use arrow_array::cast::AsArray;
use arrow_schema::{DataType, Field, Schema, SchemaBuilder, SchemaRef, TimeUnit};
use arroyo_operator::context::Collector;
use arroyo_operator::{
    context::OperatorContext,
    operator::{
        ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator,
        OperatorConstructor, Registry,
    },
};
use arroyo_planner::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use arroyo_rpc::grpc::{api::UpdatingAggregateOperator, rpc::TableConfig};
use arroyo_rpc::{updating_meta_fields, Converter, TIMESTAMP_FIELD, UPDATING_META_FIELD};
use arroyo_state::tables::expiring_time_key_map::LastKeyValueView;
use arroyo_state::timestamp_table_config;
use arroyo_types::{CheckpointBarrier, SignalMessage, Watermark};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::execution::{
    runtime_env::{RuntimeConfig, RuntimeEnv},
    SendableRecordBatchStream,
};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, PhysicalGroupBy};
use datafusion::physical_plan::sorts::sort::sort_batch;
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::{Accumulator, PhysicalExpr};
use datafusion::prelude::col;
use datafusion::{execution::context::SessionContext, physical_plan::ExecutionPlan};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use futures::Future;
use itertools::Itertools;
use prost::Message;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::log::warn;

pub struct IncrementalAggregatingFunc {
    flush_interval: Duration,
    group_by: PhysicalGroupBy,
    metadata_expr: Arc<dyn PhysicalExpr>,
    aggregates: Vec<Arc<AggregateFunctionExpr>>,
    accumulators: Arc<Mutex<HashMap<Vec<u8>, Vec<Box<dyn Accumulator>>>>>,
    partial_schema: Arc<ArroyoSchema>,
    schema_without_metadata: Arc<Schema>,
    ttl: Duration,
}

impl IncrementalAggregatingFunc {
    async fn flush(
        &mut self,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> Result<()> {
        Ok(())
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

    fn make_accumulators(&self) -> Vec<Box<dyn Accumulator>> {
        self.aggregates
            .iter()
            .map(|agg| agg.create_sliding_accumulator().unwrap())
            .collect()
    }

    fn global_aggregate(&self, batch: &RecordBatch) -> Result<Option<Vec<ArrayRef>>> {
        let retracts = Self::get_retracts(batch);
        let mut accumulator_lock = self.accumulators.lock().unwrap();

        let mut first = false;

        let accumulators = accumulator_lock.entry(vec![]).or_insert_with(|| {
            first = true;
            self.make_accumulators()
        });

        let mut retract_outputs = Vec::with_capacity(2);
        let mut values: Vec<Vec<ScalarValue>> = vec![Vec::with_capacity(2); self.aggregates.len()];

        if !first {
            // emit a retract with the current value
            for (acc, vs) in accumulators.iter_mut().zip(values.iter_mut()) {
                vs.push(acc.evaluate()?);
            }
            retract_outputs.push(true);
        }

        // update / retract the values against the accumulators
        let aggregate_input_cols = self.compute_inputs(&batch);

        if let Some(retracts) = retracts {
            for (i, r) in retracts.iter().enumerate() {
                for (inputs, accs) in aggregate_input_cols.iter().zip(accumulators.iter_mut()) {
                    let values: Vec<_> = inputs.iter().map(|c| c.slice(i, 1)).collect();
                    if r.unwrap_or_default() {
                        accs.retract_batch(&values).unwrap();
                    } else {
                        accs.update_batch(&values).unwrap();
                    }
                }
            }
        } else {
            // if these are all appends, we can much more efficiently do all of the updates at once
            for (inputs, accs) in aggregate_input_cols.iter().zip(accumulators.iter_mut()) {
                accs.update_batch(&inputs).unwrap();
            }
        }

        retract_outputs.push(false);

        // emit the append
        for (acc, vs) in accumulators.iter_mut().zip(values.iter_mut()) {
            vs.push(acc.evaluate()?);
        }

        println!("values = {:?}", values);
        if values[0].len() == 2 {
            // don't bother emitting updates that just retract / append the same values (excluding
            // the last, timestamp field)
            if values[0..values.len() - 1].iter().all(|c| c[0] == c[1]) {
                return Ok(None);
            }
        }

        let result_cols: Vec<_> = values
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect::<DFResult<_>>()?;

        // push the metadata column
        let record_batch =
            RecordBatch::try_new(self.schema_without_metadata.clone(), result_cols).unwrap();

        let metadata = self
            .metadata_expr
            .evaluate(&record_batch)
            .unwrap()
            .into_array(record_batch.num_rows())
            .unwrap();
        let metadata =
            set_retract_metadata(metadata, Arc::new(BooleanArray::from(retract_outputs)));

        let mut final_batch = record_batch.columns().to_vec();
        final_batch.push(metadata);

        Ok(Some(final_batch))
    }

    fn keyed_aggregate(
        &self,
        batch: &RecordBatch,
        in_schema: &ArroyoSchema,
    ) -> Result<Option<Vec<ArrayRef>>> {
        let retracts = Self::get_retracts(batch);

        let key_converter = RowConverter::new(in_schema.sort_fields(false)).unwrap();

        let sort_columns = &in_schema
            .sort_columns(&batch, false)
            .into_iter()
            .map(|e| e.values)
            .collect::<Vec<_>>();

        let keys = key_converter.convert_columns(sort_columns).unwrap();

        let unique_keys = keys
            .iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let mut accumulators = self.accumulators.lock().unwrap();

        let mut output_keys = Vec::with_capacity(unique_keys.len() * 2);
        let mut updated_accumulator_values =
            vec![Vec::with_capacity(unique_keys.len() * 2); self.aggregates.len()];
        let mut is_retracts = Vec::with_capacity(unique_keys.len() * 2);

        // emit retracts for existing values
        for k in &unique_keys {
            if let Some(accs) = accumulators.get_mut(k.as_ref()) {
                for (acc, vs) in accs.iter_mut().zip(updated_accumulator_values.iter_mut()) {
                    vs.push(acc.evaluate().unwrap());
                }
                is_retracts.push(true);
                output_keys.push(*k);
            }
        }

        // then update the states with the new data
        let aggregate_input_cols = self.compute_inputs(&batch);

        for (i, key) in keys.iter().enumerate() {
            let row_accumulators = if accumulators.contains_key(key.as_ref()) {
                accumulators.get_mut(key.as_ref()).unwrap()
            } else {
                let new_accumulators: Vec<_> = self.make_accumulators();
                accumulators.insert(key.as_ref().to_vec(), new_accumulators);
                accumulators.get_mut(key.as_ref()).unwrap()
            };

            let retract = retracts.map(|r| r.value(i)).unwrap_or_default();
            for (inputs, accumulator) in
                aggregate_input_cols.iter().zip(row_accumulators.iter_mut())
            {
                let values: Vec<_> = inputs.iter().map(|c| c.slice(i, 1)).collect();
                if retract {
                    accumulator.retract_batch(&values).unwrap();
                } else {
                    accumulator.update_batch(&values).unwrap();
                }
            }
        }

        let mut deleted_keys = vec![];

        // then emit appends
        for k in &unique_keys {
            let mut is_empty = false;
            let results = accumulators
                .get_mut(k.as_ref())
                .as_mut()
                .unwrap()
                .iter_mut()
                .map(|acc| {
                    let result = acc.evaluate().unwrap();
                    // if all the accumulators are null, that means we've removed all of the data from
                    // this key, and we shouldn't emit an append
                    // TODO: this doesn't work for count, which returns 0 not null
                    is_empty &= result.is_null();
                    result
                })
                .collect::<Vec<_>>();

            if is_empty {
                deleted_keys.push(*k);
            } else {
                for (vs, result) in updated_accumulator_values
                    .iter_mut()
                    .zip(results.into_iter())
                {
                    vs.push(result);
                }
                output_keys.push(*k);
                is_retracts.push(false);
            }
        }

        for k in deleted_keys {
            accumulators.remove(k.as_ref());
        }

        if output_keys.is_empty() {
            return Ok(None);
        }

        let mut result_cols = key_converter.convert_rows(output_keys.into_iter()).unwrap();

        for acc in updated_accumulator_values {
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

        Ok(Some(final_batch))
    }

    fn compute_inputs(&self, batch: &&RecordBatch) -> Vec<Vec<ArrayRef>> {
        let aggregate_input_cols = self
            .aggregates
            .iter()
            .map(|agg| {
                agg.expressions()
                    .iter()
                    .map(|ex| {
                        ex.evaluate(&batch)
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
                    f.name().to_string(),
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
        collector: &mut dyn Collector,
    ) {
        let input_schema = &ctx.in_schemas[0];
        let output_schema = ctx.out_schema.as_ref().unwrap();

        let cols = if input_schema
            .key_indices
            .as_ref()
            .map(|k| k.len() > 0)
            .unwrap_or_default()
        {
            self.keyed_aggregate(&batch, &*input_schema).unwrap()
        } else {
            self.global_aggregate(&batch).unwrap()
        };

        if let Some(cols) = cols {
            collector
                .collect(RecordBatch::try_new(output_schema.schema.clone(), cols).unwrap())
                .await;
        }
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        self.flush(ctx, collector).await.unwrap();
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
        self.flush(ctx, collector).await.unwrap();
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
            self.flush(ctx, collector).await.unwrap();
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

        let finish_schema = finish_execution_plan.schema();
        let mut schema_without_metadata = SchemaBuilder::from((*finish_schema).clone());
        schema_without_metadata.remove(finish_schema.index_of(UPDATING_META_FIELD).unwrap());

        Ok(ConstructedOperator::from_operator(Box::new(
            IncrementalAggregatingFunc {
                partial_schema: partial_schema.into(),
                flush_interval: Duration::from_micros(config.flush_interval_micros),
                group_by: aggregate_exec.group_expr().clone(),
                accumulators: Arc::new(Mutex::new(HashMap::new())),
                metadata_expr: aggregate_exec.group_expr().expr().last().unwrap().0.clone(),
                ttl: Duration::from_micros(ttl),
                aggregates: aggregate_exec.aggr_expr().to_vec(),
                schema_without_metadata: Arc::new(schema_without_metadata.finish()),
            },
        )))
    }
}
