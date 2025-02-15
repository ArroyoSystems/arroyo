use crate::arrow::decode_aggregate;
use crate::arrow::updating_cache::{Key, UpdatingCache};
use anyhow::{anyhow, bail, Result};
use arrow::row::{RowConverter, SortField};
use arrow_array::builder::{
    BinaryBuilder, TimestampNanosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, RecordBatch, StructArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaBuilder, TimeUnit};
use arroyo_operator::context::Collector;
use arroyo_operator::{
    context::OperatorContext,
    operator::{
        ArrowOperator, AsDisplayable, ConstructedOperator, DisplayableOperator,
        OperatorConstructor, Registry,
    },
};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::{api::UpdatingAggregateOperator, rpc::TableConfig};
use arroyo_rpc::{updating_meta_fields, TIMESTAMP_FIELD, UPDATING_META_FIELD};
use arroyo_state::timestamp_table_config;
use arroyo_types::{to_nanos, CheckpointBarrier, SignalMessage};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::{Accumulator, PhysicalExpr};
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use datafusion_proto::protobuf::PhysicalExprNode;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use itertools::Itertools;
use prost::Message;
use std::borrow::Cow;
use std::collections::HashSet;
use std::time::{Duration, Instant, SystemTime};
use std::{collections::HashMap, mem, sync::Arc};
use tracing::log::warn;
use tracing::debug;

#[derive(Debug, Copy, Clone)]
struct BatchData {
    count: u64,
    generation: u64,
}

impl BatchData {
    fn new() -> Self {
        Self {
            count: 1,
            generation: 0,
        }
    }

    fn inc(&mut self) {
        self.count += 1;
        self.generation += 1;
    }

    fn dec(&mut self) {
        self.count = self.count.checked_sub(1).unwrap_or_default();
        self.generation += 1;
    }
}

/// Abstract over aggregations that support retracts (sliding accumulators), which we can use
/// directly, and those that don't in which case we just need to store the raw values and aggregate
/// them on demand
enum IncrementalState {
    Sliding {
        expr: Arc<AggregateFunctionExpr>,
        accumulator: Box<dyn Accumulator>,
    },
    Batch {
        expr: Arc<AggregateFunctionExpr>,
        data: HashMap<Key, BatchData>,
        row_converter: Arc<RowConverter>,
        changed_values: HashSet<Key>,
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
                changed_values,
                ..
            } => {
                for r in row_converter.convert_columns(batch)?.iter() {
                    if data.contains_key(r.as_ref()) {
                        data.get_mut(r.as_ref()).unwrap().inc();
                        changed_values.insert(data.get_key_value(r.as_ref()).unwrap().0.clone());
                    } else {
                        let key = Key(Arc::new(r.as_ref().to_vec()));
                        data.insert(key.clone(), BatchData::new());
                        changed_values.insert(key);
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
                changed_values,
                ..
            } => {
                for r in row_converter.convert_columns(batch)?.iter() {
                    match data.get(r.as_ref()).map(|d| d.count) {
                        Some(0) => {
                            debug!("tried to retract value for key with count 0; this implies an \
                            append was lost or a retract was duplicated");
                        }
                        Some(_) => {
                            data.get_mut(r.as_ref()).unwrap().dec();
                            changed_values
                                .insert(data.get_key_value(r.as_ref()).unwrap().0.clone());
                        }
                        None => {
                            debug!(
                                "tried to retract value for missing key: {:?}; this \
                            implies an append was lost (possibly from source)",
                                batch
                            )
                        }
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
                let parser = row_converter.parser();
                let input = row_converter.convert_rows(
                    data.iter()
                        .filter(|(_, c)| c.count > 0)
                        .map(|(v, _)| parser.parse(&v.0)),
                )?;
                let mut acc = expr.create_accumulator()?;
                acc.update_batch(&input)?;
                acc.evaluate_mut()
            }
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum AccumulatorType {
    Sliding,
    Batch,
}

impl AccumulatorType {
    fn state_fields(&self, agg: &AggregateFunctionExpr) -> DFResult<Vec<Field>> {
        Ok(match self {
            AccumulatorType::Sliding => agg.sliding_state_fields()?,
            // state for batch tables is handled separately
            AccumulatorType::Batch => vec![],
        })
    }
}

#[derive(Debug)]
struct Aggregator {
    func: Arc<AggregateFunctionExpr>,
    accumulator_type: AccumulatorType,
    row_converter: Arc<RowConverter>,
    state_cols: Vec<usize>,
}

pub struct IncrementalAggregatingFunc {
    flush_interval: Duration,
    metadata_expr: Arc<dyn PhysicalExpr>,
    aggregates: Vec<Aggregator>,
    accumulators: UpdatingCache<Vec<IncrementalState>>,
    updated_keys: HashMap<Key, Option<Vec<ScalarValue>>>,
    sliding_state_schema: Arc<ArroyoSchema>,
    batch_state_schema: Arc<ArroyoSchema>,
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

    fn checkpoint_sliding(&mut self) -> DFResult<Option<Vec<ArrayRef>>> {
        if self.updated_keys.is_empty() {
            return Ok(None);
        }

        let mut states = vec![vec![]; self.sliding_state_schema.schema.fields.len()];
        let parser = self.key_converter.parser();

        let mut generation_builder = UInt64Builder::with_capacity(self.updated_keys.len());

        let mut cols = self
            .key_converter
            .convert_rows(self.updated_keys.keys().map(|k| {
                let (accumulators, generation) = self
                    .accumulators
                    .get_mut_generation(k.0.as_ref())
                    .expect("missing accumulator in cache during checkpoint");

                generation_builder.append_value(generation);

                for (state, agg) in accumulators.iter_mut().zip(self.aggregates.iter()) {
                    let IncrementalState::Sliding { expr, accumulator } = state else {
                        continue;
                    };

                    let state = accumulator.state().unwrap_or_else(|_| {
                        // if it doesn't support immutable state, we'll use the mutable one and
                        // copy and restore the state -- this should in practice never happen,
                        // because the accumulators that don't support immutable state also don't
                        // support retract, but we have this fallback in case someone implements
                        // a new aggregator that doesn't uphold that relationship
                        let state = accumulator.state().unwrap();
                        *accumulator = expr.create_sliding_accumulator().unwrap();
                        let states: Vec<_> =
                            state.iter().map(|s| s.to_array()).try_collect().unwrap();
                        accumulator.merge_batch(&states).unwrap();
                        state
                    });

                    assert_eq!(
                        agg.state_cols.len(),
                        state.len(),
                        "wrong state in {}",
                        agg.func.name()
                    );

                    for (idx, v) in agg.state_cols.iter().zip(state.into_iter()) {
                        states[*idx].push(v);
                    }
                }
                parser.parse(k.0.as_ref())
            }))?;

        cols.extend(
            states
                .into_iter()
                .skip(cols.len())
                .map(|c| ScalarValue::iter_to_array(c).unwrap()),
        );

        cols.push(Arc::new(generation_builder.finish()));

        Ok(Some(cols))
    }

    fn checkpoint_batch(&mut self) -> DFResult<Option<Vec<ArrayRef>>> {
        if self
            .aggregates
            .iter()
            .all(|agg| agg.accumulator_type == AccumulatorType::Sliding)
        {
            return Ok(None);
        }

        if self.updated_keys.is_empty() {
            return Ok(None);
        }

        // this is an under-estimate but getting the real value seems too expensive to be worth it
        let size = self.updated_keys.len();

        let mut rows = Vec::with_capacity(size);
        let mut accumulator_builder = UInt32Builder::with_capacity(size);
        let mut args_row_builder = BinaryBuilder::with_capacity(size, size * 4);
        let mut count_builder = UInt64Builder::with_capacity(size);
        let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(size);
        let mut generation_builder = UInt64Builder::with_capacity(size);

        // TODO: the timestamp should really be coming from the original _timestamp column of
        //       the rows, as it does for sliding fields
        let now = to_nanos(SystemTime::now()) as i64;

        let parser = self.key_converter.parser();
        for k in self.updated_keys.keys() {
            let row = parser.parse(&k.0);
            for (i, state) in self
                .accumulators
                .get_mut(k.0.as_ref())
                .expect("missing accumulator in cache during checkpoint")
                .iter_mut()
                .enumerate()
            {
                let IncrementalState::Batch {
                    data,
                    changed_values,
                    ..
                } = state
                else {
                    continue;
                };

                for vk in changed_values.iter() {
                    if let Some(count) = data.get(vk) {
                        accumulator_builder.append_value(i as u32);
                        args_row_builder.append_value(&*vk.0);
                        count_builder.append_value(count.count);
                        generation_builder.append_value(count.generation);
                        timestamp_builder.append_value(now);
                        rows.push(row.to_owned())
                    }
                }

                // once we've checkpointed them, we can clear out keys with 0 counts
                data.retain(|_, v| v.count > 0);
            }
        }


        let mut cols = self.key_converter.convert_rows(rows.into_iter())?;

        cols.push(Arc::new(accumulator_builder.finish()));
        cols.push(Arc::new(args_row_builder.finish()));
        cols.push(Arc::new(count_builder.finish()));
        cols.push(Arc::new(timestamp_builder.finish()));
        cols.push(Arc::new(generation_builder.finish()));

        Ok(Some(cols))
    }

    async fn initialize(&mut self, ctx: &mut OperatorContext) -> Result<()> {
        let table = ctx.table_manager.get_uncached_key_value_view("a").await?;

        // initialize the sliding accumulator cache
        let mut stream = Box::pin(table.get_all());
        let key_converter = RowConverter::new(self.sliding_state_schema.sort_fields(false))?;

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let key_cols: Vec<_> = self
                .sliding_state_schema
                .sort_columns(&batch, false)
                .into_iter()
                .map(|c| c.values)
                .collect();

            let aggregate_states = self
                .aggregates
                .iter()
                .map(|agg| {
                    agg.state_cols
                        .iter()
                        .map(|idx| batch.column(*idx).clone())
                        .collect_vec()
                })
                .collect_vec();

            let generations = batch.columns().last().unwrap().as_primitive::<UInt64Type>();

            let now = Instant::now();

            let key_rows = key_converter.convert_columns(&key_cols)?;
            for ((i, row), generation) in key_rows.iter().enumerate().zip(generations) {
                let mut accumulators = self.make_accumulators();
                for ((_, state_cols), acc) in self
                    .aggregates
                    .iter()
                    .zip(aggregate_states.iter())
                    .zip(accumulators.iter_mut())
                {
                    if let IncrementalState::Sliding { accumulator, .. } = acc {
                        accumulator
                            .merge_batch(&state_cols.iter().map(|c| c.slice(i, 1)).collect_vec())?
                    }
                }

                self.accumulators.insert(
                    Arc::new(row.as_ref().to_vec()),
                    now,
                    generation.unwrap(),
                    accumulators,
                );
            }
        }

        // initialize the batch accumulator cache, if there are batch accumulators
        if self
            .aggregates
            .iter()
            .any(|agg| agg.accumulator_type == AccumulatorType::Batch)
        {
            let table = ctx.table_manager.get_uncached_key_value_view("b").await?;
            let mut stream = Box::pin(table.get_all());
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let key_cols: Vec<_> = self
                    .sliding_state_schema
                    .sort_columns(&batch, false)
                    .into_iter()
                    .map(|c| c.values)
                    .collect();

                let count_column = batch
                    .column(self.batch_state_schema.schema.index_of("count").unwrap())
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let accumulator_column = batch
                    .column(
                        self.batch_state_schema
                            .schema
                            .index_of("accumulator")
                            .unwrap(),
                    )
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap();
                let args_row_column = batch
                    .column(self.batch_state_schema.schema.index_of("args_row").unwrap())
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap();
                let generations = batch.columns().last().unwrap().as_primitive::<UInt64Type>();

                let key_rows = self.key_converter.convert_columns(&key_cols)?;

                for (i, row) in key_rows.iter().enumerate() {
                    let Some(accumulators) = self.accumulators.get_mut(row.as_ref()) else {
                        bail!(
                            "missing accumulator for key {:?} while restoring batch",
                            row.as_ref()
                        );
                    };

                    let count = count_column.value(i);
                    let accumulator_idx = accumulator_column.value(i) as usize;
                    let args_row = args_row_column.value(i);
                    let generation = generations.value(i);

                    let IncrementalState::Batch { data, .. } = &mut accumulators[accumulator_idx]
                    else {
                        bail!("expected aggregate {accumulator_idx} to be a batch accumulator, but was sliding");
                    };

                    if let Some(existing) = data.get_mut(args_row) {
                        if existing.generation < generation {
                            existing.count = count;
                            existing.generation = generation;
                        }
                    } else {
                        data.insert(
                            Key(Arc::new(args_row.to_vec())),
                            BatchData { count, generation },
                        );
                    }
                }
            }
        }


        let mut deleted_keys = vec![];
        for (k, v) in self.accumulators.iter_mut() {
            // the state system doesn't yet support deletes, so we'll determine if a value
            // is deleted by the timestamp being null
            let is_deleted = v
                .last_mut()
                .ok_or_else(|| anyhow!("no aggregrates"))?
                .evaluate()?
                .is_null();

            if is_deleted {
                deleted_keys.push(k.clone());
            } else {
                // clear empty entries from the batch accumulators
                for is in v {
                    if let IncrementalState::Batch { data, .. } = is {
                        data.retain(|_, v| v.count > 0);
                    }
                }
            }
        }
        for k in deleted_keys {
            self.accumulators.remove(&k.0);
        }

        Ok(())
    }

    async fn checkpoint(&mut self, ctx: &mut OperatorContext) -> Result<()> {
        if self.updated_keys.is_empty() {
            return Ok(());
        }

        if let Some(sliding) = self.checkpoint_sliding()? {
            let table = ctx.table_manager.get_uncached_key_value_view("a").await?;
            table.insert_batch(sliding).await?;
        }

        if let Some(batch) = self.checkpoint_batch()? {
            let table = ctx.table_manager.get_uncached_key_value_view("b").await?;
            table.insert_batch(batch).await?;
        }

        Ok(())
    }

    async fn flush(&mut self, ctx: &mut OperatorContext) -> Result<Option<RecordBatch>> {
        self.checkpoint(ctx).await?;

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

        for acc in output_values.into_iter() {
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
            .map(|agg| match agg.accumulator_type {
                AccumulatorType::Sliding => IncrementalState::Sliding {
                    expr: agg.func.clone(),
                    accumulator: agg.func.create_sliding_accumulator().unwrap(),
                },
                AccumulatorType::Batch => IncrementalState::Batch {
                    expr: agg.func.clone(),
                    data: Default::default(),
                    row_converter: agg.row_converter.clone(),
                    changed_values: Default::default(),
                },
            })
            .collect()
    }

    fn global_aggregate(&mut self, batch: &RecordBatch) -> Result<()> {
        let retracts = Self::get_retracts(batch);

        let aggregate_input_cols = self.compute_inputs(&batch);

        let mut first = false;

        // workaround for https://github.com/rust-lang/rust-clippy/issues/13934
        #[allow(clippy::map_entry)]
        if !self.accumulators.contains_key(&GLOBAL_KEY) {
            first = true;
            self.accumulators.insert(
                Arc::new(GLOBAL_KEY),
                Instant::now(),
                0,
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
                    0,
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
            .map(|agg| {
                agg.func
                    .expressions()
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
            .map(|agg| format!("{} ({:?})", agg.func.name(), agg.accumulator_type))
            .collect::<Vec<_>>();

        DisplayableOperator {
            name: Cow::Borrowed("UpdatingAggregatingFunc"),
            fields: vec![
                ("flush_interval", AsDisplayable::Debug(&self.flush_interval)),
                ("ttl", AsDisplayable::Debug(&self.ttl)),
                (
                    "state_schema",
                    AsDisplayable::Schema(&self.sliding_state_schema.schema),
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
        if let Some(batch) = self.flush(ctx).await.unwrap() {
            collector.collect(batch).await;
        }
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        vec![
            (
                "a".to_string(),
                timestamp_table_config(
                    "a",
                    "accumulator_state",
                    self.ttl,
                    true,
                    self.sliding_state_schema.as_ref().clone(),
                ),
            ),
            (
                "b".to_string(),
                timestamp_table_config(
                    "b",
                    "batch_state",
                    self.ttl,
                    true,
                    self.batch_state_schema.as_ref().clone(),
                ),
            ),
        ]
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
        if let Some(batch) = self.flush(ctx).await.unwrap() {
            collector.collect(batch).await;
        }
    }

    async fn on_close(
        &mut self,
        final_message: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) {
        if let Some(SignalMessage::EndOfData) = final_message {
            if let Some(batch) = self.flush(ctx).await.unwrap() {
                collector.collect(batch).await;
            }
        }
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) {
        self.initialize(ctx).await.unwrap();
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

        let metadata_expr = parse_physical_expr(
            &PhysicalExprNode::decode(&mut config.metadata_expr.as_slice())?,
            registry.as_ref(),
            &input_schema.schema,
            &DefaultPhysicalExtensionCodec {},
        )?;

        let aggregate_exec = PhysicalPlanNode::decode(&mut config.aggregate_exec.as_ref())?;
        let PhysicalPlanType::Aggregate(aggregate_exec) =
            aggregate_exec.physical_plan_type.unwrap()
        else {
            bail!("invalid proto -- expected aggregate exec");
        };

        // the state schema is made up of the key fields + the state fields for each aggregator
        // (if it supports retraction) otherwise, the input data + a count
        let mut sliding_state_fields = input_schema
            .key_indices
            .as_ref()
            .map(|v| {
                v.iter()
                    .map(|idx| input_schema.schema.field(*idx).clone())
                    .collect_vec()
            })
            .unwrap_or_default();

        let mut batch_state_fields = sliding_state_fields.clone();

        let key_fields = (0..sliding_state_fields.len()).collect_vec();

        let aggregates: Vec<_> = aggregate_exec
            .aggr_expr
            .iter()
            .zip(aggregate_exec.aggr_expr_name.iter())
            .map(|(expr, name)| {
                Ok(decode_aggregate(
                    &input_schema.schema,
                    name,
                    expr,
                    registry.as_ref(),
                )?)
            })
            .map_ok(|agg| {
                let retract = match agg.create_sliding_accumulator() {
                    Ok(s) => s.supports_retract_batch(),
                    _ => false,
                };

                (
                    agg,
                    if retract {
                        AccumulatorType::Sliding
                    } else {
                        AccumulatorType::Batch
                    },
                )
            })
            .map_ok(|(agg, t)| {
                let row_converter = Arc::new(RowConverter::new(
                    agg.expressions()
                        .iter()
                        .map(|ex| Ok(SortField::new(ex.data_type(&input_schema.schema)?)))
                        .collect::<DFResult<_>>()?,
                )?);

                let fields = t.state_fields(&agg)?;

                let field_names = fields.iter().map(|f| f.name().to_string()).collect_vec();
                sliding_state_fields.extend(fields.into_iter());

                Ok::<_, anyhow::Error>((agg, t, row_converter, field_names))
            })
            .flatten_ok()
            .collect::<Result<_>>()?;

        let state_schema = Schema::new(sliding_state_fields);

        let aggregates = aggregates
            .into_iter()
            .map(|(agg, t, row_converter, field_names)| Aggregator {
                func: agg,
                accumulator_type: t,
                row_converter,
                state_cols: field_names
                    .iter()
                    .map(|f| state_schema.index_of(f).unwrap())
                    .collect(),
            })
            .collect();

        // ensure the last field (timestamp) has the expected name before creating the arroyo schema
        let mut state_fields = state_schema.fields().to_vec();
        let timestamp_field = state_fields.pop().unwrap();
        state_fields.push(Arc::new(
            (*timestamp_field).clone().with_name(TIMESTAMP_FIELD),
        ));

        let sliding_state_schema = Arc::new(ArroyoSchema::from_schema_keys(
            Arc::new(Schema::new(state_fields)),
            key_fields.clone(),
        )?);

        batch_state_fields.push(Field::new("accumulator", DataType::UInt32, false));
        batch_state_fields.push(Field::new("args_row", DataType::Binary, false));
        batch_state_fields.push(Field::new("count", DataType::UInt64, false));
        batch_state_fields.push(Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ));

        let mut key_fields = key_fields;
        // include accumulator and args_row in the keys
        key_fields.push(key_fields.len());
        key_fields.push(key_fields.len());

        let batch_state_schema = Arc::new(ArroyoSchema::from_schema_keys(
            Arc::new(Schema::new(batch_state_fields)),
            key_fields,
        )?);

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
                sliding_state_schema,
                batch_state_schema,
            },
        )))
    }
}
