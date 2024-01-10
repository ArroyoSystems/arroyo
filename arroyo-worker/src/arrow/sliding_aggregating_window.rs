use std::{
    collections::{BTreeMap, HashMap, HashSet},
    mem,
    pin::Pin,
    sync::{Arc, RwLock},
    task::Poll,
    time::SystemTime,
};

use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use arrow::{
    compute::{filter, kernels::filter, partition, sort_to_indices, take},
    row::{Row, RowConverter, Rows, SortField},
};
use arrow_array::{
    types::{Int64Type, TimestampNanosecondType},
    Array, ArrayRef, PrimitiveArray, RecordBatch, StructArray, UInt32Array,
};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use arroyo_datastream::get_hasher;
use arroyo_df::schemas::{add_timestamp_field_arrow, window_arrow_struct};
use arroyo_rpc::{
    grpc::{api, api::window::Window, TableConfig},
    ArroyoSchema,
};
use arroyo_state::timestamp_table_config;
use arroyo_types::{
    from_nanos, to_nanos, ArrowMessage, CheckpointBarrier, SignalMessage, Watermark,
};
use bincode::config::BigEndian;
use datafusion::{
    execution::context::SessionContext,
    physical_plan::{aggregates::AggregateExec, ExecutionPlan},
};
use datafusion_common::{hash_utils::create_hashes, ScalarValue};
use futures::stream::FuturesUnordered;

use crate::operator::{ArrowOperator, ArrowOperatorConstructor};
use crate::{engine::ArrowContext, operator::RunContext};
use arroyo_df::physical::{ArroyoMemExec, ArroyoPhysicalExtensionCodec, DecodingContext};
use datafusion_execution::{
    runtime_env::{RuntimeConfig, RuntimeEnv},
    FunctionRegistry, SendableRecordBatchStream,
};
use datafusion_expr::{Accumulator, AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_physical_expr::{expressions::Column, AggregateExpr, PhysicalExpr};
use datafusion_proto::{
    physical_plan::{from_proto::parse_physical_expr, AsExecutionPlan},
    protobuf::{
        physical_plan_node::PhysicalPlanType, AggregateMode, PhysicalExprNode, PhysicalPlanNode,
    },
};
use prost::Message;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_stream::{Stream, StreamExt};
use tracing::info;

use super::{sync::streams::KeyedCloneableStreamFuture, EmptyRegistry};

#[derive(Debug)]
enum SlidingWindowState {
    // We haven't received any data.
    NoData,
    // We've received data, but don't have any data in the memory_view.
    OnlyBufferedData { earliest_bin_time: SystemTime },
    // There is data in memory_view waiting to be emitted.
    // will trigger on a watermark after next_window_start + self.slide
    InMemoryData { next_window_start: SystemTime },
}

pub struct SlidingAggregatingWindowFunc<K: Copy> {
    width: Duration,
    slide: Duration,
    binning_function: Arc<dyn PhysicalExpr>,
    partial_aggregation_plan: Arc<dyn ExecutionPlan>,
    partial_schema: ArroyoSchema,
    // the partial aggregation plan shares a reference to it,
    // which is only used on the exec()
    receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
    futures: FuturesUnordered<NextBatchFuture<K>>,
    group_by_context: GroupByContext,
    execs: BTreeMap<K, BinComputingHolder<K>>,
    window_field: FieldRef,
    window_index: usize,
    state: SlidingWindowState,
}

struct GroupByContext {
    active_groups: usize,
    accumulator_data: Vec<AccumulatorData>,
    row_converter: RowConverter,
    group_by_indices: Vec<usize>,
    current_rows: Option<Rows>,
    // a key batch that matches current_rows.
    key_arrays: Option<Vec<ArrayRef>>,
    row_index_map: HashMap<u64, i32>,
    accumulators: Vec<Option<SlidingAccumulatorContainer>>,
    output_schema: SchemaRef,
}

impl GroupByContext {
    fn insert_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        info!("Inserting {:?}", batch);
        let key_batch = batch.project(&self.group_by_indices)?;
        // TODO: multiple keys
        let key_struct_array = batch.column(self.group_by_indices[0]);
        let indices = if self.group_by_indices.len() == 1 {
            sort_to_indices(&key_struct_array, None, None).context(format!(
                "sorting key struct for key_batch {:?},  batch {:?}",
                key_struct_array, batch
            ))?
        } else {
            bail!("only support grouping by one field");
        };
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).unwrap())
            .collect();
        let sorted = RecordBatch::try_new(batch.schema(), columns).unwrap();
        let sorted_keys = take(&key_struct_array, &indices, None).unwrap();
        let partitions = partition(vec![sorted_keys.clone()].as_slice()).unwrap();
        let aggregate_args = self
            .accumulator_data
            .iter()
            .map(|data| {
                let mut columns = vec![];
                for index in &data.input_indices {
                    columns.push(sorted.column(*index).clone());
                }
                columns
            })
            .collect::<Vec<_>>();

        let mut hash_buf = vec![0; batch.num_rows()];
        let hashes =
            create_hashes(&vec![sorted_keys.clone()], &get_hasher(), &mut hash_buf).unwrap();
        let rows = self.row_converter.convert_columns(&vec![sorted_keys])?;
        let mut group_values = match self.current_rows.take() {
            Some(group_values) => group_values,
            None => self.row_converter.empty_rows(0, 0),
        };
        let mut new_key = false;
        for key_range in partitions.ranges() {
            let starting_index = key_range.start;
            let key_hash = hashes[starting_index];
            let row = rows.row(starting_index);
            let row_index = self.row_index_map.get(&key_hash);
            let index = match row_index {
                Some(index) => {
                    if group_values.row(*index as usize) == row {
                        *index
                    } else {
                        let mut shifted_hash = key_hash + 1;
                        // this is a hack, collisions should be rare though.
                        loop {
                            let row_index = self.row_index_map.get(&shifted_hash);
                            match row_index {
                                Some(index) => {
                                    if group_values.row(*index as usize) == row {
                                        break *index;
                                    }
                                }
                                None => {
                                    let index = group_values.num_rows() as i32;
                                    self.accumulators.push(None);
                                    self.row_index_map.insert(shifted_hash, index);
                                    new_key = true;
                                    group_values.push(row);
                                    break index;
                                }
                            }
                            shifted_hash += 1;
                        }
                    }
                }
                None => {
                    let index = group_values.num_rows() as i32;
                    self.accumulators.push(None);
                    self.row_index_map.insert(key_hash, index);
                    new_key = true;
                    group_values.push(row);
                    index
                }
            };
            let accumulator = self.accumulators.get_mut(index as usize).unwrap();
            if accumulator.is_none() {
                self.active_groups += 1;
                *accumulator = Some(SlidingAccumulatorContainer {
                    batches_present: 0,
                    accumulators: self
                        .accumulator_data
                        .iter()
                        .map(|data| {
                            let accumulator = data.aggregate.create_accumulator()?;
                            Ok(AccumulatorBox {
                                accumulator,
                                input_indices: data.input_indices.clone(),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    row_index: index,
                });
            }
            let accumulator = accumulator.as_mut().unwrap();
            accumulator
                .accumulators
                .iter_mut()
                .zip(aggregate_args.iter())
                .try_for_each(|(accumulator, args)| accumulator.accumulator.merge_batch(args))?;
            accumulator.batches_present += 1;
        }
        if new_key {
            self.key_arrays = Some(self.row_converter.convert_rows(&group_values)?);
        }
        self.current_rows = Some(group_values);
        Ok(())
    }

    fn retract_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let key_batch = batch.project(&self.group_by_indices)?;
        // TODO: multiple keys
        let key_struct_array = batch.column(self.group_by_indices[0]);
        let indices = if self.group_by_indices.len() == 1 {
            sort_to_indices(&key_struct_array, None, None).context(format!(
                "sorting key struct for key_batch {:?},  batch {:?}",
                key_struct_array, batch
            ))?
        } else {
            bail!("only support grouping by one field");
        };
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).unwrap())
            .collect();
        let sorted = RecordBatch::try_new(batch.schema(), columns).unwrap();
        let sorted_keys = take(&key_struct_array, &indices, None).unwrap();
        let partitions = partition(vec![sorted_keys.clone()].as_slice()).unwrap();
        let aggregate_args = self
            .accumulator_data
            .iter()
            .map(|data| {
                let mut columns = vec![];
                for index in &data.input_indices {
                    columns.push(sorted.column(*index).clone());
                }
                columns
            })
            .collect::<Vec<_>>();

        let mut hash_buf = vec![0; batch.num_rows()];
        let hashes =
            create_hashes(&vec![sorted_keys.clone()], &get_hasher(), &mut hash_buf).unwrap();
        let rows = self.row_converter.convert_columns(&vec![sorted_keys])?;
        let mut group_values = match self.current_rows.take() {
            Some(group_values) => group_values,
            None => self.row_converter.empty_rows(0, 0),
        };
        for key_range in partitions.ranges() {
            let starting_index = key_range.start;
            let key_hash = hashes[starting_index];
            let row = rows.row(starting_index);
            let Some(index) = self.row_index_map.get(&key_hash) else {
                bail!("can't find row {:?}", row);
            };
            let index = if group_values.row(*index as usize) == row {
                *index
            } else {
                let mut shifted_hash = key_hash + 1;
                // this is a hack, collisions should be rare though.
                loop {
                    let row_index = self
                        .row_index_map
                        .get(&shifted_hash)
                        .ok_or_else(|| anyhow!("can't find row {:?}", row))?;

                    if group_values.row(*index as usize) == row {
                        break *row_index;
                    }
                    shifted_hash += 1;
                }
            };
            let Some(accumulator) = self.accumulators.get_mut(index as usize).unwrap() else {
                bail!("can't find accumulator for row {:?}", row);
            };
            accumulator.batches_present -= 1;
            if accumulator.batches_present == 0 {
                self.active_groups -= 1;
                // drop the accumulator
                self.accumulators[index as usize] = None;
                return Ok(());
            }
            accumulator
                .accumulators
                .iter_mut()
                .zip(aggregate_args.iter())
                .try_for_each(|(accumulator, args)| accumulator.accumulator.retract_batch(args))?;
        }
        Ok(())
    }

    fn get_batch(&mut self) -> Result<RecordBatch> {
        let Some(key_arrays) = self.key_arrays.as_ref() else {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        };
        let mut accumulator_vecs = vec![vec![]; self.accumulator_data.len()];
        let mut indices = vec![];
        for (index, accumulator) in self.accumulators.iter().enumerate() {
            if accumulator.is_none() {
                continue;
            }
            let accumulator = accumulator.as_ref().unwrap();
            indices.push(index as u32);
            accumulator
                .accumulators
                .iter()
                .zip(accumulator_vecs.iter_mut())
                .try_for_each(|(accumulator, vec)| {
                    let state = accumulator.accumulator.evaluate()?;
                    vec.push(state);
                    Ok::<(), anyhow::Error>(())
                })?;
        }

        let indices = UInt32Array::from(indices);

        let mut filtered_key_arrays = key_arrays
            .iter()
            .map(|array| take(array, &indices, None).unwrap())
            .collect::<Vec<_>>();
        let aggregate_arrays: Vec<_> = accumulator_vecs
            .into_iter()
            .map(|scalar_values| Ok(ScalarValue::iter_to_array(scalar_values)?))
            .collect::<Result<Vec<_>>>()?;
        filtered_key_arrays.extend(aggregate_arrays.into_iter());
        info!("output schema :{:?}", self.output_schema);
        info!("filtered key arrays: {:?}", filtered_key_arrays);
        Ok(RecordBatch::try_new(
            self.output_schema.clone(),
            filtered_key_arrays,
        )?)
    }
}

impl<K: Copy> SlidingAggregatingWindowFunc<K> {
    fn bin_start(&self, timestamp: SystemTime) -> SystemTime {
        if self.width == Duration::ZERO {
            return timestamp;
        }
        let mut nanos = to_nanos(timestamp);
        nanos -= nanos % self.width.as_nanos();
        let result = from_nanos(nanos);
        result
    }
}

struct BinComputingHolder<K: Copy> {
    active_exec: Option<NextBatchFuture<K>>,
    finished_batches: Vec<RecordBatch>,
    sender: Option<UnboundedSender<RecordBatch>>,
}

impl<K: Copy> Default for BinComputingHolder<K> {
    fn default() -> Self {
        Self {
            active_exec: None,
            finished_batches: Vec::new(),
            sender: None,
        }
    }
}

type NextBatchFuture<K> = KeyedCloneableStreamFuture<K, SendableRecordBatchStream>;

struct SlidingAccumulatorContainer {
    batches_present: usize,
    accumulators: Vec<AccumulatorBox>,
    row_index: i32,
}

struct AccumulatorData {
    aggregate: Arc<dyn AggregateExpr>,
    input_indices: Vec<usize>,
}

struct AccumulatorBox {
    accumulator: Box<dyn Accumulator>,
    input_indices: Vec<usize>,
}

impl ArrowOperatorConstructor<api::WindowAggregateOperator, Self>
    for SlidingAggregatingWindowFunc<SystemTime>
{
    fn from_config(proto_config: api::WindowAggregateOperator) -> Result<Self> {
        let binning_function =
            PhysicalExprNode::decode(&mut proto_config.binning_function.as_slice()).unwrap();
        let input_schema: Schema = serde_json::from_slice(proto_config.input_schema.as_slice())
            .context(format!(
                "failed to deserialize schema of length {}",
                proto_config.input_schema.len()
            ))?;

        let binning_function =
            parse_physical_expr(&binning_function, &EmptyRegistry {}, &input_schema)?;

        let mut physical_plan =
            PhysicalPlanNode::decode(&mut proto_config.physical_plan.as_slice()).unwrap();

        let Some(arroyo_rpc::grpc::api::Window {
            window: Some(Window::SlidingWindow(window)),
        }) = proto_config.window
        else {
            bail!("expected Sliding window")
        };
        let window_field = Arc::new(Field::new(
            proto_config.window_field_name,
            window_arrow_struct(),
            true,
        ));

        let receiver = Arc::new(RwLock::new(None));
        let final_batches_passer = Arc::new(RwLock::new(Vec::new()));

        let PhysicalPlanType::Aggregate(mut aggregate) = physical_plan
            .physical_plan_type
            .take()
            .ok_or_else(|| anyhow!("missing physical plan"))?
        else {
            bail!("expected aggregate physical plan, not {:?}", physical_plan);
        };

        let AggregateMode::Final = aggregate.mode() else {
            bail!("expect AggregateMode to be Final so we can decompose it for checkpointing.")
        };

        // pull the input out to be computed separately for each bin.
        let partial_aggregation_plan = aggregate.input.as_ref().unwrap();

        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::UnboundedBatchStream(receiver.clone()),
        };

        // deserialize partial aggregation into execution plan with an UnboundedBatchStream source.
        // this is behind a RwLock and will have a new channel swapped in before computation is initialized
        // for each bin.
        let partial_aggregation_plan = partial_aggregation_plan.try_into_physical_plan(
            &EmptyRegistry {},
            &RuntimeEnv::new(RuntimeConfig::new()).unwrap(),
            &codec,
        )?;

        let partial_schema = partial_aggregation_plan.schema();
        let table_provider = ArroyoMemExec {
            table_name: "partial".into(),
            schema: partial_schema.clone(),
        };

        // swap in an ArroyoMemExec as the source.
        // This is a flexible table source that can be decoded in various ways depending on what is needed.
        aggregate.input = Some(Box::new(PhysicalPlanNode::try_from_physical_plan(
            Arc::new(table_provider),
            &ArroyoPhysicalExtensionCodec::default(),
        )?));

        let finish_plan = PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Aggregate(aggregate)),
        };

        let final_codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::LockedBatchVec(final_batches_passer.clone()),
        };

        // deserialize the finish plan to read directly from a Vec<RecordBatch> behind a RWLock.
        let finish_execution_plan = finish_plan.try_into_physical_plan(
            &EmptyRegistry {},
            &RuntimeEnv::new(RuntimeConfig::new()).unwrap(),
            &final_codec,
        )?;
        let typed_aggregate: &AggregateExec =
            finish_execution_plan.as_any().downcast_ref().unwrap();

        let physical_group_by = typed_aggregate.group_expr().clone();
        if !physical_group_by.is_single() {
            bail!("expected single group by");
        }
        let group_by = physical_group_by.expr().to_vec();
        let mut field_index = 0;
        let mut group_by_indices = vec![];
        let mut sort_fields = vec![];
        for (group_by_expr, group_by_alias) in &group_by {
            let colunm_expression: &Column = group_by_expr
                .as_any()
                .downcast_ref()
                .ok_or_else(|| anyhow!("expected column expression"))?;
            if !field_index == colunm_expression.index() {
                bail!("expected group by to be in order");
            }
            group_by_indices.push(field_index);
            sort_fields.push(SortField::new(
                partial_schema.field(field_index).data_type().clone(),
            ));
            field_index += 1;
        }

        let row_converter = RowConverter::new(sort_fields)?;

        info!(
            "group by indices: {:?}, group by: {:?}",
            group_by_indices, group_by
        );

        info!("final input schema: {:?}", typed_aggregate.input().schema());

        let mut accumulator_data = vec![];

        for agg in typed_aggregate.aggr_expr() {
            let start = field_index;
            let end = start + agg.expressions().len();
            let input_indices = (start..end).into_iter().collect();
            field_index = end;
            accumulator_data.push(AccumulatorData {
                aggregate: agg.clone(),
                input_indices,
            });
        }

        let group_by_context = GroupByContext {
            active_groups: 0,
            accumulator_data,
            row_converter,
            group_by_indices,
            current_rows: None,
            key_arrays: None,
            row_index_map: HashMap::new(),
            accumulators: vec![],
            output_schema: finish_execution_plan.schema(),
        };

        // Calculate the partial schema so that it can be saved to state.
        let key_indices: Vec<_> = proto_config
            .key_fields
            .into_iter()
            .map(|x| x as usize)
            .collect();

        let schema_ref = partial_aggregation_plan.schema();
        // timestamp is stored in the bin, will be appended prior to writing to state.
        let partial_schema = add_timestamp_field_arrow(schema_ref);
        let timestamp_index = partial_schema.fields().len() - 1;
        let partial_schema = ArroyoSchema {
            schema: partial_schema,
            timestamp_index: timestamp_index,
            key_indices,
        };

        Ok(Self {
            slide: Duration::from_micros(window.slide_micros),
            width: Duration::from_micros(window.size_micros),
            binning_function,
            partial_aggregation_plan,
            partial_schema,
            receiver,
            futures: FuturesUnordered::new(),
            execs: BTreeMap::new(),
            group_by_context,
            window_field,
            window_index: proto_config.window_index as usize,
            state: SlidingWindowState::NoData,
        })
    }
}

impl SlidingAggregatingWindowFunc<SystemTime> {
    fn should_advance(&self, watermark: SystemTime) -> bool {
        let watermark_bin = self.bin_start(watermark);
        match self.state {
            SlidingWindowState::NoData => false,
            SlidingWindowState::OnlyBufferedData { earliest_bin_time } => {
                earliest_bin_time + self.slide <= watermark_bin
            }
            SlidingWindowState::InMemoryData { next_window_start } => {
                next_window_start + self.slide <= watermark_bin
            }
        }
    }

    async fn advance(&mut self, ctx: &mut ArrowContext) -> Result<()> {
        // bin start for next bin to roll into memory.
        let bin_start = match self.state {
            SlidingWindowState::NoData => unreachable!(),
            SlidingWindowState::OnlyBufferedData { earliest_bin_time } => earliest_bin_time,
            SlidingWindowState::InMemoryData { next_window_start } => next_window_start,
        };
        info!("advancing with bin start {:?}", bin_start);

        let partial_table = ctx
            .table_manager
            .get_expiring_time_key_table("t", ctx.last_present_watermark())
            .await?;

        let bin_end = bin_start + self.slide;

        // finish bin, moving inputs into memory.
        if let Some(mut bin_exec) = self.execs.remove(&bin_start) {
            // If there are any active computations, finish them and write them to state.
            if let Some(mut active_exec) = bin_exec.active_exec.take() {
                bin_exec.sender.take();
                let bucket_nanos = to_nanos(bin_start) as i64;
                while let (_bin, Some((batch, new_exec))) = active_exec.await {
                    active_exec = new_exec;
                    let batch = batch.expect("should be able to compute batch");

                    let bin_start_scalar =
                        ScalarValue::TimestampNanosecond(Some(bucket_nanos), None);
                    let timestamp_array =
                        bin_start_scalar.to_array_of_size(batch.num_rows()).unwrap();
                    let mut columns = batch.columns().to_vec();
                    columns.push(timestamp_array);
                    let state_batch =
                        RecordBatch::try_new(self.partial_schema.schema.clone(), columns).unwrap();
                    partial_table.insert(bin_start, state_batch);
                    bin_exec.finished_batches.push(batch);
                }
            }
            for batch in bin_exec.finished_batches {
                self.group_by_context.insert_record_batch(batch)?;
            }
            partial_table.flush_timestamp(bin_start).await?;
        }

        for batch in partial_table.expire_timestamp(bin_start - self.width) {
            self.group_by_context.retract_batch(batch)?;
        }

        let batch = self.group_by_context.get_batch()?;

        let timestamp_array =
            ScalarValue::TimestampNanosecond(Some(to_nanos(bin_end) as i64 - 1), None)
                .to_array_of_size(batch.num_rows())
                .unwrap();
        let mut fields = batch.schema().fields().as_ref().to_vec();
        fields.push(Arc::new(Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )));

        fields.insert(self.window_index, self.window_field.clone());

        let mut columns = batch.columns().to_vec();
        columns.push(timestamp_array);
        let DataType::Struct(struct_fields) = self.window_field.data_type() else {
            unreachable!("should have struct for window field type")
        };
        let window_scalar = ScalarValue::Struct(
            Some(vec![
                ScalarValue::TimestampNanosecond(Some(to_nanos(bin_start) as i64), None),
                ScalarValue::TimestampNanosecond(Some(to_nanos(bin_end) as i64), None),
            ]),
            struct_fields.clone(),
        );
        columns.insert(
            self.window_index,
            window_scalar.to_array_of_size(batch.num_rows()).unwrap(),
        );

        let batch_with_timestamp = RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(fields, HashMap::new())),
            columns,
        )
        .unwrap();
        self.state = if self.group_by_context.active_groups == 0 {
            match partial_table.get_min_time() {
                Some(min_time) => SlidingWindowState::OnlyBufferedData {
                    earliest_bin_time: self.bin_start(min_time),
                },
                None => SlidingWindowState::NoData,
            }
        } else {
            SlidingWindowState::InMemoryData {
                next_window_start: bin_end,
            }
        };
        ctx.collect(batch_with_timestamp).await;
        Ok(())
    }
}

#[async_trait::async_trait]

impl ArrowOperator for SlidingAggregatingWindowFunc<SystemTime> {
    fn name(&self) -> String {
        "Sliding_window".to_string()
    }

    async fn on_start(&mut self, ctx: &mut ArrowContext) {
        let watermark = ctx.last_present_watermark();
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("t", watermark)
            .await
            .expect("should be able to load table");
        for (timestamp, batch) in table.all_batches_for_watermark(watermark) {
            let bin = self.bin_start(*timestamp);
            let holder = self.execs.entry(bin).or_default();
            batch
                .iter()
                .for_each(|batch| holder.finished_batches.push(batch.clone()));
        }
    }

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut ArrowContext) {
        if batch.num_rows() == 0 {
            return;
        }
        let bin = self
            .binning_function
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let indices = sort_to_indices(bin.as_ref(), None, None).unwrap();
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).unwrap())
            .collect();
        let sorted = RecordBatch::try_new(batch.schema(), columns).unwrap();
        let sorted_bins = take(&*bin, &indices, None).unwrap();

        let partition = partition(vec![sorted_bins.clone()].as_slice()).unwrap();
        let typed_bin = sorted_bins
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
            .unwrap();

        let first_bin_start = from_nanos(typed_bin.value(0) as u128);
        let watermark = ctx.last_present_watermark();
        if watermark.is_some() && first_bin_start < self.bin_start(watermark.unwrap()) {
            return;
        }
        self.state = match self.state {
            SlidingWindowState::NoData => SlidingWindowState::OnlyBufferedData {
                earliest_bin_time: first_bin_start,
            },
            SlidingWindowState::OnlyBufferedData { earliest_bin_time } => {
                SlidingWindowState::OnlyBufferedData {
                    earliest_bin_time: earliest_bin_time.min(first_bin_start),
                }
            }
            SlidingWindowState::InMemoryData { next_window_start } => {
                SlidingWindowState::InMemoryData {
                    next_window_start: next_window_start.min(first_bin_start),
                }
            }
        };
        for range in partition.ranges() {
            // the binning function already rounded down to the bin start.
            let bin = from_nanos(typed_bin.value(range.start) as u128);
            let bin_batch = sorted.slice(range.start, range.end - range.start);
            let bin_exec = self.execs.entry(bin).or_default();
            if bin_exec.active_exec.is_none() {
                let (unbounded_sender, unbounded_receiver) = unbounded_channel();
                bin_exec.sender = Some(unbounded_sender);
                {
                    let mut internal_receiver = self.receiver.write().unwrap();
                    *internal_receiver = Some(unbounded_receiver);
                }
                let new_exec = self
                    .partial_aggregation_plan
                    .execute(0, SessionContext::new().task_ctx())
                    .unwrap();
                let next_batch_future = NextBatchFuture::new(bin, new_exec);
                info!("created future for {:?}", bin);
                self.futures.push(next_batch_future.clone());
                bin_exec.active_exec = Some(next_batch_future);
            }
            bin_exec
                .sender
                .as_ref()
                .expect("just set this")
                .send(bin_batch)
                .unwrap();
        }
    }

    async fn handle_watermark(&mut self, watermark: Watermark, ctx: &mut ArrowContext) {
        let Some(last_watermark) = ctx.last_present_watermark() else {
            return;
        };

        while self.should_advance(last_watermark) {
            self.advance(ctx).await.expect("should be able to advance");
        }
        // by default, just pass watermarks on down
        ctx.broadcast(ArrowMessage::Signal(SignalMessage::Watermark(watermark)))
            .await;
    }

    async fn handle_checkpoint(&mut self, b: CheckpointBarrier, ctx: &mut ArrowContext) {
        let watermark = ctx
            .watermark()
            .map(|watermark: Watermark| match watermark {
                Watermark::EventTime(watermark) => Some(watermark),
                Watermark::Idle => None,
            })
            .flatten();
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("t", watermark)
            .await
            .expect("should get table");

        // TODO: this was a separate map just to the active execs, which could, in corner cases, be much smaller.
        for (bin, exec) in self.execs.iter_mut() {
            exec.sender.take();
            let bucket_nanos = to_nanos(*bin) as i64;
            let mut active_exec = exec.active_exec.take().expect("this should be active");
            while let (_bin_, Some((batch, next_exec))) = active_exec.await {
                active_exec = next_exec;
                let batch = batch.expect("should be able to compute batch");
                let bin_start = ScalarValue::TimestampNanosecond(Some(bucket_nanos), None);
                let timestamp_array = bin_start.to_array_of_size(batch.num_rows()).unwrap();
                let mut columns = batch.columns().to_vec();
                columns.push(timestamp_array);
                let state_batch =
                    RecordBatch::try_new(self.partial_schema.schema.clone(), columns).unwrap();
                table.insert(*bin, state_batch);
                exec.finished_batches.push(batch);
            }
        }
        table.flush(watermark).await.unwrap();
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        vec![(
            "t".to_string(),
            timestamp_table_config(
                "t",
                "Sliding_intermediate",
                self.width,
                self.partial_schema.clone(),
            ),
        )]
        .into_iter()
        .collect()
    }

    async fn select_run<St: Stream<Item = (usize, ArrowMessage)> + Send + Sync + Unpin>(
        &mut self,
        ctx: &mut ArrowContext,
        run_context: &mut RunContext<St>,
    ) -> bool {
        tokio::select! {
            Some(control_message) = ctx.control_rx.recv() => {
                self.handle_controller_message(control_message, ctx).await;
            }

            p = run_context.sel.next() => {
                match p {
                    Some(((idx, message), s)) => {
                        return self.handle_message(idx, message, s, ctx, run_context).await;
                    }
                    None => {
                        info!("[{}] Stream completed",ctx.task_info.operator_name);
                        return true;
                    }
                }
            }
            Some((bin, batch_option)) = self.futures.next() => {
                match batch_option {
                    None => {
                            info!("future for {:?} was finished elsewhere", bin);
                    }
                    Some((batch, future)) => {
                        match self.execs.get_mut(&bin) {
                            Some(exec) => {
                                exec.finished_batches.push(batch.expect("should've been able to compute a batch"));
                                self.futures.push(future);
                            },
                            None => unreachable!("FuturesUnordered returned a batch, but we can't find the exec"),
                        }

                    }
                }
            }
        }
        false
    }
}
