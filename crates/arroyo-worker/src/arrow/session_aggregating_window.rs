use anyhow::{anyhow, bail, Context, Result};
use arrow::{
    compute::{
        concat_batches, filter_record_batch, kernels::cmp::gt_eq, lexsort_to_indices, max,
        partition, take, SortColumn,
    },
    row::{RowConverter, SortField},
};
use arrow_array::{
    types::TimestampNanosecondType, Array, BooleanArray, PrimitiveArray, RecordBatch, StructArray,
    TimestampNanosecondArray,
};
use arrow_schema::{DataType, Field, FieldRef};
use arroyo_operator::{
    context::OperatorContext,
    operator::{ArrowOperator, ConstructedOperator, OperatorConstructor},
};
use arroyo_planner::schemas::window_arrow_struct;
use arroyo_rpc::{
    grpc::{api, rpc::TableConfig},
    Converter,
};
use arroyo_state::{
    global_table_config, tables::global_keyed_map::GlobalKeyedView, timestamp_table_config,
};
use arroyo_types::{from_nanos, print_time, to_nanos, CheckpointBarrier, Watermark};
use datafusion::{execution::context::SessionContext, physical_plan::ExecutionPlan};
use std::borrow::Cow;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, RwLock},
    time::SystemTime,
};

use arroyo_operator::context::Collector;
use arroyo_operator::operator::{AsDisplayable, DisplayableOperator, Registry};
use arroyo_planner::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use prost::Message;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use tracing::{debug, warn};
// TODO: advance futures outside of method calls.

pub struct SessionAggregatingWindowFunc {
    config: Arc<SessionWindowConfig>,
    keys_by_next_watermark_action: BTreeMap<SystemTime, HashSet<Vec<u8>>>,
    key_computations: HashMap<Vec<u8>, KeyComputingHolder>,
    keys_by_start_time: BTreeMap<SystemTime, HashSet<Vec<u8>>>,
    row_converter: Converter,
}

const GLOBAL_KEY: Vec<u8> = vec![];

impl SessionAggregatingWindowFunc {
    fn should_advance(&self, watermark: SystemTime) -> bool {
        let result = self
            .keys_by_next_watermark_action
            .first_key_value()
            .map(|(next_watermark_action, _)| *next_watermark_action < watermark)
            .unwrap_or(false);
        if result {
            debug!("should advance at watermark {:?}", watermark);
        } else {
            debug!("should not advance at watermark {:?}", watermark);
        }
        result
    }

    async fn advance(
        &mut self,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> Result<()> {
        let Some(watermark) = ctx.last_present_watermark() else {
            debug!("no watermark, not advancing");
            return Ok(());
        };
        let results = self
            .results_at_watermark(watermark)
            .await
            .context("results at watermark")?;
        if !results.is_empty() {
            let result_batch = self
                .to_record_batch(results, ctx)
                .context("should convert to record batch")?;
            debug!("emitting session batch of size {}", result_batch.num_rows());
            collector.collect(result_batch).await;
        }

        Ok(())
    }

    async fn results_at_watermark(
        &mut self,
        watermark: SystemTime,
    ) -> Result<Vec<(Vec<u8>, Vec<SessionWindowResult>)>> {
        let mut results = vec![];
        while self.should_advance(watermark) {
            let (_next_watermark_action, keys) = self
                .keys_by_next_watermark_action
                .pop_first()
                .ok_or_else(|| anyhow!("should have a key to advance"))?;

            for key in keys {
                let key_computation = self
                    .key_computations
                    .get_mut(&key)
                    .ok_or_else(|| anyhow!("should have key {:?}", key))?;
                let initial_start_time = key_computation.earliest_data().unwrap();
                let flushed_batches = key_computation.watermark_update(watermark).await?;
                if !flushed_batches.is_empty() {
                    results.push((key.clone(), flushed_batches));
                }
                if key_computation.is_empty() {
                    self.key_computations.remove(&key);
                    self.keys_by_start_time
                        .get_mut(&initial_start_time)
                        .unwrap()
                        .remove(&key);
                } else {
                    let new_start_time = key_computation.earliest_data().unwrap();
                    if new_start_time != initial_start_time {
                        self.keys_by_start_time
                            .get_mut(&initial_start_time)
                            .unwrap()
                            .remove(&key);
                        self.keys_by_start_time
                            .entry(new_start_time)
                            .or_default()
                            .insert(key.clone());
                    }

                    let next_watermark_action = key_computation.next_watermark_action().unwrap();
                    if next_watermark_action == _next_watermark_action {
                        bail!(" processed a watermark at {} and next watermark action stayed at {}. batches by start time {:?}, active_session date_end():{:?} ",
                            print_time(watermark),
                            print_time(next_watermark_action), key_computation.batches_by_start_time,
                            key_computation.active_session.as_ref().map(|session| print_time(session.data_end)));
                    }
                    self.keys_by_next_watermark_action
                        .entry(next_watermark_action)
                        .or_default()
                        .insert(key);
                }
            }
        }
        Ok(results)
    }

    fn earliest_batch_time(&self) -> Option<SystemTime> {
        self.keys_by_start_time
            .first_key_value()
            .map(|(start_time, _keys)| *start_time)
    }

    #[allow(clippy::single_range_in_vec_init)]
    async fn add_at_watermark(
        &mut self,
        sorted_batch: RecordBatch,
        watermark: Option<SystemTime>,
    ) -> Result<()> {
        let partition = if !self.config.input_schema_ref.has_routing_keys() {
            // if we don't have keys, we can just partition by the whole batch.
            vec![0..sorted_batch.num_rows()]
        } else {
            partition(
                sorted_batch
                    .columns()
                    .iter()
                    // Keys are first in the schema, because of how DataFusion structures aggregates.
                    .take(
                        self.config
                            .input_schema_ref
                            .routing_keys()
                            .as_ref()
                            .unwrap()
                            .len(),
                    )
                    .cloned()
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?
            .ranges()
        };

        let key_count = self
            .config
            .input_schema_ref
            .routing_keys()
            .map(|keys| keys.len())
            .unwrap_or(0);

        for range in partition {
            let key_batch = sorted_batch.slice(range.start, range.end - range.start);

            let row = if key_count == 0 {
                GLOBAL_KEY
            } else {
                self.row_converter
                    .convert_columns(&key_batch.slice(0, 1).columns()[0..key_count])
                    .context("failed to convert rows")?
                    .as_ref()
                    .to_vec()
            };

            let key_computation =
                self.key_computations
                    .entry(row.clone())
                    .or_insert_with(|| KeyComputingHolder {
                        session_window_config: self.config.clone(),
                        active_session: None,
                        batches_by_start_time: BTreeMap::new(),
                    });

            let initial_next_watermark_action = key_computation.next_watermark_action();
            let initial_data_start = key_computation.earliest_data();
            key_computation
                .add_batch(key_batch, watermark)
                .await
                .unwrap();
            let new_next_watermark_action = key_computation
                .next_watermark_action()
                .expect("should have next watermark action");
            let new_initial_data_start = key_computation
                .earliest_data()
                .expect("should have earliest data");

            match initial_next_watermark_action {
                Some(initial_next_watermark_action) => {
                    if initial_next_watermark_action != new_next_watermark_action {
                        self.keys_by_next_watermark_action
                            .get_mut(&initial_next_watermark_action)
                            .expect("should have key")
                            .remove(&row);
                        self.keys_by_next_watermark_action
                            .entry(new_next_watermark_action)
                            .or_default()
                            .insert(row.clone());
                    }
                    let initial_data_start =
                        initial_data_start.expect("should have initial data start");
                    if initial_data_start != new_initial_data_start {
                        self.keys_by_start_time
                            .get_mut(&initial_data_start)
                            .expect("should have key")
                            .remove(&row);
                        self.keys_by_start_time
                            .entry(new_initial_data_start)
                            .or_default()
                            .insert(row);
                    }
                }
                None => {
                    self.keys_by_next_watermark_action
                        .entry(new_next_watermark_action)
                        .or_default()
                        .insert(row.clone());

                    self.keys_by_start_time
                        .entry(new_initial_data_start)
                        .or_default()
                        .insert(row);
                }
            }
        }
        Ok(())
    }

    fn sort_columns(&self, batch: &RecordBatch) -> Vec<SortColumn> {
        self.config.input_schema_ref.sort_columns(batch, true)
    }

    fn filter_batch_by_time(
        &self,
        batch: RecordBatch,
        watermark: Option<SystemTime>,
    ) -> Result<RecordBatch> {
        let Some(watermark) = watermark else {
            // no watermark, so we just return the same batch.
            return Ok(batch);
        };
        // filter out late data
        let timestamp_column = batch
            .column(self.config.input_schema_ref.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let watermark_scalar = TimestampNanosecondArray::new_scalar(to_nanos(watermark) as i64);
        let on_time = gt_eq(timestamp_column, &watermark_scalar).unwrap();
        Ok(filter_record_batch(&batch, &on_time)?)
    }

    fn sort_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let sort_columns = self.sort_columns(batch);
        let sort_indices = lexsort_to_indices(&sort_columns, None).expect("should be able to sort");
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &sort_indices, None).unwrap())
            .collect();
        Ok(RecordBatch::try_new(batch.schema(), columns)?)
    }

    fn to_record_batch(
        &self,
        results: Vec<(Vec<u8>, Vec<SessionWindowResult>)>,
        ctx: &mut OperatorContext,
    ) -> Result<RecordBatch> {
        let (rows, results): (Vec<_>, Vec<_>) = results
            .iter()
            .flat_map(|(row, session_results)| {
                session_results
                    .iter()
                    .map(move |session_result| (row, session_result))
            })
            .unzip();

        let key_columns = if let Some(parser) = self.row_converter.parser() {
            self.row_converter
                .convert_rows(rows.iter().map(|row| parser.parse(row.as_ref())).collect())?
        } else {
            vec![]
        };

        let window_start_array = PrimitiveArray::<TimestampNanosecondType>::from(
            results
                .iter()
                .map(|result| result.window_start)
                .map(|start| to_nanos(start) as i64)
                .collect::<Vec<_>>(),
        );
        let window_end_array = PrimitiveArray::<TimestampNanosecondType>::from(
            results
                .iter()
                .map(|result| result.window_end)
                .map(|end| to_nanos(end) as i64)
                .collect::<Vec<_>>(),
        );
        let timestamp_array = PrimitiveArray::<TimestampNanosecondType>::from(
            results
                .iter()
                .map(|result| result.window_end)
                .map(|end| to_nanos(end) as i64 - 1)
                .collect::<Vec<_>>(),
        );
        let merged_batch = concat_batches(
            &results[0].batch.schema(),
            results.iter().map(|result| &result.batch),
        )?;
        let DataType::Struct(window_fields) = self.config.window_field.data_type() else {
            bail!("expected window field to be a struct");
        };
        let window_struct_array = StructArray::try_new(
            window_fields.clone(),
            vec![Arc::new(window_start_array), Arc::new(window_end_array)],
            None,
        )?;
        let mut columns = key_columns;
        columns.insert(self.config.window_index, Arc::new(window_struct_array));
        columns.extend_from_slice(merged_batch.columns());
        columns.push(Arc::new(timestamp_array));
        RecordBatch::try_new(
            ctx.out_schema.as_ref().unwrap().schema.clone(),
            columns.clone(),
        )
        .context(format!(
            "failed to create batch.\nout schema:\n{:?}\ncolumns:\n{:?}",
            ctx.out_schema, columns
        ))
    }
}

struct SessionWindowConfig {
    gap: Duration,
    input_schema_ref: ArroyoSchemaRef,
    window_field: FieldRef,
    window_index: usize,
    // TODO: perform partial aggregations and checkpoint those.
    // unkeyed_aggregate_schema_ref: ArroyoSchemaRef,
    // partial_physical_exec: Arc<dyn ExecutionPlan>,
    final_physical_exec: Arc<dyn ExecutionPlan>,
    receiver: Arc<RwLock<Option<UnboundedReceiver<RecordBatch>>>>,
}

struct ActiveSession {
    // the data start time for this session
    data_start: SystemTime,
    data_end: SystemTime,
    sender: Option<UnboundedSender<RecordBatch>>,
    // the next batch's execution plan
    result_stream: SendableRecordBatchStream,
}

impl ActiveSession {
    async fn new(
        aggregation_plan: Arc<dyn ExecutionPlan>,
        initial_timestamp: SystemTime,
        sender: UnboundedSender<RecordBatch>,
    ) -> Result<Self> {
        aggregation_plan.reset()?;
        let result_exec = aggregation_plan.execute(0, SessionContext::new().task_ctx())?;
        Ok(Self {
            data_start: initial_timestamp,
            data_end: initial_timestamp,
            sender: Some(sender),
            result_stream: result_exec,
        })
    }
    // Add all data in the batch that is within gap of the current session interval,
    // updating gap as more data is added.
    // The batch is sorted and it will never be the case that the start of batch is less than data_start - gap.
    // return the remaining data.
    fn add_batch(
        &mut self,
        batch: RecordBatch,
        gap: Duration,
        timestamp_index: usize,
    ) -> Result<Option<(SystemTime, RecordBatch)>> {
        let timestamp_column = batch
            .column(timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let start = timestamp_column.value(0);
        let end = timestamp_column.value(batch.num_rows() - 1);

        if end < to_nanos(self.data_end + gap) as i64 {
            // all data in the batch is within the current session interval
            // add it to the current session and update the gap
            self.data_end = self.data_end.max(from_nanos(end as u128));
            self.data_start = self.data_start.min(from_nanos(start as u128));
            self.sender.as_ref().unwrap().send(batch)?;
            return Ok(None);
        }

        if (to_nanos(self.data_end + gap) as i64) < start {
            // all data in the batch is after the current session interval
            // return the batch
            warn!("got batch that is entirely after the current session interval");
            return Ok(Some((from_nanos(start as u128), batch)));
        }

        if start < to_nanos(self.data_start - gap) as i64 {
            bail!("received a batch that starts before the current data_start - gap, this should not have happened.");
        }
        if start < to_nanos(self.data_start) as i64 {
            self.data_start = from_nanos(start as u128);
        }
        // TODO: test best way to compute this
        let mut index = 1;
        while index < batch.num_rows() {
            let value = timestamp_column.value(index);
            index += 1;
            //
            if value < to_nanos(self.data_end) as i64 {
                continue;
            }
            if value < to_nanos(self.data_end + gap) as i64 {
                // this value is within the current session interval
                // add it to the current session and update the gap
                self.data_end = from_nanos(value as u128);
                continue;
            }
            break;
        }
        if index == batch.num_rows() {
            // all data in the batch is within the current session interval
            // we've already updated the gap, so we can just add it to the current session
            self.sender.as_ref().unwrap().send(batch)?;
            return Ok(None);
        }
        self.sender.as_ref().unwrap().send(batch.slice(0, index))?;

        let batch = batch.slice(index, batch.num_rows() - index);
        let start_time = from_nanos(timestamp_column.value(index) as u128);

        Ok(Some((start_time, batch)))
    }

    async fn finish(mut self, gap: Duration) -> Result<SessionWindowResult> {
        {
            // drop the active session sender
            self.sender.take();
        }
        let result_batches: Vec<_> = self
            .result_stream
            .map(|batch| Ok(batch?))
            .collect::<Result<_>>()
            .await?;
        if result_batches.len() != 1 {
            bail!(
                "expect active session result to be exactly one batch, not {:?}",
                result_batches
            );
        }
        let batch = result_batches.into_iter().next().unwrap();
        if batch.num_rows() != 1 {
            bail!(
                "expect active session result to be excactly one row, not {:?}",
                batch
            );
        }
        Ok(SessionWindowResult {
            window_start: self.data_start,
            window_end: self.data_end + gap,
            batch,
        })
    }
}

#[derive(Debug)]
struct SessionWindowResult {
    window_start: SystemTime,
    window_end: SystemTime,
    batch: RecordBatch,
}

struct KeyComputingHolder {
    session_window_config: Arc<SessionWindowConfig>,
    // this is computing the currently active batch.
    active_session: Option<ActiveSession>,
    // buffered batches that may not be in the current session.
    // For now checkpointing happens on incoming batches, but in the future we can checkpoint partial aggregates.
    batches_by_start_time: BTreeMap<SystemTime, Vec<RecordBatch>>,
}

impl KeyComputingHolder {
    fn next_watermark_action(&self) -> Option<SystemTime> {
        match self.active_session {
            Some(ref active_session) => {
                Some(active_session.data_end + self.session_window_config.gap)
            }
            None => self
                .batches_by_start_time
                .first_key_value()
                .map(|(start_time, _batches)| *start_time - self.session_window_config.gap),
        }
    }
    /* This method is for advancing the state machine when the watermark is incremented.
      The operator code is responsible for making sure it is called on all appropriate KeyComputingHolders.
      It assumes the invariant is already enforced that all buffered batches start after the current active session plus the gap.
    */
    async fn watermark_update(
        &mut self,
        watermark: SystemTime,
    ) -> Result<Vec<SessionWindowResult>> {
        // Check if the current session is complete. This will happen if max_active_timestamp + gap is less than the watermark.
        // If it is, we need to finish the current session and start a new one.
        let mut results = vec![];
        loop {
            if self.active_session.is_some() {
                let active_session = self.active_session.as_mut().unwrap();
                if active_session.data_end + self.session_window_config.gap < watermark {
                    let result = self
                        .active_session
                        .take()
                        .unwrap()
                        .finish(self.session_window_config.gap)
                        .await?;
                    results.push(result);
                } else {
                    // the active session is not finished, so we can stop.
                    break;
                }
            } else {
                let Some((initial_timestamp, _value)) =
                    self.batches_by_start_time.first_key_value()
                else {
                    break;
                };
                if watermark + self.session_window_config.gap < *initial_timestamp {
                    // the next batch is after the watermark + gap, so there could be a session before it.
                    break;
                }

                let (sender, unbounded_receiver) = unbounded_channel();
                {
                    let mut internal_receiver =
                        self.session_window_config.receiver.write().unwrap();
                    *internal_receiver = Some(unbounded_receiver);
                }

                self.active_session = Some(
                    ActiveSession::new(
                        self.session_window_config.final_physical_exec.clone(),
                        *initial_timestamp,
                        sender,
                    )
                    .await?,
                );
                self.fill_active_session()?;
            }
        }
        Ok(results)
    }

    /* This enforces the invariant that all buffered batches start after the current active session plus the gap.
      This is called when the watermark is advanced, and when data is added.
      There are some pathological cases, e.g. if the gap is 1.5s and one batch has evens and the others odds,
      which would strip off one row at a time, but this should be rare.
    */
    fn fill_active_session(&mut self) -> Result<()> {
        let Some(active_session) = self.active_session.as_mut() else {
            bail!("fill_active_session() should not be called when there is no active session");
        };
        loop {
            let Some((first_key, _batches)) = self.batches_by_start_time.first_key_value() else {
                break;
            };
            if active_session.data_end + self.session_window_config.gap < *first_key {
                // the next batch is after the current session + gap, so we can stop.
                break;
            }
            let (_start_time, batches) = self
                .batches_by_start_time
                .pop_first()
                .expect("will have already exited");

            for batch in batches {
                if let Some((start_time, batch)) = active_session.add_batch(
                    batch,
                    self.session_window_config.gap,
                    self.session_window_config.input_schema_ref.timestamp_index,
                )? {
                    self.batches_by_start_time
                        .entry(start_time)
                        .or_default()
                        .push(batch);
                }
            }
        }
        Ok(())
    }

    async fn add_batch(&mut self, batch: RecordBatch, watermark: Option<SystemTime>) -> Result<()> {
        if batch.num_rows() == 0 {
            warn!("was asked to add an empty batch, should be impossible");
            return Ok(());
        }
        let start_time =
            start_time_for_sorted_batch(&batch, &self.session_window_config.input_schema_ref);
        let Some(watermark) = watermark else {
            // no watermark, so we can't start an active session yet, just put it in the buffer.
            self.batches_by_start_time
                .entry(start_time)
                .or_default()
                .push(batch);
            return Ok(());
        };
        self.batches_by_start_time
            .entry(start_time)
            .or_default()
            .push(batch);

        if self.active_session.is_some() {
            // the invariant may be broken, so we should fix fill in the active session.
            // this is a little inefficient as we know only the new batch could be added,
            // but it's just a couple of map adds.
            self.fill_active_session()?;
        }
        let flushed_batches = self.watermark_update(watermark).await?;
        if !flushed_batches.is_empty() {
            bail!("should not have flushed batches when adding a batch");
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.active_session.is_none() && self.batches_by_start_time.is_empty()
    }

    fn earliest_data(&self) -> Option<SystemTime> {
        match self.active_session {
            Some(ref active_session) => Some(active_session.data_start),
            None => self
                .batches_by_start_time
                .first_key_value()
                .map(|(start_time, _batches)| *start_time),
        }
    }
}

fn start_time_for_sorted_batch(batch: &RecordBatch, schema: &ArroyoSchema) -> SystemTime {
    let timestamp_array = batch.column(schema.timestamp_index);
    let timestamp_array = timestamp_array
        .as_any()
        .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
        .unwrap();
    let min_timestamp = timestamp_array.value(0);
    from_nanos(min_timestamp as u128)
}

pub struct SessionAggregatingWindowConstructor;

impl OperatorConstructor for SessionAggregatingWindowConstructor {
    type ConfigT = api::SessionWindowAggregateOperator;
    fn with_config(
        &self,
        config: Self::ConfigT,
        registry: Arc<Registry>,
    ) -> anyhow::Result<ConstructedOperator> {
        let window_field = Arc::new(Field::new(
            config.window_field_name,
            window_arrow_struct(),
            true,
        ));

        let receiver = Arc::new(RwLock::new(None));

        let codec = ArroyoPhysicalExtensionCodec {
            context: DecodingContext::UnboundedBatchStream(receiver.clone()),
        };
        let final_plan = PhysicalPlanNode::decode(&mut config.final_aggregation_plan.as_slice())?;
        let final_execution_plan = final_plan.try_into_physical_plan(
            registry.as_ref(),
            &RuntimeEnvBuilder::new().build()?,
            &codec,
        )?;

        let input_schema: ArroyoSchema = config
            .input_schema
            .ok_or_else(|| anyhow!("missing input schema"))?
            .try_into()?;
        let row_converter = if input_schema.routing_keys().is_none() {
            let array = Arc::new(BooleanArray::from(vec![false]));
            Converter::Empty(
                RowConverter::new(vec![SortField::new(DataType::Boolean)])?,
                array,
            )
        } else {
            let key_count = input_schema.routing_keys().as_ref().unwrap().len();
            Converter::RowConverter(RowConverter::new(
                input_schema
                    .schema
                    .fields()
                    .into_iter()
                    .take(key_count)
                    .map(|field| SortField::new(field.data_type().clone()))
                    .collect(),
            )?)
        };

        let config = SessionWindowConfig {
            gap: Duration::from_micros(config.gap_micros),
            window_field,
            window_index: config.window_index as usize,
            input_schema_ref: Arc::new(input_schema),
            final_physical_exec: final_execution_plan,
            receiver,
        };

        Ok(ConstructedOperator::from_operator(Box::new(
            SessionAggregatingWindowFunc {
                config: Arc::new(config),
                keys_by_next_watermark_action: BTreeMap::new(),
                keys_by_start_time: BTreeMap::new(),
                key_computations: HashMap::new(),
                row_converter,
            },
        )))
    }
}

#[async_trait::async_trait]

impl ArrowOperator for SessionAggregatingWindowFunc {
    fn name(&self) -> String {
        "session_window".to_string()
    }

    fn display(&self) -> DisplayableOperator {
        DisplayableOperator {
            name: Cow::Borrowed("SessionAggregatingWindowFunc"),
            fields: vec![
                (
                    "gap",
                    format!("{} seconds", self.config.gap.as_secs()).into(),
                ),
                (
                    "window_field",
                    self.config.window_field.name().as_str().into(),
                ),
                (
                    "window_index",
                    AsDisplayable::Display(&self.config.window_index),
                ),
                ("physical_exec", (&*self.config.final_physical_exec).into()),
            ],
        }
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) {
        let start_times_map: &mut GlobalKeyedView<u32, Option<SystemTime>> =
            ctx.table_manager.get_global_keyed_state("e").await.unwrap();
        let start_time = start_times_map
            .get_all()
            .values()
            .filter_map(|earliest| *earliest)
            .min();
        if start_time.is_none() {
            // each subtask only writes None if it has no data at all, e.g. key_computations is empty.
            return;
        };

        let table = ctx
            .table_manager
            // TODO: this will subtract the retention from start time, so hold more than it should,
            // but we plan to overhaul it all anyway.
            .get_expiring_time_key_table("s", start_time)
            .await
            .expect("should be able to load table");

        let all_batches = table.all_batches_for_watermark(start_time);

        for (_max_timestamp, batches) in all_batches {
            for batch in batches {
                let batch = self
                    .filter_batch_by_time(batch.clone(), start_time)
                    .expect("should be able to filter");
                if batch.num_rows() == 0 {
                    continue;
                }
                let sorted = self
                    .sort_batch(&batch)
                    .expect("should be able to sort batch");

                self.add_at_watermark(sorted, start_time)
                    .await
                    .expect("should be able to add batch");
            }
        }
        let Some(watermark) = ctx.last_present_watermark() else {
            return;
        };

        let evicted_results = self
            .results_at_watermark(watermark)
            .await
            .expect("should be able to get results");
        if !evicted_results.is_empty() {
            warn!(
                "evicted {} results when restoring from state.",
                evicted_results.len()
            );
        }
    }

    // TODO: filter out late data
    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        debug!("received batch {:?}", batch);
        let current_watermark = ctx.last_present_watermark();
        let batch = if let Some(watermark) = current_watermark {
            // filter out late data
            let timestamp_column = batch
                .column(self.config.input_schema_ref.timestamp_index)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let watermark_scalar = TimestampNanosecondArray::new_scalar(to_nanos(watermark) as i64);
            let on_time = gt_eq(timestamp_column, &watermark_scalar).unwrap();
            filter_record_batch(&batch, &on_time).unwrap()
        } else {
            batch
        };
        if batch.num_rows() == 0 {
            warn!("fully filtered out a batch");
            return;
        }
        let sorted = self
            .sort_batch(&batch)
            .expect("should be able to sort batch");

        // send to state backend.
        // TODO: pre-aggregate data before sending to state backend.
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("s", current_watermark)
            .await
            .expect("should get table");

        let max_timestamp = max(sorted
            .column(self.config.input_schema_ref.timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("should have max timestamp"))
        .unwrap();

        table.insert(from_nanos(max_timestamp as u128), sorted.clone());

        self.add_at_watermark(sorted, current_watermark)
            .await
            .expect("should be able to add batch");
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> Option<Watermark> {
        self.advance(ctx, collector).await.unwrap();
        Some(watermark)
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) {
        let watermark = ctx.last_present_watermark();
        let table = ctx
            .table_manager
            .get_expiring_time_key_table("s", watermark)
            .await
            .expect("should get table");
        table.flush(watermark).await.unwrap();
        ctx.table_manager
            .get_global_keyed_state("e")
            .await
            .unwrap()
            .insert(ctx.task_info.task_index, self.earliest_batch_time())
            .await;
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        let mut tables = global_table_config("e", "earliest start time of all active batches.");
        tables.insert(
            "s".to_string(),
            timestamp_table_config(
                "s",
                "session",
                // TODO: something better
                self.config.gap * 100,
                false,
                self.config.input_schema_ref.as_ref().clone(),
            ),
        );
        tables
    }
}
