use super::new_task_context;
use crate::arrow::decode_aggregate;
use crate::arrow::updating_cache::{Key, UpdatingCache};
use anyhow::{Result, bail};
use arrow::compute::{cast, max_array};
use arrow::row::{RowConverter, SortField};
use arrow_array::builder::{
    BinaryBuilder, Int64Builder, TimestampNanosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Float64Type, Int64Type, TimestampNanosecondType, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, RecordBatch, StructArray, UInt32Array,
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
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::{api::UpdatingAggregateOperator, rpc::TableConfig};
use arroyo_rpc::{TIMESTAMP_FIELD, UPDATING_META_FIELD, updating_meta_fields};
use arroyo_state::timestamp_table_config;
use arroyo_types::{CheckpointBarrier, SignalMessage, to_nanos};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::{Accumulator, PhysicalExpr};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use datafusion_proto::protobuf::PhysicalPlanNode;
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use futures::StreamExt;
use itertools::Itertools;
use prost::Message;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant, SystemTime};
use std::{collections::HashMap, mem, sync::Arc};
use tracing::debug;
use tracing::log::warn;

/// Integer totals are kept as i128 (and checkpointed as a Decimal128), so that a total can go negative
/// while a retraction waits for its insert without overflowing
const INTEGER_TOTAL: DataType = DataType::Decimal128(38, 0);

/// The weight of a distinct argument row; this goes negative when a retraction arrives before its insert
#[derive(Debug, Copy, Clone)]
struct BatchData {
    count: i64,
    generation: u64,
}

impl BatchData {
    fn new(count: i64, generation: u64) -> Self {
        Self { count, generation }
    }

    fn add(&mut self, weight: i64) {
        self.count += weight;
        self.generation += 1;
    }
}

/// Restores a weight from the "b" table; when there are several records for a value, the one with the highest
/// generation wins
fn restore_weight(
    data: &mut BTreeMap<Key, BatchData>,
    args_row: &[u8],
    count: i64,
    generation: u64,
) {
    if let Some(existing) = data.get_mut(args_row) {
        if existing.generation < generation {
            existing.count = count;
            existing.generation = generation;
        }
    } else {
        data.insert(
            Key(Arc::new(args_row.to_vec())),
            BatchData::new(count, generation),
        );
    }
}

/// The generation for new values, which must be above any generation we've written or restored: we drop a zero weight
/// from memory once it's checkpointed, but its record stays in the table, and a value inserted again later has to win
/// over it on restore
fn generation_after(current: u64, seen: Option<u64>) -> u64 {
    seen.map_or(current, |g| current.max(g + 1))
}

/// The aggregates we can compute from a running total and row count
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum SignedKind {
    Count,
    Sum,
    Avg,
}

/// A running total, which may be negative while retractions are outstanding
#[derive(Debug, Copy, Clone, PartialEq)]
enum Total {
    Integer(i128),
    Float(f64),
}

impl Total {
    fn zero(integer: bool) -> Self {
        if integer {
            Total::Integer(0)
        } else {
            Total::Float(0.0)
        }
    }

    fn is_zero(&self) -> bool {
        match self {
            Total::Integer(v) => *v == 0,
            Total::Float(v) => *v == 0.0,
        }
    }

    fn as_f64(&self) -> f64 {
        match self {
            Total::Integer(v) => *v as f64,
            Total::Float(v) => *v,
        }
    }
}

/// MIN and MAX, which we answer from the first or last row with a positive weight
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum Extreme {
    Min,
    Max,
}

/// How we store the state for an aggregate. All three are order-independent: an insert and its retraction cancel out
/// regardless of which arrives first, so the result only depends on the rows that remain.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum AccumulatorType {
    /// COUNT, SUM and AVG over integers and floats, stored as a total and a row count
    Signed { kind: SignedKind, integer: bool },
    /// Everything else: we store each distinct argument row with its weight, and compute the aggregate over the rows
    /// with a positive weight. The row encoding preserves ordering, so MIN and MAX can read the ends of the map.
    Weighted { extreme: Option<Extreme> },
    /// The `_timestamp` the planner adds; we take the max over inserts and retractions, so that it's the time of the
    /// group's latest change
    Latest,
    /// The statistical aggregates, which we still leave to DataFusion's sliding accumulators. Unlike the others these
    /// expect a retraction to arrive after its insert.
    Sliding,
}

/// Aggregates computed from running moments (counts, sums and sums of squares and products), which DataFusion can
/// retract from directly
const STATISTICAL: &[&str] = &[
    "var",
    "var_pop",
    "stddev",
    "stddev_pop",
    "covar_samp",
    "covar_pop",
    "corr",
    "regr_slope",
    "regr_intercept",
    "regr_count",
    "regr_r2",
    "regr_avgx",
    "regr_avgy",
    "regr_sxx",
    "regr_syy",
    "regr_sxy",
];

impl AccumulatorType {
    fn of(agg: &AggregateFunctionExpr, name: &str, schema: &Schema) -> DFResult<Self> {
        if name == TIMESTAMP_FIELD {
            return Ok(AccumulatorType::Latest);
        }
        let function = agg.fun().name().to_ascii_lowercase();
        let plain = !agg.is_distinct() && agg.order_bys().is_empty();
        let input = match agg.expressions().first() {
            Some(e) => Some(e.data_type(schema)?),
            None => None,
        };
        let output = agg.field().data_type().clone();
        let numeric = |t: &Option<DataType>| match t {
            Some(t) if t.is_integer() => Some(true),
            Some(t) if t.is_floating() => Some(false),
            _ => None,
        };
        Ok(match (function.as_str(), plain) {
            ("count", true) => AccumulatorType::Signed {
                kind: SignedKind::Count,
                integer: true,
            },
            ("sum", true) => match (numeric(&input), &output) {
                (Some(true), DataType::Int64 | DataType::UInt64) => AccumulatorType::Signed {
                    kind: SignedKind::Sum,
                    integer: true,
                },
                (Some(false), DataType::Float64) => AccumulatorType::Signed {
                    kind: SignedKind::Sum,
                    integer: false,
                },
                _ => AccumulatorType::Weighted { extreme: None },
            },
            ("avg", true) => match (numeric(&input), &output) {
                (Some(integer), DataType::Float64) => AccumulatorType::Signed {
                    kind: SignedKind::Avg,
                    integer,
                },
                _ => AccumulatorType::Weighted { extreme: None },
            },
            ("min", true) => AccumulatorType::Weighted {
                extreme: Some(Extreme::Min),
            },
            ("max", true) => AccumulatorType::Weighted {
                extreme: Some(Extreme::Max),
            },
            (f, true)
                if STATISTICAL.contains(&f)
                    && agg
                        .create_sliding_accumulator()
                        .is_ok_and(|a| a.supports_retract_batch()) =>
            {
                AccumulatorType::Sliding
            }
            _ => AccumulatorType::Weighted { extreme: None },
        })
    }

    fn state_fields(&self, agg: &AggregateFunctionExpr, index: usize) -> DFResult<Vec<Field>> {
        Ok(match self {
            AccumulatorType::Signed { integer, .. } => vec![
                Field::new(
                    format!("total_{index}"),
                    if *integer {
                        INTEGER_TOTAL
                    } else {
                        DataType::Float64
                    },
                    false,
                ),
                Field::new(format!("rows_{index}"), DataType::Int64, false),
            ],
            AccumulatorType::Sliding => agg
                .sliding_state_fields()?
                .iter()
                .map(|f| (**f).clone())
                .collect(),
            AccumulatorType::Weighted { .. } | AccumulatorType::Latest => vec![],
        })
    }
}

#[derive(Debug)]
enum IncrementalState {
    Signed {
        kind: SignedKind,
        total: Total,
        rows: i64,
        output: DataType,
    },
    Weighted {
        expr: Arc<AggregateFunctionExpr>,
        extreme: Option<Extreme>,
        data: BTreeMap<Key, BatchData>,
        row_converter: Arc<RowConverter>,
        changed_values: HashSet<Key>,
    },
    Latest {
        nanos: Option<i64>,
        output: DataType,
    },
    Sliding {
        expr: Arc<AggregateFunctionExpr>,
        accumulator: Box<dyn Accumulator>,
    },
}

/// A hash that's stable across runs and platforms, so that the order we feed rows in is deterministic
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ *b as u64).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// The number of rows where every argument is non-null, which is what COUNT counts
fn valid_rows(args: &[ArrayRef]) -> i64 {
    let Some(first) = args.first() else {
        return 0;
    };
    if args.iter().all(|a| a.null_count() == 0) {
        return first.len() as i64;
    }
    (0..first.len())
        .filter(|i| args.iter().all(|a| a.is_valid(*i)))
        .count() as i64
}

fn sum_of(values: &ArrayRef, integer: bool) -> DFResult<Total> {
    Ok(if integer {
        let values = cast(values, &INTEGER_TOTAL)?;
        Total::Integer(arrow::compute::sum(values.as_primitive::<Decimal128Type>()).unwrap_or(0))
    } else {
        let values = cast(values, &DataType::Float64)?;
        Total::Float(arrow::compute::sum(values.as_primitive::<Float64Type>()).unwrap_or(0.0))
    })
}

impl IncrementalState {
    /// Adds the rows in `args` with `weight` (1 for an insert, -1 for a retraction)
    fn add(&mut self, generation: u64, args: &[ArrayRef], weight: i64) -> DFResult<()> {
        match self {
            IncrementalState::Signed {
                kind, total, rows, ..
            } => {
                *rows += weight * valid_rows(args);
                if *kind != SignedKind::Count {
                    let added = sum_of(&args[0], matches!(total, Total::Integer(_)))?;
                    *total = match (*total, added) {
                        (Total::Integer(t), Total::Integer(a)) => {
                            Total::Integer(t + weight as i128 * a)
                        }
                        (Total::Float(t), Total::Float(a)) => Total::Float(t + weight as f64 * a),
                        _ => unreachable!("a total keeps its type"),
                    };
                }
            }
            IncrementalState::Weighted {
                extreme,
                data,
                row_converter,
                changed_values,
                ..
            } => {
                for (i, r) in row_converter.convert_columns(args)?.iter().enumerate() {
                    // MIN and MAX ignore nulls, and the row encoding sorts them first, so we don't store them
                    if extreme.is_some() && args[0].is_null(i) {
                        continue;
                    }
                    match data.get_mut(r.as_ref()) {
                        Some(d) => d.add(weight),
                        None => {
                            data.insert(
                                Key(Arc::new(r.as_ref().to_vec())),
                                BatchData::new(weight, generation),
                            );
                        }
                    }
                    changed_values.insert(data.get_key_value(r.as_ref()).unwrap().0.clone());
                }
            }
            IncrementalState::Latest { nanos, .. } => {
                let times = cast(&args[0], &DataType::Timestamp(TimeUnit::Nanosecond, None))?;
                if let Some(latest) =
                    arrow::compute::max(times.as_primitive::<TimestampNanosecondType>())
                {
                    *nanos = Some(nanos.map_or(latest, |n| n.max(latest)));
                }
            }
            IncrementalState::Sliding { accumulator, .. } => {
                for _ in 0..weight.abs() {
                    if weight > 0 {
                        accumulator.update_batch(args)?;
                    } else {
                        accumulator.retract_batch(args)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        match self {
            IncrementalState::Signed {
                kind,
                total,
                rows,
                output,
            } => match kind {
                SignedKind::Count => Ok(ScalarValue::Int64(Some(*rows))),
                SignedKind::Sum if *rows <= 0 => ScalarValue::try_from(&*output),
                SignedKind::Sum => Ok(match (*total, &*output) {
                    (Total::Integer(t), DataType::Int64) => ScalarValue::Int64(Some(t as i64)),
                    (Total::Integer(t), DataType::UInt64) => ScalarValue::UInt64(Some(t as u64)),
                    (total, _) => ScalarValue::Float64(Some(total.as_f64())),
                }),
                SignedKind::Avg if *rows <= 0 => ScalarValue::try_from(&*output),
                SignedKind::Avg => Ok(ScalarValue::Float64(Some(total.as_f64() / *rows as f64))),
            },
            IncrementalState::Weighted {
                expr,
                extreme,
                data,
                row_converter,
                ..
            } => {
                let parser = row_converter.parser();
                let positive = |(_, d): &(&Key, &BatchData)| d.count > 0;
                if let Some(extreme) = extreme {
                    let found = match extreme {
                        Extreme::Min => data.iter().find(positive),
                        Extreme::Max => data.iter().rev().find(positive),
                    };
                    return match found {
                        Some((k, _)) => {
                            let column = row_converter.convert_rows([parser.parse(&k.0)])?;
                            ScalarValue::try_from_array(&column[0], 0)
                        }
                        None => ScalarValue::try_from(expr.field().data_type()),
                    };
                }
                // the map is sorted by value, and some approximate aggregates (like the t-digest ones) lose accuracy on
                // sorted input, so we feed the rows in a fixed but unsorted order
                let mut rows: Vec<_> = data.iter().filter(positive).collect();
                rows.sort_by_cached_key(|(k, _)| fnv1a(&k.0));
                let input = row_converter.convert_rows(rows.into_iter().flat_map(|(k, d)| {
                    std::iter::repeat_n(parser.parse(&k.0), d.count as usize)
                }))?;
                let mut acc = expr.create_accumulator()?;
                acc.update_batch(&input)?;
                acc.evaluate()
            }
            IncrementalState::Latest { nanos, output } => {
                let tz = match &*output {
                    DataType::Timestamp(_, tz) => tz.clone(),
                    _ => None,
                };
                Ok(ScalarValue::TimestampNanosecond(*nanos, tz))
            }
            IncrementalState::Sliding { accumulator, .. } => accumulator.evaluate(),
        }
    }

    /// Called once the changed weights are written to the "b" table
    fn written(&mut self) {
        if let IncrementalState::Weighted {
            data,
            changed_values,
            ..
        } = self
        {
            changed_values.clear();
            data.retain(|_, v| v.count != 0);
        }
    }

    /// Whether all of the weights have cancelled out (the latest timestamp doesn't count as state)
    fn is_empty(&self) -> bool {
        match self {
            IncrementalState::Signed { total, rows, .. } => *rows == 0 && total.is_zero(),
            IncrementalState::Weighted { data, .. } => data.values().all(|d| d.count == 0),
            // we can't see inside DataFusion's state, so we go by the group's row count for these
            IncrementalState::Latest { .. } | IncrementalState::Sliding { .. } => true,
        }
    }
}

/// A group is emitted while it has a positive number of rows, and kept until all of its state has cancelled out
#[derive(Debug)]
struct Group {
    rows: i64,
    states: Vec<IncrementalState>,
}

impl Group {
    fn is_emitted(&self) -> bool {
        self.rows > 0
    }

    fn is_empty(&self) -> bool {
        self.rows == 0 && self.states.iter().all(|s| s.is_empty())
    }

    fn latest(&self) -> Option<i64> {
        self.states.iter().find_map(|s| match s {
            IncrementalState::Latest { nanos, .. } => *nanos,
            _ => None,
        })
    }
}

#[derive(Debug)]
struct Aggregator {
    func: Arc<AggregateFunctionExpr>,
    accumulator_type: AccumulatorType,
    row_converter: Arc<RowConverter>,
    /// the columns for the total and row count in the "a" table, for signed aggregates
    state_cols: Vec<usize>,
}

/// Maintains grouped aggregates over an updating stream. We store each change with a weight (1 for an insert, -1 for a
/// retraction) rather than applying it in order, so the results don't depend on the order the changes arrive in, and
/// match what a batch query would compute over the rows that remain.
pub struct IncrementalAggregatingFunc {
    flush_interval: Duration,
    metadata_expr: Arc<dyn PhysicalExpr>,
    aggregates: Vec<Aggregator>,
    accumulators: UpdatingCache<Group>,
    /// keys updated since the last flush, along with what we last emitted for them (so that we can retract it)
    updated_keys: HashMap<Key, Option<Vec<ScalarValue>>>,
    sliding_state_schema: Arc<ArroyoSchema>,
    batch_state_schema: Arc<ArroyoSchema>,
    schema_without_metadata: Arc<Schema>,
    ttl: Duration,
    key_converter: RowConverter,
    /// the number of key columns at the start of both state tables
    key_count: usize,
    new_generation: u64,
}

const GLOBAL_KEY: Vec<u8> = vec![];

impl IncrementalAggregatingFunc {
    fn add(
        &mut self,
        key: &[u8],
        batch: &[Vec<ArrayRef>],
        idx: Option<usize>,
        rows: usize,
        weight: i64,
    ) -> DFResult<()> {
        let generation = self.new_generation;
        let apply = |group: &mut Group| {
            group.rows += weight * rows as i64;
            for (inputs, state) in batch.iter().zip(group.states.iter_mut()) {
                let values = if let Some(idx) = idx {
                    &inputs.iter().map(|c| c.slice(idx, 1)).collect()
                } else {
                    inputs
                };
                state.add(generation, values, weight)?;
            }
            Ok::<(), datafusion::common::DataFusionError>(())
        };
        // only inserts keep a group alive for its ttl
        if weight > 0 {
            self.accumulators
                .modify_and_update(key, Instant::now(), apply)
        } else {
            self.accumulators.modify(key, apply)
        }
        .expect("tried to update a group that does not exist")
    }

    fn evaluate(&mut self, key: &[u8]) -> DFResult<Vec<ScalarValue>> {
        self.accumulators
            .get_mut(key)
            .expect("tried to evaluate a group that does not exist")
            .states
            .iter_mut()
            .map(|s| s.evaluate())
            .collect::<DFResult<_>>()
    }

    fn emitted(&mut self, key: &[u8]) -> DFResult<Option<Vec<ScalarValue>>> {
        match self.accumulators.get_mut(key) {
            Some(group) if group.is_emitted() => self.evaluate(key).map(Some),
            _ => Ok(None),
        }
    }

    fn make_group(&self) -> Group {
        let states = self
            .aggregates
            .iter()
            .map(|agg| match agg.accumulator_type {
                AccumulatorType::Signed { kind, integer } => IncrementalState::Signed {
                    kind,
                    total: Total::zero(integer),
                    rows: 0,
                    output: agg.func.field().data_type().clone(),
                },
                AccumulatorType::Weighted { extreme } => IncrementalState::Weighted {
                    expr: agg.func.clone(),
                    extreme,
                    data: Default::default(),
                    row_converter: agg.row_converter.clone(),
                    changed_values: Default::default(),
                },
                AccumulatorType::Latest => IncrementalState::Latest {
                    nanos: None,
                    output: agg.func.field().data_type().clone(),
                },
                AccumulatorType::Sliding => IncrementalState::Sliding {
                    expr: agg.func.clone(),
                    accumulator: agg.func.create_sliding_accumulator().unwrap(),
                },
            })
            .collect();
        Group { rows: 0, states }
    }

    /// Writes the signed and sliding state, row count and latest timestamp for each updated key. Keys whose state has
    /// cancelled out are written as well, so that we drop them on restore.
    fn checkpoint_sliding(&mut self) -> DFResult<Option<Vec<ArrayRef>>> {
        if self.updated_keys.is_empty() {
            return Ok(None);
        }

        let schema = self.sliding_state_schema.schema.clone();
        let rows_col = schema.index_of("_rows")?;
        let timestamp_col = schema.index_of(TIMESTAMP_FIELD)?;
        let mut columns = vec![Vec::with_capacity(self.updated_keys.len()); schema.fields().len()];
        let mut generation_builder = UInt64Builder::with_capacity(self.updated_keys.len());
        let now = to_nanos(SystemTime::now()) as i64;

        let parser = self.key_converter.parser();
        let mut keys = Vec::with_capacity(self.updated_keys.len());
        for k in self.updated_keys.keys() {
            let (group, generation) = self
                .accumulators
                .get_mut_generation(k.0.as_ref())
                .expect("missing group in cache during checkpoint");
            generation_builder.append_value(generation);
            columns[timestamp_col].push(ScalarValue::TimestampNanosecond(
                Some(group.latest().unwrap_or(now)),
                None,
            ));
            columns[rows_col].push(ScalarValue::Int64(Some(group.rows)));

            for (state, agg) in group.states.iter_mut().zip(self.aggregates.iter()) {
                let values = match state {
                    IncrementalState::Signed { total, rows, .. } => vec![
                        match total {
                            Total::Integer(t) => ScalarValue::Decimal128(Some(*t), 38, 0),
                            Total::Float(t) => ScalarValue::Float64(Some(*t)),
                        },
                        ScalarValue::Int64(Some(*rows)),
                    ],
                    IncrementalState::Sliding { expr, accumulator } => {
                        accumulator.state().unwrap_or_else(|_| {
                            // if it doesn't support immutable state, we use the mutable one and restore from it
                            let state = accumulator.state().unwrap();
                            *accumulator = expr.create_sliding_accumulator().unwrap();
                            let states: Vec<_> =
                                state.iter().map(|s| s.to_array()).try_collect().unwrap();
                            accumulator.merge_batch(&states).unwrap();
                            state
                        })
                    }
                    IncrementalState::Weighted { .. } | IncrementalState::Latest { .. } => continue,
                };
                assert_eq!(
                    agg.state_cols.len(),
                    values.len(),
                    "wrong state in {}",
                    agg.func.name()
                );
                for (col, v) in agg.state_cols.iter().zip(values) {
                    columns[*col].push(v);
                }
            }
            keys.push(parser.parse(k.0.as_ref()));
        }

        let mut cols = self.key_converter.convert_rows(keys)?;
        for (column, field) in columns
            .into_iter()
            .zip(schema.fields())
            .skip(self.key_count)
        {
            let array = ScalarValue::iter_to_array(column)?;
            // ScalarValue builds a Decimal128 at its default precision, so we cast to the column's
            cols.push(cast(&array, field.data_type())?);
        }

        let generations = generation_builder.finish();
        self.new_generation = generation_after(
            self.new_generation,
            max_array::<UInt64Type, _>(&generations),
        );
        cols.push(Arc::new(generations));

        Ok(Some(cols))
    }

    /// Writes the changed weights for the weighted aggregates. Weights that have gone back to zero are written too, so
    /// that a restore doesn't pick up an older value.
    fn checkpoint_batch(&mut self) -> DFResult<Option<Vec<ArrayRef>>> {
        if !self
            .aggregates
            .iter()
            .any(|agg| matches!(agg.accumulator_type, AccumulatorType::Weighted { .. }))
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
        let mut count_builder = Int64Builder::with_capacity(size);
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
                .expect("missing group in cache during checkpoint")
                .states
                .iter_mut()
                .enumerate()
            {
                let IncrementalState::Weighted {
                    data,
                    changed_values,
                    ..
                } = state
                else {
                    continue;
                };

                for vk in changed_values.iter() {
                    if let Some(weight) = data.get(vk) {
                        accumulator_builder.append_value(i as u32);
                        args_row_builder.append_value(&*vk.0);
                        count_builder.append_value(weight.count);
                        generation_builder.append_value(weight.generation);
                        timestamp_builder.append_value(now);
                        rows.push(row.to_owned())
                    }
                }
            }
        }

        let mut cols = self.key_converter.convert_rows(rows)?;

        cols.push(Arc::new(accumulator_builder.finish()));
        cols.push(Arc::new(args_row_builder.finish()));
        cols.push(Arc::new(count_builder.finish()));
        cols.push(Arc::new(timestamp_builder.finish()));

        let generations = generation_builder.finish();
        self.new_generation = generation_after(
            self.new_generation,
            max_array::<UInt64Type, _>(&generations),
        );
        cols.push(Arc::new(generations));

        Ok(Some(cols))
    }

    fn restore_sliding(
        &mut self,
        key: &[u8],
        now: Instant,
        i: usize,
        batch: &RecordBatch,
        generation: u64,
    ) -> Result<()> {
        let mut group = self.make_group();
        group.rows = batch
            .column(self.sliding_state_schema.schema.index_of("_rows")?)
            .as_primitive::<Int64Type>()
            .value(i);
        let timestamps = batch.column(self.sliding_state_schema.schema.index_of(TIMESTAMP_FIELD)?);
        let latest = timestamps
            .as_primitive::<TimestampNanosecondType>()
            .value(i);
        for (agg, state) in self.aggregates.iter().zip(group.states.iter_mut()) {
            match state {
                IncrementalState::Signed { total, rows, .. } => {
                    let column = batch.column(agg.state_cols[0]);
                    *total = match total {
                        Total::Integer(_) => {
                            Total::Integer(column.as_primitive::<Decimal128Type>().value(i))
                        }
                        Total::Float(_) => {
                            Total::Float(column.as_primitive::<Float64Type>().value(i))
                        }
                    };
                    *rows = batch
                        .column(agg.state_cols[1])
                        .as_primitive::<Int64Type>()
                        .value(i);
                }
                IncrementalState::Latest { nanos, .. } => *nanos = Some(latest),
                IncrementalState::Sliding { accumulator, .. } => {
                    let state: Vec<_> = agg
                        .state_cols
                        .iter()
                        .map(|c| batch.column(*c).slice(i, 1))
                        .collect();
                    accumulator.merge_batch(&state)?;
                }
                IncrementalState::Weighted { .. } => {}
            }
        }

        self.accumulators
            .insert(Arc::new(key.to_vec()), now, generation, group);

        Ok(())
    }

    async fn initialize(&mut self, ctx: &mut OperatorContext) -> Result<()> {
        let table = ctx.table_manager.get_uncached_key_value_view("a").await?;
        let mut restored: Option<u64> = None;

        // restore the groups and their signed state
        let mut stream = Box::pin(table.get_all());
        let key_converter = RowConverter::new(self.sliding_state_schema.sort_fields(false))?;

        while let Some(batch) = stream.next().await {
            let batch = batch?;

            if batch.num_rows() == 0 {
                continue;
            }

            let key_cols: Vec<_> = self
                .sliding_state_schema
                .sort_columns(&batch, false)
                .into_iter()
                .map(|c| c.values)
                .collect();

            let generations = batch.columns().last().unwrap().as_primitive::<UInt64Type>();
            restored = restored.max(max_array::<UInt64Type, _>(generations));

            let now = Instant::now();

            if key_cols.is_empty() {
                // global aggregate
                self.restore_sliding(&GLOBAL_KEY, now, 0, &batch, generations.value(0))?;
            } else {
                let key_rows = key_converter.convert_columns(&key_cols)?;
                for ((i, row), generation) in key_rows.iter().enumerate().zip(generations) {
                    self.restore_sliding(row.as_ref(), now, i, &batch, generation.unwrap())?;
                }
            }
        }

        drop(stream);

        // restore the weights, if there are weighted aggregates
        if self
            .aggregates
            .iter()
            .any(|agg| matches!(agg.accumulator_type, AccumulatorType::Weighted { .. }))
        {
            let table = ctx.table_manager.get_uncached_key_value_view("b").await?;
            let mut stream = Box::pin(table.get_all());
            while let Some(batch) = stream.next().await {
                let batch = batch?;

                if batch.num_rows() == 0 {
                    continue;
                }

                let key_cols: Vec<_> = self
                    .sliding_state_schema
                    .sort_columns(&batch, false)
                    .into_iter()
                    .map(|c| c.values)
                    .collect();

                // checkpoints from before weights were signed store them as UInt64
                let count_column = cast(
                    batch.column(self.batch_state_schema.schema.index_of("count").unwrap()),
                    &DataType::Int64,
                )?;
                let count_column = count_column.as_primitive::<Int64Type>();
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
                restored = restored.max(max_array::<UInt64Type, _>(generations));

                let key_rows = if key_cols.is_empty() {
                    // every row belongs to the single global group
                    vec![GLOBAL_KEY; batch.num_rows()]
                } else {
                    self.key_converter
                        .convert_columns(&key_cols)?
                        .iter()
                        .map(|k| k.as_ref().to_vec())
                        .collect()
                };

                for (i, row) in key_rows.iter().enumerate() {
                    let Some(group) = self.accumulators.get_mut(row.as_ref()) else {
                        debug!("missing group for key {:?} while restoring weights", row);
                        continue;
                    };

                    let count = count_column.value(i);
                    let accumulator_idx = accumulator_column.value(i) as usize;
                    let args_row = args_row_column.value(i);
                    let generation = generations.value(i);

                    let IncrementalState::Weighted { data, .. } =
                        &mut group.states[accumulator_idx]
                    else {
                        bail!("expected aggregate {accumulator_idx} to be weighted");
                    };

                    restore_weight(data, args_row, count, generation);
                }
            }
        }

        self.new_generation = generation_after(self.new_generation, restored);

        // drop groups whose state had cancelled out when they were checkpointed
        let mut empty = vec![];
        for (k, group) in self.accumulators.iter_mut() {
            for state in group.states.iter_mut() {
                if let IncrementalState::Weighted { data, .. } = state {
                    data.retain(|_, v| v.count != 0);
                }
            }
            if group.is_empty() {
                empty.push(k.clone());
            }
        }
        for k in empty {
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
            self.weights_written();
        }

        Ok(())
    }

    /// Once the weights are written, we can forget which ones changed and clear out the zero weights; doing it any
    /// earlier would lose them if the write failed
    fn weights_written(&mut self) {
        for k in self.updated_keys.keys() {
            if let Some(group) = self.accumulators.get_mut(k.0.as_ref()) {
                group.states.iter_mut().for_each(IncrementalState::written);
            }
        }
    }

    async fn flush(&mut self, ctx: &mut OperatorContext) -> Result<Option<RecordBatch>> {
        self.checkpoint(ctx).await?;

        let mut output_keys = Vec::with_capacity(self.updated_keys.len() * 2);
        let mut output_values =
            vec![Vec::with_capacity(self.updated_keys.len() * 2); self.aggregates.len()];
        let mut is_retracts = Vec::with_capacity(self.updated_keys.len() * 2);

        let updated_keys = mem::take(&mut self.updated_keys);
        let mut empty = vec![];

        for (k, before) in updated_keys {
            let now = self.emitted(&k.0)?;

            // don't bother emitting updates that just retract / append the same values (excluding
            // the last, timestamp field)
            let unchanged = match (&before, &now) {
                (Some(b), Some(n)) => b
                    .iter()
                    .zip(n.iter())
                    .take(b.len() - 1)
                    .all(|(x, y)| x == y),
                _ => false,
            };
            if !unchanged {
                for (values, retract) in [(before, true), (now, false)] {
                    if let Some(values) = values {
                        is_retracts.push(retract);
                        output_keys.push(k.0.clone());
                        for (out, v) in output_values.iter_mut().zip(values) {
                            out.push(v);
                        }
                    }
                }
            }

            if self
                .accumulators
                .get_mut(&k.0)
                .is_some_and(|g| g.is_empty())
            {
                empty.push(k);
            }
        }

        for k in empty {
            self.accumulators.remove(&k.0);
        }

        for (k, mut group) in self.accumulators.time_out(Instant::now()) {
            // retract groups that are being ttl'd, if we emitted them
            if !group.is_emitted() {
                continue;
            }
            is_retracts.push(true);
            output_keys.push(k);
            for (out, state) in output_values.iter_mut().zip(group.states.iter_mut()) {
                out.push(state.evaluate()?);
            }
        }

        if output_keys.is_empty() {
            return Ok(None);
        }

        let row_parser = self.key_converter.parser();
        let mut result_cols = self
            .key_converter
            .convert_rows(output_keys.iter().map(|k| row_parser.parse(k.as_slice())))?;

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
        if let Some(meta_col) = batch.column_by_name(UPDATING_META_FIELD) {
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
        }
    }

    /// The first time a key is updated in a flush, we store what it currently emits so that we can retract it
    fn record_before(&mut self, key: &[u8]) -> DFResult<()> {
        if self.updated_keys.contains_key(key) {
            return Ok(());
        }
        let before = match self.accumulators.get_mut_key_value(key) {
            Some((key, group)) if group.is_emitted() => {
                let values = group
                    .states
                    .iter_mut()
                    .map(|s| s.evaluate())
                    .collect::<DFResult<_>>()?;
                (key, Some(values))
            }
            Some((key, _)) => (key, None),
            None => (Key(Arc::new(key.to_vec())), None),
        };
        self.updated_keys.insert(before.0, before.1);
        Ok(())
    }

    fn ensure_group(&mut self, key: &[u8]) {
        if !self.accumulators.contains_key(key) {
            let group = self.make_group();
            self.accumulators.insert(
                Arc::new(key.to_vec()),
                Instant::now(),
                self.new_generation,
                group,
            );
        }
    }

    fn global_aggregate(&mut self, batch: &RecordBatch) -> Result<()> {
        let retracts = Self::get_retracts(batch).cloned();
        let aggregate_input_cols = self.compute_inputs(&batch);

        self.record_before(&GLOBAL_KEY)?;
        self.ensure_group(&GLOBAL_KEY);

        if let Some(retracts) = retracts {
            for (i, r) in retracts.iter().enumerate() {
                let weight = if r.unwrap_or_default() { -1 } else { 1 };
                self.add(&GLOBAL_KEY, &aggregate_input_cols, Some(i), 1, weight)?;
            }
        } else {
            // if these are all appends, we can much more efficiently do all of the updates at once
            self.add(
                &GLOBAL_KEY,
                &aggregate_input_cols,
                None,
                batch.num_rows(),
                1,
            )?;
        }

        Ok(())
    }

    fn keyed_aggregate(&mut self, batch: &RecordBatch, ctx: &OperatorContext) -> Result<()> {
        let retracts = Self::get_retracts(batch).cloned();

        let sort_columns = &ctx.in_schemas[0]
            .sort_columns(batch, false)
            .into_iter()
            .map(|e| e.values)
            .collect::<Vec<_>>();

        let keys = self.key_converter.convert_columns(sort_columns).unwrap();

        for k in &keys {
            self.record_before(k.as_ref())?;
        }

        let aggregate_input_cols = self.compute_inputs(&batch);

        for (i, key) in keys.iter().enumerate() {
            self.ensure_group(key.as_ref());
            let retract = retracts.as_ref().map(|r| r.value(i)).unwrap_or_default();
            let weight = if retract { -1 } else { 1 };
            self.add(key.as_ref(), &aggregate_input_cols, Some(i), 1, weight)?;
        }

        Ok(())
    }

    fn compute_inputs(&self, batch: &&RecordBatch) -> Vec<Vec<ArrayRef>> {
        self.aggregates
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
            .collect::<Vec<_>>()
    }
}

#[async_trait::async_trait]
impl ArrowOperator for IncrementalAggregatingFunc {
    fn name(&self) -> String {
        "UpdatingAggregatingFunc".to_string()
    }

    fn display(&self) -> DisplayableOperator<'_> {
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
    ) -> DataflowResult<()> {
        let input_schema = &ctx.in_schemas[0];

        if input_schema
            .routing_keys()
            .map(|k| !k.is_empty())
            .unwrap_or_default()
        {
            self.keyed_aggregate(&batch, ctx)?
        } else {
            self.global_aggregate(&batch)?
        };
        Ok(())
    }

    async fn handle_checkpoint(
        &mut self,
        _: CheckpointBarrier,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if let Some(batch) = self.flush(ctx).await? {
            collector.collect(batch).await?;
        }
        Ok(())
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
    ) -> DataflowResult<()> {
        if let Some(batch) = self.flush(ctx).await? {
            collector.collect(batch).await?;
        }
        Ok(())
    }

    async fn on_close(
        &mut self,
        final_message: &Option<SignalMessage>,
        ctx: &mut OperatorContext,
        collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if let Some(SignalMessage::EndOfData) = final_message
            && let Some(batch) = self.flush(ctx).await?
        {
            collector.collect(batch).await?;
        }
        Ok(())
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        self.initialize(ctx).await?;
        Ok(())
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
        let task_context = new_task_context(registry.as_ref())?;
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
            &task_context,
            &input_schema.schema,
            &DefaultPhysicalExtensionCodec {},
        )?;

        let aggregate_exec = PhysicalPlanNode::decode(&mut config.aggregate_exec.as_ref())?;
        let PhysicalPlanType::Aggregate(aggregate_exec) =
            aggregate_exec.physical_plan_type.unwrap()
        else {
            bail!("invalid proto -- expected aggregate exec");
        };

        // both state tables start with the key fields
        let key_fields_of_input = input_schema
            .routing_keys()
            .map(|v| {
                v.iter()
                    .map(|idx| input_schema.schema.field(*idx).clone())
                    .collect_vec()
            })
            .unwrap_or_default();
        let key_fields = (0..key_fields_of_input.len()).collect_vec();

        // the "a" table holds the keys, the total and row count for each signed aggregate, the group's row count, and the
        // latest timestamp
        let mut sliding_state_fields = key_fields_of_input.clone();
        let mut aggregates = Vec::with_capacity(aggregate_exec.aggr_expr.len());
        for (i, (expr, name)) in aggregate_exec
            .aggr_expr
            .iter()
            .zip(aggregate_exec.aggr_expr_name.iter())
            .enumerate()
        {
            let agg = decode_aggregate(&input_schema.schema, name, expr, &task_context)?;
            let accumulator_type = AccumulatorType::of(&agg, name, &input_schema.schema)?;
            let row_converter = Arc::new(RowConverter::new(
                agg.expressions()
                    .iter()
                    .map(|ex| Ok(SortField::new(ex.data_type(&input_schema.schema)?)))
                    .collect::<DFResult<_>>()?,
            )?);
            let fields = accumulator_type.state_fields(&agg, i)?;
            let state_cols =
                (sliding_state_fields.len()..sliding_state_fields.len() + fields.len()).collect();
            sliding_state_fields.extend(fields);
            aggregates.push(Aggregator {
                func: agg,
                accumulator_type,
                row_converter,
                state_cols,
            });
        }
        if !aggregates
            .iter()
            .any(|a| a.accumulator_type == AccumulatorType::Latest)
        {
            bail!("an updating aggregate needs the planner's {TIMESTAMP_FIELD} aggregate");
        }
        sliding_state_fields.push(Field::new("_rows", DataType::Int64, false));
        sliding_state_fields.push(Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ));

        let sliding_state_schema = Arc::new(ArroyoSchema::from_schema_keys(
            Arc::new(Schema::new(sliding_state_fields)),
            key_fields.clone(),
        )?);

        // the "b" table holds a weight per key, aggregate and distinct argument row
        let mut batch_state_fields = key_fields_of_input;
        batch_state_fields.push(Field::new("accumulator", DataType::UInt32, false));
        batch_state_fields.push(Field::new("args_row", DataType::Binary, false));
        batch_state_fields.push(Field::new("count", DataType::Int64, false));
        batch_state_fields.push(Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ));
        let timestamp_index = batch_state_fields.len() - 1;

        let mut storage_key_fields = key_fields.clone();
        // include accumulator and args_row in the keys
        storage_key_fields.push(storage_key_fields.len());
        storage_key_fields.push(storage_key_fields.len());

        let batch_state_schema = Arc::new(ArroyoSchema::new(
            Arc::new(Schema::new(batch_state_fields)),
            timestamp_index,
            Some(storage_key_fields),
            // only include the actual keys in the routing keys
            Some(key_fields.clone()),
        ));

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
                key_count: key_fields.len(),
                sliding_state_schema,
                batch_state_schema,
                new_generation: 0,
            },
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use datafusion::functions_aggregate::min_max::max_udaf;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::col;

    fn int(values: &[i64]) -> Vec<ArrayRef> {
        vec![Arc::new(Int64Array::from(values.to_vec()))]
    }

    fn signed(kind: SignedKind, output: DataType) -> IncrementalState {
        IncrementalState::Signed {
            kind,
            total: Total::zero(true),
            rows: 0,
            output,
        }
    }

    #[test]
    fn reinserted_value_wins_over_its_zero_weight() {
        // a zero weight for 2 was checkpointed at generation 3, and dropped from memory after restoring
        let mut data = BTreeMap::new();
        restore_weight(&mut data, &[5], 1, 1);
        restore_weight(&mut data, &[2], 0, 3);
        data.retain(|_, v: &mut BatchData| v.count != 0);
        assert!(!data.contains_key(&[2][..]));

        // we insert 2 again, and the next restore reads both of its records, in either order
        let generation = generation_after(0, Some(3));
        for records in [[(0, 3), (1, generation)], [(1, generation), (0, 3)]] {
            let mut restored = BTreeMap::new();
            for (count, g) in records {
                restore_weight(&mut restored, &[2], count, g);
            }
            assert_eq!(restored[&[2][..]].count, 1);
        }
    }

    fn weighted_max() -> IncrementalState {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let expr = AggregateExprBuilder::new(max_udaf(), vec![col("v", &schema).unwrap()])
            .schema(schema)
            .alias("max_v")
            .build()
            .unwrap();
        IncrementalState::Weighted {
            expr: Arc::new(expr),
            extreme: Some(Extreme::Max),
            data: Default::default(),
            row_converter: Arc::new(
                RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap(),
            ),
            changed_values: Default::default(),
        }
    }

    fn changed(state: &IncrementalState) -> usize {
        match state {
            IncrementalState::Weighted { changed_values, .. } => changed_values.len(),
            _ => unreachable!(),
        }
    }

    #[test]
    fn changed_weights_wait_until_written() {
        let mut max = weighted_max();
        max.add(0, &int(&[2]), 1).unwrap();
        max.add(0, &int(&[2]), -1).unwrap();
        max.add(0, &int(&[5]), 1).unwrap();
        assert_eq!(
            changed(&max),
            2,
            "until they're written, a retry still sees both changes"
        );

        max.written();
        assert_eq!(changed(&max), 0);
        assert_eq!(max.evaluate().unwrap(), ScalarValue::Int64(Some(5)));
        let IncrementalState::Weighted { data, .. } = &max else {
            unreachable!()
        };
        assert_eq!(data.len(), 1, "the zero weight for 2 is cleared out");
    }

    #[test]
    fn signed_sum_passes_through_negative_rows() {
        let mut sum = signed(SignedKind::Sum, DataType::Int64);
        sum.add(0, &int(&[7]), -1).unwrap();
        assert_eq!(
            sum.evaluate().unwrap(),
            ScalarValue::Int64(None),
            "a pending retraction emits nothing"
        );
        sum.add(0, &int(&[7]), 1).unwrap();
        assert!(sum.is_empty(), "its insert cancels it");
        sum.add(0, &int(&[2]), 1).unwrap();
        assert_eq!(sum.evaluate().unwrap(), ScalarValue::Int64(Some(2)));
    }

    #[test]
    fn count_and_avg_are_signed() {
        let mut count = signed(SignedKind::Count, DataType::Int64);
        let mut avg = signed(SignedKind::Avg, DataType::Float64);
        for (values, weight) in [(&[4, 6][..], 1), (&[9][..], -1), (&[9][..], 1)] {
            count.add(0, &int(values), weight).unwrap();
            avg.add(0, &int(values), weight).unwrap();
        }
        assert_eq!(count.evaluate().unwrap(), ScalarValue::Int64(Some(2)));
        assert_eq!(avg.evaluate().unwrap(), ScalarValue::Float64(Some(5.0)));
    }
}
