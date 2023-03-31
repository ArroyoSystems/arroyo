#![allow(clippy::comparison_chain)]
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    time::Duration,
};

use anyhow::Result;
use anyhow::{anyhow, bail};
use arrow_schema::DataType;
use arroyo_datastream::{
    EdgeType::Forward, EdgeType::Shuffle, EdgeType::ShuffleJoin, Operator, Program,
    SlidingAggregatingTopN, SlidingWindowAggregator, StreamEdge, StreamNode, TumblingTopN,
    TumblingWindowAggregator, WindowAgg, WindowType,
};

use datafusion_common::{DFField, ScalarValue};
use datafusion_expr::{
    BinaryExpr, BuiltInWindowFunction, Expr, JoinConstraint, LogicalPlan, Window,
};
use petgraph::graph::{DiGraph, NodeIndex};
use quote::{format_ident, quote};
use syn::{parse_str, Type};

use crate::{
    expressions::{
        parse_expression, to_expression_generator, Column, ColumnExpression, Expression,
        ExpressionGenerator, SortExpression,
    },
    operators::{
        AggregateProjection, GroupByKind, Projection, TwoPhaseAggregateProjection,
        TwoPhaseAggregation,
    },
    types::{StructDef, StructField, TypeDef},
    ArroyoSchemaProvider, SqlConfig, SqlSource,
};

#[derive(Debug)]
pub enum SqlOperator {
    Source(String, SqlSource),
    Aggregator(Box<SqlOperator>, AggregateOperator),
    JoinOperator(Box<SqlOperator>, Box<SqlOperator>, JoinOperator),
    Window(Box<SqlOperator>, SqlWindowOperator),
    WindowAggregateTopN(
        Box<SqlOperator>,
        AggregateOperator,
        Projection,
        SqlWindowOperator,
    ),
    RecordTransform(Box<SqlOperator>, RecordTransform),
}

#[derive(Debug)]
pub enum RecordTransform {
    ValueProjection(Projection),
    #[allow(dead_code)]
    KeyProjection(Projection),
    Filter(Expression),
    #[allow(dead_code)]
    Sequence(Vec<RecordTransform>),
}
impl RecordTransform {
    fn all_structs(&self) -> Vec<StructDef> {
        match self {
            RecordTransform::ValueProjection(projection)
            | RecordTransform::KeyProjection(projection) => {
                projection.output_struct().all_structs()
            }
            RecordTransform::Filter(_) => vec![],
            RecordTransform::Sequence(sequence) => sequence
                .iter()
                .flat_map(|record_transform| record_transform.all_structs())
                .collect(),
        }
    }

    fn output_struct(&self, input_struct: StructDef) -> StructDef {
        match self {
            RecordTransform::ValueProjection(projection) => projection.output_struct(),
            RecordTransform::KeyProjection(_) | RecordTransform::Filter(_) => input_struct,
            RecordTransform::Sequence(sequence) => {
                sequence.iter().fold(input_struct, |input, transform| {
                    transform.output_struct(input)
                })
            }
        }
    }

    fn key_names(&self) -> Vec<String> {
        match self {
            RecordTransform::KeyProjection(projection) => projection.output_struct().all_names(),
            RecordTransform::ValueProjection(_) | RecordTransform::Filter(_) => vec![],
            RecordTransform::Sequence(sequence) => sequence
                .iter()
                .flat_map(|record_transform| record_transform.key_names())
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct AggregateOperator {
    key: Projection,
    window: WindowType,
    aggregating: AggregatingStrategy,
    merge: GroupByKind,
}
impl AggregateOperator {
    fn output_struct(&self) -> StructDef {
        self.merge
            .output_struct(&self.key, self.aggregating.output_struct())
    }
}

#[derive(Debug)]
pub enum AggregatingStrategy {
    AggregateProjection(AggregateProjection),
    TwoPhaseAggregateProjection(TwoPhaseAggregateProjection),
}
impl AggregatingStrategy {
    fn output_struct(&self) -> StructDef {
        match self {
            AggregatingStrategy::AggregateProjection(aggregate_production) => {
                aggregate_production.output_struct()
            }
            AggregatingStrategy::TwoPhaseAggregateProjection(two_phase) => {
                two_phase.output_struct()
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum WindowFunction {
    RowNumber,
}

#[derive(Debug, Clone)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug)]
pub struct SqlWindowOperator {
    window_fn: WindowFunction,
    partition: Projection,
    order_by: Vec<SortExpression>,
    field_name: String,
    max_value: Option<i64>,
    window: WindowType,
}
impl SqlWindowOperator {
    fn with_max_value(mut self, max_value: i64) -> SqlWindowOperator {
        self.max_value = Some(max_value);
        self
    }
}

#[derive(Debug)]
pub struct JoinOperator {
    left_key: Projection,
    right_key: Projection,
    join_type: JoinType,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    /// Inner Join
    Inner,
    /// Left Join
    Left,
    /// Right Join
    Right,
    /// Full Join
    Full,
}

impl TryFrom<datafusion_expr::JoinType> for JoinType {
    type Error = anyhow::Error;

    fn try_from(join_type: datafusion_expr::JoinType) -> Result<Self> {
        match join_type {
            datafusion_expr::JoinType::Inner => Ok(JoinType::Inner),
            datafusion_expr::JoinType::Left => Ok(JoinType::Left),
            datafusion_expr::JoinType::Right => Ok(JoinType::Right),
            datafusion_expr::JoinType::Full => Ok(JoinType::Full),
            datafusion_expr::JoinType::LeftSemi
            | datafusion_expr::JoinType::RightSemi
            | datafusion_expr::JoinType::LeftAnti
            | datafusion_expr::JoinType::RightAnti => bail!("{:?} not yet supported", join_type),
        }
    }
}

impl JoinOperator {
    fn output_struct(&self, left_struct: &StructDef, right_struct: &StructDef) -> StructDef {
        // input to join should always be two structs. Nullability determined by join type.
        let mut fields = if self.left_nullable() {
            left_struct
                .fields
                .iter()
                .map(|field| field.as_nullable())
                .collect()
        } else {
            left_struct.fields.clone()
        };
        if self.right_nullable() {
            right_struct
                .fields
                .iter()
                .map(|field| field.as_nullable())
                .for_each(|field| fields.push(field))
        } else {
            fields.append(&mut right_struct.fields.clone());
        }
        StructDef { name: None, fields }
    }

    fn join_struct_type(&self, left_struct: &StructDef, right_struct: &StructDef) -> StructDef {
        StructDef {
            name: None,
            fields: vec![
                StructField {
                    name: "left".to_string(),
                    alias: None,
                    data_type: TypeDef::StructDef(left_struct.clone(), self.left_nullable()),
                },
                StructField {
                    name: "right".to_string(),
                    alias: None,
                    data_type: TypeDef::StructDef(right_struct.clone(), self.right_nullable()),
                },
            ],
        }
    }

    pub fn left_nullable(&self) -> bool {
        match self.join_type {
            JoinType::Inner | JoinType::Left => false,
            JoinType::Right | JoinType::Full => true,
        }
    }
    pub fn right_nullable(&self) -> bool {
        match self.join_type {
            JoinType::Inner | JoinType::Right => false,
            JoinType::Left | JoinType::Full => true,
        }
    }

    fn merge_syn_expression(&self, left_struct: &StructDef, right_struct: &StructDef) -> syn::Expr {
        let mut assignments: Vec<_> = vec![];

        left_struct.fields.iter().for_each(|field| {
                let field_name = format_ident!("{}",field.field_name());
                if self.left_nullable() {
                    if field.data_type.is_optional() {
                        assignments.push(quote!(#field_name : arg.left.as_ref().map(|inner| inner.#field_name.clone()).flatten()));
                    } else {
                        assignments.push(quote!(#field_name : arg.left.as_ref().map(|inner| inner.#field_name.clone())));
                    }
                } else {
                    assignments.push(quote!(#field_name : arg.left.#field_name.clone()));
                }
            });
        right_struct.fields.iter().for_each(|field| {
                let field_name = format_ident!("{}",field.field_name());
                if self.right_nullable() {
                    if field.data_type.is_optional() {
                        assignments.push(quote!(#field_name : arg.right.as_ref().map(|inner| inner.#field_name.clone()).flatten()));
                    } else {
                        assignments.push(quote!(#field_name : arg.right.as_ref().map(|inner| inner.#field_name.clone())));
                    }
                } else {
                    assignments.push(quote!(#field_name : arg.right.#field_name.clone()));
                }
            });

        let return_struct = self.output_struct(left_struct, right_struct);
        let return_type = return_struct.get_type();
        let tokens = quote!(
                #return_type {
                    #(#assignments)
                    ,*
                }
        );
        parse_expression(tokens)
    }
}

impl SqlOperator {
    pub fn return_type(&self) -> StructDef {
        match self {
            SqlOperator::Source(_, source) => source.struct_def.clone(),
            SqlOperator::Aggregator(_input, aggregate_operator) => {
                aggregate_operator.merge.output_struct(
                    &aggregate_operator.key,
                    aggregate_operator.aggregating.output_struct(),
                )
            }
            SqlOperator::JoinOperator(left, right, operator) => {
                operator.output_struct(&left.return_type(), &right.return_type())
            }
            SqlOperator::Window(input, window) => {
                let mut input_struct = input.return_type();
                input_struct.fields.push(StructField {
                    name: window.field_name.clone(),
                    alias: None,
                    data_type: TypeDef::DataType(DataType::Int64, false),
                });
                input_struct
            }
            SqlOperator::WindowAggregateTopN(_input, _aggregate_operator, projection, window) => {
                let mut input_struct = projection.output_struct();
                input_struct.fields.push(StructField {
                    name: window.field_name.clone(),
                    alias: None,
                    data_type: TypeDef::DataType(DataType::Int64, false),
                });
                input_struct
            }
            SqlOperator::RecordTransform(input, record_transform) => {
                record_transform.output_struct(input.return_type())
            }
        }
    }

    fn all_structs(&self) -> Vec<StructDef> {
        let mut structs = vec![];
        match self {
            SqlOperator::Source(_, _) => structs.append(&mut self.return_type().all_structs()),
            SqlOperator::Aggregator(_input, aggregate_operator) => {
                let key_struct = aggregate_operator.key.output_struct();
                structs.append(&mut key_struct.all_structs());
                let aggregate_struct = aggregate_operator.aggregating.output_struct();
                structs.append(&mut aggregate_struct.all_structs());
                let merge_struct = Self::merge_struct_type(&key_struct, &aggregate_struct);
                structs.append(&mut merge_struct.all_structs());
                structs.append(
                    &mut aggregate_operator
                        .merge
                        .output_struct(
                            &aggregate_operator.key,
                            aggregate_operator.aggregating.output_struct(),
                        )
                        .all_structs(),
                );
            }
            SqlOperator::JoinOperator(left, right, operator) => {
                let left_type = left.return_type();
                let right_type = right.return_type();
                let join_struct = operator.join_struct_type(&left_type, &right_type);
                structs.append(&mut operator.left_key.output_struct().all_structs());
                structs.append(&mut join_struct.all_structs());
                structs.append(
                    &mut operator
                        .output_struct(&left_type, &right_type)
                        .all_structs(),
                );
            }
            SqlOperator::Window(_, window_operator) => {
                structs.append(&mut window_operator.partition.output_struct().all_structs());
                structs.append(&mut self.return_type().all_structs());
            }
            SqlOperator::WindowAggregateTopN(_, aggregate_operator, projection, window) => {
                let key_struct = aggregate_operator.key.output_struct();
                structs.append(&mut key_struct.all_structs());
                let aggregate_struct = aggregate_operator.aggregating.output_struct();
                structs.append(&mut aggregate_struct.all_structs());
                let merge_struct = Self::merge_struct_type(&key_struct, &aggregate_struct);
                structs.append(&mut merge_struct.all_structs());
                structs.append(&mut aggregate_operator.output_struct().all_structs());
                structs.append(&mut projection.output_struct().all_structs());
                structs.append(&mut window.partition.output_struct().all_structs());
                structs.append(
                    &mut projection
                        .output_struct()
                        .truncated_return_type(aggregate_struct.fields.len())
                        .all_structs(),
                );
                structs.append(&mut self.return_type().all_structs());
            }
            SqlOperator::RecordTransform(_input, record_transform) => {
                structs.append(&mut record_transform.all_structs());
            }
        };
        structs
    }

    fn key_names(&self) -> Result<Vec<String>> {
        let mut structs = vec![];
        match self {
            SqlOperator::Source(_, _) => {}
            SqlOperator::Aggregator(_input, aggregate_operator) => {
                let key_struct = aggregate_operator.key.output_struct();
                structs.append(&mut key_struct.all_names());
            }
            SqlOperator::JoinOperator(_left, _right, operator) => {
                structs.append(&mut operator.left_key.output_struct().all_names());
            }
            SqlOperator::Window(_, window_operator) => {
                structs.append(&mut window_operator.partition.output_struct().all_names())
            }
            SqlOperator::WindowAggregateTopN(
                _,
                aggregate_operator,
                _projection,
                window_operator,
            ) => {
                let key_struct = aggregate_operator.key.output_struct();
                structs.append(&mut key_struct.all_names());
                structs.append(&mut window_operator.partition.output_struct().all_names());
            }
            SqlOperator::RecordTransform(_, record_transform) => {
                structs.append(&mut record_transform.key_names())
            }
        };
        Ok(structs)
    }

    fn merge_struct_type(key_struct: &StructDef, aggregate_struct: &StructDef) -> StructDef {
        StructDef {
            name: None,
            fields: vec![
                StructField {
                    name: "key".to_string(),
                    alias: None,
                    data_type: TypeDef::StructDef(key_struct.clone(), false),
                },
                StructField {
                    name: "aggregate".to_string(),
                    alias: None,
                    data_type: TypeDef::StructDef(aggregate_struct.clone(), false),
                },
                StructField {
                    name: "timestamp".to_string(),
                    alias: None,
                    data_type: TypeDef::DataType(
                        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                        false,
                    ),
                },
            ],
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqlPipelineBuilder {
    pub sources: HashMap<String, SqlSource>,
}

impl SqlPipelineBuilder {
    pub fn new(sources: HashMap<String, SqlSource>) -> Self {
        SqlPipelineBuilder { sources }
    }

    pub fn insert_sql_plan(&mut self, plan: &LogicalPlan) -> Result<SqlOperator> {
        match plan {
            LogicalPlan::Projection(projection) => self.insert_projection(projection),
            LogicalPlan::Filter(filter) => self.insert_filter(filter),
            LogicalPlan::Aggregate(aggregate) => self.insert_aggregation(aggregate),
            LogicalPlan::Sort(_) => bail!("sorting is not currently supported"),
            LogicalPlan::Join(join) => self.insert_join(join),
            LogicalPlan::CrossJoin(_) => bail!("cross joins are not currently supported"),
            LogicalPlan::Repartition(_) => todo!(),
            LogicalPlan::Union(_) => todo!(),
            LogicalPlan::TableScan(table_scan) => self.insert_table_scan(table_scan),
            LogicalPlan::EmptyRelation(_) => todo!(),
            LogicalPlan::Subquery(subquery) => self.insert_sql_plan(&subquery.subquery),
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                self.insert_subquery_alias(subquery_alias)
            }
            LogicalPlan::Limit(_) => bail!("limit not currently supported"),
            LogicalPlan::CreateExternalTable(_) => todo!(),
            LogicalPlan::CreateMemoryTable(_) => todo!(),
            LogicalPlan::CreateView(_) => todo!(),
            LogicalPlan::CreateCatalogSchema(_) => todo!(),
            LogicalPlan::CreateCatalog(_) => todo!(),
            LogicalPlan::DropTable(_) => todo!(),
            LogicalPlan::DropView(_) => todo!(),
            LogicalPlan::Values(_) => todo!(),
            LogicalPlan::Explain(_) => todo!(),
            LogicalPlan::Analyze(_) => todo!(),
            LogicalPlan::Extension(_) => todo!(),
            LogicalPlan::Distinct(_) => todo!(),
            LogicalPlan::Window(window) => self.insert_window(window),
            LogicalPlan::SetVariable(_) => todo!(),
            LogicalPlan::Prepare(_) => todo!(),
            LogicalPlan::Dml(_) => todo!(),
            LogicalPlan::DescribeTable(_) => todo!(),
            LogicalPlan::Unnest(_) => todo!(),
        }
    }

    fn insert_aggregate_then_top_n(
        &mut self,
        aggregate_input: Box<SqlOperator>,
        aggregate_operator: AggregateOperator,
        projection: Projection,
        sql_window_operator: SqlWindowOperator,
    ) -> Result<SqlOperator> {
        Ok(SqlOperator::WindowAggregateTopN(
            aggregate_input,
            aggregate_operator,
            projection,
            sql_window_operator,
        ))
    }

    fn insert_filter(
        &mut self,
        filter: &datafusion_expr::logical_plan::Filter,
    ) -> Result<SqlOperator> {
        let input = self.insert_sql_plan(&filter.input)?;
        let input_struct = input.return_type();
        let predicate = to_expression_generator(&filter.predicate, &input_struct)?;
        // TODO: this should probably happen through a more principled optimization pass.
        if let SqlOperator::Window(window_input, sql_window_operator) = input {
            let field = input_struct.get_field(None, &sql_window_operator.field_name)?;
            let max_value = predicate.has_max_value(&field);
            let Some(max_value) = max_value else {
                return Ok(SqlOperator::RecordTransform(Box::new(SqlOperator::Window(
                    window_input,
                    sql_window_operator,
                )), RecordTransform::Filter(predicate)));
            };

            if let SqlOperator::RecordTransform(
                map_input,
                RecordTransform::ValueProjection(projection),
            ) = *window_input
            {
                if let SqlOperator::Aggregator(aggregator_input, aggregate_operator) = *map_input {
                    return self.insert_aggregate_then_top_n(
                        aggregator_input,
                        aggregate_operator,
                        projection,
                        sql_window_operator.with_max_value(max_value),
                    );
                }
                todo!()
            }

            Ok(SqlOperator::RecordTransform(
                Box::new(SqlOperator::Window(
                    window_input,
                    sql_window_operator.with_max_value(max_value),
                )),
                RecordTransform::Filter(predicate),
            ))
        } else {
            Ok(SqlOperator::RecordTransform(
                Box::new(input),
                RecordTransform::Filter(predicate),
            ))
        }
    }

    fn insert_projection(
        &mut self,
        projection: &datafusion_expr::logical_plan::Projection,
    ) -> Result<SqlOperator> {
        let input = self.insert_sql_plan(&projection.input)?;

        let functions = projection
            .expr
            .iter()
            .map(|expr| Ok(to_expression_generator(expr, &input.return_type())?))
            .collect::<Result<Vec<_>>>()?;

        let names = projection
            .schema
            .fields()
            .iter()
            .map(|field| Column {
                relation: field.qualifier().cloned(),
                name: field.field().name().to_string(),
            })
            .collect();

        let projection = Projection {
            field_names: names,
            field_computations: functions,
        };

        Ok(SqlOperator::RecordTransform(
            Box::new(input),
            RecordTransform::ValueProjection(projection),
        ))
    }

    fn insert_aggregation(
        &mut self,
        aggregate: &datafusion_expr::logical_plan::Aggregate,
    ) -> Result<SqlOperator> {
        let source = self.insert_sql_plan(&aggregate.input)?;
        let key = self.aggregation_key(
            &aggregate.group_expr,
            aggregate.schema.fields(),
            &source.return_type(),
        )?;

        let window = self.window(&aggregate.group_expr)?;

        let group_count = aggregate.group_expr.len();
        let aggregate_fields: Vec<_> = aggregate
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, field)| {
                if i >= group_count {
                    Some(field.clone())
                } else {
                    None
                }
            })
            .collect();
        let aggregating = self.aggregate_calculation(
            &aggregate.aggr_expr,
            aggregate_fields,
            &source.return_type(),
            window.clone(),
        )?;
        let merge = self.window_field(&aggregate.group_expr, aggregate.schema.fields())?;
        Ok(SqlOperator::Aggregator(
            Box::new(source),
            AggregateOperator {
                key,
                window,
                aggregating,
                merge,
            },
        ))
    }

    fn aggregation_key(
        &mut self,
        group_expressions: &[Expr],
        fields: &[DFField],
        input_struct: &StructDef,
    ) -> Result<Projection> {
        let field_pairs: Vec<Option<(Column, Expression)>> = group_expressions
            .iter()
            .enumerate()
            .map(|(i, expr)| {
                if self.is_window(expr) {
                    Ok(None)
                } else {
                    let field = to_expression_generator(expr, input_struct)?;
                    Ok(Some((
                        Column {
                            relation: fields[i].qualifier().cloned(),
                            name: fields[i].name().clone(),
                        },
                        field,
                    )))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let field_pairs: Vec<_> = field_pairs.into_iter().filter_map(|val| val).collect();
        let projection = Projection {
            field_names: field_pairs
                .iter()
                .map(|(column, _)| column.clone())
                .collect(),
            field_computations: field_pairs
                .into_iter()
                .map(|(_, computation)| computation)
                .collect(),
        };

        Ok(projection)
    }

    fn window(&mut self, group_expressions: &[Expr]) -> Result<WindowType> {
        let mut windows: Vec<_> = Vec::new();
        for expression in group_expressions {
            if let Some(window) = self.find_window(expression)? {
                windows.push(window);
            }
        }
        match windows.len() {
            0 => Ok(WindowType::Instant),
            1 => Ok(windows[0].clone()),
            multiple => bail!("{} windows detected, must be one or zero.", multiple),
        }
    }

    fn window_field(
        &mut self,
        group_expressions: &[Expr],
        fields: &[DFField],
    ) -> Result<GroupByKind> {
        for (i, expr) in group_expressions.iter().enumerate() {
            if let Some(window) = self.find_window(expr)? {
                if let WindowType::Instant = window {
                    bail!("don't support instant window in return type yet");
                }
                return Ok(GroupByKind::WindowOutput {
                    index: i,
                    column: Column {
                        relation: fields[i].qualifier().cloned(),
                        name: fields[i].name().clone(),
                    },
                    window_type: window,
                });
            }
        }
        Ok(GroupByKind::Basic)
    }

    fn is_window(&mut self, expression: &Expr) -> bool {
        match expression {
            Expr::ScalarUDF { fun, args: _ } => matches!(fun.name.as_str(), "hop" | "tumble"),
            Expr::Alias(exp, _) => self.is_window(exp),
            _ => false,
        }
    }

    fn find_window(&mut self, expression: &Expr) -> Result<Option<WindowType>> {
        match expression {
            Expr::ScalarUDF { fun, args } => match fun.name.as_str() {
                "hop" => {
                    if args.len() != 2 {
                        unreachable!();
                    }
                    let slide = Self::get_duration(&args[0])?;
                    let width = Self::get_duration(&args[1])?;
                    Ok(Some(WindowType::Sliding { width, slide }))
                }
                "tumble" => {
                    if args.len() != 1 {
                        unreachable!("wrong number of arguments for tumble(), expect one");
                    }
                    let width = Self::get_duration(&args[0])?;
                    Ok(Some(WindowType::Tumbling { width }))
                }
                _ => Ok(None),
            },
            Expr::Alias(expr, _alias) => self.find_window(expr),
            _ => Ok(None),
        }
    }
    fn get_duration(expression: &Expr) -> Result<Duration> {
        match expression {
            Expr::Literal(ScalarValue::IntervalDayTime(Some(val))) => {
                Ok(Duration::from_millis(*val as u64))
            }
            _ => bail!(
                "unsupported Duration expression, expect duration literal, not {}",
                expression
            ),
        }
    }

    fn insert_join(&mut self, join: &datafusion_expr::logical_plan::Join) -> Result<SqlOperator> {
        let left_input = self.insert_sql_plan(&join.left)?;
        let right_input = self.insert_sql_plan(&join.right)?;
        match join.join_constraint {
            JoinConstraint::On => {}
            JoinConstraint::Using => bail!("don't support 'using' in joins"),
        };
        let join_type = join.join_type.try_into()?;
        let mut columns = join.on.clone();
        if let Some(Expr::BinaryExpr(BinaryExpr {
            left,
            op: datafusion_expr::Operator::Eq,
            right,
        })) = join.filter.clone()
        {
            columns.push((*left, *right));
        } else if join.filter.is_some() {
            todo!("non-join filters on joins");
        }
        let join_projection_field_names: Vec<_> = columns
            .iter()
            .map(|(left, _right)| Column::convert_expr(left))
            .collect();

        let left_key = Projection {
            field_names: join_projection_field_names.clone(),
            field_computations: columns
                .iter()
                .map(|(left, _right)| to_expression_generator(left, &left_input.return_type()))
                .collect::<Result<Vec<_>>>()?,
        };
        let right_key = Projection {
            field_names: join_projection_field_names,
            field_computations: columns
                .iter()
                .map(|(_left, right)| to_expression_generator(right, &right_input.return_type()))
                .collect::<Result<Vec<_>>>()?,
        };
        Ok(SqlOperator::JoinOperator(
            Box::new(left_input),
            Box::new(right_input),
            JoinOperator {
                left_key,
                right_key,
                join_type,
            },
        ))
    }

    fn insert_table_scan(
        &mut self,
        table_scan: &datafusion::logical_expr::TableScan,
    ) -> Result<SqlOperator> {
        let source = self
            .sources
            .get(&table_scan.table_name)
            .ok_or_else(|| anyhow!("Source {} does not exist", table_scan.table_name))?;

        let source_operator = SqlOperator::Source(table_scan.table_name.clone(), source.clone());

        if let Some(projection) = table_scan.projection.as_ref() {
            let fields: Vec<StructField> = projection
                .iter()
                .map(|i| source.struct_def.fields[*i].clone())
                .collect();

            let field_names = fields
                .iter()
                .map(|t| Column {
                    relation: None,
                    name: t.name.clone(),
                })
                .collect();

            let field_computations = fields
                .iter()
                .map(|t| Expression::ColumnExpression(ColumnExpression::new(t.clone())))
                .collect();

            return Ok(SqlOperator::RecordTransform(
                Box::new(source_operator),
                RecordTransform::ValueProjection(Projection {
                    field_names,
                    field_computations,
                }),
            ));
        }

        Ok(source_operator)
    }

    fn insert_window(&mut self, window: &Window) -> Result<SqlOperator> {
        let input = self.insert_sql_plan(&window.input)?;

        if let Some(expr) = window.window_expr.get(0) {
            match expr {
                Expr::WindowFunction(w) => {
                    let window_fn = match &w.fun {
                        datafusion_expr::WindowFunction::AggregateFunction(_) => {
                            bail!("window aggregate functions not yet supported")
                        }
                        datafusion_expr::WindowFunction::BuiltInWindowFunction(
                            BuiltInWindowFunction::RowNumber,
                        ) => WindowFunction::RowNumber,
                        datafusion_expr::WindowFunction::BuiltInWindowFunction(w) => {
                            bail!("Window function {} not yet supported", w);
                        }
                        datafusion_expr::WindowFunction::AggregateUDF(_) => {
                            bail!("Window UDAFs not yet supported");
                        }
                    };
                    let input_struct = input.return_type();

                    let order_by: Vec<_> = w
                        .order_by
                        .iter()
                        .map(|expr| {
                            if let Expr::Sort(sort) = expr {
                                SortExpression::from_expression(sort, &input_struct)
                            } else {
                                panic!("expected sort expression, found {:?}", expr);
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let field_names = w
                        .partition_by
                        .iter()
                        .filter(|expr| !self.is_window(expr))
                        .enumerate()
                        .map(|(i, _t)| Column {
                            relation: None,
                            name: format!("_{}", i),
                        })
                        .collect();

                    let field_computations = w
                        .partition_by
                        .iter()
                        .filter(|expr| !self.is_window(expr))
                        .map(|expression| to_expression_generator(expression, &input_struct))
                        .collect::<Result<Vec<_>>>()?;

                    let partition = Projection {
                        field_names,
                        field_computations,
                    }
                    .without_window();
                    let field_name = window.schema.field_names().last().cloned().unwrap();
                    let window = self.window(&w.partition_by)?;

                    return Ok(SqlOperator::Window(
                        Box::new(input),
                        SqlWindowOperator {
                            window_fn,
                            partition,
                            order_by,
                            field_name,
                            max_value: None,
                            window,
                        },
                    ));
                }
                _ => {
                    panic!("non window expression for window: {:?}", expr);
                }
            }
        }

        panic!("no expression for window");
    }

    fn insert_subquery_alias(
        &mut self,
        subquery_alias: &datafusion_expr::logical_plan::SubqueryAlias,
    ) -> Result<SqlOperator> {
        let input = self.insert_sql_plan(&subquery_alias.input)?;
        let input_type = input.return_type();

        let field_computations = input_type
            .fields
            .iter()
            .map(|field| Expression::ColumnExpression(ColumnExpression::new(field.clone())))
            .collect();

        let field_names = subquery_alias
            .schema
            .fields()
            .iter()
            .map(|field| Column {
                relation: field.qualifier().cloned(),
                name: field.name().clone(),
            })
            .collect();

        let projection = Projection {
            field_names,
            field_computations,
        };
        Ok(SqlOperator::RecordTransform(
            Box::new(input),
            RecordTransform::ValueProjection(projection),
        ))
    }

    fn aggregate_calculation(
        &mut self,
        aggr_expr: &[Expr],
        aggregate_fields: Vec<DFField>,
        input_struct: &StructDef,
        window: WindowType,
    ) -> Result<AggregatingStrategy> {
        let field_names = aggregate_fields
            .iter()
            .map(|field| Column {
                relation: field.qualifier().cloned(),
                name: field.name().clone(),
            })
            .collect();
        let two_phase_field_computations = aggr_expr
            .iter()
            .map(|expr| TwoPhaseAggregation::from_expression(expr, input_struct))
            .collect::<Result<Vec<_>>>();

        match (two_phase_field_computations, window) {
            (Ok(field_computations), WindowType::Tumbling { .. })
            | (Ok(field_computations), WindowType::Instant) => {
                return Ok(AggregatingStrategy::TwoPhaseAggregateProjection(
                    TwoPhaseAggregateProjection {
                        field_names,
                        field_computations,
                    },
                ));
            }
            (Ok(field_computations), WindowType::Sliding { width, slide }) => {
                if width.as_millis() % slide.as_millis() == 0 {
                    return Ok(AggregatingStrategy::TwoPhaseAggregateProjection(
                        TwoPhaseAggregateProjection {
                            field_names,
                            field_computations,
                        },
                    ));
                }
            }
            _ => {}
        }
        let field_computations = aggr_expr
            .iter()
            .map(|expr| to_expression_generator(expr, input_struct))
            .collect::<Result<Vec<_>>>()?;
        Ok(AggregatingStrategy::AggregateProjection(
            AggregateProjection {
                field_names,
                field_computations,
            },
        ))
    }
}

#[derive(Debug)]
pub struct MethodCompiler {}

impl MethodCompiler {
    pub fn new() -> Self {
        MethodCompiler {}
    }
    pub fn key_map_operator(name: impl ToString, key_expr: syn::Expr) -> Operator {
        let expression = quote!(
            {
                let key = {
                    let arg = &record.value;
                    #key_expr
                };
                arroyo_types::Record {
                timestamp: record.timestamp,
                key: Some(key),
                value: record.value.clone()
        }
        });
        Operator::ExpressionOperator {
            name: name.to_string(),
            expression: expression.to_string(),
            return_type: arroyo_datastream::ExpressionReturnType::Record,
        }
    }

    pub fn key_and_value_map_operator(
        name: impl ToString,
        key_expr: syn::Expr,
        value_expr: syn::Expr,
    ) -> Operator {
        let expression = quote!(
            {
                let (key, value) = {
                    let arg = &record.value;
                    (#key_expr, #value_expr)
                };
                arroyo_types::Record {
                timestamp: record.timestamp,
                key: Some(key),
                value: value,
        }
        });
        Operator::ExpressionOperator {
            name: name.to_string(),
            expression: expression.to_string(),
            return_type: arroyo_datastream::ExpressionReturnType::Record,
        }
    }

    pub fn join_merge_operator(
        name: impl ToString,
        join_type: JoinType,
        merge_struct_name: Type,
        merge_expr: syn::Expr,
    ) -> Result<Operator> {
        let expression = match join_type {
            JoinType::Inner => {
                quote!({
                    let record = record.clone();
                    let lefts = record.value.0;
                    let rights = record.value.1;
                    let mut value = Vec::with_capacity(lefts.len() * rights.len());
                    for left in lefts.clone() {
                        for right in rights.clone() {
                            let arg = #merge_struct_name{left: left.clone(), right};
                            value.push(#merge_expr);
                        }
                    }
                    arroyo_types::Record {
                    timestamp: record.timestamp,
                    key: None,
                    value
            }}).to_string()}
            JoinType::Left => {
                quote!({
                    let record = record.clone();
                    let lefts = record.value.0;
                    let rights = record.value.1;
                    let value = if rights.len() == 0 {
                        let mut value = Vec::with_capacity(lefts.len());
                        for left in lefts.clone() {
                            let arg = #merge_struct_name{left, right: None};
                            value.push(#merge_expr);
                        }
                        value
                    } else {
                        let mut value = Vec::with_capacity(lefts.len() * rights.len());
                        for left in lefts.clone() {
                            for right in rights.clone() {
                                let arg = #merge_struct_name{left: left.clone(), right: Some(right)};
                                value.push(#merge_expr);
                            }
                        }
                        value
                    };

                    arroyo_types::Record {
                        timestamp: record.timestamp,
                        key: None,
                        value
                    }
                }).to_string()},
            JoinType::Right => {quote!(
                {
                    let record = record.clone();
                    let lefts = record.value.0;
                    let rights = record.value.1;
                    let value = if lefts.len() == 0 {
                        let mut value = Vec::with_capacity(rights.len());
                        for right in rights.clone() {
                            let arg = #merge_struct_name{left: None, right};
                            value.push(#merge_expr);
                        }
                        value
                    } else {
                        let mut value = Vec::with_capacity(lefts.len() * rights.len());
                        for left in lefts.clone() {
                            for right in rights.clone() {
                                let arg = #merge_struct_name{left: Some(left.clone()), right};
                                value.push(#merge_expr);
                            }
                        }
                        value
                    };

                    arroyo_types::Record {
                    timestamp: record.timestamp,
                    key: None,
                    value
            }}).to_string()},
            JoinType::Full => {
                quote!({
                    let record = record.clone();
                    let lefts = record.value.0;
                    let rights = record.value.1;
                    let value = if lefts.len() == 0 {
                        let mut value = Vec::with_capacity(rights.len());
                        for right in rights.clone() {
                            let arg = #merge_struct_name{left: None, right:Some(right)};
                            value.push(#merge_expr);
                        }
                        value
                    } else if rights.len() == 0 {
                        let mut value = Vec::with_capacity(rights.len());
                        for left in lefts.clone() {
                            let arg = #merge_struct_name{left: Some(left), right: None};
                            value.push(#merge_expr);
                        }
                        value
                    } else {
                        let mut value = Vec::with_capacity(lefts.len() * rights.len());
                        for left in lefts.clone() {
                            for right in rights.clone() {
                                let arg = #merge_struct_name{left: Some(left.clone()),right: Some(right)};
                                value.push(#merge_expr);
                            }
                        }
                        value
                    };

                    arroyo_types::Record {
                    timestamp: record.timestamp,
                    key: None,
                    value
            }
        })
        .to_string()
    },
        };

        Ok(Operator::ExpressionOperator {
            name: name.to_string(),
            expression,
            return_type: arroyo_datastream::ExpressionReturnType::Record,
        })
    }

    fn value_map_operator(name: impl ToString, map_expr: syn::Expr) -> Operator {
        let expression = quote!(
                {
                    let arg = &record.value;
                    let value = #map_expr;
                    arroyo_types::Record {
                    timestamp: record.timestamp,
                    key: None,
                    value
            }
        });
        Operator::ExpressionOperator {
            name: name.to_string(),
            expression: expression.to_string(),
            return_type: arroyo_datastream::ExpressionReturnType::Record,
        }
    }
}

pub struct GraphCompiler {
    pub config: SqlConfig,
    pub method_compiler: MethodCompiler,
    pub graph: DiGraph<StreamNode, StreamEdge>,
    pub sources: HashMap<String, NodeIndex>,
    pub types: HashSet<StructDef>,
    pub key_structs: HashSet<String>,
    pub used_sources: Vec<SqlSource>,
}

pub fn get_program_from_plan(
    config: SqlConfig,
    schema_provider: ArroyoSchemaProvider,
    logical_plan: &LogicalPlan,
) -> Result<(Program, Vec<SqlSource>)> {
    let sql_operator =
        SqlPipelineBuilder::new(schema_provider.sources.clone()).insert_sql_plan(logical_plan)?;
    get_program_from_operator(config, &sql_operator, schema_provider)
}

pub fn get_program_from_operator(
    config: SqlConfig,
    operator: &SqlOperator,
    schema_provider: ArroyoSchemaProvider,
) -> Result<(Program, Vec<SqlSource>)> {
    let mut graph_compiler = GraphCompiler::new(config.clone());
    let final_node = graph_compiler.add_operator(operator)?;
    let sink_index = graph_compiler.add_node("sink", config.sink);
    let sink_edge = StreamEdge::unkeyed_edge(operator.return_type().struct_name(), Forward);
    graph_compiler
        .graph
        .add_edge(final_node, sink_index, sink_edge);

    let mut other_defs: Vec<_> = graph_compiler
        .types
        .iter()
        .map(|s| s.def(graph_compiler.key_structs.contains(&s.struct_name())))
        .collect();

    other_defs.extend(
        schema_provider
            .source_defs
            .into_iter()
            .filter(|(k, _)| graph_compiler.sources.contains_key(k))
            .map(|(_, v)| v),
    );

    Ok((
        Program {
            // For now, we don't export any types from SQL into WASM, as there is a problem with doing serde
            // in wasm
            types: vec![],
            other_defs,
            graph: graph_compiler.graph,
        },
        graph_compiler.used_sources,
    ))
}

impl GraphCompiler {
    fn new(config: SqlConfig) -> GraphCompiler {
        GraphCompiler {
            config,
            method_compiler: MethodCompiler::new(),
            graph: DiGraph::new(),
            sources: HashMap::new(),
            types: HashSet::new(),
            key_structs: HashSet::new(),
            used_sources: vec![],
        }
    }
    fn add_operator(&mut self, operator: &SqlOperator) -> Result<NodeIndex> {
        self.types.extend(operator.all_structs());
        self.key_structs.extend(operator.key_names()?);
        match &operator {
            SqlOperator::Source(name, source) => Ok(self.add_source(name, source)),
            SqlOperator::Aggregator(input, aggregate_operator) => {
                self.add_aggregator(input, aggregate_operator)
            }
            SqlOperator::JoinOperator(left_input, right_input, join_operator) => {
                self.add_join(left_input, right_input, join_operator)
            }
            SqlOperator::Window(input, window) => {
                self.add_window_func(input, window, &operator.return_type())
            }
            SqlOperator::WindowAggregateTopN(input, aggregator, projection, window) => self
                .add_window_aggregate_top_n(
                    input,
                    aggregator,
                    projection,
                    window,
                    &operator.return_type(),
                ),
            SqlOperator::RecordTransform(input, record_transform) => {
                let input_index = self.add_operator(input)?;
                self.add_record_transform(input_index, input.return_type(), record_transform)
            }
        }
    }

    fn add_node(&mut self, descriptor: impl Display, operator: Operator) -> NodeIndex {
        self.graph.add_node(StreamNode {
            operator_id: format!("{}_{}", descriptor, self.graph.node_count()),
            operator,
            parallelism: self.config.default_parallelism,
        })
    }

    fn add_source(&mut self, name: &String, source: &SqlSource) -> NodeIndex {
        if let Some(node_index) = self.sources.get(name) {
            return *node_index;
        }
        let source_index = self.add_node("source", source.operator.clone());
        let watermark_operator =
            arroyo_datastream::Operator::Watermark(arroyo_datastream::WatermarkType::Periodic {
                period: Duration::from_secs(1),
                max_lateness: Duration::from_secs(0),
            });
        let watermark_index = self.add_node("watermark", watermark_operator);
        let watermark_edge = StreamEdge::unkeyed_edge(source.struct_def.struct_name(), Forward);
        self.graph
            .add_edge(source_index, watermark_index, watermark_edge);
        self.sources.insert(name.clone(), watermark_index);
        self.used_sources.push(source.clone());
        watermark_index
    }

    fn add_map(
        &mut self,
        input_index: NodeIndex,
        input_struct: StructDef,
        map: &Projection,
    ) -> Result<NodeIndex> {
        let map_method = map.to_syn_expression();

        let map_operator = MethodCompiler::value_map_operator("map", map_method);
        let node_index = self.add_node("map", map_operator);
        self.graph.add_edge(
            input_index,
            node_index,
            StreamEdge::unkeyed_edge(input_struct.struct_name(), Forward),
        );
        Ok(node_index)
    }

    fn add_filter(
        &mut self,
        input_index: NodeIndex,
        input_struct: StructDef,
        predicate: &Expression,
    ) -> Result<NodeIndex> {
        let predicate_expr = predicate.to_syn_expression();

        let expression = match predicate.return_type().is_optional() {
            true => quote!(
                {
                    let arg = &record.value;
                    (#predicate_expr).unwrap_or(false)
                }
            )
            .to_string(),
            false => quote!(
                {
                    let arg = &record.value;
                    #predicate_expr
                }
            )
            .to_string(),
        };
        let operator = arroyo_datastream::Operator::ExpressionOperator {
            name: "filter".to_string(),
            expression,
            return_type: arroyo_datastream::ExpressionReturnType::Predicate,
        };
        let node_index = self.add_node("filter", operator);
        self.graph.add_edge(
            input_index,
            node_index,
            StreamEdge::unkeyed_edge(input_struct.struct_name(), Forward),
        );
        Ok(node_index)
    }

    fn add_window_func(
        &mut self,
        input: &SqlOperator,
        window: &SqlWindowOperator,
        return_struct: &StructDef,
    ) -> Result<NodeIndex> {
        let input_index = self.add_operator(input)?;
        let input_struct = input.return_type();

        let key_method = window.partition.to_syn_expression();
        let key_struct = window.partition.output_struct();
        let key_operator = MethodCompiler::key_map_operator("partition_by", key_method);
        let key_index = self.add_node("partition_by", key_operator);
        let key_edge = StreamEdge::unkeyed_edge(input_struct.struct_name(), Forward);

        self.graph.add_edge(input_index, key_index, key_edge);

        let sort_tokens: Vec<_> = window
            .order_by
            .iter()
            .map(|sort_expression| sort_expression.to_syn_expr())
            .collect();

        let window_field = return_struct.fields.last().unwrap().field_ident();
        let output_struct = format_ident!("{}", return_struct.struct_name());
        let mut field_assignments: Vec<_> = input_struct
            .fields
            .iter()
            .map(|f| {
                let ident = f.field_ident();
                quote! { #ident: arg.#ident.clone() }
            })
            .collect();

        match window.window_fn {
            WindowFunction::RowNumber => {
                field_assignments.push(quote! {
                    #window_field: i as i64
                });
            }
        }
        let output_expression = quote!(#output_struct {
            #(#field_assignments, )*
        });

        let operator = if let (Some(max_elements), Some(width)) = (
            window.max_value,
            match &window.window {
                WindowType::Tumbling { width } => Some(width.clone()),
                WindowType::Sliding { .. } => None,
                WindowType::Instant => Some(Duration::ZERO),
            },
        ) {
            let extractor = quote!(
                |arg| {
                #((#sort_tokens,))*
                }
            )
            .to_string();
            let converter = quote!(
                |arg, i| #output_expression
            )
            .to_string();
            let sort_types: Vec<_> = window
                .order_by
                .iter()
                .map(|sort_expression| sort_expression.tuple_type())
                .collect();
            let partition_key_type = quote!(#((#sort_types,))*).to_string();

            arroyo_datastream::Operator::TumblingTopN(TumblingTopN {
                width,
                max_elements: max_elements as usize,
                extractor,
                partition_key_type,
                converter,
            })
        } else {
            let sort = if sort_tokens.len() > 0 {
                Some(quote!(arg.sort_by_key(|arg| #(#sort_tokens,)*);))
            } else {
                None
            };
            arroyo_datastream::Operator::Window {
                typ: window.window.clone(),
                agg: Some(WindowAgg::Expression {
                    name: "sql_window".to_string(),
                    expression: quote! {
                        {
                            #sort
                            let mut result = vec![];
                            for (index, arg) in arg.iter().enumerate() {
                                let i = index + 1;
                                result.push(#output_expression);
                            }
                            result
                        }
                    }
                    .to_string(),
                }),
                flatten: true,
            }
        };

        let node_index = self.add_node("window", operator);
        self.graph.add_edge(
            key_index,
            node_index,
            StreamEdge::keyed_edge(
                key_struct.struct_name(),
                input.return_type().struct_name(),
                Shuffle,
            ),
        );

        // unkey
        let unkey_operator = arroyo_datastream::Operator::ExpressionOperator {
            name: "unkey".to_string(),
            expression: quote! {
                arroyo_types::Record {
                    timestamp: record.timestamp,
                    key: None,
                    value: record.value.clone(),
                }
            }
            .to_string(),
            return_type: arroyo_datastream::ExpressionReturnType::Record,
        };

        let unkey_index = self.add_node("unkey", unkey_operator);
        self.graph.add_edge(
            node_index,
            unkey_index,
            StreamEdge::keyed_edge(
                key_struct.struct_name(),
                return_struct.struct_name(),
                Forward,
            ),
        );

        Ok(unkey_index)
    }

    fn add_aggregator(
        &mut self,
        input: &SqlOperator,
        aggregate_operator: &AggregateOperator,
    ) -> Result<NodeIndex> {
        let input_index = self.add_operator(input)?;
        let input_struct = input.return_type();

        let key_method = aggregate_operator.key.to_syn_expression();
        let key_struct = aggregate_operator.key.output_struct();
        let key_operator = MethodCompiler::key_map_operator("aggregator_key", key_method);
        let key_index = self.add_node("aggregator_key", key_operator);
        let key_edge = StreamEdge::unkeyed_edge(input_struct.struct_name(), Forward);

        self.graph.add_edge(input_index, key_index, key_edge);

        let window_operator = match &aggregate_operator.aggregating {
            AggregatingStrategy::AggregateProjection(aggregate_projection) => {
                let aggregate_expr = aggregate_projection.to_syn_expression();
                arroyo_datastream::Operator::Window {
                    typ: aggregate_operator.window.clone(),
                    agg: Some(WindowAgg::Expression {
                        // TODO: find a way to get a more useful name
                        name: "aggregation".to_string(),
                        expression: quote! {
                            #aggregate_expr
                        }
                        .to_string(),
                    }),
                    flatten: false,
                }
            }
            AggregatingStrategy::TwoPhaseAggregateProjection(two_phase_aggregation) => {
                let (width, slide) = match aggregate_operator.window {
                    WindowType::Tumbling { width } => (width, width),
                    WindowType::Sliding { width, slide } => (width, slide),
                    WindowType::Instant => (Duration::ZERO, Duration::ZERO),
                };
                // this is a tumbling window, which is easier.
                if width == slide {
                    let aggregate_expr =
                        two_phase_aggregation.tumbling_aggregation_syn_expression();
                    let bin_merger = two_phase_aggregation.bin_merger_syn_expression();
                    let bin_type = two_phase_aggregation.bin_type();
                    arroyo_datastream::Operator::TumblingWindowAggregator(
                        TumblingWindowAggregator {
                            width,
                            aggregator: quote!(|arg| {#aggregate_expr}).to_string(),
                            bin_merger: quote!(|arg, current_bin| {#bin_merger}).to_string(),
                            bin_type: quote!(#bin_type).to_string(),
                        },
                    )
                } else {
                    let aggregate_expr = two_phase_aggregation.sliding_aggregation_syn_expression();
                    let bin_merger = two_phase_aggregation.bin_merger_syn_expression();
                    let in_memory_add = two_phase_aggregation.memory_add_syn_expression();
                    let in_memory_remove = two_phase_aggregation.memory_remove_syn_expression();
                    let bin_type = two_phase_aggregation.bin_type();
                    let mem_type = two_phase_aggregation.memory_type();
                    arroyo_datastream::Operator::SlidingWindowAggregator(SlidingWindowAggregator {
                        width,
                        slide,
                        aggregator: quote!(|arg| {#aggregate_expr}).to_string(),
                        bin_merger: quote!(|arg, current_bin| {#bin_merger}).to_string(),
                        in_memory_add: quote!(|current, bin_value| {#in_memory_add}).to_string(),
                        in_memory_remove: quote!(|current, bin_value| {#in_memory_remove})
                            .to_string(),
                        bin_type: quote!(#bin_type).to_string(),
                        mem_type: quote!(#mem_type).to_string(),
                    })
                }
            }
        };

        let window_index = self.add_node("aggregate_window", window_operator);
        let window_edge = StreamEdge {
            key: key_struct.struct_name(),
            value: input_struct.struct_name(),
            typ: Shuffle,
        };

        self.graph.add_edge(key_index, window_index, window_edge);

        let aggregate_struct = aggregate_operator.aggregating.output_struct();
        let merge_struct = SqlOperator::merge_struct_type(&key_struct, &aggregate_struct);

        let merge_struct_ident = merge_struct.get_type();
        let merge_expr = aggregate_operator.merge.to_syn_expression(
            &aggregate_operator.key,
            aggregate_operator.aggregating.output_struct(),
        );
        let expression = quote!(
            {
                let aggregate = &record.value;
                let key = record.key.clone().unwrap();
                let arg = #merge_struct_ident{key, aggregate: aggregate.clone(), timestamp: record.timestamp};
                let value = #merge_expr;
                arroyo_types::Record {
                    timestamp: record.timestamp,
                    key: None,
                    value
                }
            }
        )
        .to_string();

        let aggregate_operator = Operator::ExpressionOperator {
            name: "aggregation".to_string(),
            expression,
            return_type: arroyo_datastream::ExpressionReturnType::Record,
        };

        let aggregate_index = self.add_node("aggregation", aggregate_operator);
        let aggregate_edge = StreamEdge::keyed_edge(
            key_struct.struct_name(),
            aggregate_struct.struct_name(),
            Forward,
        );
        self.graph
            .add_edge(window_index, aggregate_index, aggregate_edge);
        Ok(aggregate_index)
    }

    fn add_join(
        &mut self,
        left_input: &SqlOperator,
        right_input: &SqlOperator,
        join_operator: &JoinOperator,
    ) -> Result<NodeIndex> {
        let left_index = self.add_operator(left_input)?;
        let left_struct = left_input.return_type();
        let left_key = join_operator.left_key.to_syn_expression();
        let key_type = join_operator.left_key.output_struct();

        let left_key_operator = MethodCompiler::key_map_operator("left_join_key", left_key);
        let left_key_index = self.add_node("left_join_key", left_key_operator);
        let left_key_edge = StreamEdge::unkeyed_edge(left_struct.struct_name(), Forward);
        self.graph
            .add_edge(left_index, left_key_index, left_key_edge);

        let right_index = self.add_operator(right_input)?;
        let right_struct = right_input.return_type();
        let right_key = join_operator.right_key.to_syn_expression();

        let right_key_operator = MethodCompiler::key_map_operator("right_join_key", right_key);
        let right_key_index = self.add_node("right_join_key", right_key_operator);
        let right_key_edge = StreamEdge::unkeyed_edge(right_struct.struct_name(), Forward);
        self.graph
            .add_edge(right_index, right_key_index, right_key_edge);

        let window_operator = Operator::WindowJoin {
            window: WindowType::Instant,
        };
        let window_index = self.add_node("join_window", window_operator);

        let left_window_edge = StreamEdge::keyed_edge(
            key_type.struct_name(),
            left_struct.struct_name(),
            ShuffleJoin(0),
        );
        self.graph
            .add_edge(left_key_index, window_index, left_window_edge);

        let right_window_edge = StreamEdge::keyed_edge(
            key_type.struct_name(),
            right_struct.struct_name(),
            ShuffleJoin(1),
        );
        self.graph
            .add_edge(right_key_index, window_index, right_window_edge);

        let merge_struct = join_operator.join_struct_type(&left_struct, &right_struct);
        let merge = join_operator.merge_syn_expression(&left_struct, &right_struct);
        let merge_operator = MethodCompiler::join_merge_operator(
            "join_merge",
            join_operator.join_type.clone(),
            merge_struct.get_type(),
            merge,
        )?;
        let merge_index = self.add_node("join_merge", merge_operator);
        let merge_edge = StreamEdge::keyed_edge(
            key_type.struct_name(),
            format!(
                "(Vec<{}>,Vec<{}>)",
                left_struct.struct_name(),
                right_struct.struct_name()
            ),
            Forward,
        );
        self.graph.add_edge(window_index, merge_index, merge_edge);

        let flatten_index = self.add_node(
            "join_flatten",
            Operator::FlattenOperator {
                name: "join_flatten".to_string(),
            },
        );
        let flatten_edge = StreamEdge::unkeyed_edge(
            format!(
                "Vec<{}>",
                join_operator
                    .output_struct(&left_struct, &right_struct)
                    .struct_name()
            ),
            Forward,
        );
        self.graph
            .add_edge(merge_index, flatten_index, flatten_edge);
        Ok(flatten_index)
    }

    fn add_window_aggregate_top_n(
        &mut self,
        input: &SqlOperator,
        aggregate_operator: &AggregateOperator,
        projection: &Projection,
        window: &SqlWindowOperator,
        return_struct: &StructDef,
    ) -> Result<NodeIndex> {
        let input_index = self.add_operator(input)?;
        let input_struct = input.return_type();

        let key_method = aggregate_operator.key.to_syn_expression();
        let key_struct = aggregate_operator.key.output_struct();

        let AggregatingStrategy::TwoPhaseAggregateProjection(two_phase_aggregation) = &aggregate_operator.aggregating else {
            bail!("haven't implemented this case")
        };

        let bin_merger = two_phase_aggregation.bin_merger_syn_expression();
        let bin_type = two_phase_aggregation.bin_type();

        let bin_map_closure = quote!({
                let current_bin: Option<#bin_type> = None;
                #bin_merger
        })
        .to_string();

        let pre_operator = MethodCompiler::key_and_value_map_operator(
            "pre-aggregator",
            key_method,
            parse_str(&bin_map_closure).unwrap(),
        );

        let pre_operator_index = self.add_node("pre-aggregator", pre_operator);

        let partition_function = window.partition.to_syn_expression();
        let key_edge = StreamEdge::unkeyed_edge(input_struct.struct_name(), Forward);

        self.graph
            .add_edge(input_index, pre_operator_index, key_edge);

        let (width, slide) = match aggregate_operator.window {
            WindowType::Tumbling { width } => (width, width),
            WindowType::Sliding { width, slide } => (width, slide),
            WindowType::Instant => (Duration::ZERO, Duration::ZERO),
        };
        let bin_merger = two_phase_aggregation.combine_bin_syn_expr();

        let local_tumble_aggregate = Operator::TumblingWindowAggregator(TumblingWindowAggregator {
            width: slide.clone(),
            aggregator: quote!(|arg| { arg.clone() }).to_string(),
            bin_merger: quote!(|arg, current_bin| {#bin_merger}).to_string(),
            bin_type: quote!(#bin_type).to_string(),
        });

        let local_tumble_index = self.add_node("local_tumble", local_tumble_aggregate);

        self.graph.add_edge(
            pre_operator_index,
            local_tumble_index,
            StreamEdge::keyed_edge(
                key_struct.struct_name(),
                quote!(#bin_type).to_string(),
                Forward,
            ),
        );

        let sort_tokens: Vec<_> = window
            .order_by
            .iter()
            .map(|sort_expression| sort_expression.to_syn_expr())
            .collect();
        let aggregate_expr = two_phase_aggregation.sliding_aggregation_syn_expression();

        let bin_merger = two_phase_aggregation.combine_bin_syn_expr();
        let in_memory_add = two_phase_aggregation.memory_add_syn_expression();
        let in_memory_remove = two_phase_aggregation.memory_remove_syn_expression();
        let mem_type = two_phase_aggregation.memory_type();
        let sort_types: Vec<_> = window
            .order_by
            .iter()
            .map(|sort_expression| sort_expression.tuple_type())
            .collect();
        let sort_key_type = quote!(#((#sort_types,))*).to_string();

        let aggregate_struct = aggregate_operator.aggregating.output_struct();
        let merge_struct = SqlOperator::merge_struct_type(&key_struct, &aggregate_struct);

        let merge_struct_ident = merge_struct.get_type();
        let merge_expr = aggregate_operator.merge.to_syn_expression(
            &aggregate_operator.key,
            aggregate_operator.aggregating.output_struct(),
        );
        let projection_expr = projection.to_syn_expression();
        let aggregator = quote!(|timestamp, key, aggregate_value|
            {
                let key = key.clone();
                let arg = #merge_struct_ident{key, aggregate: {let arg = aggregate_value; #aggregate_expr}, timestamp};
                let arg = #merge_expr;
                #projection_expr
            }
        )
        .to_string();

        let aggregate_projection_expr =
            projection.to_truncated_syn_expression(aggregate_struct.fields.len());

        let extractor = quote!(
            |key, arg| {
                let arg = &#aggregate_expr;
                let arg = #aggregate_projection_expr;
                #((#sort_tokens,))*
            }
        )
        .to_string();
        let window_operator =
            arroyo_datastream::Operator::SlidingAggregatingTopN(SlidingAggregatingTopN {
                width,
                slide,
                bin_merger: quote!(|arg, current_bin| {#bin_merger}).to_string(),
                in_memory_add: quote!(|current, bin_value| {#in_memory_add}).to_string(),
                in_memory_remove: quote!(|current, bin_value| {#in_memory_remove}).to_string(),
                partitioning_func: quote!(|arg| {#partition_function}).to_string(),
                extractor,
                aggregator,
                bin_type: quote!(#bin_type).to_string(),
                mem_type: quote!(#mem_type).to_string(),
                sort_key_type: sort_key_type.clone(),
                max_elements: window.max_value.unwrap() as usize,
            });

        let window_index = self.add_node("sliding_aggregating_top_n_window", window_operator);
        let window_edge = StreamEdge {
            key: key_struct.struct_name(),
            value: quote!(#bin_type).to_string(),
            typ: Shuffle,
        };

        self.graph
            .add_edge(local_tumble_index, window_index, window_edge);

        let window_field = return_struct.fields.last().unwrap().field_ident();
        let output_struct = format_ident!("{}", return_struct.struct_name());
        let mut field_assignments: Vec<_> = projection
            .output_struct()
            .fields
            .iter()
            .map(|f| {
                let ident = f.field_ident();
                quote! { #ident: arg.#ident.clone() }
            })
            .collect();

        match window.window_fn {
            WindowFunction::RowNumber => {
                field_assignments.push(quote! {
                    #window_field: i as i64
                });
            }
        }
        let output_expression = quote!(#output_struct {
            #(#field_assignments, )*
        });

        let extractor = quote!(
            |arg| {
            #((#sort_tokens,))*
            }
        )
        .to_string();
        let converter = quote!(
            |arg, i| #output_expression
        )
        .to_string();

        let final_operator = arroyo_datastream::Operator::TumblingTopN(TumblingTopN {
            width: Duration::ZERO,
            max_elements: window.max_value.unwrap() as usize,
            extractor,
            partition_key_type: sort_key_type,
            converter,
        });

        let top_n_edge = StreamEdge {
            key: window.partition.output_struct().struct_name(),
            value: projection.output_struct().struct_name(),
            typ: Shuffle,
        };

        let aggregate_index = self.add_node("final_top_n", final_operator);
        self.graph
            .add_edge(window_index, aggregate_index, top_n_edge);

        // unkey
        let unkey_operator = arroyo_datastream::Operator::ExpressionOperator {
            name: "unkey".to_string(),
            expression: quote! {
                arroyo_types::Record {
                    timestamp: record.timestamp,
                    key: None,
                    value: record.value.clone(),
                }
            }
            .to_string(),
            return_type: arroyo_datastream::ExpressionReturnType::Record,
        };

        let unkey_index = self.add_node("unkey", unkey_operator);
        self.graph.add_edge(
            aggregate_index,
            unkey_index,
            StreamEdge::keyed_edge(
                window.partition.output_struct().struct_name(),
                return_struct.struct_name(),
                Forward,
            ),
        );
        Ok(unkey_index)
    }

    fn add_record_transform(
        &mut self,
        input_index: NodeIndex,
        input_struct: StructDef,
        record_transform: &RecordTransform,
    ) -> Result<NodeIndex> {
        match record_transform {
            RecordTransform::ValueProjection(projection) => {
                self.add_map(input_index, input_struct, projection)
            }
            RecordTransform::KeyProjection(_) => {
                unimplemented!("still need a key projection strategy")
            }
            RecordTransform::Filter(filter) => self.add_filter(input_index, input_struct, filter),
            RecordTransform::Sequence(sequence) => {
                let (index, _output_struct) = sequence.iter().fold(
                    Ok((input_index, input_struct)),
                    |result_pair: Result<_>, record_transform| {
                        let (input_index, input_struct) = result_pair?;
                        let output_struct = record_transform.output_struct(input_struct.clone());
                        Ok((
                            self.add_record_transform(input_index, input_struct, record_transform)?,
                            output_struct,
                        ))
                    },
                )?;
                Ok(index)
            }
        }
    }
}
