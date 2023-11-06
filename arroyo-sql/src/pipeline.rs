#![allow(clippy::comparison_chain)]
use std::collections::HashMap;
use std::iter::once;

use std::time::Duration;
use std::unreachable;

use anyhow::{anyhow, bail};
use anyhow::{Ok, Result};
use arrow_schema::DataType;
use arroyo_datastream::{Operator, WindowType};
use datafusion_common::{DFField, ScalarValue};
use datafusion_expr::expr::ScalarUDF;
use datafusion_expr::{
    BinaryExpr, BuiltInWindowFunction, Expr, JoinConstraint, LogicalPlan, Window, WriteOp,
};

use quote::quote;

use crate::code_gen::{CodeGenerator, ValuePointerContext, VecAggregationContext};
use crate::expressions::{AggregateComputation, AggregateResultExtraction, ExpressionContext};
use crate::external::{ProcessingMode, SqlSink, SqlSource};
use crate::operators::{UnnestFieldType, UnnestProjection};
use crate::schemas::window_type_def;
use crate::tables::{Insert, Table};
use crate::{
    expressions::{Column, ColumnExpression, Expression, SortExpression},
    operators::{AggregateProjection, Projection},
    types::{interval_month_day_nanos_to_duration, StructDef, StructField, TypeDef},
    ArroyoSchemaProvider,
};

#[derive(Debug, Clone)]
pub enum SqlOperator {
    Source(SourceOperator),
    Aggregator(Box<SqlOperator>, AggregateOperator),
    JoinOperator(Box<SqlOperator>, Box<SqlOperator>, JoinOperator),
    Window(Box<SqlOperator>, SqlWindowOperator),
    RecordTransform(Box<SqlOperator>, RecordTransform),
    Union(Vec<SqlOperator>),
    Sink(String, SqlSink, Box<SqlOperator>),
    NamedTable(String, Box<SqlOperator>),
}

#[derive(Debug, Clone)]
pub enum RecordTransform {
    ValueProjection(Projection),
    KeyProjection(Projection),
    UnnestProjection(UnnestProjection),
    TimestampAssignment(Expression),
    Filter(Expression),
}

#[derive(Debug, Clone)]
pub struct SourceOperator {
    pub name: String,
    pub source: SqlSource,
    pub virtual_field_projection: Option<Projection>,
    pub timestamp_override: Option<Expression>,
    pub watermark_column: Option<Expression>,
}

impl SourceOperator {
    fn return_type(&self) -> StructDef {
        if let Some(ref projection) = self.virtual_field_projection {
            projection.expression_type(&ValuePointerContext::new())
        } else {
            self.source.struct_def.clone()
        }
    }
}

impl RecordTransform {
    pub fn expressions(&mut self) -> impl Iterator<Item = &mut Expression> {
        match self {
            RecordTransform::ValueProjection(projection) => {
                Box::new(projection.expressions()) as Box<dyn Iterator<Item = &mut Expression>>
            }
            RecordTransform::KeyProjection(projection) => {
                Box::new(projection.expressions()) as Box<dyn Iterator<Item = &mut Expression>>
            }
            RecordTransform::UnnestProjection(projection) => Box::new(
                projection
                    .fields
                    .iter_mut()
                    .map(|(_, expression, _)| expression)
                    .chain(once(&mut projection.unnest_inner)),
            )
                as Box<dyn Iterator<Item = &mut Expression>>,
            RecordTransform::TimestampAssignment(expression) => {
                Box::new(once(expression)) as Box<dyn Iterator<Item = &mut Expression>>
            }
            RecordTransform::Filter(expression) => {
                Box::new(once(expression)) as Box<dyn Iterator<Item = &mut Expression>>
            }
        }
    }

    pub fn output_struct(&self, input_struct: StructDef) -> StructDef {
        match self {
            RecordTransform::ValueProjection(projection) => {
                projection.expression_type(&ValuePointerContext::new())
            }
            RecordTransform::KeyProjection(_) | RecordTransform::Filter(_) => input_struct,
            RecordTransform::TimestampAssignment(_) => input_struct,
            RecordTransform::UnnestProjection(projection) => {
                projection.expression_type(&ValuePointerContext::new())
            }
        }
    }

    pub fn as_operator(&self, is_updating: bool) -> Operator {
        match self {
            RecordTransform::ValueProjection(projection) => {
                if is_updating {
                    let updating_record_expression = ValuePointerContext::new()
                        .compile_updating_value_map_expression(projection);
                    MethodCompiler::optional_record_expression_operator(
                        "updating_value_map",
                        updating_record_expression,
                    )
                } else {
                    let record_expression =
                        ValuePointerContext::new().compile_value_map_expr(projection);
                    MethodCompiler::record_expression_operator("value_map", record_expression)
                }
            }
            RecordTransform::KeyProjection(projection) => {
                if is_updating {
                    let key_closure =
                        ValuePointerContext::new().compile_updating_key_map_closure(projection);
                    MethodCompiler::updating_key_operator("updating_key_map", key_closure)
                } else {
                    let record_expression =
                        ValuePointerContext::new().compile_key_map_expression(projection);
                    MethodCompiler::record_expression_operator("key_map", record_expression)
                }
            }
            RecordTransform::Filter(expression) => {
                if is_updating {
                    let updating_filter_optional_record_expr = ValuePointerContext::new()
                        .compile_updating_filter_optional_record_expression(expression);
                    MethodCompiler::optional_record_expression_operator(
                        "updating_filter",
                        updating_filter_optional_record_expr,
                    )
                } else {
                    let filter_expression =
                        ValuePointerContext::new().compile_filter_expression(expression);
                    MethodCompiler::predicate_expression_operator("value_filter", filter_expression)
                }
            }
            RecordTransform::TimestampAssignment(timestamp_expression) => {
                let timestamp_record_expression = ValuePointerContext::new()
                    .compile_timestamp_record_expression(timestamp_expression);

                MethodCompiler::record_expression_operator(
                    "timestamp_assigner",
                    timestamp_record_expression,
                )
            }
            RecordTransform::UnnestProjection(p) => {
                if is_updating {
                    unreachable!("unnest is not supported for updating data");
                }

                let record_expression = ValuePointerContext::new().compile_flatmap_expr(p);
                MethodCompiler::flatmap_operator("flatmap", record_expression)
            }
        }
    }

    pub fn name(&self) -> String {
        match self {
            RecordTransform::ValueProjection(_) => "value_project".into(),
            RecordTransform::KeyProjection(_) => "key_project".into(),
            RecordTransform::Filter(_) => "filter".into(),
            RecordTransform::TimestampAssignment(_) => "timestamp".into(),
            RecordTransform::UnnestProjection(_) => "unnest_project".into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AggregateOperator {
    pub key: Projection,
    pub window: WindowType,
    pub aggregating: AggregateProjection,
}

impl AggregateOperator {
    pub fn output_struct(&self) -> StructDef {
        self.aggregating
            .expression_type(&VecAggregationContext::new())
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum WindowFunction {
    RowNumber,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct SqlWindowOperator {
    pub window_fn: WindowFunction,
    pub partition: Projection,
    pub order_by: Vec<SortExpression>,
    pub field_name: String,
    pub window_type: WindowType,
}

#[derive(Debug, Clone)]
pub struct JoinOperator {
    pub left_key: Projection,
    pub right_key: Projection,
    pub join_type: JoinType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

impl From<JoinType> for arroyo_types::JoinType {
    fn from(value: JoinType) -> Self {
        match value {
            JoinType::Inner => arroyo_types::JoinType::Inner,
            JoinType::Left => arroyo_types::JoinType::Left,
            JoinType::Right => arroyo_types::JoinType::Right,
            JoinType::Full => arroyo_types::JoinType::Full,
        }
    }
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

impl JoinType {
    pub fn output_struct(&self, left_struct: &StructDef, right_struct: &StructDef) -> StructDef {
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
        StructDef::for_fields(fields)
    }

    pub fn join_struct_type(&self, left_struct: &StructDef, right_struct: &StructDef) -> StructDef {
        StructDef::for_fields(vec![
            StructField::new(
                "left".to_string(),
                None,
                TypeDef::StructDef(left_struct.clone(), self.left_nullable()),
            ),
            StructField::new(
                "right".to_string(),
                None,
                TypeDef::StructDef(right_struct.clone(), self.right_nullable()),
            ),
        ])
    }

    pub fn left_nullable(&self) -> bool {
        match self {
            JoinType::Inner | JoinType::Left => false,
            JoinType::Right | JoinType::Full => true,
        }
    }
    pub fn right_nullable(&self) -> bool {
        match self {
            JoinType::Inner | JoinType::Right => false,
            JoinType::Left | JoinType::Full => true,
        }
    }
}

impl SqlOperator {
    pub fn return_type(&self) -> StructDef {
        match self {
            SqlOperator::Source(source_operator) => source_operator.return_type(),
            SqlOperator::Aggregator(_input, aggregate_operator) => {
                aggregate_operator.output_struct()
            }
            SqlOperator::JoinOperator(left, right, operator) => operator
                .join_type
                .output_struct(&left.return_type(), &right.return_type()),
            SqlOperator::Window(input, window) => {
                let mut input_struct = input.return_type();
                input_struct.fields.push(StructField::new(
                    window.field_name.clone(),
                    None,
                    TypeDef::DataType(DataType::UInt64, false),
                ));
                input_struct
            }
            SqlOperator::RecordTransform(input, record_transform) => {
                record_transform.output_struct(input.return_type())
            }
            SqlOperator::Sink(_, sql_sink, _) => sql_sink.struct_def.clone(),
            SqlOperator::NamedTable(_table_name, table) => table.return_type(),
            SqlOperator::Union(inputs) => inputs[0].return_type(),
        }
    }

    pub fn has_window(&self) -> bool {
        match self {
            SqlOperator::Source(_) => false,
            SqlOperator::Aggregator(input, aggregator) => {
                !matches!(aggregator.window, WindowType::Instant) || input.has_window()
            }
            SqlOperator::JoinOperator(left, right, _) => left.has_window() || right.has_window(),
            SqlOperator::Window(_, _) => true,
            SqlOperator::RecordTransform(input, _) => input.has_window(),
            SqlOperator::Sink(_, _, input) => input.has_window(),
            SqlOperator::NamedTable(_, input) => input.has_window(),
            SqlOperator::Union(inputs) => inputs[0].has_window(),
        }
    }

    pub fn is_updating(&self) -> bool {
        match self {
            SqlOperator::Source(source) => source.source.processing_mode == ProcessingMode::Update,
            SqlOperator::Aggregator(input, aggregate_operator) => {
                input.is_updating()
                    || (!input.has_window() && aggregate_operator.window == WindowType::Instant
                        // non-windowed aggregates without aggregate functions are not updating, as
                        // they cannot have retractions
                        && !aggregate_operator.aggregating.aggregates.is_empty())
            }
            SqlOperator::JoinOperator(left, right, join_operator) => {
                // the join will be updating if one of the sides is updating or if a non-window side is nullable.
                left.is_updating()
                    || right.is_updating()
                    || (!left.has_window() && join_operator.join_type.left_nullable())
                    || (!right.has_window() && join_operator.join_type.right_nullable())
            }
            SqlOperator::Window(input, sql_window_operator) => {
                input.is_updating() // TODO: figure out when this second case is supposed to be triggered.
                    || (!input.has_window() && sql_window_operator.window_type == WindowType::Instant)
            }
            SqlOperator::RecordTransform(input, _) => input.is_updating(),
            SqlOperator::Sink(_, _, input) => input.is_updating(),
            SqlOperator::NamedTable(_, table_operator) => table_operator.is_updating(),
            SqlOperator::Union(inputs) => inputs[0].is_updating(),
        }
    }

    pub(crate) fn get_window(&self) -> Option<WindowType> {
        match self {
            SqlOperator::Source(_) => None,
            SqlOperator::Aggregator(input, aggregator) => match &aggregator.window {
                WindowType::Tumbling { .. }
                | WindowType::Sliding { .. }
                | WindowType::Session { .. } => Some(aggregator.window.clone()),
                WindowType::Instant => input.get_window(),
            },
            SqlOperator::JoinOperator(left, _, _) => left.get_window(),
            SqlOperator::Window(_, sql_window_operator) => {
                Some(sql_window_operator.window_type.clone())
            }
            SqlOperator::RecordTransform(input, _) => input.get_window(),
            SqlOperator::Sink(_, _, input) => input.get_window(),
            SqlOperator::NamedTable(_, input) => input.get_window(),
            SqlOperator::Union(inputs) => inputs[0].get_window(),
        }
    }
}

#[derive(Debug)]
pub struct SqlPipelineBuilder<'a> {
    pub schema_provider: &'a ArroyoSchemaProvider,
    pub planned_tables: HashMap<String, SqlOperator>,
    pub insert_nodes: Vec<SqlOperator>,
}

impl<'a> SqlPipelineBuilder<'a> {
    pub fn new(schema_provider: &'a ArroyoSchemaProvider) -> Self {
        SqlPipelineBuilder {
            schema_provider,
            planned_tables: HashMap::new(),
            insert_nodes: vec![],
        }
    }

    fn ctx(&'a self, input_struct: &'a StructDef) -> ExpressionContext<'a> {
        ExpressionContext {
            schema_provider: self.schema_provider,
            input_struct,
        }
    }

    pub fn insert_sql_plan(&mut self, plan: &LogicalPlan) -> Result<SqlOperator> {
        match plan {
            LogicalPlan::Projection(projection) => self.insert_projection(projection),
            LogicalPlan::Filter(filter) => self.insert_filter(filter),
            LogicalPlan::Aggregate(aggregate) => self.insert_aggregation(aggregate),
            LogicalPlan::Sort(_) => bail!("sorting is not currently supported"),
            LogicalPlan::Join(join) => self.insert_join(join),
            LogicalPlan::CrossJoin(_) => bail!("cross joins are not currently supported"),
            LogicalPlan::Repartition(_) => bail!("repartitions are not currently supported"),
            LogicalPlan::Union(union) => self.insert_union(union),
            LogicalPlan::TableScan(table_scan) => self.insert_table_scan(table_scan),
            LogicalPlan::EmptyRelation(_) => bail!("empty relations not currently supported"),
            LogicalPlan::Subquery(subquery) => self.insert_sql_plan(&subquery.subquery),
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                self.insert_subquery_alias(subquery_alias)
            }
            LogicalPlan::Limit(_) => bail!("limit not currently supported"),
            LogicalPlan::Ddl(ddl_statement) => match ddl_statement {
                datafusion_expr::DdlStatement::CreateExternalTable(_) => {
                    bail!("creating external tables is not currently supported")
                }
                datafusion_expr::DdlStatement::CreateMemoryTable(create_memory_table) => {
                    bail!(
                        "creating memory tables is not currently supported: {:?}",
                        create_memory_table.input,
                    )
                }
                datafusion_expr::DdlStatement::CreateView(_) => {
                    bail!("creating views is not currently supported")
                }
                datafusion_expr::DdlStatement::CreateCatalogSchema(_) => {
                    bail!("creating catalog schemas is not currently supported")
                }
                datafusion_expr::DdlStatement::CreateCatalog(_) => {
                    bail!("creating catalogs is not currently supported")
                }
                datafusion_expr::DdlStatement::DropTable(_) => {
                    bail!("dropping tables is not currently supported")
                }
                datafusion_expr::DdlStatement::DropView(_) => {
                    bail!("dropping views is not currently supported")
                }
                datafusion_expr::DdlStatement::DropCatalogSchema(_) => {
                    bail!("dropping catalog schemas is not currently supported")
                }
            },
            LogicalPlan::Values(_) => bail!("values are not currently supported"),
            LogicalPlan::Explain(_) => bail!("explain is not currently supported"),
            LogicalPlan::Analyze(_) => bail!("analyze is not currently supported"),
            LogicalPlan::Extension(_) => bail!("extensions are not currently supported"),
            LogicalPlan::Distinct(_) => bail!("distinct is not currently supported"),
            LogicalPlan::Window(window) => self.insert_window(window),
            LogicalPlan::Prepare(_) => bail!("prepare commands are not currently supported"),
            LogicalPlan::Dml(dml) => self.insert_dml(dml),
            LogicalPlan::DescribeTable(_) => bail!("describe table not currently supported"),
            LogicalPlan::Unnest(_) => bail!("unnest not currently supported"),
            LogicalPlan::Statement(_) => bail!("statements not currently supported"),
            LogicalPlan::Copy(_) => bail!("copy not currently supported"),
        }
    }

    fn insert_dml(
        &mut self,
        dml_statement: &datafusion_expr::logical_plan::DmlStatement,
    ) -> Result<SqlOperator> {
        if !matches!(dml_statement.op, WriteOp::InsertInto) {
            bail!("only insert statements are currently supported")
        }
        let input = self.insert_sql_plan(&dml_statement.input)?;
        self.schema_provider
            .get_table(&dml_statement.table_name.to_string())
            .ok_or_else(|| {
                anyhow!(
                    "table {} not found in schema provider",
                    dml_statement.table_name
                )
            })?
            .as_sql_sink(input)
    }

    fn insert_filter(
        &mut self,
        filter: &datafusion_expr::logical_plan::Filter,
    ) -> Result<SqlOperator> {
        let input = self.insert_sql_plan(&filter.input)?;
        let struct_def = input.return_type();
        let ctx = self.ctx(&struct_def);
        let mut predicate = ctx.compile_expr(&filter.predicate)?;

        Self::assert_no_unnest("where", &mut predicate)?;

        Ok(SqlOperator::RecordTransform(
            Box::new(input),
            RecordTransform::Filter(predicate),
        ))
    }

    fn split_unnest(expr: &mut Expression) -> Result<Option<Expression>> {
        let mut c: Option<Result<Expression>> = None;

        expr.traverse_mut(&mut c, &|ctx, e| match e {
            Expression::Unnest(expr, taken) => {
                if *taken {
                    ctx.replace(Err(anyhow!(
                        "expression contains multiple unnests, which is not currently supported"
                    )));
                } else {
                    *taken = true;
                    ctx.replace(Ok(*expr.clone()));
                }
            }
            _ => {}
        });

        c.transpose()
    }

    fn assert_no_unnest(ctx: &str, expr: &Expression) -> Result<()> {
        let mut found = false;
        let mut expr = expr.clone();
        expr.traverse_mut(&mut found, &|ctx, e| {
            *ctx = *ctx || matches!(e, Expression::Unnest(_, _));
        });

        if found {
            bail!("{} may not include unnest", ctx);
        } else {
            Ok(())
        }
    }

    fn insert_projection(
        &mut self,
        projection: &datafusion_expr::logical_plan::Projection,
    ) -> Result<SqlOperator> {
        let input = self.insert_sql_plan(&projection.input)?;

        let struct_def = input.return_type();
        let ctx = self.ctx(&struct_def);

        let functions = projection
            .expr
            .iter()
            .map(|expr| ctx.compile_expr(expr))
            .collect::<Result<Vec<_>>>()?;

        let names = projection
            .schema
            .fields()
            .iter()
            .map(|field| Column::convert(&field.qualified_column()));

        let fields: Vec<_> = names.zip(functions).collect();

        let mut unnest = None;
        let fields = fields
            .into_iter()
            .map(|(col, mut expr)| {
                let typ = if let Some(e) = Self::split_unnest(&mut expr)? {
                    if let Some(prev) = unnest.replace(e) {
                        if &prev != unnest.as_ref().unwrap() {
                            bail!("multiple unnested values, which is not currently supported");
                        }
                    }
                    UnnestFieldType::UnnestOuter
                } else {
                    UnnestFieldType::Default
                };

                Ok((col, expr, typ))
            })
            .collect::<Result<Vec<_>>>()?;

        if let Some(unnest_inner) = unnest {
            Ok(SqlOperator::RecordTransform(
                Box::new(input),
                RecordTransform::UnnestProjection(UnnestProjection {
                    fields,
                    unnest_inner,
                    format: None,
                }),
            ))
        } else {
            let projection = Projection::new(fields.into_iter().map(|(a, b, _)| (a, b)).collect());

            Ok(SqlOperator::RecordTransform(
                Box::new(input),
                RecordTransform::ValueProjection(projection),
            ))
        }
    }

    fn insert_aggregation(
        &mut self,
        aggregate: &datafusion_expr::logical_plan::Aggregate,
    ) -> Result<SqlOperator> {
        let source = self.insert_sql_plan(&aggregate.input)?;
        /*if source.is_updating() {
            bail!("can't aggregate over updating inputs");
        }*/
        let key = self.aggregation_key(
            &aggregate.group_expr,
            aggregate.schema.fields(),
            &source.return_type(),
        )?;

        let window = self.window(&aggregate.group_expr)?;

        let source_return_type = source.return_type();
        let mut ctx = self.ctx(&source_return_type);

        let group_bys = aggregate
            .group_expr
            .iter()
            .zip(aggregate.schema.fields().iter())
            .map(|(expr, field)| {
                let column = Column::convert(&field.qualified_column());

                let expression = ctx.compile_expr(expr)?;
                Self::assert_no_unnest("group by", &expression)?;

                let (data_type, extraction) = if let Some(window) = Self::find_window(expr)? {
                    if let WindowType::Instant = window {
                        bail!("don't support instant window in return type yet");
                    }
                    (window_type_def(), AggregateResultExtraction::WindowTake)
                } else {
                    let data_type = expression.expression_type(&ValuePointerContext::new());
                    (data_type, AggregateResultExtraction::KeyColumn)
                };
                if let TypeDef::DataType(DataType::Struct(_), _) = &data_type {
                    bail!("structs should be struct-defs {:?}", expr);
                }
                Ok((
                    StructField::new(column.name, column.relation, data_type),
                    extraction,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let aggregates = aggregate
            .schema
            .fields()
            .iter()
            .skip(aggregate.group_expr.len()) // the group bys always come first
            .zip(aggregate.aggr_expr.iter())
            .map(|(field, expr)| {
                AggregateComputation::try_from_expression(&mut ctx, &field.qualified_column(), expr)
            })
            .collect::<Result<Vec<_>>>()?;

        let aggregating = AggregateProjection {
            aggregates,
            group_bys,
        };

        if !source.has_window()
            && matches!(window, WindowType::Instant)
            && !aggregating.supports_two_phase()
        {
            bail!("updating aggregates only support two phase aggregations. Currently count distinct is not supported");
        }

        Ok(SqlOperator::Aggregator(
            Box::new(source),
            AggregateOperator {
                key,
                window,
                aggregating,
            },
        ))
    }

    fn aggregation_key(
        &mut self,
        group_expressions: &[Expr],
        fields: &[DFField],
        input_struct: &StructDef,
    ) -> Result<Projection> {
        let ctx = self.ctx(input_struct);

        let field_pairs: Vec<Option<(Column, Expression)>> = group_expressions
            .iter()
            .enumerate()
            .map(|(i, expr)| {
                if Self::is_window(expr) {
                    Ok(None)
                } else {
                    let field = ctx.compile_expr(expr)?;
                    Ok(Some((
                        Column::convert(&fields[i].qualified_column()),
                        field,
                    )))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let field_pairs: Vec<_> = field_pairs.into_iter().flatten().collect();
        let projection = Projection::new(field_pairs);

        Ok(projection)
    }

    fn window(&mut self, group_expressions: &[Expr]) -> Result<WindowType> {
        let mut windows: Vec<_> = Vec::new();
        for expression in group_expressions {
            if let Some(window) = Self::find_window(expression)? {
                windows.push(window);
            }
        }
        match windows.len() {
            0 => Ok(WindowType::Instant),
            1 => Ok(windows[0].clone()),
            multiple => bail!("{} windows detected, must be one or zero.", multiple),
        }
    }

    fn is_window(expression: &Expr) -> bool {
        match expression {
            Expr::ScalarUDF(ScalarUDF { fun, args: _ }) => {
                matches!(fun.name.as_str(), "hop" | "tumble" | "session")
            }
            Expr::Alias(datafusion_expr::expr::Alias { expr, name: _ }) => Self::is_window(expr),
            _ => false,
        }
    }

    fn find_window(expression: &Expr) -> Result<Option<WindowType>> {
        match expression {
            Expr::ScalarUDF(ScalarUDF { fun, args }) => match fun.name.as_str() {
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
                "session" => {
                    if args.len() != 1 {
                        unreachable!("wrong number of arguments for session(), expected one");
                    }
                    let gap = Self::get_duration(&args[0])?;
                    Ok(Some(WindowType::Session { gap }))
                }
                _ => Ok(None),
            },
            Expr::Alias(datafusion_expr::expr::Alias { expr, name: _ }) => Self::find_window(expr),
            _ => Ok(None),
        }
    }
    fn get_duration(expression: &Expr) -> Result<Duration> {
        match expression {
            Expr::Literal(ScalarValue::IntervalDayTime(Some(val))) => {
                Ok(Duration::from_millis(*val as u64))
            }
            Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(val))) => {
                Ok(interval_month_day_nanos_to_duration(*val))
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
        if left_input.is_updating() || right_input.is_updating() {
            bail!("don't support joins with updating inputs");
        }
        match join.join_constraint {
            JoinConstraint::On => {}
            JoinConstraint::Using => bail!("don't support 'using' in joins"),
        };
        let join_type = join.join_type.try_into()?;
        // check supported join types
        match (left_input.has_window(), right_input.has_window()) {
            (true, false) | (false, true) => {
                bail!("windowing join mismatch. both sides must either have or not have windows")
            }
            _ => {}
        }
        let mut join_pairs = join.on.clone();
        if let Some(Expr::BinaryExpr(BinaryExpr { left, op, right })) = &join.filter {
            if *op != datafusion_expr::Operator::Eq {
                bail!("only equality joins are supported");
            }
            // check which side each column comes from. Assumes there's at least one field
            let left_relation = join
                .schema
                .fields()
                .first()
                .unwrap()
                .qualifier()
                .as_ref()
                .unwrap()
                .to_string();
            let right_relation = join
                .schema
                .fields()
                .last()
                .unwrap()
                .qualifier()
                .as_ref()
                .unwrap()
                .to_string();
            let left_table = Column::convert_expr(left)?.relation.unwrap();
            let right_table = Column::convert_expr(right)?.relation.unwrap();
            let pair = if right_table == right_relation && left_table == left_relation {
                (left.as_ref().clone(), right.as_ref().clone())
            } else if left_table == right_relation && right_table == left_relation {
                ((right.as_ref()).clone(), left.as_ref().clone())
            } else {
                bail!("join filter must contain at least one column from each side of the join");
            };
            join_pairs.push(pair);
        } else if join.filter.is_some() {
            bail!(
                "only equality joins are supported, not filter {:?}",
                join.filter
            );
        }

        let join_projection_field_names: Vec<_> = join_pairs
            .iter()
            .map(|(left, _right)| Column::convert_expr(left))
            .collect::<Result<Vec<_>>>()?;
        let (left_computations, right_computations): (Vec<_>, Vec<_>) = join
            .on
            .iter()
            .map(|(left, right)| {
                Ok((
                    self.ctx(&left_input.return_type()).compile_expr(left)?,
                    self.ctx(&right_input.return_type()).compile_expr(right)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip();

        left_computations
            .iter()
            .chain(right_computations.iter())
            .map(|e| Self::assert_no_unnest("join", e))
            .collect::<Result<Vec<()>>>()?;

        let left_key = Projection::new(
            join_projection_field_names
                .clone()
                .into_iter()
                .zip(left_computations)
                .collect(),
        );

        let right_key = Projection::new(
            join_projection_field_names
                .into_iter()
                .zip(right_computations)
                .collect(),
        );

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
        let table_name = table_scan.table_name.to_string();
        let source = self
            .schema_provider
            .get_table(&table_name)
            .ok_or_else(|| anyhow!("table {} not found", table_scan.table_name))?;
        let source = source
            .as_sql_source(self)
            .map_err(|e| anyhow!("failed to plan {}: {}", table_scan.table_name, e))?;

        if let Some(projection) = table_scan.projection.as_ref() {
            let fields = projection
                .iter()
                .map(|i| source.return_type().fields[*i].clone())
                .map(|t| {
                    (
                        Column {
                            relation: Some(table_scan.table_name.to_string()),
                            name: t.name.clone(),
                        },
                        Expression::Column(ColumnExpression::new(t.clone())),
                    )
                })
                .collect();

            return Ok(SqlOperator::RecordTransform(
                Box::new(source),
                RecordTransform::ValueProjection(Projection::new(fields)),
            ));
        }

        Ok(source)
    }

    fn insert_window(&mut self, window: &Window) -> Result<SqlOperator> {
        let input = self.insert_sql_plan(&window.input)?;

        if input.is_updating() {
            bail!("don't support window functions over updating inputs");
        }

        if let Some(expr) = window.window_expr.get(0) {
            let w = match expr {
                Expr::Alias(datafusion_expr::expr::Alias { expr, name: _ }) => match **expr {
                    Expr::WindowFunction(ref w) => w,
                    _ => bail!("expected window function"),
                },
                Expr::WindowFunction(window_function) => window_function,
                _ => bail!("expected window function"),
            };
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
                datafusion_expr::WindowFunction::WindowUDF(_) => {
                    bail!("Window UDFs not yet supported");
                }
            };

            let input_struct = input.return_type();
            let mut ctx = self.ctx(&input_struct);

            let order_by: Vec<_> = w
                .order_by
                .iter()
                .map(|expr| {
                    if let Expr::Sort(sort) = expr {
                        SortExpression::from_expression(&mut ctx, sort)
                    } else {
                        panic!("expected sort expression, found {:?}", expr);
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let first_term = w.partition_by.first().ok_or_else(|| {
                anyhow!("window function must have at least one partition expression")
            })?;
            let first_expression = ctx.compile_expr(first_term)?;
            let window_type = first_expression.get_window_type(&input)?.ok_or_else(|| {
                anyhow!("window function must be partitioned by a window as the first argument")
            })?;

            let field_names = w
                .partition_by
                .iter()
                .skip(1)
                .enumerate()
                .map(|(i, _t)| Column {
                    relation: None,
                    name: format!("_{}", i),
                });

            let field_computations = w
                .partition_by
                .iter().skip(1)
                .map(|expression| {
                    let expr = ctx.compile_expr(expression)?;
                    Self::assert_no_unnest("window", &expr)?;
                    if expr.get_window_type(&input)?.is_some() {
                        bail!("window functions can only be partitioned by a window as the first argument");
                    } else {
                        Ok(expr)
                    }
                })
            .collect::<Result<Vec<_>>>()?;

            let partition = Projection::new(field_names.zip(field_computations).collect());
            let field_name = window.schema.field_names().last().cloned().unwrap();

            return Ok(SqlOperator::Window(
                Box::new(input),
                SqlWindowOperator {
                    window_fn,
                    partition,
                    order_by,
                    field_name,
                    window_type,
                },
            ));
        }

        bail!("no expression for window");
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
            .map(|field| Expression::Column(ColumnExpression::new(field.clone())));

        let field_names = subquery_alias
            .schema
            .fields()
            .iter()
            .map(|field| Column::convert(&field.qualified_column()));

        let projection = Projection::new(field_names.zip(field_computations).collect());
        Ok(SqlOperator::RecordTransform(
            Box::new(input),
            RecordTransform::ValueProjection(projection),
        ))
    }

    pub(crate) fn add_insert(&mut self, insert: Insert) -> Result<()> {
        match insert {
            Insert::InsertQuery {
                sink_name,
                logical_plan,
            } => {
                let input = self.insert_sql_plan(&logical_plan)?;
                let sink = self.schema_provider.get_table(&sink_name).ok_or_else(|| {
                    anyhow!("Could not find sink {} in schema provider", sink_name)
                })?;
                match sink {
                    Table::MemoryTable { name, fields } => {
                        if self.planned_tables.contains_key(name) {
                            bail!("can't insert into {} twice", name);
                        }
                        let input_struct = input.return_type();
                        // insert into is done column-wise, and DataFusion will have already coerced all the types.
                        let mapping =
                            RecordTransform::ValueProjection(Projection::new(
                                fields
                                    .iter()
                                    .map(|f| Column {
                                        relation: None,
                                        name: f.name.clone(),
                                    })
                                    .zip(input_struct.fields.iter().map(|f| {
                                        Expression::Column(ColumnExpression::new(f.clone()))
                                    }))
                                    .collect(),
                            ));
                        self.planned_tables.insert(
                            name.clone(),
                            SqlOperator::RecordTransform(Box::new(input), mapping),
                        );
                    }
                    Table::ConnectorTable(c) => {
                        self.insert_nodes.push(
                            c.as_sql_sink(input)
                                .map_err(|e| anyhow!("failed to plan {}: {}", c.name, e))?,
                        );
                    }
                    Table::TableFromQuery {
                        name: _,
                        logical_plan: _,
                    } => todo!(),
                }
            }
            Insert::Anonymous { logical_plan } => {
                let operator = self.insert_sql_plan(&logical_plan)?;
                self.insert_nodes.push(operator);
            }
        }
        Ok(())
    }

    fn insert_union(&mut self, union: &datafusion_expr::Union) -> Result<SqlOperator> {
        let inputs = union
            .inputs
            .iter()
            .map(|input| self.insert_sql_plan(input))
            .collect::<Result<Vec<_>>>()?;
        // check that all inputs have the same schema, updating behavior and windowing behavior
        let first_input = &inputs[0];
        for input in &inputs[1..] {
            if !first_input
                .return_type()
                .field_types_match(&input.return_type())
            {
                bail!("union inputs must have the same schema");
            }
            if input.is_updating() != first_input.is_updating() {
                bail!("union inputs must have the same updating behavior");
            }
            if input.get_window() != first_input.get_window() {
                bail!("union inputs must have the same windowing behavior");
            }
        }
        Ok(SqlOperator::Union(inputs))
    }
}

#[derive(Debug)]
pub struct MethodCompiler {}

impl MethodCompiler {
    pub fn record_expression_operator(name: impl ToString, expression: syn::Expr) -> Operator {
        Operator::ExpressionOperator {
            name: name.to_string(),
            expression: quote!(#expression).to_string(),
            return_type: arroyo_datastream::ExpressionReturnType::Record,
        }
    }

    fn flatmap_operator(name: impl ToString, expression: syn::Expr) -> Operator {
        Operator::FlatMapOperator {
            name: name.to_string(),
            expression: quote!(#expression).to_string(),
        }
    }

    pub fn predicate_expression_operator(name: impl ToString, expression: syn::Expr) -> Operator {
        Operator::ExpressionOperator {
            name: name.to_string(),
            expression: quote!(#expression).to_string(),
            return_type: arroyo_datastream::ExpressionReturnType::Predicate,
        }
    }

    pub fn optional_record_expression_operator(
        name: impl ToString,
        expression: syn::Expr,
    ) -> Operator {
        Operator::ExpressionOperator {
            name: name.to_string(),
            expression: quote!(#expression).to_string(),
            return_type: arroyo_datastream::ExpressionReturnType::OptionalRecord,
        }
    }

    pub fn value_updating_operator(
        name: impl ToString,
        updating_expression: syn::Expr,
    ) -> Operator {
        Operator::UpdatingOperator {
            name: name.to_string(),
            expression: quote!(#updating_expression).to_string(),
        }
    }

    fn updating_key_operator(name: impl ToString, key_closure: syn::ExprClosure) -> Operator {
        Operator::UpdatingKeyOperator {
            name: name.to_string(),
            expression: quote!(#key_closure).to_string(),
        }
    }
}
