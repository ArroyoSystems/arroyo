#![allow(clippy::comparison_chain)]
use std::collections::HashMap;

use std::time::Duration;
use std::unreachable;

use anyhow::Result;
use anyhow::{anyhow, bail};
use arrow_schema::DataType;
use arroyo_datastream::{Operator, WindowType};

use datafusion_common::{DFField, ScalarValue};
use datafusion_expr::{
    BinaryExpr, BuiltInWindowFunction, Expr, JoinConstraint, LogicalPlan, Window, WriteOp,
};

use quote::{format_ident, quote};
use syn::{parse_quote, Type};

use crate::expressions::ExpressionContext;
use crate::external::{SqlSink, SqlSource};
use crate::{
    expressions::{AggregationExpression, Column, ColumnExpression, Expression, SortExpression},
    operators::{AggregateProjection, GroupByKind, Projection, TwoPhaseAggregateProjection},
    types::{interval_month_day_nanos_to_duration, StructDef, StructField, TypeDef},
    ArroyoSchemaProvider,
};
use crate::{FieldSpec, Table};

#[derive(Debug, Clone)]
pub enum SqlOperator {
    Source(SourceOperator),
    Aggregator(Box<SqlOperator>, AggregateOperator),
    JoinOperator(Box<SqlOperator>, Box<SqlOperator>, JoinOperator),
    Window(Box<SqlOperator>, SqlWindowOperator),
    RecordTransform(Box<SqlOperator>, RecordTransform),
    Sink(String, SqlSink, Box<SqlOperator>),
    NamedTable(String, Box<SqlOperator>),
}

#[derive(Debug, Clone)]
pub enum RecordTransform {
    ValueProjection(Projection),
    KeyProjection(Projection),
    TimestampAssignment(Expression),
    Filter(Expression),
}

#[derive(Debug, Clone)]
pub struct SourceOperator {
    pub name: String,
    pub source: SqlSource,
    pub virtual_field_projection: Option<Projection>,
    pub timestamp_override: Option<Expression>,
}
impl SourceOperator {
    fn return_type(&self) -> StructDef {
        if let Some(ref projection) = self.virtual_field_projection {
            projection.output_struct()
        } else {
            self.source.struct_def.clone()
        }
    }
}

impl RecordTransform {
    pub fn output_struct(&self, input_struct: StructDef) -> StructDef {
        match self {
            RecordTransform::ValueProjection(projection) => projection.output_struct(),
            RecordTransform::KeyProjection(_) | RecordTransform::Filter(_) => input_struct,
            RecordTransform::TimestampAssignment(_) => input_struct,
        }
    }

    pub fn as_operator(&self) -> Operator {
        match self {
            RecordTransform::ValueProjection(projection) => {
                let map_method = projection.to_syn_expression();
                MethodCompiler::value_map_operator("value_map", map_method)
            }
            RecordTransform::KeyProjection(projection) => {
                MethodCompiler::key_map_operator("key_map", projection.to_syn_expression())
            }
            RecordTransform::Filter(expression) => MethodCompiler::filter_operator(
                "filter",
                expression.to_syn_expression(),
                expression.nullable(),
            ),
            RecordTransform::TimestampAssignment(timestamp_expression) => {
                MethodCompiler::timestamp_assigning_operator(
                    "timestamp",
                    timestamp_expression.to_syn_expression(),
                    timestamp_expression.nullable(),
                )
            }
        }
    }

    pub fn name(&self) -> String {
        match self {
            RecordTransform::ValueProjection(_) => "value_project".into(),
            RecordTransform::KeyProjection(_) => "key_project".into(),
            RecordTransform::Filter(_) => "filter".into(),
            RecordTransform::TimestampAssignment(_) => "timestamp".into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AggregateOperator {
    pub key: Projection,
    pub window: WindowType,
    pub aggregating: AggregatingStrategy,
    pub merge: GroupByKind,
}

impl AggregateOperator {
    pub fn output_struct(&self) -> StructDef {
        self.merge
            .output_struct(&self.key.output_struct(), &self.aggregating.output_struct())
    }
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct SqlWindowOperator {
    pub window_fn: WindowFunction,
    pub partition: Projection,
    pub order_by: Vec<SortExpression>,
    pub field_name: String,
    pub window: WindowType,
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
        StructDef { name: None, fields }
    }

    pub fn join_struct_type(&self, left_struct: &StructDef, right_struct: &StructDef) -> StructDef {
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

    pub fn merge_syn_expression(
        &self,
        left_struct: &StructDef,
        right_struct: &StructDef,
    ) -> syn::Expr {
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
        parse_quote!(
                #return_type {
                    #(#assignments)
                    ,*
                }
        )
    }
}

impl SqlOperator {
    pub fn return_type(&self) -> StructDef {
        match self {
            SqlOperator::Source(source_operator) => source_operator.return_type(),
            SqlOperator::Aggregator(_input, aggregate_operator) => {
                aggregate_operator.merge.output_struct(
                    &aggregate_operator.key.output_struct(),
                    &aggregate_operator.aggregating.output_struct(),
                )
            }
            SqlOperator::JoinOperator(left, right, operator) => operator
                .join_type
                .output_struct(&left.return_type(), &right.return_type()),
            SqlOperator::Window(input, window) => {
                let mut input_struct = input.return_type();
                input_struct.fields.push(StructField {
                    name: window.field_name.clone(),
                    alias: None,
                    data_type: TypeDef::DataType(DataType::UInt64, false),
                });
                input_struct
            }
            SqlOperator::RecordTransform(input, record_transform) => {
                record_transform.output_struct(input.return_type())
            }
            SqlOperator::Sink(_, sql_sink, _) => sql_sink.struct_def.clone(),
            SqlOperator::NamedTable(_table_name, table) => table.return_type(),
        }
    }

    pub fn merge_struct_type(key_struct: &StructDef, aggregate_struct: &StructDef) -> StructDef {
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

    pub fn has_window(&self) -> bool {
        match self {
            SqlOperator::Source(_) => false,
            SqlOperator::Aggregator(_, _) => true,
            SqlOperator::JoinOperator(left, right, _) => left.has_window() || right.has_window(),
            SqlOperator::Window(_, _) => true,
            SqlOperator::RecordTransform(input, _) => input.has_window(),
            SqlOperator::Sink(_, _, input) => input.has_window(),
            SqlOperator::NamedTable(_, input) => input.has_window(),
        }
    }
}

#[derive(Debug)]
pub struct SqlPipelineBuilder<'a> {
    pub schema_provider: &'a ArroyoSchemaProvider,
    pub planned_tables: HashMap<String, SqlOperator>,
    pub output_nodes: Vec<SqlOperator>,
}

impl<'a> SqlPipelineBuilder<'a> {
    pub fn new(schema_provider: &'a ArroyoSchemaProvider) -> Self {
        SqlPipelineBuilder {
            schema_provider,
            planned_tables: HashMap::new(),
            output_nodes: vec![],
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
            LogicalPlan::Union(_) => bail!("unions are not currently supported"),
            LogicalPlan::TableScan(table_scan) => self.insert_table_scan(table_scan),
            LogicalPlan::EmptyRelation(_) => bail!("empty relations not currently supported"),
            LogicalPlan::Subquery(subquery) => self.insert_sql_plan(&subquery.subquery),
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                self.insert_subquery_alias(subquery_alias)
            }
            LogicalPlan::Limit(_) => bail!("limit not currently supported"),
            LogicalPlan::CreateExternalTable(_) => {
                bail!("creating external tables is not currently supported")
            }
            LogicalPlan::CreateMemoryTable(create_memory_table) => {
                bail!(
                    "creating memory tables is not currently supported: {:?}, {:?}",
                    create_memory_table.input,
                    create_memory_table.primary_key
                )
            }
            LogicalPlan::CreateView(_) => bail!("creating views is not currently supported"),
            LogicalPlan::CreateCatalogSchema(_) => {
                bail!("creating catalog schemas is not currently supported")
            }
            LogicalPlan::CreateCatalog(_) => bail!("creating catalogs is not currently supported"),
            LogicalPlan::DropTable(_) => bail!("dropping tables is not currently supported"),
            LogicalPlan::DropView(_) => bail!("dropping views is not currently supported"),
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
        }
    }
    fn insert_dml(
        &mut self,
        dml_statement: &datafusion_expr::logical_plan::DmlStatement,
    ) -> Result<SqlOperator> {
        if !matches!(dml_statement.op, WriteOp::Insert) {
            bail!("only insert statements are currently supported")
        }
        let input = self.insert_sql_plan(&dml_statement.input)?;
        let insert_table = self
            .schema_provider
            .get_table(&dml_statement.table_name.to_string())
            .ok_or_else(|| {
                anyhow!(
                    "table {} not found in schema provider",
                    dml_statement.table_name
                )
            })?;
        match insert_table {
            crate::Table::SavedSource {
                name: _,
                id: _,
                fields: _,
                type_name: _,
                source_config: _,
                serialization_mode: _,
            } => bail!("inserting into saved sources is not currently supported"),
            crate::Table::SavedSink {
                name,
                id,
                sink_config,
            } => Ok(SqlOperator::Sink(
                name.clone(),
                SqlSink {
                    id: Some(*id),
                    struct_def: input.return_type(),
                    sink_config: sink_config.clone(),
                },
                Box::new(input),
            )),
            crate::Table::MemoryTable { name, fields: _ } => {
                Ok(SqlOperator::NamedTable(name.clone(), Box::new(input)))
            }
            crate::Table::MemoryTableWithConnectionConfig {
                name: _,
                fields: _,
                connection: _,
                connection_config: _,
            } => todo!(),
            crate::Table::TableFromQuery {
                name: _,
                logical_plan: _,
            } => todo!(),
            crate::Table::InsertQuery {
                sink_name: _,
                logical_plan: _,
            } => todo!(),
            crate::Table::Anonymous { logical_plan: _ } => todo!(),
        }
    }

    fn insert_filter(
        &mut self,
        filter: &datafusion_expr::logical_plan::Filter,
    ) -> Result<SqlOperator> {
        let input = self.insert_sql_plan(&filter.input)?;
        let struct_def = input.return_type();
        let ctx = self.ctx(&struct_def);
        let predicate = ctx.compile_expr(&filter.predicate)?;
        // TODO: this should probably happen through a more principled optimization pass.
        Ok(SqlOperator::RecordTransform(
            Box::new(input),
            RecordTransform::Filter(predicate),
        ))
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
            .map(|field| Column::convert(&field.qualified_column()))
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

    fn window_field(
        &mut self,
        group_expressions: &[Expr],
        fields: &[DFField],
    ) -> Result<GroupByKind> {
        for (i, expr) in group_expressions.iter().enumerate() {
            if let Some(window) = Self::find_window(expr)? {
                if let WindowType::Instant = window {
                    bail!("don't support instant window in return type yet");
                }
                return Ok(GroupByKind::WindowOutput {
                    index: i,
                    column: Column::convert(&fields[i].qualified_column()),
                    window_type: window,
                });
            }
        }
        Ok(GroupByKind::Basic)
    }

    fn is_window(expression: &Expr) -> bool {
        match expression {
            Expr::ScalarUDF { fun, args: _ } => matches!(fun.name.as_str(), "hop" | "tumble"),
            Expr::Alias(exp, _) => Self::is_window(exp),
            _ => false,
        }
    }

    fn find_window(expression: &Expr) -> Result<Option<WindowType>> {
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
            Expr::Alias(expr, _alias) => Self::find_window(expr),
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
            (false, false) => {
                if join_type != JoinType::Inner {
                    bail!("non-inner join over windows not supported")
                }
            }
            _ => {}
        }

        let mut columns = join.on.clone();
        if let Some(Expr::BinaryExpr(BinaryExpr {
            left,
            op: datafusion_expr::Operator::Eq,
            right,
        })) = join.filter.clone()
        {
            columns.push((*left, *right));
        } else if join.filter.is_some() {
            bail!(
                "non-join filters on joins. This doesn't seem to actually happen in practice, {:?}",
                join.on
            );
        }
        let join_projection_field_names: Vec<_> = columns
            .iter()
            .map(|(left, _right)| Column::convert_expr(left))
            .collect::<Result<Vec<_>>>()?;

        let left_key = Projection {
            field_names: join_projection_field_names.clone(),
            field_computations: columns
                .iter()
                .map(|(left, _right)| self.ctx(&left_input.return_type()).compile_expr(left))
                .collect::<Result<Vec<_>>>()?,
        };
        let right_key = Projection {
            field_names: join_projection_field_names,
            field_computations: columns
                .iter()
                .map(|(_left, right)| self.ctx(&right_input.return_type()).compile_expr(right))
                .collect::<Result<Vec<_>>>()?,
        };

        if right_key.output_struct() != left_key.output_struct() {
            bail!("join key types must match. Try casting?");
        }
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
        let source = match source {
            crate::Table::SavedSource {
                name: _,
                id,
                fields,
                type_name,
                source_config,
                serialization_mode,
            } => {
                let source = SqlSource {
                    id: Some(*id),
                    struct_def: StructDef {
                        name: type_name.clone(),
                        fields: fields.clone(),
                    },
                    source_config: source_config.clone(),
                    serialization_mode: *serialization_mode,
                };
                SqlOperator::Source(SourceOperator {
                    name: table_name,
                    source,
                    virtual_field_projection: None,
                    timestamp_override: None,
                })
            }
            crate::Table::SavedSink {
                name: _,
                id: _,
                sink_config: _,
            } => bail!("can't read from a saved sink."),
            crate::Table::MemoryTable { name, fields: _ } => {
                let planned_table = self.planned_tables.get(name).ok_or_else(|| {
                    anyhow!(
                        "memory table {} not found in planned tables. This is a bug.",
                        name
                    )
                })?;
                planned_table.clone()
            }
            crate::Table::MemoryTableWithConnectionConfig {
                name: _,
                fields,
                connection,
                connection_config,
            } => {
                let physical_fields = fields
                    .iter()
                    .filter_map(|field| match field {
                        FieldSpec::VirtualStructField(..) => None,
                        FieldSpec::StructField(struct_field) => Some(struct_field.clone()),
                    })
                    .collect::<Vec<_>>();
                let has_virtual_fields = physical_fields.len() < fields.len();
                let physical_source = SqlSource::try_new(
                    None,
                    StructDef {
                        name: None,
                        fields: physical_fields,
                    },
                    connection.clone(),
                    connection_config,
                )?;
                // check for virtual fields
                let virtual_field_projection = if has_virtual_fields {
                    let (field_names, field_computations) = fields
                        .iter()
                        .map(|field| match field {
                            FieldSpec::StructField(struct_field) => (
                                Column {
                                    relation: None,
                                    name: struct_field.name.clone(),
                                },
                                Expression::Column(ColumnExpression::new(struct_field.clone())),
                            ),
                            FieldSpec::VirtualStructField(struct_field, expr) => (
                                Column {
                                    relation: None,
                                    name: struct_field.name.clone(),
                                },
                                expr.clone(),
                            ),
                        })
                        .unzip();
                    Some(Projection {
                        field_names,
                        field_computations,
                    })
                } else {
                    None
                };
                let timestamp_override = if let Some(field_name) =
                    connection_config.get("event_time_field")
                {
                    // check that a column exists and it is a timestamp
                    let Some(event_column) = fields.iter().find_map(|f| match f {
                        FieldSpec::StructField(struct_field) |
                        FieldSpec::VirtualStructField(struct_field, _) =>
                        if struct_field.name == *field_name
                            && matches!(struct_field.data_type, TypeDef::DataType(DataType::Timestamp(..), _)) {
                            Some(struct_field.clone())
                            } else {
                                None
                            },
                    }) else {
                        bail!("event_time_field {} not found or not a timestamp", field_name)
                    };
                    Some(Expression::Column(ColumnExpression::new(event_column)))
                } else {
                    None
                };
                SqlOperator::Source(SourceOperator {
                    name: table_name,
                    source: physical_source,
                    virtual_field_projection,
                    timestamp_override,
                })
            }
            crate::Table::TableFromQuery {
                name: _,
                logical_plan,
            } => self.insert_sql_plan(&logical_plan.clone())?,
            crate::Table::InsertQuery {
                sink_name: _,
                logical_plan: _,
            } => {
                bail!("insert queries can't be a table scan");
            }
            crate::Table::Anonymous { logical_plan: _ } => {
                bail!("anonymous queries can't be table sources.")
            }
        };

        if let Some(projection) = table_scan.projection.as_ref() {
            let fields: Vec<StructField> = projection
                .iter()
                .map(|i| source.return_type().fields[*i].clone())
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
                .map(|t| Expression::Column(ColumnExpression::new(t.clone())))
                .collect();

            return Ok(SqlOperator::RecordTransform(
                Box::new(source),
                RecordTransform::ValueProjection(Projection {
                    field_names,
                    field_computations,
                }),
            ));
        }

        Ok(source)
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

                    let field_names = w
                        .partition_by
                        .iter()
                        .filter(|expr| !Self::is_window(expr))
                        .enumerate()
                        .map(|(i, _t)| Column {
                            relation: None,
                            name: format!("_{}", i),
                        })
                        .collect();

                    let field_computations = w
                        .partition_by
                        .iter()
                        .filter(|expr| !Self::is_window(expr))
                        .map(|expression| ctx.compile_expr(expression))
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
                            window,
                        },
                    ));
                }
                _ => {
                    bail!("non window expression for window: {:?}", expr);
                }
            }
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
            .map(|field| Expression::Column(ColumnExpression::new(field.clone())))
            .collect();

        let field_names = subquery_alias
            .schema
            .fields()
            .iter()
            .map(|field| Column::convert(&field.qualified_column()))
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
        let mut ctx = self.ctx(input_struct);

        let field_names = aggregate_fields
            .iter()
            .map(|field| Column::convert(&field.qualified_column()))
            .collect();
        let two_phase_field_computations = aggr_expr
            .iter()
            .map(|expr| ctx.as_two_phase_aggregation(expr))
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
            .map(|expr| AggregationExpression::try_from_expression(&mut ctx, expr))
            .collect::<Result<Vec<_>>>()?;
        Ok(AggregatingStrategy::AggregateProjection(
            AggregateProjection {
                field_names,
                field_computations,
            },
        ))
    }

    pub(crate) fn insert_table(&mut self, table: Table) -> Result<()> {
        match table {
            Table::SavedSource {
                name: _,
                id: _,
                fields: _,
                type_name: _,
                source_config: _,
                serialization_mode: _,
            } => todo!(),
            Table::SavedSink {
                name: _,
                id: _,
                sink_config: _,
            } => todo!(),
            Table::MemoryTable { name: _, fields: _ } => todo!(),
            Table::MemoryTableWithConnectionConfig {
                name: _,
                fields: _,
                connection: _,
                connection_config: _,
            } => todo!(),
            Table::TableFromQuery {
                name: _,
                logical_plan: _,
            } => todo!(),
            Table::InsertQuery {
                sink_name,
                logical_plan,
            } => {
                let input = self.insert_sql_plan(&logical_plan)?;
                let sink = self.schema_provider.get_table(&sink_name).ok_or_else(|| {
                    anyhow!("Could not find sink {} in schema provider", sink_name)
                })?;
                match sink {
                    Table::SavedSource {
                        name: _,
                        id: _,
                        fields: _,
                        type_name: _,
                        source_config: _,
                        serialization_mode: _,
                    } => bail!("can't insert into a saved source"),
                    Table::SavedSink {
                        name,
                        id,
                        sink_config,
                    } => {
                        let sql_operator = SqlOperator::Sink(
                            name.clone(),
                            SqlSink {
                                id: Some(*id),
                                struct_def: input.return_type(),
                                sink_config: sink_config.clone(),
                            },
                            Box::new(input),
                        );
                        self.output_nodes.push(sql_operator);
                    }
                    Table::MemoryTable { name, fields } => {
                        if self.planned_tables.contains_key(name) {
                            bail!("can't insert into {} twice", name);
                        }
                        let input_struct = input.return_type();
                        // insert into is done column-wise, and DataFusion will have already coerced all the types.
                        let mapping = RecordTransform::ValueProjection(Projection {
                            field_names: fields
                                .iter()
                                .map(|f| Column {
                                    relation: None,
                                    name: f.name.clone(),
                                })
                                .collect(),
                            field_computations: input_struct
                                .fields
                                .iter()
                                .map(|f| Expression::Column(ColumnExpression::new(f.clone())))
                                .collect(),
                        });
                        self.planned_tables.insert(
                            name.clone(),
                            SqlOperator::RecordTransform(Box::new(input), mapping),
                        );
                    }
                    Table::MemoryTableWithConnectionConfig {
                        name,
                        fields: _,
                        connection,
                        connection_config,
                    } => {
                        let sql_operator = SqlOperator::Sink(
                            name.clone(),
                            SqlSink::try_new(
                                None,
                                input.return_type(),
                                connection.clone(),
                                connection_config.clone(),
                            )?,
                            Box::new(input),
                        );
                        self.output_nodes.push(sql_operator);
                    }
                    Table::TableFromQuery {
                        name: _,
                        logical_plan: _,
                    } => todo!(),
                    Table::InsertQuery {
                        sink_name: _,
                        logical_plan: _,
                    } => todo!(),
                    Table::Anonymous { logical_plan: _ } => todo!(),
                }
            }
            Table::Anonymous { logical_plan } => {
                let operator = self.insert_sql_plan(&logical_plan)?;
                self.output_nodes.push(operator);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MethodCompiler {}

impl MethodCompiler {
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

    pub fn filter_operator(
        name: impl ToString,
        filter_expr: syn::Expr,
        expression_nullable: bool,
    ) -> Operator {
        let expression: syn::Expr = if expression_nullable {
            parse_quote!(
                {
                    let arg = &record.value;
                    (#filter_expr).unwrap_or(false)
                }
            )
        } else {
            parse_quote!(
                {
                    let arg = &record.value;
                    #filter_expr
                }
            )
        };
        Operator::ExpressionOperator {
            name: name.to_string(),
            expression: quote!(#expression).to_string(),
            return_type: arroyo_datastream::ExpressionReturnType::Predicate,
        }
    }

    pub fn timestamp_assigning_operator(
        name: impl ToString,
        timestamp_expr: syn::Expr,
        expression_nullable: bool,
    ) -> Operator {
        let unwrap_tokens = if expression_nullable {
            Some(quote!(.expect("require a non-null timestamp")))
        } else {
            None
        };
        let expression: syn::Expr = parse_quote!(
            {
                let arg = &record.value;
                let timestamp = (#timestamp_expr)#unwrap_tokens;
                arroyo_types::Record {
                    timestamp,
                    key: record.key.clone(),
                    value: record.value.clone()
                }
            }
        );
        Operator::ExpressionOperator {
            name: name.to_string(),
            expression: quote!(#expression).to_string(),
            return_type: arroyo_datastream::ExpressionReturnType::Record,
        }
    }

    pub fn merge_pair_operator(
        name: impl ToString,
        merge_struct_name: Type,
        merge_expr: syn::Expr,
    ) -> Result<Operator> {
        let expression: syn::Expr = parse_quote!({
            let arg = #merge_struct_name {
                left: record.value.0.clone(),
                right: record.value.1.clone()
            };
            arroyo_types::Record {
                timestamp: record.timestamp.clone(),
                key: None,
                value: #merge_expr
            }
        }
        );
        Ok(Operator::ExpressionOperator {
            name: name.to_string(),
            expression: quote!(#expression).to_string(),
            return_type: arroyo_datastream::ExpressionReturnType::Record,
        })
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
}
