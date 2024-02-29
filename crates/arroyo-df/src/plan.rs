use std::{collections::HashMap, fmt::Formatter, sync::Arc};

use anyhow::Result;
use arrow::datatypes::IntervalMonthDayNanoType;

use arroyo_datastream::WindowType;
use arroyo_rpc::TIMESTAMP_FIELD;
use datafusion_common::{
    plan_err,
    tree_node::{TreeNode, TreeNodeRewriter, TreeNodeVisitor, VisitRecursion},
    Column, DFField, DFSchema, DFSchemaRef, DataFusionError, OwnedTableReference,
    Result as DFResult, ScalarValue,
};
use datafusion_execution::FunctionRegistry;
use datafusion_expr::{
    expr::{Alias, ScalarFunction},
    Aggregate, BinaryExpr, BuiltinScalarFunction, Case, DmlStatement, Expr, Extension, Join,
    JoinConstraint, JoinType, LogicalPlan, Projection, ScalarFunctionDefinition,
    UserDefinedLogicalNodeCore, WriteOp,
};

use crate::{
    find_window,
    physical::new_registry,
    rewriters::SourceRewriter,
    schemas::{add_timestamp_field, has_timestamp_field},
    tables::{ConnectorTable, Table},
    ArroyoSchemaProvider, WindowBehavior,
};

/* Calculation for computing keyed data, with a vec of keys
   that will be used for shuffling data to the correct nodes.
*/
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct KeyCalculationExtension {
    pub(crate) name: Option<String>,
    pub(crate) input: LogicalPlan,
    pub(crate) keys: Vec<usize>,
    pub(crate) schema: DFSchemaRef,
}

impl KeyCalculationExtension {
    pub fn new_named_and_trimmed(input: LogicalPlan, keys: Vec<usize>, name: String) -> Self {
        let output_fields: Vec<_> = input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                if !keys.contains(&index) {
                    Some(field.clone())
                } else {
                    None
                }
            })
            .collect();
        let schema =
            DFSchema::new_with_metadata(output_fields, input.schema().metadata().clone()).unwrap();
        Self {
            name: Some(name),
            input,
            keys,
            schema: Arc::new(schema),
        }
    }
    pub fn new(input: LogicalPlan, keys: Vec<usize>) -> Self {
        let schema = input.schema().clone();
        Self {
            name: None,
            input,
            keys,
            schema,
        }
    }
}

impl UserDefinedLogicalNodeCore for KeyCalculationExtension {
    fn name(&self) -> &str {
        "KeyCalculationExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "KeyCalculationExtension: {}",
            self.schema()
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        match self.name {
            Some(ref name) => {
                Self::new_named_and_trimmed(inputs[0].clone(), self.keys.clone(), name.clone())
            }
            None => Self::new(inputs[0].clone(), self.keys.clone()),
        }
    }
}

/* Lightweight extension that allows us to segment the graph and merge nodes with the same name.
  An Extension Planner will be used to isolate computation to individual nodes.
*/
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RemoteTableExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) name: OwnedTableReference,
    pub(crate) schema: DFSchemaRef,
}

impl UserDefinedLogicalNodeCore for RemoteTableExtension {
    fn name(&self) -> &str {
        "RemoteTableExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RemoteTableExtension: {}",
            self.schema
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self {
            input: inputs[0].clone(),
            name: self.name.clone(),
            schema: self.schema.clone(),
        }
    }
}

/*
This is a plan used for appending a _timestamp field to an existing record batch.
 */
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TimestampAppendExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) qualifier: Option<OwnedTableReference>,
    pub(crate) schema: DFSchemaRef,
}

impl TimestampAppendExtension {
    fn new(input: LogicalPlan, qualifier: Option<OwnedTableReference>) -> Self {
        if has_timestamp_field(input.schema().clone()) {
            unreachable!("shouldn't be adding timestamp to a plan that already has it");
        }
        let schema = add_timestamp_field(input.schema().clone(), qualifier.clone()).unwrap();
        Self {
            input,
            qualifier,
            schema,
        }
    }
}

impl UserDefinedLogicalNodeCore for TimestampAppendExtension {
    fn name(&self) -> &str {
        "TimestampAppendExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "TimestampAppendExtension({:?}): {}",
            self.qualifier,
            self.schema
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self::new(inputs[0].clone(), self.qualifier.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WindowAppendExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) window_field: DFField,
    pub(crate) window_index: usize,
    pub(crate) schema: DFSchemaRef,
}

impl WindowAppendExtension {
    fn new(input: LogicalPlan, window_field: DFField, window_index: usize) -> Self {
        let mut fields = input.schema().fields().clone();
        fields.insert(window_index, window_field.clone());
        let metadata = input.schema().metadata().clone();
        Self {
            input,
            window_field,
            window_index,
            schema: Arc::new(DFSchema::new_with_metadata(fields, metadata).unwrap()),
        }
    }
}

impl UserDefinedLogicalNodeCore for WindowAppendExtension {
    fn name(&self) -> &str {
        "WindowAppendExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "WindowAppendExtension: field {:?} at {}",
            self.window_field, self.window_index
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self::new(
            inputs[0].clone(),
            self.window_field.clone(),
            self.window_index,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AggregateExtension {
    pub(crate) window_behavior: WindowBehavior,
    pub(crate) aggregate: LogicalPlan,
    pub(crate) schema: DFSchemaRef,
    pub(crate) key_fields: Vec<usize>,
    pub(crate) final_calculation: LogicalPlan,
}

impl AggregateExtension {
    pub fn new(
        window_behavior: WindowBehavior,
        aggregate: LogicalPlan,
        key_fields: Vec<usize>,
    ) -> Self {
        let final_calculation =
            Self::final_projection(&aggregate, window_behavior.clone()).unwrap();

        Self {
            window_behavior,
            aggregate,
            schema: final_calculation.schema().clone(),
            key_fields,
            final_calculation,
        }
    }

    // projection assuming that _timestamp has been populated with the start of the bin.
    pub fn final_projection(
        aggregate_plan: &LogicalPlan,
        window_behavior: WindowBehavior,
    ) -> DFResult<LogicalPlan> {
        let timestamp_field = aggregate_plan.inputs()[0]
            .schema()
            .field_with_unqualified_name(TIMESTAMP_FIELD)
            .unwrap()
            .clone();
        let timestamp_append = LogicalPlan::Extension(Extension {
            node: Arc::new(TimestampAppendExtension::new(
                aggregate_plan.clone(),
                timestamp_field.qualifier().cloned(),
            )),
        });
        let mut aggregate_fields = aggregate_plan.schema().fields().clone();
        let mut aggregate_expressions: Vec<_> = aggregate_fields
            .iter()
            .map(|field| Expr::Column(field.qualified_column()))
            .collect();
        let (window_field, window_index, width) = match window_behavior {
            WindowBehavior::InData => return Ok(timestamp_append),
            WindowBehavior::FromOperator {
                window,
                window_field,
                window_index,
            } => match window {
                WindowType::Tumbling { width, .. } | WindowType::Sliding { width, .. } => {
                    (window_field, window_index, width)
                }
                WindowType::Session { .. } => {
                    return Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(WindowAppendExtension::new(
                            timestamp_append,
                            window_field,
                            window_index,
                        )),
                    }))
                }
                WindowType::Instant => return Ok(timestamp_append),
            },
        };

        let timestamp_column =
            Column::new(timestamp_field.qualifier().cloned(), timestamp_field.name());
        aggregate_fields.insert(window_index, window_field.clone());
        let registry = new_registry();
        let window_expression = Expr::ScalarFunction(ScalarFunction {
            func_def: ScalarFunctionDefinition::UDF(registry.udf("window")?),
            args: vec![
                // copy bin_start as first argument
                Expr::Column(timestamp_column.clone()),
                // add width interval to _timestamp for bin end
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(Expr::Column(timestamp_column.clone())),
                    op: datafusion_expr::Operator::Plus,
                    right: Box::new(Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(
                        IntervalMonthDayNanoType::make_value(0, 0, width.as_nanos() as i64),
                    )))),
                }),
            ],
        });
        aggregate_expressions.insert(
            window_index,
            window_expression
                .alias_qualified(window_field.qualifier().cloned(), window_field.name()),
        );
        aggregate_fields.push(timestamp_field.clone());
        let bin_end_calculation = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(timestamp_column.clone())),
            op: datafusion_expr::Operator::Plus,
            right: Box::new(Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(0, 0, (width.as_nanos() - 1) as i64),
            )))),
        });
        aggregate_expressions.push(bin_end_calculation);
        Ok(LogicalPlan::Projection(
            datafusion_expr::Projection::try_new_with_schema(
                aggregate_expressions,
                Arc::new(timestamp_append),
                Arc::new(DFSchema::new_with_metadata(
                    aggregate_fields,
                    HashMap::new(),
                )?),
            )?,
        ))
    }
}

impl UserDefinedLogicalNodeCore for AggregateExtension {
    fn name(&self) -> &str {
        "AggregateExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.aggregate]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "AggregateExtension: {} | window_behavior: {:?}",
            self.schema()
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", "),
            match &self.window_behavior {
                WindowBehavior::InData => "InData".to_string(),
                WindowBehavior::FromOperator { window, .. } =>
                    format!("FromOperator({:?})", window),
            }
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Self::new(
            self.window_behavior.clone(),
            inputs[0].clone(),
            self.key_fields.clone(),
        )
    }
}

#[derive(Debug, Default)]
pub struct AggregateRewriter {}

impl TreeNodeRewriter for AggregateRewriter {
    type N = LogicalPlan;

    fn mutate(&mut self, node: Self::N) -> DFResult<Self::N> {
        let LogicalPlan::Aggregate(Aggregate {
            input,
            mut group_expr,
            aggr_expr,
            schema,
            ..
        }) = node
        else {
            return Ok(node);
        };
        // Remove timestamp group by that was inserted
        group_expr.retain(|expr| {
            if let Expr::Column(Column { name, .. }) = expr {
                name != "_timestamp"
            } else {
                true
            }
        });
        let mut window_group_expr: Vec<_> = group_expr
            .iter()
            .enumerate()
            .filter_map(|(i, expr)| {
                find_window(expr)
                    .map(|option| option.map(|inner| (i, inner)))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;

        if window_group_expr.len() > 1 {
            return Err(datafusion_common::DataFusionError::NotImplemented(format!(
                "do not support {} window expressions in group by",
                window_group_expr.len()
            )));
        }

        let mut key_fields: Vec<DFField> = schema
            .fields()
            .iter()
            .take(group_expr.len())
            .cloned()
            .map(|field| {
                DFField::new(
                    field.qualifier().cloned(),
                    &format!("_key_{}", field.name()),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>();

        let window = WindowDetectingVisitor::get_window(&input)?;
        let window_behavior = match (window.is_some(), !window_group_expr.is_empty()) {
            (true, true) => {
                return Err(DataFusionError::NotImplemented(
                    "query has both a window in group by and input is windowed.".to_string(),
                ))
            }
            (true, false) => WindowBehavior::InData,
            (false, true) => {
                // strip out window from group by, will be handled by operator.
                let (window_index, window_type) = window_group_expr.pop().unwrap();
                group_expr.remove(window_index);
                key_fields.remove(window_index);
                let window_field = schema.field(window_index).clone();
                WindowBehavior::FromOperator {
                    window: window_type,
                    window_field,
                    window_index,
                }
            }
            (false, false) => {
                return Err(DataFusionError::NotImplemented(
                    format!("must have window in aggregate. Plan that failed has group expressions {:?} and input {:?}", group_expr, input),
                ))
            }
        };

        let key_count = key_fields.len();
        key_fields.extend(input.schema().fields().clone());

        let key_schema = Arc::new(DFSchema::new_with_metadata(
            key_fields,
            schema.metadata().clone(),
        )?);

        let mut key_projection_expressions = group_expr.clone();
        key_projection_expressions.extend(
            input
                .schema()
                .fields()
                .iter()
                .map(|field| Expr::Column(Column::new(field.qualifier().cloned(), field.name()))),
        );

        let key_projection =
            LogicalPlan::Projection(datafusion_expr::Projection::try_new_with_schema(
                key_projection_expressions.clone(),
                input.clone(),
                key_schema.clone(),
            )?);

        let key_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(KeyCalculationExtension::new(
                key_projection,
                (0..key_count).collect(),
            )),
        });
        let mut aggregate_schema_fields = schema.fields().clone();
        if let WindowBehavior::FromOperator {
            window: _,
            window_field: _,
            window_index,
        } = &window_behavior
        {
            aggregate_schema_fields.remove(*window_index);
        }
        let internal_schema = Arc::new(DFSchema::new_with_metadata(
            aggregate_schema_fields,
            schema.metadata().clone(),
        )?);

        let rewritten_aggregate = Aggregate::try_new_with_schema(
            Arc::new(key_plan),
            group_expr,
            aggr_expr,
            internal_schema,
        )?;

        let aggregate_extension = AggregateExtension::new(
            window_behavior,
            LogicalPlan::Aggregate(rewritten_aggregate),
            (0..key_count).collect(),
        );
        let final_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(aggregate_extension),
        });
        Ok(final_plan)
    }
}

struct WindowDetectingVisitor {
    window: Option<WindowType>,
}

impl WindowDetectingVisitor {
    fn get_window(logical_plan: &LogicalPlan) -> DFResult<Option<WindowType>> {
        let mut visitor = WindowDetectingVisitor { window: None };
        logical_plan.visit(&mut visitor)?;
        Ok(visitor.window.take())
    }
}

impl TreeNodeVisitor for WindowDetectingVisitor {
    type N = LogicalPlan;

    fn pre_visit(&mut self, node: &Self::N) -> DFResult<VisitRecursion> {
        match node {
            LogicalPlan::Aggregate(Aggregate {
                input: _,
                group_expr,
                aggr_expr: _,
                schema: _,
                ..
            }) => {
                let window_expressions = group_expr
                    .iter()
                    .filter_map(|expr| {
                        find_window(expr)
                            .map_err(|err| DataFusionError::Plan(err.to_string()))
                            .transpose()
                    })
                    .collect::<DFResult<Vec<_>>>()?;
                for window in window_expressions {
                    // if there's already a window they should match
                    if let Some(existing_window) = &self.window {
                        if *existing_window != window {
                            return Err(DataFusionError::Plan(
                                "window expressions do not match".to_string(),
                            ));
                        }
                    } else {
                        self.window = Some(window);
                    }
                }
            }
            LogicalPlan::Extension(Extension { node }) => {
                if let Some(aggregate_extension) =
                    node.as_any().downcast_ref::<AggregateExtension>()
                {
                    if let WindowBehavior::FromOperator { window, .. } =
                        &aggregate_extension.window_behavior
                    {
                        self.window = Some(window.clone());
                    }
                }
            }
            _ => {}
        }
        Ok(VisitRecursion::Continue)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TableSourceExtension {
    pub(crate) name: OwnedTableReference,
    pub(crate) table: ConnectorTable,
    pub(crate) schema: DFSchemaRef,
}

impl TableSourceExtension {
    pub fn new(name: OwnedTableReference, table: ConnectorTable) -> Self {
        let physical_fields = table
            .fields
            .iter()
            .filter_map(|field| match field {
                crate::tables::FieldSpec::StructField(field) => {
                    Some(DFField::from_qualified(&name, Arc::new(field.clone())))
                }
                crate::tables::FieldSpec::VirtualField { .. } => None,
            })
            .collect::<Vec<_>>();
        let schema =
            Arc::new(DFSchema::new_with_metadata(physical_fields, HashMap::new()).unwrap());
        let schema = add_timestamp_field(schema, Some(name.clone())).unwrap();
        Self {
            name,
            table,
            schema,
        }
    }
}

impl UserDefinedLogicalNodeCore for TableSourceExtension {
    fn name(&self) -> &str {
        "TableSourceExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "TableSourceExtension: {}",
            self.schema
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
        Self {
            name: self.name.clone(),
            table: self.table.clone(),
            schema: self.schema.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SinkExtension {
    pub(crate) name: OwnedTableReference,
    pub(crate) table: Table,
    pub(crate) schema: DFSchemaRef,
    pub(crate) input: Arc<LogicalPlan>,
}

impl UserDefinedLogicalNodeCore for SinkExtension {
    fn name(&self) -> &str {
        "SinkExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SinkExtension({:?}): {}",
            self.name,
            self.schema
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Self {
            name: self.name.clone(),
            table: self.table.clone(),
            schema: self.schema.clone(),
            input: Arc::new(inputs[0].clone()),
        }
    }
}

/// Rewrites a logical plan to replace InsertIntos with a SinkExtension that knows everything about Arroyo sinks.
pub struct SinkRewriter {
    pub(crate) schema_provider: ArroyoSchemaProvider,
}

impl TreeNodeRewriter for SinkRewriter {
    type N = LogicalPlan;

    fn mutate(&mut self, node: Self::N) -> DFResult<Self::N> {
        match &node {
            LogicalPlan::Dml(DmlStatement {
                table_name,
                table_schema,
                op,
                input,
            }) => {
                let WriteOp::InsertInto = op else {
                    // error out
                    return Err(DataFusionError::Plan(
                        "only InsertInto is supported".to_string(),
                    ));
                };
                let table = self
                    .schema_provider
                    .get_table(table_name.to_string())
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!("table {} not found", table_name))
                    })?;
                let sink_extension = SinkExtension {
                    name: table_name.clone(),
                    table: table.clone(),
                    schema: table_schema.clone(),
                    input: input.clone(),
                };
                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(sink_extension),
                }))
            }
            _ => Ok(node),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct JoinExtension {
    pub(crate) rewritten_join: LogicalPlan,
    pub(crate) is_instant: bool,
}

impl UserDefinedLogicalNodeCore for JoinExtension {
    fn name(&self) -> &str {
        "JoinExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.rewritten_join]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.rewritten_join.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "JoinExtension: {}",
            self.schema()
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        Self {
            rewritten_join: inputs[0].clone(),
            is_instant: self.is_instant,
        }
    }
}

pub(crate) struct JoinRewriter {}

impl JoinRewriter {
    fn check_join_windowing(join: &Join) -> DFResult<bool> {
        let left_window = WindowDetectingVisitor::get_window(&join.left)?;
        let right_window = WindowDetectingVisitor::get_window(&join.right)?;
        match (left_window, right_window) {
            (None, None) => {
                if join.join_type == JoinType::Inner {
                    Ok(false)
                } else {
                    Err(DataFusionError::NotImplemented(
                        "can't handle non-inner joins without windows".into(),
                    ))
                }
            }
            (None, Some(_)) => Err(DataFusionError::NotImplemented(
                "can't handle mixed windowing between left (non-windowed) and right (windowed)."
                    .into(),
            )),
            (Some(_), None) => Err(DataFusionError::NotImplemented(
                "can't handle mixed windowing between left (windowed) and right (non-windowed)."
                    .into(),
            )),
            (Some(left_window), Some(right_window)) => {
                if left_window != right_window {
                    return Err(DataFusionError::NotImplemented(
                        "can't handle mixed windowing between left and right".into(),
                    ));
                }
                // exclude session windows
                if let WindowType::Session { .. } = left_window {
                    return Err(DataFusionError::NotImplemented(
                        "can't handle session windows in joins".into(),
                    ));
                }

                Ok(true)
            }
        }
    }

    fn create_join_key_plan(
        &self,
        input: Arc<LogicalPlan>,
        mut join_expressions: Vec<Expr>,
        name: &'static str,
    ) -> DFResult<LogicalPlan> {
        let key_count = join_expressions.len();
        join_expressions.extend(
            input
                .schema()
                .fields()
                .iter()
                .map(|field| Expr::Column(Column::new(field.qualifier().cloned(), field.name()))),
        );
        // Calculate initial projection with default names
        let mut projection = Projection::try_new(join_expressions, input)?;
        let fields = projection
            .schema
            .fields()
            .into_iter()
            .enumerate()
            .map(|(index, field)| {
                // rename to avoid collisions
                if index < key_count {
                    DFField::new(
                        field.qualifier().cloned(),
                        &format!("_key_{}", field.name()),
                        field.data_type().clone(),
                        field.is_nullable(),
                    )
                } else {
                    field.clone()
                }
            });
        let rewritten_schema = Arc::new(DFSchema::new_with_metadata(
            fields.collect(),
            projection.schema.metadata().clone(),
        )?);
        projection.schema = rewritten_schema;
        let key_calculation_extension = KeyCalculationExtension::new_named_and_trimmed(
            LogicalPlan::Projection(projection),
            (0..key_count).into_iter().collect(),
            name.to_string(),
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(key_calculation_extension),
        }))
    }

    fn post_join_timestamp_projection(&mut self, input: LogicalPlan) -> DFResult<LogicalPlan> {
        let schema = input.schema().clone();
        let mut schema_with_timestamp = schema.fields().clone();
        let timestamp_fields = schema_with_timestamp
            .iter()
            .filter(|field| field.name() == "_timestamp")
            .cloned()
            .collect::<Vec<_>>();
        if timestamp_fields.len() != 2 {
            return Err(DataFusionError::NotImplemented(
                "join must have two timestamp fields".to_string(),
            ));
        }
        schema_with_timestamp.retain(|field| field.name() != "_timestamp");
        let mut projection_expr = schema_with_timestamp
            .iter()
            .map(|field| {
                Expr::Column(Column {
                    relation: field.qualifier().cloned(),
                    name: field.name().to_string(),
                })
            })
            .collect::<Vec<_>>();
        // add a _timestamp field to the schema
        schema_with_timestamp.push(timestamp_fields[0].clone());

        let output_schema = Arc::new(DFSchema::new_with_metadata(
            schema_with_timestamp,
            schema.metadata().clone(),
        )?);
        // then take a max of the two timestamp columns
        let left_field = &timestamp_fields[0];
        let left_column = Expr::Column(Column {
            relation: left_field.qualifier().cloned(),
            name: left_field.name().to_string(),
        });
        let right_field = &timestamp_fields[1];
        let right_column = Expr::Column(Column {
            relation: right_field.qualifier().cloned(),
            name: right_field.name().to_string(),
        });
        let max_timestamp = Expr::Case(Case {
            expr: Some(Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left_column.clone()),
                op: datafusion_expr::Operator::GtEq,
                right: Box::new(right_column.clone()),
            }))),
            when_then_expr: vec![
                (
                    Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)))),
                    Box::new(left_column.clone()),
                ),
                (
                    Box::new(Expr::Literal(ScalarValue::Boolean(Some(false)))),
                    Box::new(right_column.clone()),
                ),
            ],
            else_expr: Some(Box::new(Expr::ScalarFunction(ScalarFunction::new(
                BuiltinScalarFunction::Coalesce,
                vec![left_column.clone(), right_column.clone()],
            )))),
        });

        projection_expr.push(Expr::Alias(Alias {
            expr: Box::new(max_timestamp),
            relation: timestamp_fields[0].qualifier().cloned(),
            name: timestamp_fields[0].name().to_string(),
        }));
        Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
            projection_expr,
            Arc::new(input),
            output_schema.clone(),
        )?))
    }
}

impl TreeNodeRewriter for JoinRewriter {
    type N = LogicalPlan;

    fn mutate(&mut self, node: Self::N) -> DFResult<Self::N> {
        let LogicalPlan::Join(join) = node else {
            return Ok(node);
        };
        let is_instant = Self::check_join_windowing(&join)?;

        let Join {
            left,
            right,
            on,
            filter,
            join_type,
            join_constraint: JoinConstraint::On,
            schema,
            null_equals_null: false,
        } = join
        else {
            return Err(DataFusionError::NotImplemented(
                "can't handle join constraint other than ON".into(),
            ));
        };

        let (left_expressions, right_expressions): (Vec<_>, Vec<_>) =
            on.clone().into_iter().unzip();
        let left_input = self.create_join_key_plan(left.clone(), left_expressions, "left")?;
        let right_input = self.create_join_key_plan(right.clone(), right_expressions, "right")?;
        let rewritten_join = LogicalPlan::Join(Join {
            left: Arc::new(left_input),
            right: Arc::new(right_input),
            on,
            join_type,
            join_constraint: JoinConstraint::On,
            schema: schema.clone(),
            null_equals_null: false,
            filter,
        });

        let final_logical_plan = self.post_join_timestamp_projection(rewritten_join)?;

        let join_extension = JoinExtension {
            rewritten_join: final_logical_plan,
            is_instant,
        };

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(join_extension),
        }))
    }
}

// This is one rewriter so that we can rely on inputs having already been rewritten
// ensuring they have _timestamp field, amongst other things.
pub struct ArroyoRewriter<'a> {
    pub(crate) schema_provider: &'a ArroyoSchemaProvider,
}

impl<'a> TreeNodeRewriter for ArroyoRewriter<'a> {
    type N = LogicalPlan;

    fn mutate(&mut self, mut node: Self::N) -> DFResult<Self::N> {
        match node {
            LogicalPlan::Projection(ref mut projection) => {
                if !has_timestamp_field(projection.schema.clone()) {
                    let timestamp_field = projection
                        .input
                        .schema()
                        .fields_with_unqualified_name(TIMESTAMP_FIELD)[0];
                    projection.schema = add_timestamp_field(
                        projection.schema.clone(),
                        timestamp_field.qualifier().cloned(),
                    )
                    .expect("in projection");
                    projection.expr.push(Expr::Column(Column {
                        relation: timestamp_field.qualifier().cloned(),
                        name: "_timestamp".to_string(),
                    }));
                }
            }
            LogicalPlan::Aggregate(aggregate) => {
                return AggregateRewriter {}.mutate(LogicalPlan::Aggregate(aggregate));
            }
            LogicalPlan::Join(join) => {
                return JoinRewriter {}.mutate(LogicalPlan::Join(join));
            }
            LogicalPlan::TableScan(table_scan) => {
                return SourceRewriter {
                    schema_provider: &self.schema_provider,
                }
                .mutate(LogicalPlan::TableScan(table_scan));
            }
            LogicalPlan::Filter(_) => {}
            LogicalPlan::Window(_) => {
                return plan_err!(
                    "SQL window functions are not currently supported ({})",
                    node.display()
                );
            }
            LogicalPlan::Sort(_) => {
                return plan_err!("ORDER BY is not currently supported ({})", node.display());
            }
            LogicalPlan::CrossJoin(_) => {
                return plan_err!("CROSS JOIN is not currently supported ({})", node.display());
            }
            LogicalPlan::Repartition(_) => {
                return plan_err!(
                    "Repartitions are not currently supported ({})",
                    node.display()
                );
            }
            LogicalPlan::Union(_) => {}
            LogicalPlan::EmptyRelation(_) => {}
            LogicalPlan::Subquery(_) => {}
            LogicalPlan::SubqueryAlias(_) => {}
            LogicalPlan::Limit(_) => {
                return plan_err!("LIMIT is not currently supported ({})", node.display());
            }
            LogicalPlan::Statement(s) => {
                return plan_err!("Unsupported statement: {}", s.display());
            }
            LogicalPlan::Values(_) => {}
            LogicalPlan::Explain(_) => {
                return plan_err!("EXPLAIN is not supported ({})", node.display());
            }
            LogicalPlan::Analyze(_) => {
                return plan_err!("ANALYZE is not supported ({})", node.display());
            }
            LogicalPlan::Extension(_) => {}
            LogicalPlan::Distinct(_) => {}
            LogicalPlan::Prepare(_) => {
                return plan_err!("Prepared statements are not supported ({})", node.display())
            }
            LogicalPlan::Dml(_) => {}
            LogicalPlan::Ddl(_) => {}
            LogicalPlan::Copy(_) => {
                return plan_err!("COPY is not supported ({})", node.display());
            }
            LogicalPlan::DescribeTable(_) => {
                return plan_err!("DESCRIBE is not supported ({})", node.display());
            }
            LogicalPlan::Unnest(_) => {}
            LogicalPlan::RecursiveQuery(_) => {
                return plan_err!("Recursive CTEs are not supported ({})", node.display());
            }
        }
        Ok(node)
    }
}
