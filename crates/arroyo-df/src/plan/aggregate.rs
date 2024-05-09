use crate::extension::aggregate::AggregateExtension;
use crate::extension::key_calculation::KeyCalculationExtension;
use crate::extension::updating_aggregate::UpdatingAggregateExtension;
use crate::plan::WindowDetectingVisitor;
use crate::{find_window, WindowBehavior};
use arroyo_rpc::{IS_RETRACT_FIELD, TIMESTAMP_FIELD};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::{
    not_impl_err, plan_err, DFField, DFSchema, DataFusionError, Result as DFResult,
};
use datafusion::logical_expr;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{aggregate_function, Aggregate, Expr, Extension, LogicalPlan};
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Default)]
pub struct AggregateRewriter {}

impl AggregateRewriter {
    pub fn rewrite_non_windowed_aggregate(
        input: Arc<LogicalPlan>,
        mut key_fields: Vec<DFField>,
        group_expr: Vec<Expr>,
        mut aggr_expr: Vec<Expr>,
        schema: Arc<DFSchema>,
    ) -> DFResult<Transformed<LogicalPlan>> {
        if input
            .schema()
            .has_column_with_unqualified_name(IS_RETRACT_FIELD)
        {
            return plan_err!("can't currently nest updating aggregates");
        }

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
                .map(|field| Expr::Column(field.qualified_column())),
        );

        let key_projection =
            LogicalPlan::Projection(logical_expr::Projection::try_new_with_schema(
                key_projection_expressions.clone(),
                input.clone(),
                key_schema.clone(),
            )?);

        info!(
            "key projection fields: {:?}",
            key_projection
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );

        let key_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(KeyCalculationExtension::new(
                key_projection,
                (0..key_count).collect(),
            )),
        });
        let Some(timestamp_field) = key_plan
            .schema()
            .fields()
            .iter()
            .find(|field| field.name() == TIMESTAMP_FIELD)
        else {
            return plan_err!("no timestamp field found in schema");
        };
        let column = timestamp_field.qualified_column();
        aggr_expr.push(Expr::AggregateFunction(AggregateFunction::new(
            aggregate_function::AggregateFunction::Max,
            vec![Expr::Column(column.clone())],
            false,
            None,
            None,
            None,
        )));
        let mut output_schema_fields = schema.fields().clone();
        output_schema_fields.push(timestamp_field.clone());
        let output_schema = Arc::new(DFSchema::new_with_metadata(
            output_schema_fields,
            schema.metadata().clone(),
        )?);
        let aggregate = Aggregate::try_new_with_schema(
            Arc::new(key_plan),
            group_expr,
            aggr_expr,
            output_schema,
        )?;
        info!(
            "aggregate field names: {:?}",
            aggregate
                .schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );
        let updating_aggregate_extension = UpdatingAggregateExtension::new(
            LogicalPlan::Aggregate(aggregate),
            (0..key_count).collect(),
            column.relation,
        );
        let final_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(updating_aggregate_extension),
        });
        Ok(Transformed::yes(final_plan))
    }
}

impl TreeNodeRewriter for AggregateRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        let LogicalPlan::Aggregate(Aggregate {
            input,
            mut group_expr,
            aggr_expr,
            schema,
            ..
        }) = node
        else {
            return Ok(Transformed::no(node));
        };
        let mut window_group_expr: Vec<_> = group_expr
            .iter()
            .enumerate()
            .filter_map(|(i, expr)| {
                find_window(expr)
                    .map(|option| option.map(|inner| (i, inner)))
                    .transpose()
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;

        if window_group_expr.len() > 1 {
            return not_impl_err!(
                "do not support {} window expressions in group by",
                window_group_expr.len()
            );
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

        let mut window_detecting_visitor = WindowDetectingVisitor::default();
        input.visit(&mut window_detecting_visitor)?;

        let window = window_detecting_visitor.window;
        let window_behavior = match (window.is_some(), !window_group_expr.is_empty()) {
            (true, true) => {
                let input_window = window.unwrap();
                let (window_index, group_by_window_type) = window_group_expr.pop().unwrap();
                if group_by_window_type != input_window {
                    return Err(DataFusionError::NotImplemented(
                        "window in group by does not match input window".to_string(),
                    ));
                }
                let matching_field = window_detecting_visitor.fields.iter().next();
                match matching_field {
                    Some(field) => {
                        group_expr[window_index] = Expr::Column(field.qualified_column());
                        WindowBehavior::InData
                    }
                    None => {
                        if matches!(input_window, arroyo_datastream::WindowType::Session { .. }) {
                            return plan_err!(
                                "can't reinvoke session window in nested aggregates. Need to pass the window struct up from the source query."
                            );
                        }
                        group_expr.remove(window_index);
                        key_fields.remove(window_index);
                        let window_field = schema.field(window_index).clone();
                        WindowBehavior::FromOperator {
                            window: input_window,
                            window_field,
                            window_index,
                            is_nested: true,
                        }
                    }
                }
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
                    is_nested: false,
                }
            }
            (false, false) => {
                return Self::rewrite_non_windowed_aggregate(
                    input, key_fields, group_expr, aggr_expr, schema,
                );
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
                .map(|field| Expr::Column(field.qualified_column())),
        );

        let key_projection =
            LogicalPlan::Projection(logical_expr::Projection::try_new_with_schema(
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
            is_nested: _,
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
        // check that the windowing is correct
        WindowDetectingVisitor::get_window(&final_plan)?;
        Ok(Transformed::yes(final_plan))
    }
}
