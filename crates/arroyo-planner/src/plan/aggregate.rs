use crate::extension::aggregate::AggregateExtension;
use crate::extension::key_calculation::KeyCalculationExtension;
use crate::extension::updating_aggregate::UpdatingAggregateExtension;
use crate::plan::WindowDetectingVisitor;
use crate::{
    fields_with_qualifiers, find_window, schema_from_df_fields_with_metadata, ArroyoSchemaProvider,
    DFField, WindowBehavior,
};
use arroyo_rpc::{IS_RETRACT_FIELD, TIMESTAMP_FIELD};
use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::{not_impl_err, plan_err, DFSchema, DataFusionError, Result};
use datafusion::logical_expr;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{aggregate_function, Aggregate, Expr, Extension, LogicalPlan};
use std::sync::Arc;
use tracing::debug;

pub struct AggregateRewriter<'a> {
    pub schema_provider: &'a ArroyoSchemaProvider,
}

impl<'a> AggregateRewriter<'a> {
    pub fn rewrite_non_windowed_aggregate(
        input: Arc<LogicalPlan>,
        mut key_fields: Vec<DFField>,
        group_expr: Vec<Expr>,
        mut aggr_expr: Vec<Expr>,
        schema: Arc<DFSchema>,
        schema_provider: &ArroyoSchemaProvider,
    ) -> Result<Transformed<LogicalPlan>> {
        if input
            .schema()
            .has_column_with_unqualified_name(IS_RETRACT_FIELD)
        {
            return plan_err!("can't currently nest updating aggregates");
        }

        let key_count = key_fields.len();
        key_fields.extend(fields_with_qualifiers(input.schema()));

        let key_schema = Arc::new(schema_from_df_fields_with_metadata(
            &key_fields,
            schema.metadata().clone(),
        )?);

        let mut key_projection_expressions = group_expr.clone();
        key_projection_expressions.extend(
            fields_with_qualifiers(input.schema())
                .iter()
                .map(|field| Expr::Column(field.qualified_column())),
        );

        let key_projection =
            LogicalPlan::Projection(logical_expr::Projection::try_new_with_schema(
                key_projection_expressions.clone(),
                input.clone(),
                key_schema.clone(),
            )?);

        debug!(
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
        let Ok(timestamp_field) = key_plan
            .schema()
            .qualified_field_with_unqualified_name(TIMESTAMP_FIELD)
        else {
            return plan_err!("no timestamp field found in schema");
        };
        let timestamp_field: DFField = timestamp_field.into();
        let column = timestamp_field.qualified_column();
        aggr_expr.push(Expr::AggregateFunction(AggregateFunction::new(
            aggregate_function::AggregateFunction::Max,
            vec![Expr::Column(column.clone())],
            false,
            None,
            None,
            None,
        )));
        let mut output_schema_fields = fields_with_qualifiers(&schema);
        output_schema_fields.push(timestamp_field.clone());
        let output_schema = Arc::new(schema_from_df_fields_with_metadata(
            &output_schema_fields,
            schema.metadata().clone(),
        )?);
        let aggregate = Aggregate::try_new_with_schema(
            Arc::new(key_plan),
            group_expr,
            aggr_expr,
            output_schema,
        )?;
        debug!(
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
            schema_provider.planning_options.ttl,
        );
        let final_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(updating_aggregate_extension),
        });
        Ok(Transformed::yes(final_plan))
    }
}

impl<'a> TreeNodeRewriter for AggregateRewriter<'a> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
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
            .collect::<Result<Vec<_>>>()?;

        if window_group_expr.len() > 1 {
            return not_impl_err!(
                "do not support {} window expressions in group by",
                window_group_expr.len()
            );
        }

        let mut key_fields: Vec<DFField> = fields_with_qualifiers(&schema)
            .iter()
            .take(group_expr.len())
            .cloned()
            .map(|field| {
                DFField::new(
                    field.qualifier().cloned(),
                    format!("_key_{}", field.name()),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>();

        let mut window_detecting_visitor = WindowDetectingVisitor::default();
        input.visit_with_subqueries(&mut window_detecting_visitor)?;

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
                        let window_field = schema.qualified_field(window_index).into();
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
                let window_field = schema.qualified_field(window_index).into();
                WindowBehavior::FromOperator {
                    window: window_type,
                    window_field,
                    window_index,
                    is_nested: false,
                }
            }
            (false, false) => {
                return Self::rewrite_non_windowed_aggregate(
                    input,
                    key_fields,
                    group_expr,
                    aggr_expr,
                    schema,
                    self.schema_provider,
                );
            }
        };

        let key_count = key_fields.len();
        key_fields.extend(fields_with_qualifiers(input.schema()));

        let key_schema = Arc::new(schema_from_df_fields_with_metadata(
            &key_fields,
            schema.metadata().clone(),
        )?);

        let mut key_projection_expressions = group_expr.clone();
        key_projection_expressions.extend(
            fields_with_qualifiers(input.schema())
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
        let mut aggregate_schema_fields = fields_with_qualifiers(&schema);
        if let WindowBehavior::FromOperator {
            window: _,
            window_field: _,
            window_index,
            is_nested: _,
        } = &window_behavior
        {
            aggregate_schema_fields.remove(*window_index);
        }
        let internal_schema = Arc::new(schema_from_df_fields_with_metadata(
            &aggregate_schema_fields,
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
