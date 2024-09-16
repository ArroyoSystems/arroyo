use arroyo_datastream::WindowType;
use arroyo_rpc::{IS_RETRACT_FIELD, TIMESTAMP_FIELD};
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::common::{
    plan_err,
    tree_node::{TreeNode, TreeNodeRewriter, TreeNodeVisitor},
    Column, DataFusionError, Result, TableReference,
};
use std::{collections::HashSet, sync::Arc};

use aggregate::AggregateRewriter;
use datafusion::logical_expr::{
    expr::Alias, Aggregate, Expr, Extension, Filter, LogicalPlan, SubqueryAlias,
};
use join::JoinRewriter;

use self::window_fn::WindowFunctionRewriter;
use crate::rewriters::TimeWindowNullCheckRemover;
use crate::{
    extension::{
        aggregate::{AggregateExtension, AGGREGATE_EXTENSION_NAME},
        join::JOIN_NODE_NAME,
    },
    fields_with_qualifiers, find_window,
    rewriters::SourceRewriter,
    schema_from_df_fields_with_metadata,
    schemas::{add_timestamp_field, has_timestamp_field},
    ArroyoSchemaProvider, DFField, WindowBehavior,
};
use crate::{
    extension::{remote_table::RemoteTableExtension, ArroyoExtension},
    rewriters::AsyncUdfRewriter,
};

mod aggregate;
mod join;
mod window_fn;

#[derive(Debug, Default)]
struct WindowDetectingVisitor {
    window: Option<WindowType>,
    fields: HashSet<DFField>,
}

impl WindowDetectingVisitor {
    fn get_window(logical_plan: &LogicalPlan) -> Result<Option<WindowType>> {
        let mut visitor = WindowDetectingVisitor {
            window: None,
            fields: HashSet::new(),
        };
        logical_plan.visit_with_subqueries(&mut visitor)?;
        Ok(visitor.window.take())
    }
}

fn extract_column(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(column) => Some(column),
        Expr::Alias(Alias { expr, .. }) => extract_column(expr),
        _ => None,
    }
}

impl TreeNodeVisitor<'_> for WindowDetectingVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };

        // handle Join in the pre-join, as each side needs to be checked separately.
        if node.name() == JOIN_NODE_NAME {
            let input_windows: HashSet<_> = node
                .inputs()
                .iter()
                .map(|input| Self::get_window(input))
                .collect::<Result<HashSet<_>>>()?;
            if input_windows.len() > 1 {
                return Err(DataFusionError::Plan(
                    "can't handle mixed windowing between left and right".to_string(),
                ));
            }
            self.window = input_windows
                .into_iter()
                .next()
                .expect("join has at least one input");
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        match node {
            LogicalPlan::Projection(projection) => {
                let window_expressions = projection
                    .expr
                    .iter()
                    .enumerate()
                    .filter_map(|(index, expr)| {
                        if let Some(column) = extract_column(expr) {
                            let input_field = projection
                                .input
                                .schema()
                                .field_with_name(column.relation.as_ref(), &column.name);
                            let input_field = match input_field {
                                Ok(field) => field,
                                Err(err) => {
                                    return Some(Err(err));
                                }
                            };
                            if self.fields.contains(
                                &(column.relation.clone(), Arc::new(input_field.clone())).into(),
                            ) {
                                return self.window.clone().map(|window| Ok((index, window)));
                            }
                        }
                        find_window(expr)
                            .map(|option| option.map(|inner| (index, inner)))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.fields.clear();
                for (index, window) in window_expressions {
                    // if there's already a window they should match
                    if let Some(existing_window) = &self.window {
                        if *existing_window != window {
                            return plan_err!(
                                "can't window by both {:?} and {:?}",
                                existing_window,
                                window
                            );
                        }
                        self.fields
                            .insert(projection.schema.qualified_field(index).into());
                    } else {
                        // If the input doesn't have an input window, we shouldn't be creating a window.
                        return plan_err!(
                            "can't call a windowing function without grouping by it in an aggregate"
                        );
                    }
                }
            }
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                // translate the fields to the output schema
                self.fields = self
                    .fields
                    .drain()
                    .map(|field| {
                        Ok(subquery_alias
                            .schema
                            .qualified_field(
                                subquery_alias
                                    .input
                                    .schema()
                                    .index_of_column(&field.qualified_column())?,
                            )
                            .into())
                    })
                    .collect::<Result<HashSet<_>>>()?;
            }
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr: _,
                schema,
                ..
            }) => {
                let window_expressions = group_expr
                    .iter()
                    .enumerate()
                    .filter_map(|(index, expr)| {
                        if let Some(column) = extract_column(expr) {
                            let input_field = input
                                .schema()
                                .field_with_name(column.relation.as_ref(), &column.name);
                            let input_field = match input_field {
                                Ok(field) => field,
                                Err(err) => {
                                    return Some(Err(err));
                                }
                            };
                            if self
                                .fields
                                .contains(&(column.relation.as_ref(), input_field).into())
                            {
                                return self.window.clone().map(|window| Ok((index, window)));
                            }
                        }
                        find_window(expr)
                            .map(|option| option.map(|inner| (index, inner)))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.fields.clear();
                for (index, window) in window_expressions {
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
                    self.fields.insert(schema.qualified_field(index).into());
                }
            }
            LogicalPlan::Extension(Extension { node }) => {
                if node.name() == AGGREGATE_EXTENSION_NAME {
                    let aggregate_extension = node
                        .as_any()
                        .downcast_ref::<AggregateExtension>()
                        .expect("should be aggregate extension");

                    match &aggregate_extension.window_behavior {
                        WindowBehavior::FromOperator {
                            window,
                            window_field,
                            window_index: _,
                            is_nested,
                        } => {
                            if self.window.is_some() && !*is_nested {
                                return Err(DataFusionError::Plan(
                                    "aggregate node should not be recalculating window, as input is windowed.".to_string(),
                                ));
                            }
                            self.window = Some(window.clone());
                            self.fields.insert(window_field.clone());
                        }
                        WindowBehavior::InData => {
                            let input_fields = self.fields.clone();
                            self.fields.clear();
                            for field in fields_with_qualifiers(node.schema()) {
                                if input_fields.contains(&field) {
                                    self.fields.insert(field);
                                }
                            }
                            if self.fields.is_empty() {
                                return Err(DataFusionError::Plan(
                                    "must have window in aggregate. Make sure you are calling one of the windowing functions (hop, tumble, session) or using the window field of the input".to_string(),
                                ));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

// This is one rewriter so that we can rely on inputs having already been rewritten
// ensuring they have _timestamp field, amongst other things.
pub struct ArroyoRewriter<'a> {
    pub(crate) schema_provider: &'a ArroyoSchemaProvider,
}

impl<'a> TreeNodeRewriter for ArroyoRewriter<'a> {
    type Node = LogicalPlan;

    fn f_up(&mut self, mut node: Self::Node) -> Result<Transformed<Self::Node>> {
        match node {
            LogicalPlan::Projection(ref mut projection) => {
                if !has_timestamp_field(&projection.schema) {
                    let timestamp_field: DFField = projection
                        .input
                        .schema()
                        .qualified_field_with_unqualified_name(TIMESTAMP_FIELD).map_err(|_| {
                            DataFusionError::Plan(format!("No timestamp field found in projection input ({}). Query should've been rewritten", projection.input.display()))
                        })?.into();
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
                if projection
                    .input
                    .schema()
                    .has_column_with_unqualified_name(IS_RETRACT_FIELD)
                    && !projection
                        .schema
                        .has_column_with_unqualified_name(IS_RETRACT_FIELD)
                {
                    let field: DFField = projection
                        .input
                        .schema()
                        .qualified_field_with_unqualified_name(IS_RETRACT_FIELD)?
                        .into();
                    let mut output_fields = fields_with_qualifiers(&projection.schema);
                    output_fields.push(field.clone());
                    projection.schema = Arc::new(schema_from_df_fields_with_metadata(
                        &output_fields,
                        projection.schema.metadata().clone(),
                    )?);
                    projection.expr.push(Expr::Column(field.qualified_column()));
                }

                return AsyncUdfRewriter::new(self.schema_provider).f_up(node);
            }
            LogicalPlan::Aggregate(aggregate) => {
                return AggregateRewriter {
                    schema_provider: self.schema_provider,
                }
                .f_up(LogicalPlan::Aggregate(aggregate));
            }
            LogicalPlan::Join(join) => {
                return JoinRewriter {
                    schema_provider: self.schema_provider,
                }
                .f_up(LogicalPlan::Join(join));
            }
            LogicalPlan::TableScan(table_scan) => {
                return SourceRewriter {
                    schema_provider: self.schema_provider,
                }
                .f_up(LogicalPlan::TableScan(table_scan));
            }
            LogicalPlan::Filter(f) => {
                // Joins with windows in the join condition can cause IS NOT NULL predicates to get
                // pushed down to the table scan; however windows can never be null, and they can't
                // be evaluated in filters—so we just remove them
                let expr = f
                    .predicate
                    .clone()
                    .rewrite(&mut TimeWindowNullCheckRemover {})?;
                return Ok(if expr.transformed {
                    Transformed::yes(LogicalPlan::Filter(Filter::try_new(expr.data, f.input)?))
                } else {
                    Transformed::no(LogicalPlan::Filter(f))
                });
            }
            LogicalPlan::Window(_) => {
                return WindowFunctionRewriter {}.f_up(node);
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
            LogicalPlan::Union(mut union) => {
                union.schema = union.inputs[0].schema().clone();

                // Need all the elements of the union to be materialized
                for input in union.inputs.iter_mut() {
                    if let LogicalPlan::Extension(Extension { node }) = input.as_ref() {
                        let arroyo_extension: &dyn ArroyoExtension = node.try_into().unwrap();
                        if !arroyo_extension.transparent() {
                            continue;
                        }
                    }
                    let remote_table_extension = Arc::new(RemoteTableExtension {
                        input: input.as_ref().clone(),
                        name: TableReference::bare("union_input"),
                        schema: union.schema.clone(),
                        materialize: false,
                    });
                    *input = Arc::new(LogicalPlan::Extension(Extension {
                        node: remote_table_extension,
                    }));
                }

                return Ok(Transformed::yes(LogicalPlan::Union(union)));
            }
            LogicalPlan::EmptyRelation(_) => {}
            LogicalPlan::Subquery(_) => {}
            LogicalPlan::SubqueryAlias(sa) => {
                // recreate from our children, in case the schemas have changed
                return Ok(Transformed::yes(LogicalPlan::SubqueryAlias(
                    SubqueryAlias::try_new(sa.input, sa.alias)?,
                )));
            }
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
        Ok(Transformed::no(node))
    }
}
