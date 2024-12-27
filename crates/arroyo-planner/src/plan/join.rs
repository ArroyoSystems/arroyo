use crate::extension::join::JoinExtension;
use crate::extension::key_calculation::KeyCalculationExtension;
use crate::plan::WindowDetectingVisitor;
use crate::{fields_with_qualifiers, schema_from_df_fields_with_metadata, ArroyoSchemaProvider};
use arroyo_datastream::WindowType;
use arroyo_rpc::UPDATING_META_FIELD;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor};
use datafusion::common::{
    not_impl_err, plan_err, Column, DataFusionError, JoinConstraint, JoinType, Result, ScalarValue,
    TableReference,
};
use datafusion::logical_expr;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{
    build_join_schema, BinaryExpr, Case, Expr, Extension, Join, LogicalPlan, Projection,
};
use datafusion::prelude::coalesce;
use std::sync::Arc;
use crate::extension::lookup::{LookupJoin, LookupSource};
use crate::schemas::add_timestamp_field;
use crate::tables::ConnectorTable;

pub(crate) struct JoinRewriter<'a> {
    pub schema_provider: &'a ArroyoSchemaProvider,
}

impl JoinRewriter<'_> {
    fn check_join_windowing(join: &Join) -> Result<bool> {
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

    fn check_updating(left: &LogicalPlan, right: &LogicalPlan) -> Result<()> {
        if left
            .schema()
            .has_column_with_unqualified_name(UPDATING_META_FIELD)
        {
            return plan_err!("can't handle updating left side of join");
        }
        if right
            .schema()
            .has_column_with_unqualified_name(UPDATING_META_FIELD)
        {
            return plan_err!("can't handle updating right side of join");
        }
        Ok(())
    }

    fn create_join_key_plan(
        &self,
        input: Arc<LogicalPlan>,
        join_expressions: Vec<Expr>,
        name: &'static str,
    ) -> Result<LogicalPlan> {
        let key_count = join_expressions.len();

        let join_expressions: Vec<_> = join_expressions
            .into_iter()
            .enumerate()
            .map(|(index, expr)| {
                expr.alias_qualified(
                    Some(TableReference::bare("_arroyo")),
                    format!("_key_{}", index),
                )
            })
            .chain(
                fields_with_qualifiers(input.schema())
                    .iter()
                    .map(|field| Expr::Column(field.qualified_column())),
            )
            .collect();

        // Calculate initial projection with default names
        let projection = Projection::try_new(join_expressions, input)?;
        let key_calculation_extension = KeyCalculationExtension::new_named_and_trimmed(
            LogicalPlan::Projection(projection),
            (0..key_count).collect(),
            name.to_string(),
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(key_calculation_extension),
        }))
    }

    fn post_join_timestamp_projection(&mut self, input: LogicalPlan) -> Result<LogicalPlan> {
        let schema = input.schema().clone();
        let mut schema_with_timestamp = fields_with_qualifiers(&schema);
        let timestamp_fields = schema_with_timestamp
            .iter()
            .filter(|field| field.name() == "_timestamp")
            .cloned()
            .collect::<Vec<_>>();

        if timestamp_fields.len() != 2 {
            return not_impl_err!("join must have two timestamp fields");
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

        let output_schema = Arc::new(schema_from_df_fields_with_metadata(
            &schema_with_timestamp,
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
                op: logical_expr::Operator::GtEq,
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
            else_expr: Some(Box::new(coalesce(vec![
                left_column.clone(),
                right_column.clone(),
            ]))),
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

#[derive(Default)]
struct FindLookupExtension {
    table: Option<ConnectorTable>,
    filter: Option<Expr>,
    alias: Option<TableReference>,
}

impl <'a> TreeNodeVisitor<'a> for FindLookupExtension {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        match node {
            LogicalPlan::Extension(e) => {
                if let Some(s) = e.node.as_any().downcast_ref::<LookupSource>() {
                    self.table = Some(s.table.clone());
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            LogicalPlan::Filter(filter) => {
                if self.filter.replace(filter.predicate.clone()).is_some() {
                    return plan_err!("multiple filters found in lookup join, which is not supported");
                }
            }
            LogicalPlan::SubqueryAlias(s) => {
                self.alias = Some(s.alias.clone());
            }
            _ => {
                return plan_err!("lookup tables must be used directly within a join");
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

fn has_lookup(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|p| Ok(match p {
        LogicalPlan::Extension(e) => e.node.as_any().is::<LookupSource>(),
        _ => false
    }))
}

fn maybe_plan_lookup_join(join: &Join) -> Result<Option<LogicalPlan>> {
    println!("Planning lookup join");
    if has_lookup(&join.left)? {
        return plan_err!("lookup sources must be on the right side of an inner or left join");
    }
    
    if !has_lookup(&join.right)? {
        return Ok(None);
    }
    
    println!("JOin = {:?} {:?}\n{:#?}", join.join_constraint, join.join_type, join.on);
    
    match join.join_type {
        JoinType::Inner | JoinType::Left => {}
        t => {
            return plan_err!("{} join is not supported for lookup tables; must be a left or inner join", t);
        }
    }
    
    if join.filter.is_some() {
        return plan_err!("filter join conditions are not supported for lookup joins; must have an equality condition");
    }

    let mut lookup = FindLookupExtension::default();
    join.right.visit(&mut lookup)?;

    let connector = lookup.table.expect("right side of join does not have lookup");

    Ok(Some(LogicalPlan::Extension(Extension {
        node: Arc::new(LookupJoin {
            input: (*join.left).clone(),
            schema: add_timestamp_field(join.schema.clone(), None)?,
            connector,
            on: join.on.clone(),
            filter: lookup.filter,
            alias: lookup.alias,
        })
    })))
}

impl TreeNodeRewriter for JoinRewriter<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        let LogicalPlan::Join(join) = node else {
            return Ok(Transformed::no(node));
        };
        
        if let Some(plan) = maybe_plan_lookup_join(&join)? {
            return Ok(Transformed::yes(plan));
        }
        
        let is_instant = Self::check_join_windowing(&join)?;

        let Join {
            left,
            right,
            on,
            filter,
            join_type,
            join_constraint: JoinConstraint::On,
            schema: _,
            null_equals_null: false,
        } = join
        else {
            return not_impl_err!("can't handle join constraint other than ON");
        };
        Self::check_updating(&left, &right)?;

        if on.is_empty() && !is_instant {
            return not_impl_err!("Updating joins must include an equijoin condition");
        }

        let (left_expressions, right_expressions): (Vec<_>, Vec<_>) =
            on.clone().into_iter().unzip();

        let left_input = self.create_join_key_plan(left, left_expressions, "left")?;
        let right_input = self.create_join_key_plan(right, right_expressions, "right")?;
        let rewritten_join = LogicalPlan::Join(Join {
            schema: Arc::new(build_join_schema(
                left_input.schema(),
                right_input.schema(),
                &join_type,
            )?),
            left: Arc::new(left_input),
            right: Arc::new(right_input),
            on,
            join_type,
            join_constraint: JoinConstraint::On,
            null_equals_null: false,
            filter,
        });

        let final_logical_plan = self.post_join_timestamp_projection(rewritten_join)?;

        let join_extension = JoinExtension {
            rewritten_join: final_logical_plan,
            is_instant,
            // only non-instant (updating) joins have a TTL
            ttl: (!is_instant).then_some(self.schema_provider.planning_options.ttl),
        };

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(join_extension),
        })))
    }
}
