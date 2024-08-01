use crate::extension::join::JoinExtension;
use crate::extension::key_calculation::KeyCalculationExtension;
use crate::plan::WindowDetectingVisitor;
use crate::{DFField, fields_with_qualifiers, schema_from_df_fields_with_metadata};
use arrow_schema::{DataType, TimeUnit};
use arroyo_datastream::WindowType;
use arroyo_rpc::IS_RETRACT_FIELD;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::{
    not_impl_err, plan_err, Column, DFSchemaRef, DataFusionError, JoinConstraint, JoinType, Result,
    ScalarValue, TableReference,
};
use datafusion::logical_expr;
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{BinaryExpr, build_join_schema, Case, Expr, ExprSchemable, Extension, Join, LogicalPlan, Operator, Projection};
use datafusion::prelude::{coalesce, get_field};
use std::sync::Arc;

pub(crate) struct JoinRewriter {}

impl JoinRewriter {
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
            .has_column_with_unqualified_name(IS_RETRACT_FIELD)
        {
            return plan_err!("can't handle updating left side of join");
        }
        if right
            .schema()
            .has_column_with_unqualified_name(IS_RETRACT_FIELD)
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
                fields_with_qualifiers(&input.schema())
                    .iter()
                    .map(|field| Expr::Column(field.qualified_column())))
            .collect();
        
        println!("Join expression ({}): {:?}", name, join_expressions);

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

struct StructEqRewriter {
    schema: DFSchemaRef,
}

impl TreeNodeRewriter for StructEqRewriter {
    type Node = Expr;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        if let Expr::BinaryExpr(BinaryExpr {
            op: Operator::Eq,
            left,
            right,
        }) = &node
        {
            let (left_t, _) = left.data_type_and_nullable(&self.schema)?;
            let (right_t, _) = right.data_type_and_nullable(&self.schema)?;

            if let DataType::Struct(fields) = &left_t {
                if fields.iter().find(|e| e.data_type().is_nested()).is_some() {
                    return plan_err!("Joins on struct fields are only supported for structs with a single layer of nesting (in {})",
                        node.canonical_name());
                }

                if left_t != right_t {
                    return plan_err!(
                        "Joins on structs must have the same types on both sides of '=' (in {})",
                        node.canonical_name()
                    );
                }

                let mut exprs = fields.iter().map(|f| {
                    get_field((**left).clone(), f.name().clone())
                        .eq(get_field((**right).clone(), f.name().clone()))
                });

                let Some(mut expr) = exprs.next() else {
                    return plan_err!(
                        "Struct types used in join comparison must have at least one field"
                    );
                };

                for next in exprs {
                    expr = expr.and(next);
                }

                return Ok(Transformed::yes(expr));
            }
        }

        Ok(Transformed::no(node))
    }
}

impl TreeNodeRewriter for JoinRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        let LogicalPlan::Join(join) = node else {
            return Ok(Transformed::no(node));
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
            return not_impl_err!("can't handle join constraint other than ON");
        };
        Self::check_updating(&left, &right)?;

        if on.is_empty() && !is_instant {
            return not_impl_err!("Updating joins must include an equijoin condition");
        }

        let (left_expressions, right_expressions): (Vec<_>, Vec<_>) =
            on.clone().into_iter().unzip();

        // let filter = filter
        //     .map(|expr| {
        //         expr.rewrite(&mut StructEqRewriter {
        //             schema: schema.clone(),
        //         })
        //         .map(|e| e.data)
        //     })
        //     .transpose()?;
        
        println!("LEFT = {:?}\nRIGHT = {:?}", left, right);
        println!("LEFT SCHEMA = {:?}\nRIGHT SCHEMA = {:?}", left.schema(), right.schema());
        
        println!("FILTER = {:?}\nON={:?}", filter, on);

        let left_input = self.create_join_key_plan(left, left_expressions, "left")?;
        let right_input = self.create_join_key_plan(right, right_expressions, "right")?;
        let rewritten_join = LogicalPlan::Join(Join {
            schema: Arc::new(build_join_schema(left_input.schema(), right_input.schema(), &join_type)?),
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
        };

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(join_extension),
        })))
    }
}
