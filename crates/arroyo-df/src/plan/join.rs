use crate::extension::join::JoinExtension;
use crate::extension::key_calculation::KeyCalculationExtension;
use crate::plan::WindowDetectingVisitor;
use arroyo_datastream::WindowType;
use datafusion_common::tree_node::TreeNodeRewriter;
use datafusion_common::{
    Column, DFField, DFSchema, DataFusionError, JoinConstraint, JoinType, Result as DFResult,
    ScalarValue,
};
use datafusion_expr::expr::{Alias, ScalarFunction};
use datafusion_expr::{
    BinaryExpr, BuiltinScalarFunction, Case, Expr, Extension, Join, LogicalPlan, Projection,
};
use std::sync::Arc;

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
            .iter()
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
            (0..key_count).collect(),
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
