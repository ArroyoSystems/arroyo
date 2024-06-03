use std::{collections::HashMap, sync::Arc};

use arroyo_datastream::WindowType;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{
    plan_err,
    tree_node::{TreeNode, TreeNodeRewriter},
    DFSchema, Result as DFResult,
};
use datafusion::logical_expr;
use datafusion::logical_expr::{
    expr::WindowFunction, Expr, Extension, LogicalPlan, Projection, Sort, Window,
};
use tracing::debug;

use crate::{
    extension::{key_calculation::KeyCalculationExtension, window_fn::WindowFunctionExtension},
    plan::extract_column,
};

use super::WindowDetectingVisitor;

pub(crate) struct WindowFunctionRewriter {}

fn get_window_and_name(expr: &Expr) -> DFResult<(WindowFunction, String)> {
    match expr {
        Expr::Alias(alias) => {
            let (window, _) = get_window_and_name(&alias.expr)?;
            Ok((window, alias.name.clone()))
        }
        Expr::WindowFunction(window_function) => {
            Ok((window_function.clone(), expr.name_for_alias()?))
        }
        _ => plan_err!("Expect a column or alias expression, not {:?}", expr),
    }
}

impl TreeNodeRewriter for WindowFunctionRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        let LogicalPlan::Window(window) = node else {
            return Ok(Transformed::no(node));
        };
        debug!(
            "Rewriting window function: {:?}",
            LogicalPlan::Window(window.clone())
        );
        let mut window_detecting_visitor = WindowDetectingVisitor::default();
        window.input.visit(&mut window_detecting_visitor)?;

        let Some(input_window) = window_detecting_visitor.window else {
            return plan_err!("Window functions require already windowed input");
        };
        if matches!(input_window, WindowType::Session { .. }) {
            return plan_err!("Window functions do not support session windows");
        }

        let input_window_fields = window_detecting_visitor.fields;

        let Window {
            input, window_expr, ..
        } = window;
        // error if there isn't exactly one window expr
        if window_expr.len() != 1 {
            return plan_err!("Window functions require exactly one window expression");
        }
        // the window_expr can be renamed by optimizers, in which case there will be an alias
        let (
            WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
                null_treatment,
            },
            original_name,
        ) = get_window_and_name(&window_expr[0])?;

        let mut window_field: Vec<_> = partition_by
            .iter()
            .enumerate()
            .filter_map(|(index, expr)| {
                if let Some(column) = extract_column(expr) {
                    let Ok(input_field) = input
                        .schema()
                        .field_with_name(column.relation.as_ref(), &column.name)
                    else {
                        return Some(plan_err!(
                            "Column {} not found in input schema",
                            column.name
                        ));
                    };
                    if input_window_fields.contains(input_field) {
                        return Some(Ok((input_field.clone(), index)));
                    }
                }
                None
            })
            .collect::<DFResult<_>>()?;
        if window_field.len() != 1 {
            return plan_err!(
                "Window function requires exactly one window expression in partition_by"
            );
        }

        let (_window_field, index) = window_field.pop().unwrap();
        let mut additional_keys = partition_by.clone();
        // because the operator will have grouped by the timestamp of each row,
        // don't need to shuffle or partition by the window.
        additional_keys.remove(index);
        let key_count = additional_keys.len();

        let new_window_func = WindowFunction {
            fun,
            args,
            partition_by: additional_keys.clone(),
            order_by,
            window_frame,
            null_treatment,
        };

        let mut key_projection_expressions: Vec<_> = additional_keys
            .iter()
            .enumerate()
            .map(|(index, expression)| expression.clone().alias(format!("_key_{}", index)))
            .collect();

        key_projection_expressions.extend(
            input
                .schema()
                .fields()
                .iter()
                .map(|field| Expr::Column(field.qualified_column())),
        );

        // this two-step sequence is necessary because we need to
        // know the types of the partition expressions before constructing the appropriate schema.
        let auto_schema =
            Projection::try_new(key_projection_expressions.clone(), input.clone())?.schema;
        let mut key_fields = auto_schema
            .fields()
            .iter()
            .take(additional_keys.clone().len())
            .cloned()
            .collect::<Vec<_>>();
        key_fields.extend(input.schema().fields().iter().cloned());
        let key_schema = Arc::new(DFSchema::new_with_metadata(key_fields, HashMap::new())?);
        let key_projection = LogicalPlan::Projection(Projection::try_new_with_schema(
            key_projection_expressions,
            input.clone(),
            key_schema,
        )?);
        let key_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(KeyCalculationExtension::new(
                key_projection,
                (0..key_count).collect(),
            )),
        });

        let mut sort_expressions: Vec<_> = additional_keys
            .iter()
            .map(|partition| {
                Expr::Sort(logical_expr::expr::Sort {
                    expr: Box::new(partition.clone()),
                    asc: true,
                    nulls_first: false,
                })
            })
            .collect();
        sort_expressions.extend(new_window_func.order_by.clone());

        // This sort seems to be necessary to not fail at execution time.
        let shuffle = LogicalPlan::Sort(Sort {
            expr: sort_expressions,
            input: Arc::new(key_plan),
            fetch: None,
        });

        let window_expr = Expr::WindowFunction(new_window_func).alias_if_changed(original_name)?;

        let rewritten_window_plan =
            LogicalPlan::Window(Window::try_new(vec![window_expr], Arc::new(shuffle))?);

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(WindowFunctionExtension::new(
                rewritten_window_plan,
                (0..key_count).collect(),
            )),
        })))
    }
}
