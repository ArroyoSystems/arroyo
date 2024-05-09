use crate::extension::debezium::DebeziumUnrollingExtension;
use crate::extension::remote_table::RemoteTableExtension;
use crate::extension::sink::SinkExtension;
use crate::extension::table_source::TableSourceExtension;
use crate::extension::watermark_node::WatermarkNode;
use crate::schemas::add_timestamp_field;
use crate::tables::ConnectorTable;
use crate::tables::FieldSpec;
use crate::tables::Table;
use crate::{ArroyoSchemaProvider, ASYNC_RESULT_FIELD};

use arrow_schema::DataType;
use arroyo_rpc::IS_RETRACT_FIELD;
use arroyo_rpc::TIMESTAMP_FIELD;

use crate::extension::AsyncUDFExtension;
use arroyo_udf_host::parse::{AsyncOptions, UdfType};
use datafusion::common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion::common::{
    plan_err, Column, DFField, DFSchema, DataFusionError, OwnedTableReference, Result as DFResult,
    ScalarValue,
};
use datafusion::logical_expr;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    BinaryExpr, Expr, Extension, LogicalPlan, Projection, ScalarFunctionDefinition, TableScan,
    Unnest,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// Rewrites a logical plan to move projections out of table scans
/// and into a separate projection node which may include virtual fields,
/// and adds a watermark node.
pub struct SourceRewriter<'a> {
    pub(crate) schema_provider: &'a ArroyoSchemaProvider,
}

impl<'a> SourceRewriter<'a> {
    fn watermark_expression(table: &ConnectorTable) -> DFResult<Expr> {
        let expr = match table.watermark_field.clone() {
            Some(watermark_field) => table
                .fields
                .iter()
                .find_map(|f| {
                    if f.field().name() == &watermark_field {
                        return match f {
                            FieldSpec::StructField(f) => Some(Expr::Column(Column {
                                relation: None,
                                name: f.name().to_string(),
                            })),
                            FieldSpec::VirtualField { expression, .. } => Some(expression.clone()),
                        };
                    }
                    None
                })
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Watermark field {} not found", watermark_field))
                })?,
            None => Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "_timestamp".to_string(),
                })),
                op: logical_expr::Operator::Minus,
                right: Box::new(Expr::Literal(ScalarValue::DurationNanosecond(Some(
                    Duration::from_secs(1).as_nanos() as i64,
                )))),
            }),
        };
        Ok(expr)
    }

    fn projection_expressions(
        table: &ConnectorTable,
        qualifier: &OwnedTableReference,
        projection: &Option<Vec<usize>>,
    ) -> DFResult<Vec<Expr>> {
        let mut expressions = table
            .fields
            .iter()
            .map(|field| match field {
                FieldSpec::StructField(f) => Expr::Column(Column {
                    relation: Some(qualifier.clone()),
                    name: f.name().to_string(),
                }),
                FieldSpec::VirtualField { field, expression } => expression
                    .clone()
                    .alias_qualified(Some(qualifier.clone()), field.name().to_string()),
            })
            .collect::<Vec<_>>();

        if let Some(projection) = projection {
            expressions = projection.iter().map(|i| expressions[*i].clone()).collect();
        }

        // Add event time field if present
        if let Some(event_time_field) = table.event_time_field.clone() {
            let event_time_field = table
                .fields
                .iter()
                .find_map(|f| {
                    if f.field().name() == &event_time_field {
                        return match f {
                            FieldSpec::StructField(f) => Some(Expr::Column(Column {
                                relation: Some(qualifier.clone()),
                                name: f.name().to_string(),
                            })),
                            FieldSpec::VirtualField { expression, .. } => Some(expression.clone()),
                        };
                    }
                    None
                })
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Event time field {} not found",
                        event_time_field
                    ))
                })?;

            let event_time_field =
                event_time_field.alias_qualified(Some(qualifier.clone()), "_timestamp".to_string());
            expressions.push(event_time_field);
        } else {
            expressions.push(Expr::Column(Column::new(
                Some(qualifier.clone()),
                TIMESTAMP_FIELD,
            )))
        }
        if table.is_updating() {
            expressions.push(Expr::Column(Column::new(
                Some(qualifier.clone()),
                IS_RETRACT_FIELD,
            )))
        }
        Ok(expressions)
    }

    fn projection(&self, table_scan: &TableScan, table: &ConnectorTable) -> DFResult<LogicalPlan> {
        let qualifier = table_scan.table_name.clone();

        let table_source_extension = LogicalPlan::Extension(Extension {
            node: Arc::new(TableSourceExtension::new(
                qualifier.to_owned(),
                table.clone(),
            )),
        });

        let (projection_input, projection) = if table.is_updating() {
            let mut projection_offsets = table_scan.projection.clone();
            if let Some(offsets) = projection_offsets.as_mut() {
                offsets.push(table.fields.len())
            }
            (
                LogicalPlan::Extension(Extension {
                    node: Arc::new(DebeziumUnrollingExtension::try_new(table_source_extension)?),
                }),
                None,
            )
        } else {
            (table_source_extension, table_scan.projection.clone())
        };

        Ok(LogicalPlan::Projection(Projection::try_new(
            Self::projection_expressions(table, &qualifier, &projection)?,
            Arc::new(projection_input),
        )?))
    }

    fn mutate_connector_table(
        &self,
        table_scan: &TableScan,
        table: &ConnectorTable,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let input = self.projection(table_scan, table)?;

        let schema = input.schema().clone();
        let remote = LogicalPlan::Extension(Extension {
            node: Arc::new(RemoteTableExtension {
                input,
                name: table_scan.table_name.to_owned(),
                schema,
                materialize: true,
            }),
        });

        let watermark_node = WatermarkNode::new(
            remote,
            table_scan.table_name.clone(),
            Self::watermark_expression(table)?,
        )
        .map_err(|err| {
            DataFusionError::Internal(format!("failed to create watermark expression: {}", err))
        })?;

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(watermark_node),
        })))
    }

    fn mutate_table_from_query(
        &self,
        table_scan: &TableScan,
        logical_plan: &LogicalPlan,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let column_expressions: Vec<_> = if let Some(projection) = &table_scan.projection {
            logical_plan
                .schema()
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(i, f)| {
                    if projection.contains(&i) {
                        Some(Expr::Column(Column::new(
                            f.qualifier().cloned(),
                            f.name().to_string(),
                        )))
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            logical_plan
                .schema()
                .fields()
                .iter()
                .map(|f| Expr::Column(Column::new(f.qualifier().cloned(), f.name().to_string())))
                .collect()
        };
        let expressions = column_expressions
            .into_iter()
            .zip(table_scan.projected_schema.fields().iter())
            .map(|(expr, field)| {
                expr.alias_qualified(field.qualifier().cloned(), field.name().to_string())
            })
            .collect();
        let projection = LogicalPlan::Projection(Projection::try_new_with_schema(
            expressions,
            Arc::new(logical_plan.clone()),
            table_scan.projected_schema.clone(),
        )?);
        Ok(Transformed::yes(projection))
    }
}

impl<'a> TreeNodeRewriter for SourceRewriter<'a> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        let LogicalPlan::TableScan(mut table_scan) = node else {
            return Ok(Transformed::no(node));
        };

        let table_name = table_scan.table_name.table();
        let table = self
            .schema_provider
            .get_table(table_name)
            .ok_or_else(|| DataFusionError::Plan(format!("Table {} not found", table_name)))?;

        match table {
            Table::ConnectorTable(table) => self.mutate_connector_table(&table_scan, table),
            Table::MemoryTable {
                name,
                fields: _,
                logical_plan,
            } => {
                let Some(logical_plan) = logical_plan else {
                    return plan_err!(
                        "Can't query from memory table {} without first inserting into it.",
                        name
                    );
                };
                // this can only be done here, otherwise the query planner will be upset about the timestamp column.
                table_scan.projected_schema = add_timestamp_field(
                    table_scan.projected_schema.clone(),
                    Some(table_scan.table_name.clone()),
                )?;

                self.mutate_table_from_query(&table_scan, logical_plan)
            }
            Table::TableFromQuery {
                name: _,
                logical_plan,
            } => self.mutate_table_from_query(&table_scan, logical_plan),
            Table::PreviewSink { .. } => Err(DataFusionError::Plan(
                "can't select from a preview sink".to_string(),
            )),
        }
    }
}

pub const UNNESTED_COL: &str = "__unnested";

pub struct UnnestRewriter {}

impl UnnestRewriter {
    fn split_unnest(expr: Expr) -> DFResult<(Expr, Option<Expr>)> {
        let mut c: Option<Expr> = None;

        let expr = expr.transform_up_mut(&mut |e| {
            if let Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(udf),
                args,
            }) = &e
            {
                if udf.name() == "unnest" {
                    match args.len() {
                        1 => {
                            if c.replace(args[0].clone()).is_some() {
                                return Err(DataFusionError::Plan(
                                    "Multiple unnests in expression, which is not allowed"
                                        .to_string(),
                                ));
                            };

                            return Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                                UNNESTED_COL,
                            ))));
                        }
                        n => {
                            panic!(
                                "Unnest has wrong number of arguments (expected 1, found {})",
                                n
                            );
                        }
                    }
                }
            };
            Ok(Transformed::no(e))
        })?;

        Ok((expr.data, c))
    }
}

impl TreeNodeRewriter for UnnestRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        let LogicalPlan::Projection(projection) = &node else {
            if node.expressions().iter().any(|e| {
                let e = Self::split_unnest(e.clone());
                e.is_err() || e.unwrap().1.is_some()
            }) {
                return Err(DataFusionError::Plan(
                    "unnest is only supported in SELECT statements".to_string(),
                ));
            }
            return Ok(Transformed::no(node));
        };

        let mut unnest = None;
        let exprs = projection
            .expr
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, expr)| {
                let (expr, opt) = Self::split_unnest(expr)?;
                let typ = if let Some(e) = opt {
                    if let Some(prev) = unnest.replace((e, i)) {
                        if &prev != unnest.as_ref().unwrap() {
                            return Err(DataFusionError::Plan("Projection contains multiple unnests, which is not currently supported".to_string()));
                        }
                    }
                    true
                } else {
                    false
                };

                Ok((expr, typ))
            })
            .collect::<DFResult<Vec<_>>>()?;

        if let Some((unnest_inner, unnest_idx)) = unnest {
            let produce_list = Arc::new(LogicalPlan::Projection(
                Projection::try_new(
                    exprs
                        .iter()
                        .cloned()
                        .map(|(e, is_unnest)| {
                            if is_unnest {
                                unnest_inner.clone().alias(UNNESTED_COL)
                            } else {
                                e
                            }
                        })
                        .collect(),
                    projection.input.clone(),
                )
                .unwrap(),
            ));

            let unnest_fields = produce_list
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    if i == unnest_idx {
                        let DataType::List(inner) = f.data_type() else {
                            return Err(DataFusionError::Plan(format!(
                                "Argument '{}' to unnest is not a List",
                                f.qualified_name()
                            )));
                        };

                        Ok(DFField::new_unqualified(
                            UNNESTED_COL,
                            inner.data_type().clone(),
                            inner.is_nullable(),
                        ))
                    } else {
                        Ok((*f).clone())
                    }
                })
                .collect::<DFResult<Vec<_>>>()?;

            let unnest_node = LogicalPlan::Unnest(Unnest {
                column: produce_list.schema().fields()[unnest_idx].qualified_column(),
                input: produce_list,
                schema: Arc::new(
                    DFSchema::new_with_metadata(unnest_fields, HashMap::new()).unwrap(),
                ),
                options: Default::default(),
            });

            let output_node = LogicalPlan::Projection(Projection::try_new(
                exprs
                    .iter()
                    .enumerate()
                    .map(|(i, (expr, has_unnest))| {
                        if *has_unnest {
                            expr.clone()
                        } else {
                            Expr::Column(unnest_node.schema().fields()[i].qualified_column())
                        }
                    })
                    .collect(),
                Arc::new(unnest_node),
            )?);

            Ok(Transformed::yes(output_node))
        } else {
            Ok(Transformed::no(LogicalPlan::Projection(projection.clone())))
        }
    }
}

pub struct AsyncUdfRewriter<'a> {
    provider: &'a ArroyoSchemaProvider,
}

type AsyncSplitResult = (String, AsyncOptions, Vec<Expr>);

impl<'a> AsyncUdfRewriter<'a> {
    pub fn new(provider: &'a ArroyoSchemaProvider) -> Self {
        Self { provider }
    }

    fn split_async(
        expr: Expr,
        provider: &ArroyoSchemaProvider,
    ) -> DFResult<(Expr, Option<AsyncSplitResult>)> {
        let mut c: Option<(String, AsyncOptions, Vec<Expr>)> = None;
        let expr = expr.transform_up_mut(&mut |e| {
            if let Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(udf),
                args,
            }) = &e
            {
                if let Some(UdfType::Async(opts)) =
                    provider.udf_defs.get(udf.name()).map(|udf| udf.udf_type)
                {
                    if c.replace((udf.name().to_string(), opts, args.clone()))
                        .is_some()
                    {
                        return plan_err!(
                            "multiple async calls in the same expression, which is not allowed"
                        );
                    }
                    return Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                        ASYNC_RESULT_FIELD,
                    ))));
                }
            }
            Ok(Transformed::no(e))
        })?;

        Ok((expr.data, c))
    }
}

impl<'a> TreeNodeRewriter for AsyncUdfRewriter<'a> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        let LogicalPlan::Projection(mut projection) = node else {
            for e in node.expressions() {
                if let (_, Some((udf, _, _))) = Self::split_async(e.clone(), self.provider)? {
                    return plan_err!(
                        "async UDFs are only supported in projections, but {udf} was called in another context"
                    );
                }
            }
            return Ok(Transformed::no(node));
        };

        let mut args = None;

        for e in projection.expr.iter_mut() {
            let (new_e, Some(udf)) = Self::split_async(e.clone(), self.provider)? else {
                continue;
            };

            if let Some((prev, _, _)) = args.replace(udf) {
                return plan_err!(
                    "Projection contains multiple async UDFs, which is not supported \
                    \n(hint: two async UDFs calls, {} and {}, appear in the same SELECT statement)",
                    prev,
                    args.unwrap().0
                );
            }

            *e = new_e;
        }

        let Some((name, opts, args)) = args else {
            return Ok(Transformed::no(LogicalPlan::Projection(projection)));
        };

        let udf = self.provider.dylib_udfs.get(&name).unwrap().clone();

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(AsyncUDFExtension {
                input: projection.input,
                name,
                udf,
                arg_exprs: args,
                final_exprs: projection.expr,
                ordered: opts.ordered,
                max_concurrency: opts.max_concurrency,
                timeout: opts.timeout,
                final_schema: projection.schema,
            }),
        })))
    }
}

pub struct SourceMetadataVisitor<'a> {
    schema_provider: &'a ArroyoSchemaProvider,
    pub connection_ids: HashSet<i64>,
}

impl<'a> SourceMetadataVisitor<'a> {
    pub fn new(schema_provider: &'a ArroyoSchemaProvider) -> Self {
        Self {
            schema_provider,
            connection_ids: HashSet::new(),
        }
    }
}

impl<'a> SourceMetadataVisitor<'a> {
    fn get_connection_id(&self, node: &LogicalPlan) -> Option<i64> {
        let LogicalPlan::Extension(Extension { node }) = node else {
            return None;
        };
        // extract the name if it is a sink or source.
        let table_name = match node.name() {
            "TableSourceExtension" => {
                let TableSourceExtension { name, .. } =
                    node.as_any().downcast_ref::<TableSourceExtension>()?;
                name.to_string()
            }
            "SinkExtension" => {
                let SinkExtension { name, .. } = node.as_any().downcast_ref::<SinkExtension>()?;
                name.to_string()
            }
            _ => return None,
        };
        let table = self.schema_provider.get_table(&table_name)?;
        match table {
            Table::ConnectorTable(table) => table.id,
            _ => None,
        }
    }
}

impl<'a> TreeNodeVisitor for SourceMetadataVisitor<'a> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {
        if let Some(id) = self.get_connection_id(node) {
            self.connection_ids.insert(id);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

struct TimeWindowExprChecker {}

pub struct TimeWindowUdfChecker {}

impl TreeNodeVisitor for TimeWindowExprChecker {
    type Node = Expr;

    fn f_down(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {
        if let Expr::ScalarFunction(ScalarFunction { func_def, args: _ }) = node {
            match func_def.name() {
                "tumble" | "hop" | "session" => {
                    return plan_err!(
                        "time window function {} is not allowed in this context. Are you missing a GROUP BY clause?",
                        func_def.name()
                    );
                }
                _ => {}
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

impl TreeNodeVisitor for TimeWindowUdfChecker {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {
        node.expressions().iter().try_for_each(|expr| {
            let mut checker = TimeWindowExprChecker {};
            expr.visit(&mut checker)?;
            Ok::<(), DataFusionError>(())
        })?;
        Ok(TreeNodeRecursion::Continue)
    }
}
