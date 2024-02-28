use crate::plan::{RemoteTableExtension, SinkExtension, TableSourceExtension};
use crate::tables::ConnectorTable;
use crate::tables::FieldSpec;
use crate::tables::Table;
use crate::watermark_node::WatermarkNode;
use crate::ArroyoSchemaProvider;

use arrow_schema::DataType;
use arroyo_rpc::TIMESTAMP_FIELD;

use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRewriter, TreeNodeVisitor, VisitRecursion,
};
use datafusion_common::{
    Column, DFField, DFSchema, DataFusionError, OwnedTableReference, Result as DFResult,
    ScalarValue,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
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
                op: datafusion_expr::Operator::Minus,
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
                FieldSpec::VirtualField { field, expression } => {
                    expression.clone().alias(field.name().to_string())
                }
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
        Ok(expressions)
    }

    fn projection(&self, table_scan: &TableScan, table: &ConnectorTable) -> DFResult<LogicalPlan> {
        let qualifier = table_scan.table_name.clone();

        let table_source_extension = TableSourceExtension::new(qualifier.to_owned(), table.clone());

        Ok(LogicalPlan::Projection(
            datafusion_expr::Projection::try_new(
                Self::projection_expressions(table, &qualifier, &table_scan.projection)?,
                Arc::new(LogicalPlan::Extension(Extension {
                    node: Arc::new(table_source_extension),
                })),
            )?,
        ))
    }
}

impl<'a> TreeNodeRewriter for SourceRewriter<'a> {
    type N = LogicalPlan;

    fn mutate(&mut self, node: Self::N) -> DFResult<Self::N> {
        let LogicalPlan::TableScan(table_scan) = &node else {
            return Ok(node);
        };

        let table_name = table_scan.table_name.table();
        let table = self
            .schema_provider
            .get_table(table_name)
            .ok_or_else(|| DataFusionError::Plan(format!("Table {} not found", table_name)))?;

        let Table::ConnectorTable(table) = table else {
            return Ok(node);
        };
        let input = self.projection(&table_scan, table)?;
        let schema = input.schema().clone();
        let remote = LogicalPlan::Extension(Extension {
            node: Arc::new(RemoteTableExtension {
                input,
                name: table_scan.table_name.to_owned(),
                schema,
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

        return Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(watermark_node),
        }));
    }
}

pub const UNNESTED_COL: &str = "__unnested";

pub struct UnnestRewriter {}

impl UnnestRewriter {
    fn split_unnest(expr: Expr) -> DFResult<(Expr, Option<Expr>)> {
        let mut c: Option<Expr> = None;

        let expr = expr.transform_up_mut(&mut |e| {
            match &e {
                Expr::ScalarFunction(ScalarFunction {
                    func_def: ScalarFunctionDefinition::UDF(udf),
                    args,
                }) => {
                    if udf.name() == "unnest" {
                        match args.len() {
                            1 => {
                                if c.replace(args[0].clone()).is_some() {
                                    return Err(DataFusionError::Plan(
                                        "Multiple unnests in expression, which is not allowed"
                                            .to_string(),
                                    ));
                                };

                                return Ok(Transformed::Yes(Expr::Column(
                                    Column::new_unqualified(UNNESTED_COL),
                                )));
                            }
                            n => {
                                panic!(
                                    "Unnest has wrong number of arguments (expected 1, found {})",
                                    n
                                );
                            }
                        }
                    }
                }
                _ => {}
            };
            Ok(Transformed::No(e))
        })?;

        Ok((expr, c))
    }
}

impl TreeNodeRewriter for UnnestRewriter {
    type N = LogicalPlan;

    fn mutate(&mut self, node: Self::N) -> DFResult<Self::N> {
        let LogicalPlan::Projection(projection) = &node else {
            if node.expressions().iter().any(|e| {
                let e = Self::split_unnest(e.clone());
                e.is_err() || e.unwrap().1.is_some()
            }) {
                return Err(DataFusionError::Plan(
                    "unnest is only supported in SELECT statements".to_string(),
                ));
            }
            return Ok(node);
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

            Ok(output_node)
        } else {
            Ok(LogicalPlan::Projection(projection.clone()))
        }
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
            Table::ConnectorTable(table) => table.id.clone(),
            _ => None,
        }
    }
}

impl<'a> TreeNodeVisitor for SourceMetadataVisitor<'a> {
    type N = LogicalPlan;

    fn pre_visit(&mut self, node: &Self::N) -> DFResult<VisitRecursion> {
        if let Some(id) = self.get_connection_id(node) {
            self.connection_ids.insert(id);
        }
        Ok(VisitRecursion::Continue)
    }
}
