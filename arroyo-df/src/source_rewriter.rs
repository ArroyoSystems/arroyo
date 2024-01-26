use crate::tables::ConnectorTable;
use crate::tables::FieldSpec;
use crate::tables::Table;
use crate::watermark_node::WatermarkNode;
use crate::ArroyoSchemaProvider;
use arrow_schema::Schema;
use datafusion_common::tree_node::TreeNodeRewriter;
use datafusion_common::{
    Column, DFSchema, DataFusionError, OwnedTableReference, Result as DFResult, ScalarValue,
};
use datafusion_expr::{BinaryExpr, Expr, Extension, LogicalPlan, TableScan};
use std::sync::Arc;
use std::time::Duration;

/// Rewrites a logical plan to move projections out of table scans
/// and into a separate projection node which may include virtual fields,
/// and adds a watermark node.
pub struct SourceRewriter {
    pub(crate) schema_provider: ArroyoSchemaProvider,
}

impl SourceRewriter {
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
            None => {
                // If no watermark field is present, calculate it as a fixed lateness duration of 1 second
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(Expr::Column(Column {
                        relation: None,
                        name: "_timestamp".to_string(),
                    })),
                    op: datafusion_expr::Operator::Minus,
                    right: Box::new(Expr::Literal(ScalarValue::DurationNanosecond(Some(
                        Duration::from_secs(1).as_nanos() as i64,
                    )))),
                })
            }
        };
        Ok(expr)
    }

    fn projection_expressions(
        table: &ConnectorTable,
        qualifier: &OwnedTableReference,
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

            let event_time_field = event_time_field.alias("_timestamp".to_string());
            expressions.push(event_time_field);
        };
        Ok(expressions)
    }

    fn projection(&self, table_scan: &TableScan, table: &ConnectorTable) -> DFResult<LogicalPlan> {
        let qualifier = table_scan.table_name.clone();
        let non_virtual_fields = table
            .fields
            .iter()
            .filter_map(|field| match field {
                FieldSpec::StructField(f) => Some(f.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        let table_scan_schema = DFSchema::try_from_qualified_schema(
            qualifier.clone(),
            &Schema::new(non_virtual_fields.clone()),
        )?;

        let table_scan_table_source = self
            .schema_provider
            .get_table_source_with_fields(&table.name, non_virtual_fields)
            .unwrap();

        let input_table_scan = LogicalPlan::TableScan(TableScan {
            table_name: table_scan.table_name.clone(),
            source: table_scan_table_source,
            projection: None, // None because we are taking it out
            projected_schema: Arc::new(table_scan_schema),
            filters: table_scan.filters.clone(),
            fetch: table_scan.fetch,
        });

        Ok(LogicalPlan::Projection(
            datafusion_expr::Projection::try_new(
                Self::projection_expressions(table, &qualifier)?,
                Arc::new(input_table_scan),
            )?,
        ))
    }
}

impl TreeNodeRewriter for SourceRewriter {
    type N = LogicalPlan;

    fn mutate(&mut self, node: Self::N) -> DFResult<Self::N> {
        let LogicalPlan::TableScan(table_scan) = node.clone() else {
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

        let watermark_schema = DFSchema::try_from_qualified_schema(
            table_scan.table_name.clone(),
            &Schema::new(
                table
                    .fields
                    .iter()
                    .map(|field| field.field().clone())
                    .collect::<Vec<_>>()
                    .clone(),
            ),
        )?;

        let watermark_node = WatermarkNode {
            input: self.projection(&table_scan, table)?,
            watermark_expression: Some(Self::watermark_expression(table)?),
            schema: Arc::new(watermark_schema),
        };

        return Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(watermark_node),
        }));
    }
}
