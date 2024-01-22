use crate::tables::FieldSpec;
use crate::tables::Table::ConnectorTable;
use crate::ArroyoSchemaProvider;
use arrow_schema::Schema;
use datafusion_common::tree_node::TreeNodeRewriter;
use datafusion_common::{Column, DFSchema, DataFusionError, Result as DFResult};
use datafusion_expr::{Expr, LogicalPlan, TableScan};
use std::sync::Arc;

/// Rewrites a logical plan to move projections out of table scans
/// and into a separate projection node which may include virtual fields.
pub struct SourceRewriter {
    pub(crate) schema_provider: ArroyoSchemaProvider,
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

        let ConnectorTable(table) = table else {
            return Ok(node);
        };

        let qualifier = table_scan.table_name.clone();

        let expressions = table
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
            .get_table_source_with_fields(table_name, non_virtual_fields)
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
            datafusion_expr::Projection::try_new(expressions, Arc::new(input_table_scan))?,
        ))
    }
}
