use std::{collections::HashMap, sync::Arc};

use anyhow::{bail, Result};

use arroyo_datastream::logical::{LogicalNode, OperatorName};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use datafusion::common::{DFField, DFSchema, DFSchemaRef, DataFusionError, OwnedTableReference};

use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use prost::Message;

use crate::{
    builder::{NamedNode, Planner},
    schemas::add_timestamp_field,
    tables::ConnectorTable,
};

use super::{ArroyoExtension, DebeziumUnrollingExtension, NodeWithIncomingEdges};
pub(crate) const TABLE_SOURCE_NAME: &str = "TableSourceExtension";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TableSourceExtension {
    pub(crate) name: OwnedTableReference,
    pub(crate) table: ConnectorTable,
    pub(crate) schema: DFSchemaRef,
}

impl TableSourceExtension {
    pub fn new(name: OwnedTableReference, table: ConnectorTable) -> Self {
        let physical_fields = table
            .fields
            .iter()
            .filter_map(|field| match field {
                crate::tables::FieldSpec::StructField(field) => {
                    Some(DFField::from_qualified(&name, Arc::new(field.clone())))
                }
                crate::tables::FieldSpec::VirtualField { .. } => None,
            })
            .collect::<Vec<_>>();
        let base_schema =
            Arc::new(DFSchema::new_with_metadata(physical_fields, HashMap::new()).unwrap());
        let schema = if table.is_updating() {
            DebeziumUnrollingExtension::as_debezium_schema(&base_schema, Some(name.clone()))
                .unwrap()
        } else {
            base_schema
        };
        let schema = add_timestamp_field(schema, Some(name.clone())).unwrap();
        Self {
            name,
            table,
            schema,
        }
    }
}

impl UserDefinedLogicalNodeCore for TableSourceExtension {
    fn name(&self) -> &str {
        TABLE_SOURCE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "TableSourceExtension: {}",
            self.schema
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, _exprs: &[Expr], _inputs: &[LogicalPlan]) -> Self {
        Self {
            name: self.name.clone(),
            table: self.table.clone(),
            schema: self.schema.clone(),
        }
    }
}

impl ArroyoExtension for TableSourceExtension {
    fn node_name(&self) -> Option<NamedNode> {
        Some(NamedNode::Source(self.name.clone()))
    }

    fn plan_node(
        &self,
        _planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        if !input_schemas.is_empty() {
            bail!("TableSourceExtension should not have inputs");
        }
        let sql_source = self.table.as_sql_source().map_err(|e| {
            DataFusionError::Plan(format!("Error turning table into a SQL source: {}", e))
        })?;
        let node = LogicalNode {
            operator_id: format!("source_{}_{}", self.name, index),
            description: sql_source.source.config.description.clone(),
            operator_name: OperatorName::ConnectorSource,
            operator_config: sql_source.source.config.encode_to_vec(),
            parallelism: 1,
        };
        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![],
        })
    }

    fn output_schema(&self) -> ArroyoSchema {
        ArroyoSchema::from_schema_keys(Arc::new(self.schema.as_ref().into()), vec![]).unwrap()
    }
}
