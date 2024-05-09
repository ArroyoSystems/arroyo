use std::sync::Arc;

use anyhow::{bail, Result};

use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::{
    df::{ArroyoSchema, ArroyoSchemaRef},
    IS_RETRACT_FIELD,
};
use datafusion::common::{
    plan_err, DFSchemaRef, DataFusionError, OwnedTableReference, Result as DFResult,
};

use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};

use prost::Message;

use crate::{
    builder::{NamedNode, Planner},
    tables::Table,
};

use super::{
    debezium::ToDebeziumExtension, remote_table::RemoteTableExtension, ArroyoExtension,
    NodeWithIncomingEdges,
};

pub(crate) const SINK_NODE_NAME: &str = "SinkExtension";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SinkExtension {
    pub(crate) name: OwnedTableReference,
    pub(crate) table: Table,
    pub(crate) schema: DFSchemaRef,
    input: Arc<LogicalPlan>,
}

impl SinkExtension {
    pub fn new(
        name: OwnedTableReference,
        table: Table,
        mut schema: DFSchemaRef,
        mut input: Arc<LogicalPlan>,
    ) -> DFResult<Self> {
        let input_is_updating = input
            .schema()
            .has_column_with_unqualified_name(IS_RETRACT_FIELD);
        match &table {
            Table::ConnectorTable(connector_table) => {
                match (input_is_updating, connector_table.is_updating()) {
                    (_, true) => {
                        let to_debezium_extension =
                            ToDebeziumExtension::try_new(input.as_ref().clone())?;
                        input = Arc::new(LogicalPlan::Extension(Extension {
                            node: Arc::new(to_debezium_extension),
                        }));
                        schema = input.schema().clone();
                    }
                    (true, false) => {
                        return plan_err!("input is updating, but sink is not updating");
                    }
                    (false, false) => {}
                }
            }
            Table::MemoryTable { .. } => return plan_err!("memory tables not supported"),
            Table::TableFromQuery { .. } => {}
            Table::PreviewSink { .. } => {
                if input_is_updating {
                    let to_debezium_extension =
                        ToDebeziumExtension::try_new(input.as_ref().clone())?;
                    input = Arc::new(LogicalPlan::Extension(Extension {
                        node: Arc::new(to_debezium_extension),
                    }));
                    schema = input.schema().clone();
                }
            }
        }
        Self::add_remote_if_necessary(&name, &schema, &mut input);

        Ok(Self {
            name,
            table,
            schema,
            input,
        })
    }

    /* The input to a sink needs to be a non-transparent logical plan extension.
      If it isn't, wrap the input in a RemoteTableExtension.
    */
    pub fn add_remote_if_necessary(
        name: &OwnedTableReference,
        schema: &DFSchemaRef,
        input: &mut Arc<LogicalPlan>,
    ) {
        if let LogicalPlan::Extension(node) = input.as_ref() {
            let arroyo_extension: &dyn ArroyoExtension = (&node.node).try_into().unwrap();
            if !arroyo_extension.transparent() {
                return;
            }
        }
        let remote_table_extension = RemoteTableExtension {
            input: input.as_ref().clone(),
            name: name.clone(),
            schema: schema.clone(),
            materialize: false,
        };
        *input = Arc::new(LogicalPlan::Extension(Extension {
            node: Arc::new(remote_table_extension),
        }));
    }
}

impl UserDefinedLogicalNodeCore for SinkExtension {
    fn name(&self) -> &str {
        SINK_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
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
            "SinkExtension({:?}): {}",
            self.name,
            self.schema
                .fields()
                .iter()
                .map(|f| f.qualified_name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Self {
            name: self.name.clone(),
            table: self.table.clone(),
            schema: self.schema.clone(),
            input: Arc::new(inputs[0].clone()),
        }
    }
}

impl ArroyoExtension for SinkExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        _planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        if input_schemas.len() != 1 {
            bail!("sink should have exactly one input");
        }
        // should have exactly one input
        let input_schema = input_schemas[0].clone();

        let operator_config = (self.table.connector_op().map_err(|e| {
            DataFusionError::Plan(format!("failed to calculate connector op error: {}", e))
        })?)
        .encode_to_vec();
        let node = LogicalNode {
            operator_id: format!("sink_{}_{}", self.name, index),
            description: self.table.connector_op().unwrap().description.clone(),
            operator_name: OperatorName::ConnectorSink,
            parallelism: 1,
            operator_config,
        };
        let edge = LogicalEdge::project_all(LogicalEdgeType::Forward, (*input_schema).clone());
        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![edge],
        })
    }

    fn output_schema(&self) -> ArroyoSchema {
        // this is kinda fake?
        ArroyoSchema::from_schema_keys(Arc::new(self.input.schema().as_ref().into()), vec![])
            .unwrap()
    }
}
