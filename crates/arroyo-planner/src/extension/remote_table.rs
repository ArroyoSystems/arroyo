use std::{fmt::Formatter, sync::Arc};

use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::{
    df::{ArroyoSchema, ArroyoSchemaRef},
    grpc::api::ValuePlanOperator,
};
use datafusion::common::{internal_err, plan_err, DFSchemaRef, Result, TableReference};

use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use prost::Message;

use crate::{
    builder::{NamedNode, Planner},
    multifield_partial_ord,
    physical::ArroyoPhysicalExtensionCodec,
};

use super::{ArroyoExtension, NodeWithIncomingEdges};

pub(crate) const REMOTE_TABLE_NAME: &str = "RemoteTableExtension";

/* Lightweight extension that allows us to segment the graph and merge nodes with the same name.
  An Extension Planner will be used to isolate computation to individual nodes.
*/
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RemoteTableExtension {
    pub(crate) input: LogicalPlan,
    pub(crate) name: TableReference,
    pub(crate) schema: DFSchemaRef,
    pub(crate) materialize: bool,
}

multifield_partial_ord!(RemoteTableExtension, input, name, materialize);

impl ArroyoExtension for RemoteTableExtension {
    fn node_name(&self) -> Option<NamedNode> {
        if self.materialize {
            Some(NamedNode::RemoteTable(self.name.to_owned()))
        } else {
            None
        }
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        match input_schemas.len() {
            0 => return plan_err!("RemoteTableExtension should have exactly one input"),
            1 => {}
            _multiple_inputs => {
                // check they are all the same
                let first = input_schemas[0].clone();
                for schema in input_schemas.iter().skip(1) {
                    if *schema != first {
                        return plan_err!(
                            "If a node has multiple inputs, they must all have the same schema"
                        );
                    }
                }
            }
        }
        let physical_plan = planner.sync_plan(&self.input)?;
        let physical_plan_node = PhysicalPlanNode::try_from_physical_plan(
            physical_plan,
            &ArroyoPhysicalExtensionCodec::default(),
        )?;
        let config = ValuePlanOperator {
            name: format!("value_calculation({})", self.name),
            physical_plan: physical_plan_node.encode_to_vec(),
        };
        let node = LogicalNode::single(
            index as u32,
            format!("value_{index}"),
            OperatorName::ArrowValue,
            config.encode_to_vec(),
            self.name.to_string(),
            1,
        );

        let edges = input_schemas
            .into_iter()
            .map(|schema| LogicalEdge::project_all(LogicalEdgeType::Forward, (*schema).clone()))
            .collect();
        Ok(NodeWithIncomingEdges { node, edges })
    }

    fn output_schema(&self) -> ArroyoSchema {
        ArroyoSchema::from_schema_keys(Arc::new(self.schema.as_ref().into()), vec![]).unwrap()
    }
}

impl UserDefinedLogicalNodeCore for RemoteTableExtension {
    fn name(&self) -> &str {
        REMOTE_TABLE_NAME
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

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RemoteTableExtension: {}", self.schema)
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("input size inconsistent");
        }

        Ok(Self {
            input: inputs[0].clone(),
            name: self.name.clone(),
            schema: self.schema.clone(),
            materialize: self.materialize,
        })
    }
}
