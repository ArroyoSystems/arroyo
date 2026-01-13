use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef, Result, internal_err};
use std::{fmt::Formatter, sync::Arc};

use super::{ArroyoExtension, NodeWithIncomingEdges};
use crate::{
    DFField,
    builder::{NamedNode, Planner},
    multifield_partial_ord, schema_from_df_fields,
};
use arroyo_rpc::grpc::api::ProjectionOperator;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use itertools::Itertools;
use prost::Message;

pub(crate) const PROJECTION_NAME: &str = "ProjectionExtension";

/// Projection operations
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ProjectionExtension {
    pub(crate) inputs: Vec<LogicalPlan>,
    pub(crate) name: Option<String>,
    pub(crate) exprs: Vec<Expr>,
    pub(crate) schema: DFSchemaRef,
    pub(crate) shuffle: bool,
}

multifield_partial_ord!(ProjectionExtension, name, exprs);

impl ProjectionExtension {
    pub(crate) fn new(inputs: Vec<LogicalPlan>, name: Option<String>, exprs: Vec<Expr>) -> Self {
        let input_schema = inputs.first().unwrap().schema();
        let fields = exprs
            .iter()
            .map(|e| DFField::from(e.to_field(input_schema).unwrap()))
            .collect_vec();

        let schema = Arc::new(schema_from_df_fields(&fields).unwrap());

        Self {
            inputs,
            name,
            exprs,
            schema,
            shuffle: false,
        }
    }

    pub(crate) fn shuffled(mut self) -> Self {
        self.shuffle = true;
        self
    }
}

impl ArroyoExtension for ProjectionExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        let input_schema = (*input_schemas[0]).clone();

        // check that all inputs have the same schemas
        for s in input_schemas.iter().skip(1) {
            if **s != input_schema {
                return internal_err!("all input schemas to a projection node must mast");
            }
        }

        let input_df_schema = Arc::new(DFSchema::try_from(input_schema.schema.as_ref().clone())?);
        let mut physical_exprs = vec![];

        for e in &self.exprs {
            let phys = planner
                .create_physical_expr(e, &input_df_schema)
                .map_err(|e| e.context("projection"))?;
            physical_exprs.push(
                serialize_physical_expr(&phys, &DefaultPhysicalExtensionCodec {})?.encode_to_vec(),
            );
        }

        let config = ProjectionOperator {
            name: self.name.as_deref().unwrap_or("projection").to_string(),
            input_schema: Some(input_schema.clone().into()),

            output_schema: Some(self.output_schema().into()),
            exprs: physical_exprs,
        };

        let node = LogicalNode::single(
            index as u32,
            format!("projection_{index}"),
            OperatorName::Projection,
            config.encode_to_vec(),
            format!("ArrowProjection<{}>", self.name.as_deref().unwrap_or("_")),
            1,
        );

        let edge_type = if self.shuffle {
            LogicalEdgeType::Shuffle
        } else {
            LogicalEdgeType::Forward
        };

        let edge = LogicalEdge::project_all(edge_type, input_schema);
        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![edge],
        })
    }

    fn output_schema(&self) -> ArroyoSchema {
        ArroyoSchema::from_schema_unkeyed(Arc::new(self.schema.as_arrow().clone())).unwrap()
    }
}

impl UserDefinedLogicalNodeCore for ProjectionExtension {
    fn name(&self) -> &str {
        PROJECTION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "KeyCalculationExtension: {}", self.schema())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            name: self.name.clone(),
            inputs,
            exprs,
            schema: self.schema.clone(),
            shuffle: self.shuffle,
        })
    }
}
