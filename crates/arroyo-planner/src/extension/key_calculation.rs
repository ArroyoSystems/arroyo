use arrow_schema::{Field, Schema};
use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::{
    df::{ArroyoSchema, ArroyoSchemaRef},
    grpc::api::KeyPlanOperator,
};
use datafusion::common::{internal_err, plan_err, DFSchema, DFSchemaRef, Result};
use std::{fmt::Formatter, sync::Arc};

use super::{ArroyoExtension, NodeWithIncomingEdges};
use crate::{
    builder::{NamedNode, Planner},
    fields_with_qualifiers, multifield_partial_ord,
    physical::ArroyoPhysicalExtensionCodec,
    schema_from_df_fields_with_metadata,
};
use arroyo_rpc::grpc::api::ProjectionOperator;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::col;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use itertools::Itertools;
use prost::Message;

pub(crate) const KEY_CALCULATION_NAME: &str = "KeyCalculationExtension";

/// Two ways of specifying keys — either as col indexes in the existing data or as a set of
/// exprs to evaluate
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum KeysOrExprs {
    Keys(Vec<usize>),
    Exprs(Vec<Expr>),
}

/// Calculation for computing keyed data, with a vec of keys
/// that will be used for shuffling data to the correct nodes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct KeyCalculationExtension {
    pub(crate) name: Option<String>,
    pub(crate) input: LogicalPlan,
    pub(crate) keys: KeysOrExprs,
    pub(crate) schema: DFSchemaRef,
}

multifield_partial_ord!(KeyCalculationExtension, name, input, keys);

impl KeyCalculationExtension {
    pub fn new_named_and_trimmed(input: LogicalPlan, keys: Vec<usize>, name: String) -> Self {
        let output_fields: Vec<_> = fields_with_qualifiers(input.schema())
            .into_iter()
            .enumerate()
            .filter_map(|(index, field)| {
                if !keys.contains(&index) {
                    Some(field.clone())
                } else {
                    None
                }
            })
            .collect();

        let schema =
            schema_from_df_fields_with_metadata(&output_fields, input.schema().metadata().clone())
                .unwrap();
        Self {
            name: Some(name),
            input,
            keys: KeysOrExprs::Keys(keys),
            schema: Arc::new(schema),
        }
    }
    pub fn new(input: LogicalPlan, keys: KeysOrExprs) -> Self {
        let schema = input.schema().clone();
        Self {
            name: None,
            input,
            keys,
            schema,
        }
    }
}

impl ArroyoExtension for KeyCalculationExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        // check there's only one input
        if input_schemas.len() != 1 {
            return plan_err!("KeyCalculationExtension should have exactly one input");
        }
        let input_schema = (*input_schemas[0]).clone();
        let input_df_schema = Arc::new(DFSchema::try_from(input_schema.schema.as_ref().clone())?);

        let physical_plan = planner.sync_plan(&self.input)?;

        let physical_plan_node: PhysicalPlanNode = PhysicalPlanNode::try_from_physical_plan(
            physical_plan,
            &ArroyoPhysicalExtensionCodec::default(),
        )?;

        let (config, name) = match &self.keys {
            KeysOrExprs::Keys(keys) => (
                KeyPlanOperator {
                    name: "key".into(),
                    physical_plan: physical_plan_node.encode_to_vec(),
                    key_fields: keys.iter().map(|k| *k as u64).collect(),
                }
                .encode_to_vec(),
                OperatorName::ArrowKey,
            ),
            KeysOrExprs::Exprs(key_exprs) => {
                let mut exprs = vec![];
                for k in key_exprs {
                    exprs.push(k.clone())
                }

                for f in input_schema.schema.fields.iter() {
                    exprs.push(col(f.name()));
                }

                let output_schema = self.output_schema();

                // ensure that the exprs generate the output schema
                for (expr, expected) in exprs.iter().zip(output_schema.schema.fields()) {
                    let (data_type, nullable) = expr.data_type_and_nullable(&input_df_schema)?;
                    assert_eq!(data_type, *expected.data_type());
                    assert_eq!(nullable, expected.is_nullable());
                }

                let mut physical_exprs = vec![];

                for e in exprs {
                    let phys = planner
                        .create_physical_expr(&e, &input_df_schema)
                        .map_err(|e| e.context("in PARTITION BY"))?;
                    physical_exprs.push(
                        serialize_physical_expr(&phys, &DefaultPhysicalExtensionCodec {})?
                            .encode_to_vec(),
                    );
                }

                let config = ProjectionOperator {
                    name: self.name.as_deref().unwrap_or("key").to_string(),
                    input_schema: Some(input_schema.clone().into()),

                    output_schema: Some(self.output_schema().into()),
                    exprs: physical_exprs,
                };

                (config.encode_to_vec(), OperatorName::Projection)
            }
        };

        let node = LogicalNode::single(
            index as u32,
            format!("key_{index}"),
            name,
            config,
            format!("ArrowKey<{}>", self.name.as_deref().unwrap_or("_")),
            1,
        );
        let edge = LogicalEdge::project_all(LogicalEdgeType::Forward, input_schema);
        Ok(NodeWithIncomingEdges {
            node,
            edges: vec![edge],
        })
    }

    fn output_schema(&self) -> ArroyoSchema {
        let arrow_schema = self.input.schema().as_ref();

        match &self.keys {
            KeysOrExprs::Keys(keys) => {
                ArroyoSchema::from_schema_keys(Arc::new(arrow_schema.into()), keys.clone()).unwrap()
            }
            KeysOrExprs::Exprs(exprs) => {
                let mut fields = vec![];

                for (i, e) in exprs.iter().enumerate() {
                    let (dt, nullable) = e.data_type_and_nullable(arrow_schema).unwrap();
                    fields.push(Field::new(format!("__key_{i}"), dt, nullable).into());
                }

                for f in arrow_schema.fields().iter() {
                    fields.push(f.clone());
                }

                ArroyoSchema::from_schema_keys(
                    Arc::new(Schema::new(fields)),
                    (1..=exprs.len()).collect_vec(),
                )
                .unwrap()
            }
        }
    }
}

impl UserDefinedLogicalNodeCore for KeyCalculationExtension {
    fn name(&self) -> &str {
        KEY_CALCULATION_NAME
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
        write!(f, "KeyCalculationExtension: {}", self.schema())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("input size inconsistent");
        }

        let keys = match &self.keys {
            KeysOrExprs::Keys(k) => KeysOrExprs::Keys(k.clone()),
            KeysOrExprs::Exprs(_) => KeysOrExprs::Exprs(exprs),
        };

        Ok(Self {
            name: self.name.clone(),
            input: inputs[0].clone(),
            keys,
            schema: self.schema.clone(),
        })
    }
}
