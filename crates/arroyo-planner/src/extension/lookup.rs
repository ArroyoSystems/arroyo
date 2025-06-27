use crate::builder::{NamedNode, Planner};
use crate::extension::{ArroyoExtension, NodeWithIncomingEdges};
use crate::multifield_partial_ord;
use crate::schemas::add_timestamp_field_arrow;
use crate::tables::ConnectorTable;
use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use arroyo_rpc::grpc::api::{ConnectorOp, LookupJoinCondition, LookupJoinOperator};
use datafusion::common::{internal_err, plan_err, Column, DFSchemaRef, JoinType};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::sql::TableReference;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use prost::Message;
use std::fmt::Formatter;
use std::sync::Arc;

pub const SOURCE_EXTENSION_NAME: &str = "LookupSource";
pub const JOIN_EXTENSION_NAME: &str = "LookupJoin";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LookupSource {
    pub(crate) table: ConnectorTable,
    pub(crate) schema: DFSchemaRef,
}

multifield_partial_ord!(LookupSource, table);

impl UserDefinedLogicalNodeCore for LookupSource {
    fn name(&self) -> &str {
        SOURCE_EXTENSION_NAME
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

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "LookupSource: {}", self.schema)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if !inputs.is_empty() {
            return internal_err!("LookupSource cannot have inputs");
        }

        Ok(Self {
            table: self.table.clone(),
            schema: self.schema.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LookupJoin {
    pub(crate) input: LogicalPlan,
    pub(crate) schema: DFSchemaRef,
    pub(crate) connector: ConnectorTable,
    pub(crate) on: Vec<(Expr, Column)>,
    pub(crate) filter: Option<Expr>,
    pub(crate) alias: Option<TableReference>,
    pub(crate) join_type: JoinType,
}

multifield_partial_ord!(LookupJoin, input, connector, on, filter, alias);

impl ArroyoExtension for LookupJoin {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> datafusion::common::Result<NodeWithIncomingEdges> {
        let schema = ArroyoSchema::from_schema_unkeyed(Arc::new(self.schema.as_ref().into()))?;
        let lookup_schema = ArroyoSchema::from_schema_unkeyed(add_timestamp_field_arrow(
            self.connector.physical_schema(),
        ))?;
        let join_config = LookupJoinOperator {
            input_schema: Some(schema.into()),
            lookup_schema: Some(lookup_schema.into()),
            connector: Some(ConnectorOp {
                connector: self.connector.connector.clone(),
                config: self.connector.config.clone(),
                description: self.connector.description.clone(),
            }),
            key_exprs: self
                .on
                .iter()
                .map(|(l, r)| {
                    let expr = planner.create_physical_expr(l, &self.schema)?;
                    let expr = serialize_physical_expr(&expr, &DefaultPhysicalExtensionCodec {})?;
                    Ok(LookupJoinCondition {
                        left_expr: expr.encode_to_vec(),
                        right_key: r.name.clone(),
                    })
                })
                .collect::<datafusion::error::Result<Vec<_>>>()?,
            join_type: match self.join_type {
                JoinType::Inner => arroyo_rpc::grpc::api::JoinType::Inner as i32,
                JoinType::Left => arroyo_rpc::grpc::api::JoinType::Left as i32,
                j => {
                    return plan_err!("unsupported join type '{j}' for lookup join; only inner and left joins are supported");
                }
            },
            ttl_micros: self
                .connector
                .lookup_cache_ttl
                .map(|t| t.as_micros() as u64),
            max_capacity_bytes: self.connector.lookup_cache_max_bytes,
        };

        let incoming_edge =
            LogicalEdge::project_all(LogicalEdgeType::Shuffle, (*input_schemas[0]).clone());

        Ok(NodeWithIncomingEdges {
            node: LogicalNode::single(
                index as u32,
                format!("lookupjoin_{index}"),
                OperatorName::LookupJoin,
                join_config.encode_to_vec(),
                format!("LookupJoin<{}>", self.connector.name),
                1,
            ),
            edges: vec![incoming_edge],
        })
    }

    fn output_schema(&self) -> ArroyoSchema {
        ArroyoSchema::from_schema_unkeyed(self.schema.inner().clone()).unwrap()
    }
}

impl UserDefinedLogicalNodeCore for LookupJoin {
    fn name(&self) -> &str {
        JOIN_EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut e: Vec<_> = self.on.iter().map(|(l, _)| l.clone()).collect();

        if let Some(filter) = &self.filter {
            e.push(filter.clone());
        }

        e
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "LookupJoinExtension: {}", self.schema)
    }

    fn with_exprs_and_inputs(
        &self,
        _: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        Ok(Self {
            input: inputs[0].clone(),
            schema: self.schema.clone(),
            connector: self.connector.clone(),
            on: self.on.clone(),
            filter: self.filter.clone(),
            alias: self.alias.clone(),
            join_type: self.join_type,
        })
    }
}
