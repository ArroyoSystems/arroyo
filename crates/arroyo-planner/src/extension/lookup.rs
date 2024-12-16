use std::fmt::Formatter;
use datafusion::common::{internal_err, DFSchemaRef};
use datafusion::logical_expr::{Expr, Join, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::sql::TableReference;
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use crate::builder::{NamedNode, Planner};
use crate::extension::{ArroyoExtension, NodeWithIncomingEdges};
use crate::multifield_partial_ord;
use crate::tables::ConnectorTable;

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

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> datafusion::common::Result<Self> {
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
    pub(crate) on: Vec<(Expr, Expr)>,
    pub(crate) filter: Option<Expr>,
    pub(crate) alias: Option<TableReference>,
}

multifield_partial_ord!(LookupJoin, input, connector, on, filter, alias);

impl ArroyoExtension for LookupJoin {
    fn node_name(&self) -> Option<NamedNode> {
        todo!()
    }

    fn plan_node(&self, planner: &Planner, index: usize, input_schemas: Vec<ArroyoSchemaRef>) -> datafusion::common::Result<NodeWithIncomingEdges> {
        todo!()
    }

    fn output_schema(&self) -> ArroyoSchema {
        todo!()
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
        let mut e: Vec<_> = self.on.iter()
            .flat_map(|(l, r)| vec![l.clone(), r.clone()])
            .collect();
        
        if let Some(filter) = &self.filter {
            e.push(filter.clone());
        }
        
        e
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "LookupJoinExtension: {}", self.schema)
    }

    fn with_exprs_and_inputs(&self, _: Vec<Expr>, inputs: Vec<LogicalPlan>) -> datafusion::common::Result<Self> {
        Ok(Self {
            input: inputs[0].clone(),
            schema: self.schema.clone(),
            connector: self.connector.clone(),
            on: self.on.clone(),
            filter: self.filter.clone(),
            alias: self.alias.clone(),
        })
    }
}