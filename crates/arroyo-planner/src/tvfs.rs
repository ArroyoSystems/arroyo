use std::any::Any;
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;
use arrow_schema::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{internal_err, plan_err, Column, DFSchemaRef, TableReference, ToDFSchema};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, Subquery, TableScan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::error::Result;
use itertools::Itertools;
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use crate::{get_duration, multifield_partial_ord, ArroyoSchemaProvider};
use crate::builder::{NamedNode, Planner};
use crate::extension::{ArroyoExtension, NodeWithIncomingEdges};
use crate::schemas::{add_timestamp_field_arrow, window_arrow_struct};
use crate::tables::Table;

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub enum WindowTVFInput {
    Table(Table),
    Subquery(Subquery),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, PartialOrd)]
pub enum WindowTVFType {
    Hop {
        slide: Duration,
        width: Duration,
    },
    Tumble {
        width: Duration
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct WindowTVF {
    pub schema: SchemaRef,
    pub table: WindowTVFInput,
    pub window_type: WindowTVFType,
    pub time_col: Column,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct WindowTVFExtension {
    pub schema: DFSchemaRef,
    pub input: Arc<LogicalPlan>,
    pub window_type: WindowTVFType,
    pub time_col: Column,
}


multifield_partial_ord!(WindowTVFExtension, input, window_type, time_col);

impl UserDefinedLogicalNodeCore for WindowTVFExtension {
    fn name(&self) -> &str {
        "WindowTVFExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![
            &self.input
        ]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.input.expressions()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "WindowTVFExtension: {} | window: {:?}",
            self.schema,
            self.window_type
        )
    }

    fn with_exprs_and_inputs(&self, _: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("wrong number of inputs for window tvf extension: {}, expected 1", inputs.len());
        }
        
        Ok(Self {
            schema: self.schema.clone(),
            input: Arc::new(inputs.into_iter().next().unwrap()),
            window_type: self.window_type,
            time_col: self.time_col.clone(),
        })
    }
}

impl ArroyoExtension for WindowTVFExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(&self, planner: &Planner, index: usize, input_schemas: Vec<ArroyoSchemaRef>) -> Result<NodeWithIncomingEdges> {
        
    }

    fn output_schema(&self) -> ArroyoSchema {
        todo!()
    }
}


#[async_trait]
impl TableProvider for WindowTVF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        Some(Cow::Owned(LogicalPlan::Extension(Extension {
            node: Arc::new(
                WindowTVFExtension {
                    schema: Arc::new(self.schema.clone().to_dfschema().unwrap()),
                    input: match &self.table {
                        WindowTVFInput::Table(table) => {
                            Arc::new(LogicalPlan::TableScan(TableScan {
                                table_name: TableReference::bare(table.name()),
                                source: table.as_table_source(),
                                projection: None,
                                projected_schema: Arc::new(table.get_schema().try_into().unwrap()),
                                filters: vec![],
                                fetch: None,
                            }))
                        }
                        WindowTVFInput::Subquery(_) => {
                            todo!()
                        }
                    },
                    window_type: self.window_type.clone(),
                    time_col: self.time_col.clone(),
                }
            ),
        })))
    }

    async fn scan(&self, _: &dyn Session, _: Option<&Vec<usize>>, _: &[Expr], _: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!("HopTVFs are only used at plan time");
    }
}

#[derive(Debug)]
pub struct HopTVFFunc {
}

pub trait ArroyoTableFunctionImpl {
    fn call(&self, args: &[Expr], provider: &ArroyoSchemaProvider)  -> Result<Arc<dyn TableProvider>>;
}

impl ArroyoTableFunctionImpl for HopTVFFunc {
    fn call(&self, args: &[Expr], provider: &ArroyoSchemaProvider) -> Result<Arc<dyn TableProvider>> {
        let Some((table,
                     Expr::Column(time),
                     slide,
                     width)) = args.iter().next_tuple() else {
            return plan_err!("incorrect args for hop TVF; expected hop(table, time_column, slide, width)")
        };


        let (table, mut fields) = match table {
            Expr::Column(c) => {
                let Some(table) = provider.get_table(&c.name) else {
                    return plan_err!("unknown table '{}' in HOP", c.name);
                };

                (WindowTVFInput::Table(table.clone()), table.get_fields().clone())
            }
            Expr::ScalarSubquery(s) => {
                (WindowTVFInput::Subquery(s.clone()), s.subquery.schema().fields().iter().cloned().collect())
            }
            _ => {
                return plan_err!("invalid argument to hop TVF; expected table or subquery");
            }
        };

        fields.push(Arc::new(Field::new("window", window_arrow_struct(), false)));

        let tvf: Arc<dyn TableProvider> = Arc::new(WindowTVF {
            schema: add_timestamp_field_arrow(Schema::new(fields)),
            table,
            window_type: WindowTVFType::Hop {
                slide: get_duration(slide)?,
                width: get_duration(width)?,
            },
            time_col: time.clone(),
        });

        Ok(tvf)
    }
}