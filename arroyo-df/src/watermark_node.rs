use crate::schemas::add_timestamp_field;
use anyhow::{anyhow, Result};
use arroyo_rpc::df::ArroyoSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::{fmt::Formatter, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WatermarkNode {
    pub input: LogicalPlan,
    pub watermark_expression: Option<Expr>,
    pub schema: DFSchemaRef,
    timestamp_index: usize,
}

impl UserDefinedLogicalNodeCore for WatermarkNode {
    fn name(&self) -> &str {
        "WatermarkNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        if let Some(expr) = &self.watermark_expression {
            vec![expr.clone()]
        } else {
            vec![]
        }
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "WaterMarkNode")
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 1, "expression size inconsistent");
        let schema = add_timestamp_field(self.input.schema().clone()).unwrap();
        let timestamp_index = schema
            .index_of_column_by_name(None, "_timestamp")
            .unwrap()
            .unwrap();

        Self {
            input: inputs[0].clone(),
            watermark_expression: Some(exprs[0].clone()),
            schema,
            timestamp_index,
        }
    }
}

impl WatermarkNode {
    pub(crate) fn new(input: LogicalPlan, watermark_expression: Option<Expr>) -> Result<Self> {
        let schema = add_timestamp_field(input.schema().clone())?;
        let timestamp_index = schema
            .index_of_column_by_name(None, "_timestamp")?
            .ok_or_else(|| anyhow!("missing _timestamp column"))?;
        Ok(Self {
            input,
            watermark_expression,
            schema,
            timestamp_index,
        })
    }
    pub(crate) fn arroyo_schema(&self) -> ArroyoSchema {
        ArroyoSchema::new(
            Arc::new(self.schema.as_ref().into()),
            self.timestamp_index,
            vec![],
        )
    }
}
