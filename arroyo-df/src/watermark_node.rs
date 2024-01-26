use crate::schemas::add_timestamp_field;
use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::fmt::Formatter;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WatermarkNode {
    pub input: LogicalPlan,
    pub watermark_expression: Option<Expr>,
    pub schema: DFSchemaRef,
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

        Self {
            input: inputs[0].clone(),
            watermark_expression: Some(exprs[0].clone()),
            schema: add_timestamp_field(self.input.schema().clone()).unwrap(),
        }
    }
}
