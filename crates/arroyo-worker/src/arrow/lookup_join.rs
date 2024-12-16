use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_expr::PhysicalExpr;
use arroyo_connectors::LookupConnector;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;


pub struct LookupJoin {
    connector: Box<dyn LookupConnector + Send>,
    key_exprs: Vec<dyn PhysicalExpr>,
}

#[async_trait]
impl ArrowOperator for LookupJoin {
    fn name(&self) -> String {
        format!("LookupJoin<{}>", self.connector.name())
    }

    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut OperatorContext, collector: &mut dyn Collector) {
        let keys = self.key_exprs.iter()
            .map(|expr| expr.evaluate(&batch).unwrap().into_array().unwrap())
            .collect::<Vec<_>>();

        
        
        for i in 0..keys.num_rows() {

        }
    }
}