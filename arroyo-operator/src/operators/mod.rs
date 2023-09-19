use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use arrow::datatypes::Schema;
use datafusion_execution::FunctionRegistry;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use prost::Message;
use arroyo_types::{from_millis, to_millis, Window};

pub mod console;
pub mod impulse;
pub mod projection;
pub mod window;

struct Registry {}

impl FunctionRegistry for Registry {
    fn udfs(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn udf(&self, name: &str) -> datafusion_common::Result<Arc<ScalarUDF>> {
        todo!()
    }

    fn udaf(&self, name: &str) -> datafusion_common::Result<Arc<AggregateUDF>> {
        todo!()
    }

    fn udwf(&self, name: &str) -> datafusion_common::Result<Arc<WindowUDF>> {
        todo!()
    }
}

pub fn exprs_from_proto(expressions: Vec<Vec<u8>>) -> Vec<Arc<dyn PhysicalExpr>> {
    let registry = Registry {};
    let schema = Schema::empty();

    let exprs: Vec<_> = expressions
        .into_iter()
        .map(|expr| PhysicalExprNode::decode(&mut expr.as_slice()).unwrap())
        .map(|expr| parse_physical_expr(&expr, &registry, &schema).unwrap())
        .collect();

    exprs
}

#[cfg(test)]
mod test {
    use std::time::{Duration, SystemTime};
    use arroyo_types::{from_millis, to_millis};
    use crate::operators::{SlidingWindowAssigner, TimeWindowAssigner};

    #[test]
    fn test_sliding_window_assignment() {
        let assigner = SlidingWindowAssigner {
            size: Duration::from_secs(5),
            slide: Duration::from_secs(5),
        };
        let start_millis = to_millis(SystemTime::now());
        let truncated_start_millis =
            start_millis - (start_millis % Duration::from_secs(10).as_millis() as u64);
        let start = from_millis(truncated_start_millis);
        assert_eq!(
            1,
            <SlidingWindowAssigner as TimeWindowAssigner>::windows(&assigner, start).len()
        );
    }

}