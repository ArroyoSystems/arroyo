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

pub trait TimeWindowAssigner: Send {
    fn windows(&self, ts: SystemTime) -> Vec<Window>;

    fn next(&self, window: Window) -> Window;

    fn safe_retention_duration(&self) -> Option<Duration>;
}

pub trait WindowAssigner: Send {}

#[derive(Clone, Copy)]
pub struct TumblingWindowAssigner {
    pub size: Duration,
}

impl TimeWindowAssigner for TumblingWindowAssigner {
    fn windows(&self, ts: SystemTime) -> Vec<Window> {
        let key = to_millis(ts) / (self.size.as_millis() as u64);
        vec![Window {
            start: from_millis(key * self.size.as_millis() as u64),
            end: from_millis((key + 1) * (self.size.as_millis() as u64)),
        }]
    }

    fn next(&self, window: Window) -> Window {
        Window {
            start: window.end,
            end: window.end + self.size,
        }
    }

    fn safe_retention_duration(&self) -> Option<Duration> {
        Some(self.size)
    }
}
#[derive(Clone, Copy)]
pub struct InstantWindowAssigner {}

impl TimeWindowAssigner for InstantWindowAssigner {
    fn windows(&self, ts: SystemTime) -> Vec<Window> {
        vec![Window {
            start: ts,
            end: ts + Duration::from_nanos(1),
        }]
    }

    fn next(&self, window: Window) -> Window {
        Window {
            start: window.start + Duration::from_micros(1),
            end: window.end + Duration::from_micros(1),
        }
    }

    fn safe_retention_duration(&self) -> Option<Duration> {
        Some(Duration::ZERO)
    }
}

#[derive(Copy, Clone)]
pub struct SlidingWindowAssigner {
    pub size: Duration,
    pub slide: Duration,
}
//  012345678
//  --x------
// [--x]
//  [-x-]
//   [x--]
//    [---]

impl SlidingWindowAssigner {
    fn start(&self, ts: SystemTime) -> SystemTime {
        let ts_millis = to_millis(ts);
        let earliest_window_start = ts_millis - self.size.as_millis() as u64;

        let remainder = earliest_window_start % (self.slide.as_millis() as u64);

        from_millis(earliest_window_start - remainder + self.slide.as_millis() as u64)
    }
}

impl TimeWindowAssigner for SlidingWindowAssigner {
    fn windows(&self, ts: SystemTime) -> Vec<Window> {
        let mut windows =
            Vec::with_capacity(self.size.as_millis() as usize / self.slide.as_millis() as usize);

        let mut start = self.start(ts);

        while start <= ts {
            windows.push(Window {
                start,
                end: start + self.size,
            });
            start += self.slide;
        }

        windows
    }

    fn next(&self, window: Window) -> Window {
        let start_time = window.start + self.slide;
        Window {
            start: start_time,
            end: start_time + self.size,
        }
    }

    fn safe_retention_duration(&self) -> Option<Duration> {
        Some(self.size)
    }
}

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