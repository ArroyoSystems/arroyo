use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::api_types::metrics::{
    Metric, MetricGroup, MetricName, OperatorMetricGroup, SubtaskMetrics,
};
use arroyo_types::{from_micros, to_micros};
use petgraph::prelude::NodeIndex;
use std::array::from_fn;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::warn;

pub const COLLECTION_RATE: Duration = Duration::from_secs(2);
const COLLECTION_TIME: Duration = Duration::from_secs(5 * 60);
const NUM_BUCKETS: usize = (COLLECTION_TIME.as_secs() / COLLECTION_RATE.as_secs()) as usize;
const EWMA_ALPHA: f64 = 0.1;

pub const METRICS: [MetricName; 4] = [
    MetricName::BytesRecv,
    MetricName::BytesSent,
    MetricName::MessagesRecv,
    MetricName::MessagesSent,
];

pub fn get_metric_idx(name: &str) -> Option<usize> {
    match name {
        "arroyo_worker_bytes_recv" => Some(0),
        "arroyo_worker_bytes_sent" => Some(1),
        "arroyo_worker_messages_recv" => Some(2),
        "arroyo_worker_messages_sent" => Some(3),
        _ => None,
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct TaskKey {
    pub operator_id: u32,
    pub subtask_idx: u32,
}

#[derive(Clone)]
pub struct JobMetrics {
    program: Arc<LogicalProgram>,
    job_id: Arc<String>,
    tasks: Arc<RwLock<HashMap<TaskKey, TaskMetrics>>>,
}

impl JobMetrics {
    pub fn new(job_id: Arc<String>, program: Arc<LogicalProgram>) -> Self {
        let mut tasks = HashMap::new();
        for op in program.graph.node_indices() {
            for i in 0..program.graph[op].parallelism {
                tasks.insert(
                    TaskKey {
                        operator_id: op.index() as u32,
                        subtask_idx: i as u32,
                    },
                    TaskMetrics::new(),
                );
            }
        }

        Self {
            program,
            job_id,
            tasks: Arc::new(RwLock::new(tasks)),
        }
    }

    pub async fn update(&self, operator_id: u32, subtask_idx: u32, values: [u64; METRICS.len()]) {
        let mut lock = self.tasks.write().await;
        let Some(metrics) = lock.get_mut(&TaskKey {
            operator_id,
            subtask_idx,
        }) else {
            warn!(
                messge = "tried to update metrics for non-existent operator or subtask",
                job_id = *self.job_id,
                operator_id = operator_id,
                subtask_idx = subtask_idx
            );
            return;
        };

        let now = SystemTime::now();
        for (rate, value) in metrics.rates.iter_mut().zip(values) {
            rate.add(now, value);
        }
    }

    pub async fn get_groups(&self) -> Vec<OperatorMetricGroup> {
        let mut metric_groups: HashMap<u32, HashMap<MetricName, Vec<SubtaskMetrics>>> =
            HashMap::new();

        for (k, v) in self.tasks.read().await.iter() {
            let op = metric_groups.entry(k.operator_id).or_default();

            let mut metric_rates: [Vec<Metric>; METRICS.len()] = Default::default();

            for (rate, output) in v.rates.iter().zip(metric_rates.iter_mut()) {
                for (t, v) in rate.iter() {
                    output.push(Metric {
                        time: to_micros(t),
                        value: v,
                    });
                }
            }

            for (name, metrics) in METRICS.iter().zip(metric_rates) {
                op.entry(*name).or_default().push(SubtaskMetrics {
                    index: k.subtask_idx,
                    metrics,
                });
            }
        }

        metric_groups
            .into_iter()
            .map(|(op_id, metrics)| {
                let operator_id = self
                    .program
                    .graph
                    .node_weight(NodeIndex::new(op_id as usize))
                    .unwrap()
                    .operator_id
                    .clone();
                OperatorMetricGroup {
                    operator_id,
                    metric_groups: metrics
                        .into_iter()
                        .map(|(name, subtasks)| MetricGroup { name, subtasks })
                        .collect(),
                }
            })
            .collect()
    }
}

pub struct TaskMetrics {
    rates: [RateMetric; METRICS.len()],
}

impl TaskMetrics {
    pub fn new() -> Self {
        Self {
            rates: from_fn(|_| RateMetric::new()),
        }
    }
}

/// Calculates an exponentially-weighted moving average over metrics collected from the job
pub struct RateMetric {
    rates: [f64; NUM_BUCKETS],
    timestamps: [SystemTime; NUM_BUCKETS],
    cur_idx: usize,
    prev_value: u64,
}

impl RateMetric {
    pub fn new() -> Self {
        Self {
            rates: [0.0; NUM_BUCKETS],
            timestamps: [UNIX_EPOCH; NUM_BUCKETS],
            cur_idx: 0,
            prev_value: 0,
        }
    }

    pub fn add(&mut self, time: SystemTime, value: u64) {
        let prev_idx = self.cur_idx;
        self.cur_idx = (prev_idx + 1) % NUM_BUCKETS;

        let prev_value = self.prev_value;
        self.timestamps[self.cur_idx] = time;
        self.prev_value = value;

        if self.timestamps[prev_idx] == UNIX_EPOCH {
            return;
        }

        let delta_t = time
            .duration_since(self.timestamps[prev_idx])
            .unwrap_or_default()
            .as_secs_f64();

        if delta_t > 0.0 {
            // TODO: The unwrap case should never happen because a new RateMetric will be created
            //       for any process restart, but for full generality we'd want to implement counter
            //       reset behavior for it
            let diff = value.checked_sub(prev_value).unwrap_or_default();
            let r = diff as f64 / delta_t;
            self.rates[self.cur_idx] = (1.0 - EWMA_ALPHA) * (self.rates[prev_idx]) + EWMA_ALPHA * r;
        }
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps[self.cur_idx] == UNIX_EPOCH
    }

    pub fn iter(&self) -> RateMetricsIter {
        let mut start_idx = (self.cur_idx + 1) % NUM_BUCKETS;
        if !self.is_empty() {
            while self.timestamps[start_idx] == UNIX_EPOCH {
                start_idx = (start_idx + 1) % NUM_BUCKETS;
            }
        }

        RateMetricsIter {
            rate_metrics: self,
            cur_idx: start_idx,
            start_idx,
            started: false,
        }
    }
}

pub struct RateMetricsIter<'a> {
    rate_metrics: &'a RateMetric,
    cur_idx: usize,
    start_idx: usize,
    started: bool,
}

impl<'a> Iterator for RateMetricsIter<'a> {
    type Item = (SystemTime, f64);

    fn next(&mut self) -> Option<Self::Item> {
        let rate = self.rate_metrics.rates[self.cur_idx];
        let timestamp = self.rate_metrics.timestamps[self.cur_idx];

        if timestamp == UNIX_EPOCH || self.started && self.cur_idx == self.start_idx {
            return None;
        }

        self.started = true;
        self.cur_idx = (self.cur_idx + 1) % NUM_BUCKETS;

        Some((timestamp, rate))
    }
}
//
// pub struct BackpressureMetric {
//     buckets: [u64; NUM_BUCKETS],
//
// }

#[cfg(test)]
mod tests {
    use crate::job_controller::job_metrics::{RateMetric, COLLECTION_RATE, NUM_BUCKETS};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn test_rate_metrics() {
        let now = SystemTime::now();
        let mut rate_metric = RateMetric::new();

        let mut times = vec![];

        for i in 0..5 {
            let time = now + Duration::from_secs(i as u64 * COLLECTION_RATE.as_secs());
            rate_metric.add(time, i as u64);
            times.push(time);
        }

        let results: Vec<_> = rate_metric.iter().collect();
        assert_eq!(results.len(), 5);

        for (i, (time, v)) in results.iter().enumerate() {
            if i > 0 {
                assert!(*v > 0.0);
            }
            assert_eq!(*time, times[i]);
        }

        for i in 0..NUM_BUCKETS * 7 {
            let time = now + Duration::from_secs(i as u64 * COLLECTION_RATE.as_secs());
            rate_metric.add(time, i as u64);
        }

        let results: Vec<_> = rate_metric.iter().collect();
        assert_eq!(results.len(), NUM_BUCKETS);

        let mut last_time = UNIX_EPOCH;
        for (time, v) in results {
            assert!(v > 0.0);
            assert!(time > last_time);
            last_time = time;
        }
    }
}
