use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::api_types::metrics::{
    Metric, MetricGroup, MetricName, OperatorMetricGroup, SubtaskMetrics,
};
use arroyo_types::to_micros;
use petgraph::prelude::NodeIndex;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::warn;

pub const COLLECTION_RATE: Duration = Duration::from_secs(2);
const COLLECTION_TIME: Duration = Duration::from_secs(5 * 60);
const NUM_BUCKETS: usize = (COLLECTION_TIME.as_secs() / COLLECTION_RATE.as_secs()) as usize;
const EWMA_ALPHA: f64 = 0.1;

pub const RATE_METRICS: [MetricName; 4] = [
    MetricName::BytesRecv,
    MetricName::BytesSent,
    MetricName::MessagesRecv,
    MetricName::MessagesSent,
];

pub fn get_metric_name(name: &str) -> Option<MetricName> {
    MetricName::from_str(&name["arroyo_worker_".len()..]).ok()
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct TaskKey {
    pub operator_id: u32,
    pub subtask_idx: u32,
}

#[derive(Clone)]
pub struct JobMetrics {
    program: Arc<LogicalProgram>,
    tasks: Arc<RwLock<HashMap<TaskKey, TaskMetrics>>>,
}

impl JobMetrics {
    pub fn new(program: Arc<LogicalProgram>) -> Self {
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
            tasks: Arc::new(RwLock::new(tasks)),
        }
    }

    pub async fn update(
        &self,
        operator_id: u32,
        subtask_idx: u32,
        values: &HashMap<MetricName, u64>,
    ) {
        let now = SystemTime::now();

        let key = TaskKey {
            operator_id,
            subtask_idx,
        };
        let mut tasks = self.tasks.write().await;
        let Some(task) = tasks.get_mut(&key) else {
            warn!(
                "Task not found for operator_id: {}, subtask_idx: {}",
                operator_id, subtask_idx
            );
            return;
        };

        for (metric, value) in values {
            if let Some(rate) = task.rates.get_mut(metric) {
                rate.add(now, *value);
            }
        }

        let queue_size = values
            .get(&MetricName::TxQueueSize)
            .copied()
            .unwrap_or_default() as f64;
        let queue_remaining = values
            .get(&MetricName::TxQueueRem)
            .copied()
            .unwrap_or_default() as f64;
        // add 1 to each value to account for uninitialized values (which report 0); this can happen when a task
        // never reads any data
        let backpressure = 1.0 - (queue_remaining + 1.0) / (queue_size + 1.0);
        task.update_backpressure(now, backpressure);
    }

    pub async fn get_groups(&self) -> Vec<OperatorMetricGroup> {
        let mut metric_groups: HashMap<u32, HashMap<MetricName, Vec<SubtaskMetrics>>> =
            HashMap::new();

        for (k, v) in self.tasks.read().await.iter() {
            let op = metric_groups.entry(k.operator_id).or_default();

            for (metric, rate) in &v.rates {
                op.entry(*metric).or_default().push(SubtaskMetrics {
                    index: k.subtask_idx,
                    metrics: rate
                        .iter()
                        .map(|(t, v)| Metric {
                            time: to_micros(t),
                            value: v,
                        })
                        .collect(),
                });
            }

            op.entry(MetricName::Backpressure)
                .or_default()
                .push(SubtaskMetrics {
                    index: k.subtask_idx,
                    metrics: v
                        .backpressure
                        .iter()
                        .map(|(t, v)| Metric {
                            time: to_micros(t),
                            value: v,
                        })
                        .collect(),
                });
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
                        .map(|(name, subtasks)| MetricGroup {
                            name,
                            subtasks: subtasks
                                .into_iter()
                                .filter(|t| !t.metrics.is_empty())
                                .collect(),
                        })
                        .collect(),
                }
            })
            .collect()
    }
}

pub struct TaskMetrics {
    rates: HashMap<MetricName, RateMetric>,
    backpressure: CircularBuffer<(SystemTime, f64), NUM_BUCKETS>,
}

impl TaskMetrics {
    pub fn new() -> Self {
        Self {
            rates: RATE_METRICS
                .iter()
                .map(|&m| (m, RateMetric::new()))
                .collect(),
            backpressure: CircularBuffer::new((UNIX_EPOCH, 0.0)),
        }
    }

    pub fn update_backpressure(&mut self, time: SystemTime, value: f64) {
        self.backpressure.push((time, value));
    }
}

/// Calculates an exponentially-weighted moving average over metrics collected from the job
pub struct RateMetric {
    values: CircularBuffer<(SystemTime, f64), NUM_BUCKETS>,
    prev_value: Option<(SystemTime, u64)>,
}

impl RateMetric {
    pub fn new() -> Self {
        Self {
            values: CircularBuffer::new((UNIX_EPOCH, 0.0)),
            prev_value: None,
        }
    }

    pub fn add(&mut self, time: SystemTime, value: u64) {
        let prev_value = self.prev_value;
        self.prev_value = Some((time, value));

        let Some((prev_time, prev_value)) = prev_value else {
            return;
        };

        let delta_t = time
            .duration_since(prev_time)
            .unwrap_or_default()
            .as_secs_f64();

        if delta_t > 0.0 {
            // TODO: The unwrap case should never happen because a new RateMetric will be created
            //       for any process restart, but for full generality we'd want to implement counter
            //       reset behavior for it
            let diff = value.checked_sub(prev_value).unwrap_or_default();
            let r = diff as f64 / delta_t;
            self.values
                .push(if let Some((_, last_r)) = self.values.last() {
                    (time, (1.0 - EWMA_ALPHA) * last_r + EWMA_ALPHA * r)
                } else {
                    (time, r)
                });
        }
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (SystemTime, f64)> + '_ {
        self.values.iter()
    }
}

pub struct CircularBuffer<T: Copy, const N: usize> {
    values: [T; N],
    next_idx: usize,
    size: u64,
}

impl<T: Copy, const N: usize> CircularBuffer<T, N> {
    pub fn new(initial: T) -> Self {
        Self {
            values: [initial; N],
            next_idx: 0,
            size: 0,
        }
    }

    pub fn push(&mut self, value: T) {
        self.values[self.next_idx] = value;
        self.next_idx = (self.next_idx + 1) % N;
        self.size = (self.size + 1).min(N as u64);
    }

    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        self.values
            .iter()
            .cycle()
            .skip(if self.len() < N { 0 } else { self.next_idx })
            .take(self.size as usize)
            .copied()
    }

    pub fn len(&self) -> usize {
        self.size as usize
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn last(&self) -> Option<T> {
        if self.is_empty() {
            None
        } else {
            Some(self.values[(self.next_idx as isize - 1).rem_euclid(N as isize) as usize])
        }
    }
}

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
        assert_eq!(results.len(), 4);

        for (i, (time, v)) in results.iter().enumerate() {
            if i > 0 {
                assert!(*v > 0.0);
            }
            assert_eq!(*time, times[i + 1]);
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
