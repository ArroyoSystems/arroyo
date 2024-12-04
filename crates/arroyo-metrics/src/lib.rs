use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use arroyo_types::{
    ChainInfo, BATCHES_RECV, BATCHES_SENT, BYTES_RECV, BYTES_SENT, DESERIALIZATION_ERRORS,
    MESSAGES_RECV, MESSAGES_SENT,
};
use lazy_static::lazy_static;
use prometheus::{
    labels, register_histogram, register_int_counter_vec, register_int_gauge, Histogram,
    HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts,
};

pub fn gauge_for_task(
    chain_info: &ChainInfo,
    name: &'static str,
    help: &'static str,
    mut labels: HashMap<String, String>,
) -> Option<IntGauge> {
    let mut opts = Opts::new(name, help);
    labels.extend(chain_info.metric_label_map());

    opts.const_labels = labels;

    register_int_gauge!(opts).ok()
}

pub fn histogram_for_task(
    chain_info: &ChainInfo,
    name: &'static str,
    help: &'static str,
    mut labels: HashMap<String, String>,
    buckets: Vec<f64>,
) -> Option<Histogram> {
    labels.extend(chain_info.metric_label_map());
    let opts = HistogramOpts::new(name, help)
        .const_labels(labels)
        .buckets(buckets);

    register_histogram!(opts).ok()
}

lazy_static! {
    pub static ref TASK_METRIC_LABELS: Vec<&'static str> =
        vec!["node_id", "subtask_idx", "operator_name"];
    pub static ref MESSAGE_RECV_COUNTER: IntCounterVec = register_int_counter_vec!(
        MESSAGES_RECV,
        "Count of messages received by this subtask",
        &TASK_METRIC_LABELS
    )
    .unwrap();
    pub static ref MESSAGES_SENT_COUNTER: IntCounterVec = register_int_counter_vec!(
        MESSAGES_SENT,
        "Count of messages sent by this subtask",
        &TASK_METRIC_LABELS
    )
    .unwrap();
    pub static ref BYTES_RECEIVED_COUNTER: IntCounterVec = register_int_counter_vec!(
        BYTES_RECV,
        "Count of bytes received by this subtask",
        &TASK_METRIC_LABELS
    )
    .unwrap();
    pub static ref BYTES_SENT_COUNTER: IntCounterVec = register_int_counter_vec!(
        BYTES_SENT,
        "Count of bytes sent by this subtask",
        &TASK_METRIC_LABELS
    )
    .unwrap();
    pub static ref BATCHES_RECEIVED_COUNTER: IntCounterVec = register_int_counter_vec!(
        BATCHES_RECV,
        "Number of batches received by this subtask",
        &TASK_METRIC_LABELS
    )
    .unwrap();
    pub static ref BATCHES_SENT_COUNTER: IntCounterVec = register_int_counter_vec!(
        BATCHES_SENT,
        "Number of batches sent by this subtask",
        &TASK_METRIC_LABELS
    )
    .unwrap();
    pub static ref DESERIALIZATION_ERRORS_COUNTER: IntCounterVec = register_int_counter_vec!(
        DESERIALIZATION_ERRORS,
        "Count of deserialization errors",
        &TASK_METRIC_LABELS
    )
    .unwrap();
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub enum TaskCounters {
    MessagesReceived,
    MessagesSent,
    BatchesReceived,
    BatchesSent,
    BytesReceived,
    BytesSent,
    DeserializationErrors,
}

impl TaskCounters {
    pub fn variants() -> [TaskCounters; 7] {
        use TaskCounters::*;

        [
            MessagesReceived,
            MessagesSent,
            BatchesReceived,
            BatchesSent,
            BytesReceived,
            BytesSent,
            DeserializationErrors,
        ]
    }
}

#[allow(clippy::type_complexity)]
impl TaskCounters {
    fn metric(&self) -> &'static IntCounterVec {
        match self {
            TaskCounters::MessagesReceived => &MESSAGE_RECV_COUNTER,
            TaskCounters::MessagesSent => &MESSAGES_SENT_COUNTER,
            TaskCounters::BatchesReceived => &BATCHES_RECEIVED_COUNTER,
            TaskCounters::BatchesSent => &BATCHES_SENT_COUNTER,
            TaskCounters::BytesReceived => &BYTES_RECEIVED_COUNTER,
            TaskCounters::BytesSent => &BYTES_SENT_COUNTER,
            TaskCounters::DeserializationErrors => &DESERIALIZATION_ERRORS_COUNTER,
        }
    }

    pub fn for_task<F>(&self, chain_info: &Arc<ChainInfo>, f: F)
    where
        F: Fn(&IntCounter),
    {
        static CACHE: OnceLock<Arc<RwLock<HashMap<(TaskCounters, Arc<ChainInfo>), IntCounter>>>> =
            OnceLock::new();
        let cache = CACHE.get_or_init(|| Arc::new(RwLock::new(HashMap::new())));

        {
            if let Some(counter) = cache.read().unwrap().get(&(*self, chain_info.clone())) {
                f(counter);
                return;
            }
        }

        let counter = self.metric().with_label_values(&[
            &chain_info.node_id.to_string(),
            &chain_info.task_index.to_string(),
            &chain_info.description.to_string(),
        ]);

        f(&counter);

        cache
            .write()
            .unwrap()
            .insert((*self, chain_info.clone()), counter);
    }
}

pub type QueueGauges = Vec<Vec<Option<IntGauge>>>;

pub fn register_queue_gauge<T>(
    name: &'static str,
    help: &'static str,
    chain_info: &ChainInfo,
    out_qs: &[Vec<T>],
    initial: i64,
) -> QueueGauges {
    out_qs
        .iter()
        .enumerate()
        .map(|(i, qs)| {
            qs.iter()
                .enumerate()
                .map(|(j, _)| {
                    let mut g = gauge_for_task(
                        chain_info,
                        name,
                        help,
                        labels! {
                            "next_node".to_string() => format!("{}", i),
                            "next_node_idx".to_string() => format!("{}", j)
                        },
                    );

                    if let Some(g) = &mut g {
                        g.set(initial);
                    }

                    g
                })
                .collect()
        })
        .collect()
}
