use std::collections::HashMap;

use arroyo_types::{
    TaskInfo, BYTES_RECV, BYTES_SENT, DESERIALIZATION_ERRORS, MESSAGES_RECV, MESSAGES_SENT,
};
use lazy_static::lazy_static;
use prometheus::{
    labels, register_histogram, register_int_counter_vec, register_int_gauge, Histogram,
    HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts,
};

pub fn gauge_for_task(
    task_info: &TaskInfo,
    name: &'static str,
    help: &'static str,
    mut labels: HashMap<String, String>,
) -> Option<IntGauge> {
    let mut opts = Opts::new(name, help);
    labels.extend(task_info.metric_label_map().into_iter());

    opts.const_labels = labels;

    register_int_gauge!(opts).ok()
}

pub fn histogram_for_task(
    task_info: &TaskInfo,
    name: &'static str,
    help: &'static str,
    mut labels: HashMap<String, String>,
    buckets: Vec<f64>,
) -> Option<Histogram> {
    labels.extend(task_info.metric_label_map().into_iter());
    let opts = HistogramOpts::new(name, help)
        .const_labels(labels)
        .buckets(buckets);

    register_histogram!(opts).ok()
}

lazy_static! {
    pub static ref TASK_METRIC_LABELS: Vec<&'static str> =
        vec!["operator_id", "subtask_idx", "operator_name"];
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
    pub static ref DESERIALIZATION_ERRORS_COUNTER: IntCounterVec = register_int_counter_vec!(
        DESERIALIZATION_ERRORS,
        "Count of deserialization errors",
        &TASK_METRIC_LABELS
    )
    .unwrap();
}

pub enum TaskCounters {
    MessagesReceived,
    MessagesSent,
    BytesReceived,
    BytesSent,
    DeserializationErrors,
}

impl TaskCounters {
    pub fn for_task(&self, task_info: &TaskInfo) -> IntCounter {
        match self {
            TaskCounters::MessagesReceived => MESSAGE_RECV_COUNTER.with_label_values(&[
                &task_info.operator_id,
                &task_info.task_index.to_string(),
                &task_info.operator_name,
            ]),
            TaskCounters::MessagesSent => MESSAGES_SENT_COUNTER.with_label_values(&[
                &task_info.operator_id,
                &task_info.task_index.to_string(),
                &task_info.operator_name,
            ]),
            TaskCounters::BytesReceived => BYTES_RECEIVED_COUNTER.with_label_values(&[
                &task_info.operator_id,
                &task_info.task_index.to_string(),
                &task_info.operator_name,
            ]),
            TaskCounters::BytesSent => BYTES_SENT_COUNTER.with_label_values(&[
                &task_info.operator_id,
                &task_info.task_index.to_string(),
                &task_info.operator_name,
            ]),
            TaskCounters::DeserializationErrors => DESERIALIZATION_ERRORS_COUNTER
                .with_label_values(&[
                    &task_info.operator_id,
                    &task_info.task_index.to_string(),
                    &task_info.operator_name,
                ]),
        }
    }
}

pub type QueueGauges = Vec<Vec<Option<IntGauge>>>;

pub fn register_queue_gauges<T>(
    task_info: &TaskInfo,
    out_qs: &Vec<Vec<T>>,
) -> (QueueGauges, QueueGauges) {
    let tx_queue_size_gauges = out_qs
        .iter()
        .enumerate()
        .map(|(i, qs)| {
            qs.iter()
                .enumerate()
                .map(|(j, _)| {
                    gauge_for_task(
                        task_info,
                        "arroyo_worker_tx_queue_size",
                        "Size of a tx queue",
                        labels! {
                            "next_node".to_string() => format!("{}", i),
                            "next_node_idx".to_string() => format!("{}", j)
                        },
                    )
                })
                .collect()
        })
        .collect();

    let tx_queue_rem_gauges = out_qs
        .iter()
        .enumerate()
        .map(|(i, qs)| {
            qs.iter()
                .enumerate()
                .map(|(j, _)| {
                    gauge_for_task(
                        task_info,
                        "arroyo_worker_tx_queue_rem",
                        "Remaining space in a tx queue",
                        labels! {
                            "next_node".to_string() => format!("{}", i),
                            "next_node_idx".to_string() => format!("{}", j)
                        },
                    )
                })
                .collect()
        })
        .collect();

    (tx_queue_size_gauges, tx_queue_rem_gauges)
}
