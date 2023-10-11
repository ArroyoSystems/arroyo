use crate::engine::OutQueue;
use arroyo_metrics::gauge_for_task;
use arroyo_types::{TaskInfo, BYTES_RECV, BYTES_SENT, MESSAGES_RECV, MESSAGES_SENT};
use lazy_static::lazy_static;
use prometheus::{labels, register_int_counter_vec, IntCounter, IntCounterVec, IntGauge};

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
}

pub enum TaskCounters {
    MessagesReceived,
    MessagesSent,
    BytesReceived,
    BytesSent,
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
        }
    }
}

pub type QueueGauges = Vec<Vec<Option<IntGauge>>>;

pub fn register_queue_gauges(
    task_info: &TaskInfo,
    out_qs: &Vec<Vec<OutQueue>>,
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
