use crate::engine::OutQueue;
use arroyo_metrics::{counter_for_task, gauge_for_task};
use arroyo_types::{TaskInfo, BYTES_RECV, BYTES_SENT, MESSAGES_RECV, MESSAGES_SENT};
use prometheus::{labels, IntCounter, IntGauge};
use std::collections::HashMap;

pub fn register_counters(task_info: &TaskInfo) -> HashMap<&'static str, IntCounter> {
    let mut counters = HashMap::new();

    if let Some(c) = counter_for_task(
        &task_info,
        MESSAGES_RECV,
        "Count of messages received by this subtask",
        HashMap::new(),
    ) {
        counters.insert(MESSAGES_RECV, c);
    }

    if let Some(c) = counter_for_task(
        &task_info,
        MESSAGES_SENT,
        "Count of messages sent by this subtask",
        HashMap::new(),
    ) {
        counters.insert(MESSAGES_SENT, c);
    }

    if let Some(c) = counter_for_task(
        &task_info,
        BYTES_RECV,
        "Count of bytes received by this subtask",
        HashMap::new(),
    ) {
        counters.insert(BYTES_RECV, c);
    }

    if let Some(c) = counter_for_task(
        &task_info,
        BYTES_SENT,
        "Count of bytes sent by this subtask",
        HashMap::new(),
    ) {
        counters.insert(BYTES_SENT, c);
    }

    counters
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
