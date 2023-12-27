use std::collections::HashMap;

use arroyo_types::TaskInfo;
use prometheus::{
    register_histogram, register_int_gauge, Histogram, HistogramOpts, IntGauge, Opts,
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
