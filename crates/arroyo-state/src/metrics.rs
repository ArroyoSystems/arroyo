use lazy_static::lazy_static;
use prometheus::{register_gauge_vec, GaugeVec};

lazy_static! {
    pub static ref WORKER_LABELS_NAMES: Vec<&'static str> = vec!["node_id", "task_id"];
    pub static ref CURRENT_FILES_GAUGE: GaugeVec = register_gauge_vec!(
        "arroyo_worker_current_files",
        "Number of parquet files in the checkpoint",
        &WORKER_LABELS_NAMES
    )
    .unwrap();
    pub static ref TABLE_LABELS_NAMES: Vec<&'static str> = vec!["node_id", "task_id", "table_char"];
    pub static ref TABLE_SIZE_GAUGE: GaugeVec = register_gauge_vec!(
        "arroyo_worker_table_size_keys",
        "Number of keys in the table",
        &TABLE_LABELS_NAMES
    )
    .unwrap();
}
