//! Typed events shared by analytics backends.

use crate::EventLevel;
use crate::config::Scheduler;
use serde::Serialize;
use std::collections::HashSet;

/// An event's identity, level, and named JSON fields.
pub trait AnalyticsEvent {
    fn event_type(&self) -> &'static str;
    /// Event fields without the transport envelope or process metadata.
    fn to_json(&self) -> serde_json::Result<serde_json::Value>;
    fn level() -> EventLevel
    where
        Self: Sized;
}

#[derive(Debug, Serialize)]
pub struct ApiError<'a> {
    pub error: &'a str,
}

impl AnalyticsEvent for ApiError<'_> {
    fn event_type(&self) -> &'static str {
        "api_error"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Analytics
    }
}

#[derive(Debug, Serialize)]
pub struct JobCreated<'a> {
    pub service: &'a str,
    pub is_preview: bool,
    pub job_id: &'a str,
    pub parallelism: u64,
    pub has_udfs: bool,
    pub rust_udfs: bool,
    pub python_udfs: bool,
    pub features: HashSet<String>,
}

impl AnalyticsEvent for JobCreated<'_> {
    fn event_type(&self) -> &'static str {
        "job_created"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Analytics
    }
}

#[derive(Debug, Serialize)]
pub struct ServiceStartup<'a> {
    pub service: &'a str,
    pub scheduler: &'a Scheduler,
}

impl AnalyticsEvent for ServiceStartup<'_> {
    fn event_type(&self) -> &'static str {
        "service_startup"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Analytics
    }
}

#[derive(Debug, Serialize)]
pub struct PipelineClusterStart {}

impl AnalyticsEvent for PipelineClusterStart {
    fn event_type(&self) -> &'static str {
        "pipeline_cluster_start"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Analytics
    }
}

#[derive(Debug, Serialize)]
pub struct StateTransition<'a> {
    pub service: &'a str,
    pub job_id: &'a str,
    pub from: &'a str,
    pub to: &'a str,
    pub scheduler: &'a Scheduler,
    pub duration_ms: f64,
}

impl AnalyticsEvent for StateTransition<'_> {
    fn event_type(&self) -> &'static str {
        "state_transition"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Analytics
    }
}

#[derive(Debug, Serialize)]
pub struct FatalStateError<'a> {
    pub service: &'a str,
    pub job_id: &'a str,
    pub state: &'a str,
    pub error_message: &'a str,
    pub domain: &'a str,
    pub error: &'a str,
    pub retries: u32,
}

impl AnalyticsEvent for FatalStateError<'_> {
    fn event_type(&self) -> &'static str {
        "fatal_state_error"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Analytics
    }
}

#[derive(Debug, Serialize)]
pub struct StateError<'a> {
    pub service: &'a str,
    pub job_id: &'a str,
    pub state: &'a str,
    pub error_message: &'a str,
    pub domain: &'a str,
    pub error: &'a str,
    pub retries: f64,
}

impl AnalyticsEvent for StateError<'_> {
    fn event_type(&self) -> &'static str {
        "state_error"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Analytics
    }
}

#[derive(Debug, Serialize)]
pub struct JobRunning<'a> {
    pub service: &'a str,
    pub job_id: &'a str,
    pub scheduler: &'a Scheduler,
    pub duration_ms: f64,
}

impl AnalyticsEvent for JobRunning<'_> {
    fn event_type(&self) -> &'static str {
        "job_running"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Analytics
    }
}

#[derive(Debug, Serialize)]
pub struct TaskError<'a> {
    pub service: &'a str,
    pub job_id: &'a str,
    pub operator_id: &'a str,
    pub subtask_index: u32,
    pub error: &'a str,
    pub domain: &'a str,
}

impl AnalyticsEvent for TaskError<'_> {
    fn event_type(&self) -> &'static str {
        "task_error"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Analytics
    }
}

#[derive(Debug, Serialize)]
pub struct RunningError<'a> {
    pub service: &'a str,
    pub job_id: &'a str,
    pub error: &'a str,
}

impl AnalyticsEvent for RunningError<'_> {
    fn event_type(&self) -> &'static str {
        "running_error"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Analytics
    }
}

#[derive(Debug, Serialize)]
pub struct Checkpoint<'a> {
    pub operator_id: Option<&'a str>,
    pub operator_name: Option<&'a str>,
    pub subtask_idx: &'a str,
    pub epoch: &'a str,
    pub size_bytes: f64,
    pub total_time: f64,
    pub alignment_time: f64,
    pub sync_time: f64,
    pub async_time: f64,
    pub commit_time: f64,
}

impl AnalyticsEvent for Checkpoint<'_> {
    fn event_type(&self) -> &'static str {
        "checkpoint"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Trace
    }
}

#[derive(Debug, Serialize)]
pub struct FilesystemWrite<'a> {
    pub operator_id: &'a str,
    pub operator_name: &'a str,
    pub connection_id: &'a str,
    pub write_error_reason: &'a str,
    pub subtask_idx: u32,
    pub output_format: &'a str,
    pub table_format: &'a str,
    pub bytes_written: f64,
    pub files_written: f64,
    pub write_time_ms: f64,
    pub write_failures: f64,
    pub row_groups_written: f64,
    pub uncompressed_bytes_written: f64,
    pub rows_written: f64,
}

impl AnalyticsEvent for FilesystemWrite<'_> {
    fn event_type(&self) -> &'static str {
        "filesystem_write"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Trace
    }
}

#[derive(Debug, Serialize)]
pub struct FilesystemOutputSkipped<'a> {
    pub job_id: &'a str,
    pub object: &'a str,
    pub operator_id: &'a str,
    pub pipeline_id: &'a str,
    pub expected_size: f64,
}

impl AnalyticsEvent for FilesystemOutputSkipped<'_> {
    fn event_type(&self) -> &'static str {
        "filesystem_output_skipped"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Analytics
    }
}

#[derive(Debug, Serialize)]
pub struct UserError<'a> {
    pub error_family: &'a str,
    pub error_type: &'a str,
    pub details: &'a str,
}

impl AnalyticsEvent for UserError<'_> {
    fn event_type(&self) -> &'static str {
        "user_error"
    }

    fn to_json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self)
    }

    fn level() -> EventLevel {
        EventLevel::Analytics
    }
}
