use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    pub epoch: u32,
    pub backend: String,
    pub start_time: u64,
    pub finish_time: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointSpanType {
    Alignment,
    Sync,
    Async,
    Committing,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointEventSpan {
    pub start_time: u64,
    pub finish_time: u64,
    pub span_type: CheckpointSpanType,
    pub description: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubtaskCheckpointGroup {
    pub index: u32,
    pub bytes: u64,
    pub event_spans: Vec<CheckpointEventSpan>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OperatorCheckpointGroup {
    pub operator_id: String,
    pub bytes: u64,
    pub subtasks: Vec<SubtaskCheckpointGroup>,
}
