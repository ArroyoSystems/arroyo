use arroyo_types::to_micros;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    pub epoch: u32,
    pub backend: String,
    pub start_time: u64,
    pub finish_time: Option<u64>,
    pub events: Vec<CheckpointEventSpan>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointEventSpan {
    pub start_time: u64,
    pub finish_time: u64,
    pub event: String,
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
    pub started_metadata_write: Option<u64>,
    pub finish_time: Option<u64>,
    pub subtasks: Vec<SubtaskCheckpointGroup>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum JobCheckpointEventType {
    Checkpointing,
    CheckpointingOperators,
    WritingMetadata,
    Compacting,
    Committing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCheckpointSpan {
    pub event: JobCheckpointEventType,
    pub start_time: u64,
    pub finish_time: Option<u64>,
}

impl JobCheckpointSpan {
    pub fn now(event: JobCheckpointEventType) -> Self {
        Self {
            event,
            start_time: to_micros(SystemTime::now()),
            finish_time: None,
        }
    }

    pub fn finish(&mut self) {
        if self.finish_time.is_none() {
            self.finish_time = Some(to_micros(SystemTime::now()));
        }
    }
}

impl From<JobCheckpointSpan> for CheckpointEventSpan {
    fn from(value: JobCheckpointSpan) -> Self {
        let description = match value.event {
            JobCheckpointEventType::Checkpointing => "The entire checkpointing process",
            JobCheckpointEventType::CheckpointingOperators => {
                "The time spent checkpointing operator states"
            }
            JobCheckpointEventType::WritingMetadata => "Writing the final checkpoint metadata",
            JobCheckpointEventType::Compacting => "Compacting old checkpoints",
            JobCheckpointEventType::Committing => {
                "Running two-phase commit for transactional connectors"
            }
        }
        .to_string();

        Self {
            start_time: value.start_time,
            finish_time: value.finish_time.unwrap_or_default(),
            event: format!("{:?}", value.event),
            description,
        }
    }
}
