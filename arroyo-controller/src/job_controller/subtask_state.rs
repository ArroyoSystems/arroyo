use arroyo_rpc::grpc::{
    SubtaskCheckpointMetadata, TaskCheckpointCompletedReq, TaskCheckpointEventReq,
    TaskCheckpointEventType,
};
use arroyo_types::from_micros;
use std::time::SystemTime;

pub struct SubtaskState {
    pub(crate) start_time: Option<SystemTime>,
    pub(crate) finish_time: Option<SystemTime>,
    pub(crate) metadata: Option<SubtaskCheckpointMetadata>,
}

impl SubtaskState {
    pub fn new() -> Self {
        Self {
            start_time: None,
            finish_time: None,
            metadata: None,
        }
    }

    pub fn event(&mut self, c: TaskCheckpointEventReq) {
        if c.event_type() == TaskCheckpointEventType::StartedCheckpointing {
            self.start_time = Some(from_micros(c.time));
        }
    }

    pub fn finish(&mut self, c: TaskCheckpointCompletedReq) {
        self.finish_time = Some(from_micros(c.time));
        self.metadata = Some(c.metadata.unwrap());
    }

    pub fn done(&self) -> bool {
        self.finish_time.is_some()
    }
}
