use arroyo_rpc::grpc::{
    SubtaskCheckpointMetadata, TaskCheckpointCompletedReq, TaskCheckpointEventReq,
    TaskCheckpointEventType,
};
use arroyo_types::from_micros;
use std::time::SystemTime;

#[allow(unused)]
pub struct SubtaskState {
    pub start_time: Option<SystemTime>,
    pub finish_time: Option<SystemTime>,
    pub metadata: Option<SubtaskCheckpointMetadata>,
}

#[allow(unused)]
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
