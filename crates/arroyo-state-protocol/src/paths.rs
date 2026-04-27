use crate::types::validate_path_component;
use crate::{CheckpointRef, Epoch, Generation, ProtocolError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolPaths {
    pipeline_id: String,
    job_id: String,
}

impl ProtocolPaths {
    pub fn new(
        pipeline_id: impl Into<String>,
        job_id: impl Into<String>,
    ) -> Result<Self, ProtocolError> {
        let pipeline_id = pipeline_id.into();
        let job_id = job_id.into();

        validate_path_component("pipeline_id", &pipeline_id)?;
        validate_path_component("job_id", &job_id)?;

        Ok(Self {
            pipeline_id,
            job_id,
        })
    }

    pub fn current_generation(&self) -> CheckpointRef {
        self.path("current-generation.json")
    }

    pub fn generation_manifest(&self, generation: Generation) -> CheckpointRef {
        self.path(format!("generations/{generation}/generation-manifest.json"))
    }

    pub fn checkpoint_dir(&self, generation: Generation, epoch: Epoch) -> CheckpointRef {
        self.path(format!(
            "generations/{generation}/checkpoints/checkpoint-{epoch:07}"
        ))
    }

    pub fn checkpoint_manifest(&self, generation: Generation, epoch: Epoch) -> CheckpointRef {
        self.path(format!(
            "generations/{generation}/checkpoints/checkpoint-{epoch:07}/checkpoint-manifest.pb"
        ))
    }

    pub fn committed_marker(&self, generation: Generation, epoch: Epoch) -> CheckpointRef {
        self.path(format!(
            "generations/{generation}/checkpoints/checkpoint-{epoch:07}/committed.json"
        ))
    }

    pub fn epoch_record(&self, epoch: Epoch) -> CheckpointRef {
        self.path(format!("epochs/epoch-{epoch:07}.record"))
    }

    fn path(&self, suffix: impl AsRef<str>) -> CheckpointRef {
        CheckpointRef::from_validated(format!(
            "{}/{}/{}",
            self.pipeline_id,
            self.job_id,
            suffix.as_ref()
        ))
    }
}
