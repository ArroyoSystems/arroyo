use anyhow::{Context, Result, ensure};
use arroyo_rpc::grpc::api::ArrowProgram;
use prost::Message;

/// The persisted program's protobuf scheduling view and serialization version.
/// Runtime payloads remain opaque to the controller.
/// Scheduling updates the decoded program's parallelism in memory.
#[derive(Debug)]
pub struct ControllerProgram {
    pub decoded: ArrowProgram,
    pub program_version: u32,
}

impl ControllerProgram {
    pub fn from_bytes(program_version: i32, program_bytes: &[u8]) -> Result<Self> {
        let program_version = u32::try_from(program_version).context("negative program version")?;
        ensure!(program_version > 0, "program version must be non-zero");
        let decoded = ArrowProgram::decode(program_bytes).context("decoding program topology")?;
        Ok(Self {
            decoded,
            program_version,
        })
    }
}
