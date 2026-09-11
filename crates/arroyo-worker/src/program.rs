use arroyo_rpc::grpc::rpc::StartExecutionReq;
use tonic::Status;

const SUPPORTED_PROGRAM_VERSION: u32 = 2;

#[derive(Debug)]
pub(crate) enum ProgramAdmissionError {
    UnsupportedVersion(u32),
    InvalidArgument(String),
}

impl ProgramAdmissionError {
    pub(crate) fn into_status(self) -> Status {
        match self {
            Self::UnsupportedVersion(version) => Status::failed_precondition(format!(
                "unsupported program version {version}; this worker supports version {SUPPORTED_PROGRAM_VERSION}"
            )),
            Self::InvalidArgument(message) => Status::invalid_argument(message),
        }
    }
}

pub(crate) fn validate_start_execution_program(
    req: &StartExecutionReq,
) -> Result<(), ProgramAdmissionError> {
    // Controllers predating this field only send v2 programs.
    let program_version = req.program_version.unwrap_or(2);
    if program_version != SUPPORTED_PROGRAM_VERSION {
        return Err(ProgramAdmissionError::UnsupportedVersion(program_version));
    }
    if req.program.is_none() {
        return Err(ProgramAdmissionError::InvalidArgument(
            "start execution request is missing a program".into(),
        ));
    }
    Ok(())
}
