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

#[cfg(test)]
mod tests {
    use super::*;
    use arroyo_rpc::grpc::api::ArrowProgram;
    use prost::Message;
    use tonic::Code;

    fn legacy_request() -> StartExecutionReq {
        StartExecutionReq {
            program: Some(ArrowProgram::default()),
            ..Default::default()
        }
    }

    #[test]
    fn accepts_supported_and_legacy_versions() {
        for program_version in [None, Some(2)] {
            let req = StartExecutionReq {
                program_version,
                ..legacy_request()
            };
            validate_start_execution_program(&req).unwrap();
        }
    }

    #[test]
    fn rejects_unsupported_versions() {
        for version in [0, 3, u32::MAX] {
            let mut req = legacy_request();
            req.program_version = Some(version);
            let req = StartExecutionReq::decode(req.encode_to_vec().as_slice()).unwrap();
            let error = validate_start_execution_program(&req)
                .unwrap_err()
                .into_status();
            assert_eq!(error.code(), Code::FailedPrecondition);
            assert!(
                error
                    .message()
                    .contains(&format!("unsupported program version {version}")),
                "{error}"
            );
        }
    }

    #[test]
    fn rejects_missing_program() {
        let error = validate_start_execution_program(&StartExecutionReq::default())
            .unwrap_err()
            .into_status();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert!(error.message().contains("missing a program"));
    }
}
