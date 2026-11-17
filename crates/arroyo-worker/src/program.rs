use arroyo_connectors::{ConnectorLookupError, connector_for_type};
use arroyo_datastream::logical::OperatorName;
use arroyo_rpc::grpc::api::{self, ArrowProgram};
use arroyo_rpc::grpc::rpc::StartExecutionReq;
use prost::Message;
use std::str::FromStr;
use tonic::Status;

const SUPPORTED_PROGRAM_VERSION: u32 = 3;

#[derive(Debug)]
pub(crate) enum ProgramAdmissionError {
    UnsupportedVersion(u32),
    InvalidArgument(String),
    ConnectorUnavailable(ConnectorLookupError),
}

impl ProgramAdmissionError {
    pub(crate) fn into_status(self) -> Status {
        match self {
            Self::UnsupportedVersion(version) => Status::failed_precondition(format!(
                "unsupported program version {version}; this worker supports version {SUPPORTED_PROGRAM_VERSION}"
            )),
            Self::InvalidArgument(message) => Status::invalid_argument(message),
            Self::ConnectorUnavailable(err) => Status::failed_precondition(err.to_string()),
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
    let Some(program) = &req.program else {
        return Err(ProgramAdmissionError::InvalidArgument(
            "start execution request is missing a program".into(),
        ));
    };
    check_connectors_available(program)
}

/// Rejects a program that uses a connector this worker wasn't built with, so
/// the job fails with a clear error instead of the worker panicking while
/// constructing operators.
fn check_connectors_available(program: &ArrowProgram) -> Result<(), ProgramAdmissionError> {
    let operators = program.nodes.iter().flat_map(|node| &node.operators);

    for op in operators {
        // Malformed operators are reported when the program is constructed.
        let Ok(name) = OperatorName::from_str(&op.operator_name) else {
            continue;
        };
        let connector = match name {
            OperatorName::ConnectorSource | OperatorName::ConnectorSink => {
                api::ConnectorOp::decode(op.operator_config.as_slice())
                    .ok()
                    .map(|c| c.connector)
            }
            OperatorName::LookupJoin => {
                api::LookupJoinOperator::decode(op.operator_config.as_slice())
                    .ok()
                    .and_then(|c| c.connector)
                    .map(|c| c.connector)
            }
            OperatorName::ArrowValue
            | OperatorName::ArrowKey
            | OperatorName::Projection
            | OperatorName::AsyncUdf
            | OperatorName::ExpressionWatermark
            | OperatorName::Join
            | OperatorName::InstantJoin
            | OperatorName::WindowFunction
            | OperatorName::TumblingWindowAggregate
            | OperatorName::SlidingWindowAggregate
            | OperatorName::SessionWindowAggregate
            | OperatorName::UpdatingAggregate => None,
        };

        if let Some(connector) = connector {
            connector_for_type(&connector).map_err(ProgramAdmissionError::ConnectorUnavailable)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arroyo_rpc::grpc::api::{ArrowNode, ChainedOperator};

    #[test]
    fn rejects_program_with_unavailable_connector() {
        let config = api::ConnectorOp {
            connector: "not-a-connector".to_string(),
            ..Default::default()
        };
        let req = StartExecutionReq {
            program_version: Some(SUPPORTED_PROGRAM_VERSION),
            program: Some(ArrowProgram {
                nodes: vec![ArrowNode {
                    operators: vec![ChainedOperator {
                        operator_name: OperatorName::ConnectorSource.to_string(),
                        operator_config: config.encode_to_vec(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };

        let status = validate_start_execution_program(&req)
            .expect_err("program should be rejected")
            .into_status();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), "unknown connector 'not-a-connector'");
    }
}
