use arroyo_rpc::grpc::api::{JobLogLevel, JobLogMessage};
use cornucopia_async::GenericClient;
use tonic::Status;

use crate::queries::api_queries;
use crate::types::public::LogLevel;
use crate::{log_and_map, to_micros};

pub(crate) async fn get_operator_errors(
    job_id: &str,
    client: &impl GenericClient,
) -> Result<Vec<JobLogMessage>, Status> {
    let messages = api_queries::get_operator_errors()
        .bind(client, &job_id)
        .all()
        .await
        .map_err(log_and_map)?;

    Ok(messages
        .into_iter()
        .map(|message| {
            let level = match message.log_level {
                LogLevel::info => JobLogLevel::Info,
                LogLevel::warn => JobLogLevel::Warn,
                LogLevel::error => JobLogLevel::Error,
            };

            JobLogMessage {
                operator_id: Some(message.operator_id),
                task_index: Some(message.task_index),
                created_at: to_micros(message.created_at),
                level: i32::from(level),
                message: message.message,
                details: message.details,
            }
        })
        .collect())
}
