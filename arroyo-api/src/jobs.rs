use arroyo_datastream::Program;
use arroyo_rpc::grpc::{
    self,
    api::{
        CheckpointDetailsResp, CheckpointOverview, CreateJobReq, JobDetailsResp, JobStatus,
        OutputData, PipelineProgram, StopType,
    },
    controller_grpc_client::ControllerGrpcClient,
};
use cornucopia_async::GenericClient;
use deadpool_postgres::{Pool, Transaction};
use prost::Message;
use rand::{distributions::Alphanumeric, Rng};
use serde_json::from_str;
use std::{collections::HashMap, time::Duration};
use tokio::sync::mpsc::Receiver;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Channel, Request, Status, Streaming};
use tracing::info;

const PREVIEW_TTL: Duration = Duration::from_secs(60);

use crate::{log_and_map, pipelines, queries::api_queries, to_micros, types::public, AuthData};

fn gen_id() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

pub(crate) async fn create_job<'a>(
    request: CreateJobReq,
    auth: AuthData,
    client: &Transaction<'a>,
) -> Result<String, Status> {
    let pipeline = pipelines::get_pipeline(&request.pipeline_id, &auth, client).await?;

    let checkpoint_interval = if request.preview {
        Duration::from_secs(24 * 60 * 60)
    } else {
        Duration::from_micros(request.checkpoint_interval_micros)
    };

    if checkpoint_interval < Duration::from_secs(1)
        || checkpoint_interval > Duration::from_secs(24 * 60 * 60)
    {
        return Err(Status::invalid_argument(
            "checkpoint_interval_micros must be between 1 second and 1 day",
        ));
    }

    let running_jobs = get_jobs(&auth, client)
        .await?
        .iter()
        .filter(|j| j.running_desired && j.state != "Failed" && j.state != "Finished")
        .count();

    if running_jobs > auth.org_metadata.max_running_jobs as usize {
        return Err(Status::failed_precondition(format!("You have exceeded the maximum number
            of running jobs in your plan ({}). Stop an existing job or contact support@arroyo.systems for
            an increase", auth.org_metadata.max_running_jobs)));
    }

    let job_id = gen_id();

    // TODO: handle chance of collision in ids
    api_queries::create_job()
        .bind(
            client,
            &job_id,
            &auth.organization_id,
            &pipeline.name,
            &auth.user_id,
            &from_str(&pipeline.pipeline_id).unwrap(),
            &(checkpoint_interval.as_micros() as i64),
            &(if request.preview {
                Some(PREVIEW_TTL.as_micros() as i64)
            } else {
                None
            }),
        )
        .await
        .map_err(log_and_map)?;

    api_queries::create_job_status()
        .bind(client, &job_id, &auth.organization_id)
        .await
        .map_err(log_and_map)?;

    Ok(job_id)
}

pub(crate) async fn get_jobs(
    auth: &AuthData,
    client: &impl GenericClient,
) -> Result<Vec<JobStatus>, Status> {
    let res = api_queries::get_jobs()
        .bind(client, &auth.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;

    Ok(res
        .into_iter()
        .map(|rec| JobStatus {
            job_id: rec.id,
            pipeline_name: rec.pipeline_name,
            running_desired: rec.stop == public::StopMode::none,
            state: rec.state.unwrap_or_else(|| "Created".to_string()),
            run_id: rec.run_id.unwrap_or(0) as u64,
            start_time: rec.start_time.map(to_micros),
            finish_time: rec.finish_time.map(to_micros),
            tasks: rec.tasks.map(|t| t as u64),
            definition: rec.textual_repr,
            definition_id: format!("{}", rec.pipeline_definition),
            failure_message: rec.failure_message,
        })
        .collect())
}

pub(crate) async fn get_job_details(
    job_id: &str,
    auth: &AuthData,
    client: &impl GenericClient,
) -> Result<JobDetailsResp, Status> {
    enum Progress {
        InProgress,
        Stable,
    }

    use Progress::*;
    use StopType::*;

    let res = api_queries::get_job_details()
        .bind(client, &auth.organization_id, &job_id)
        .opt()
        .await
        .map_err(log_and_map)?
        .ok_or_else(|| Status::not_found(format!("There is no job with id '{}'", job_id)))?;

    let mut program: Program = PipelineProgram::decode(&res.program[..])
        .map_err(log_and_map)?
        .try_into()
        .map_err(log_and_map)?;

    program.update_parallelism(
        &res.parallelism_overrides
            .as_object()
            .unwrap()
            .into_iter()
            .map(|(k, v)| (k.clone(), v.as_u64().unwrap() as usize))
            .collect(),
    );

    let state = res.state.unwrap_or_else(|| "Created".to_string());
    let running_desired = res.stop == public::StopMode::none;

    let (action_text, action, in_progress) = match (state.as_ref(), running_desired) {
        ("Created", true) => ("Stop", Some(Checkpoint), InProgress),
        ("Created", false) => ("Start", Some(None), Stable),

        ("Compiling", true) => ("Stop", Some(Checkpoint), InProgress),
        ("Compiling", false) => ("Stopping", Option::None, InProgress),

        ("Scheduling", true) => ("Stop", Some(Checkpoint), InProgress),
        ("Scheduling", false) => ("Stopping", Option::None, InProgress),

        ("Running", true) => ("Stop", Some(Checkpoint), Stable),
        ("Running", false) => ("Stopping", Option::None, InProgress),

        ("Rescaling", true) => ("Stop", Some(Checkpoint), InProgress),
        ("Rescaling", false) => ("Stopping", Option::None, InProgress),

        ("CheckpointStopping", true) => ("Force Stop", Some(Immediate), InProgress),
        ("CheckpointStopping", false) => ("Force Stop", Some(Immediate), InProgress),

        ("Recovering", true) => ("Stop", Some(Checkpoint), InProgress),
        ("Recovering", false) => ("Stopping", Option::None, InProgress),

        ("Stopping", true) => ("Stopping", Some(Checkpoint), InProgress),
        ("Stopping", false) => ("Stopping", Option::None, InProgress),

        ("Stopped", true) => ("Starting", Option::None, InProgress),
        ("Stopped", false) => ("Start", Some(None), Stable),

        ("Finishing", true) => ("Finishing", Option::None, InProgress),
        ("Finishing", false) => ("Finishing", Option::None, InProgress),

        ("Finished", true) => ("Finished", Option::None, Stable),
        ("Finished", false) => ("Finished", Option::None, Stable),

        ("Failed", true) => ("Failed", Option::None, Stable),
        ("Failed", false) => ("Start", Some(None), Stable),

        _ => panic!("unhandled state {}", state),
    };

    let status = JobStatus {
        job_id: job_id.to_string(),
        pipeline_name: res.pipeline_name,
        running_desired,
        state,
        run_id: res.run_id.unwrap_or(0) as u64,
        start_time: res.start_time.map(to_micros),
        finish_time: res.finish_time.map(to_micros),
        tasks: res.tasks.map(|t| t as u64),
        definition: res.textual_repr,
        definition_id: format!("{}", res.pipeline_definition),
        failure_message: res.failure_message,
    };

    Ok(JobDetailsResp {
        job_status: Some(status),
        job_graph: Some(program.as_job_graph()),

        action: action.map(|action| action as i32),
        action_text: action_text.to_string(),
        in_progress: match in_progress {
            InProgress => true,
            Stable => false,
        },
    })
}

pub(crate) async fn get_job_checkpoints(
    job_id: &str,
    auth: AuthData,
    client: &impl GenericClient,
) -> Result<Vec<CheckpointOverview>, Status> {
    let res = api_queries::get_job_checkpoints()
        .bind(client, &job_id, &auth.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;

    Ok(res
        .into_iter()
        .map(|rec| CheckpointOverview {
            epoch: rec.epoch as u32,
            backend: rec.state_backend,
            start_time: to_micros(rec.start_time),
            finish_time: rec.finish_time.map(to_micros),
        })
        .collect())
}

pub(crate) async fn checkpoint_details(
    job_id: &str,
    epoch: u32,
    auth: AuthData,
    client: &impl GenericClient,
) -> Result<CheckpointDetailsResp, Status> {
    let res = api_queries::get_checkpoint_details()
        .bind(client, &job_id, &auth.organization_id, &(epoch as i32))
        .opt()
        .await
        .map_err(log_and_map)?
        .ok_or_else(|| {
            Status::not_found(format!(
                "There is no checkpoint with epoch for job {} '{}'",
                epoch, job_id
            ))
        })?;

    Ok(CheckpointDetailsResp {
        overview: Some(CheckpointOverview {
            epoch,
            backend: res.state_backend,
            start_time: to_micros(res.start_time),
            finish_time: res.finish_time.map(to_micros),
        }),
        operators: res
            .operators
            .map(|o| serde_json::from_value(o).unwrap())
            .unwrap_or_else(HashMap::new),
    })
}

pub(crate) async fn delete_job(job_id: &str, auth: AuthData, pool: &Pool) -> Result<(), Status> {
    let mut client = pool.get().await.map_err(log_and_map)?;

    let transaction = client.transaction().await.map_err(log_and_map)?;

    let job_details = get_job_details(job_id, &auth, &transaction).await?;

    if let Some(status) = job_details.job_status {
        if !(status.state == "Stopped" || status.state == "Finished" || status.state == "Failed") {
            return Err(Status::failed_precondition(
                "Job must be in a terminal state (stopped, finished, or failed)
                before it can be deleted",
            ));
        }
    }

    api_queries::delete_pipeline_for_job()
        .bind(&transaction, &job_id, &auth.organization_id)
        .await
        .map_err(log_and_map)?;

    transaction.commit().await.map_err(log_and_map)?;

    Ok(())
}

pub struct JobStream {
    job_id: String,
    stream: Streaming<arroyo_rpc::grpc::OutputData>,
}

impl JobStream {
    fn new(job_id: String, stream: Streaming<arroyo_rpc::grpc::OutputData>) -> Self {
        Self { job_id, stream }
    }
}

pub(crate) async fn get_job_stream(
    job_id: String,
    mut controller: ControllerGrpcClient<Channel>,
    auth: AuthData,
    tx: &impl GenericClient,
) -> Result<JobStream, Status> {
    // subscribe to the output
    let details = get_job_details(&job_id, &auth, tx).await?;
    if !details
        .job_graph
        .unwrap()
        .nodes
        .iter()
        .any(|n| n.operator.contains("GrpcSink"))
    {
        // TODO: make this check more robust
        return Err(Status::invalid_argument(format!(
            "Job {} does not have a web sink",
            job_id
        )));
    }

    info!("connected to controller");

    let stream = controller
        .subscribe_to_output(Request::new(grpc::GrpcOutputSubscription {
            job_id: job_id.clone(),
        }))
        .await
        .map_err(log_and_map)?
        .into_inner();

    Ok(JobStream::new(job_id, stream))
}

pub(crate) async fn get_lines_from_job_output(
    mut stream: JobStream,
    num_lines: usize,
) -> Vec<Result<OutputData, Status>> {
    let mut captured = Vec::<Result<OutputData, Status>>::new();
    while let Some(d) = stream.stream.next().await {
        if d.as_ref().map(|t| t.done).unwrap_or(false) {
            info!("Stream done for {}", stream.job_id);
            break;
        }

        let v = d.map(|d| OutputData {
            operator_id: d.operator_id,
            timestamp: d.timestamp,
            key: d.key,
            value: d.value,
        });

        captured.push(v);

        if captured.len() >= num_lines {
            break;
        }
    }
    captured
}

pub(crate) async fn get_stream_from_job_output(
    stream: JobStream,
) -> ReceiverStream<Result<OutputData, Status>> {
    let (tx, rx) = tokio::sync::mpsc::channel(32);
    tokio::spawn(async move {
        let mut _stream = stream.stream;
        while let Some(d) = _stream.next().await {
            if d.as_ref().map(|t| t.done).unwrap_or(false) {
                info!("Stream done for {}", stream.job_id);
                break;
            }

            let v = d.map(|d| OutputData {
                operator_id: d.operator_id,
                timestamp: d.timestamp,
                key: d.key,
                value: d.value,
            });

            if tx.send(v).await.is_err() {
                break;
            }
        }

        info!("Closing watch stream for {}", stream.job_id);
    });
    ReceiverStream::new(rx)
}
