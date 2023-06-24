use std::time::{Duration, Instant};

use anyhow::Result;
use arroyo_rpc::grpc::api::{
    api_grpc_client::ApiGrpcClient, create_pipeline_req, CreateJobReq, CreatePipelineReq,
    GetJobsReq, JobCheckpointsReq, JobDetailsReq, StopType, UpdateJobReq,
};
use arroyo_types::DatabaseConfig;
use rand::RngCore;
use tokio_postgres::NoTls;
use tonic::transport::Channel;
use tracing::{info, warn};

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("../arroyo-api/migrations");
}

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

pub fn run_service(name: String, args: &[&str], env: Vec<(String, String)>) -> Result<()> {
    let child = tokio::process::Command::new(&name)
        .args(args)
        .envs(env)
        .kill_on_drop(true)
        .spawn()?;

    tokio::spawn(async move {
        let output = child.wait_with_output().await.unwrap();
        info!(
            "--------------\n{} exited with code {:?}\nstderr: {}",
            name,
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
    });

    Ok(())
}

async fn wait_for_state(client: &mut ApiGrpcClient<Channel>, job_id: &str, expected_state: &str) {
    let mut last_state = "None".to_string();
    while last_state != expected_state {
        let resp = client
            .get_job_details(JobDetailsReq {
                job_id: job_id.to_string(),
            })
            .await
            .unwrap()
            .into_inner();

        let status = resp.job_status.unwrap();
        if last_state != status.state {
            info!("Job transitioned to {}", status.state);
            last_state = status.state;
        }

        if last_state == "Failed" {
            panic!("Job transitioned to failed");
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn connect() -> ApiGrpcClient<Channel> {
    let start = Instant::now();

    loop {
        if start.elapsed() > CONNECT_TIMEOUT {
            panic!(
                "Failed to connect to API server after {:?}",
                CONNECT_TIMEOUT
            );
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        let Ok(mut client) = ApiGrpcClient::connect("http://localhost:8001").await else {
            continue;
        };

        if client.get_jobs(GetJobsReq {}).await.is_ok() {
            return client;
        }
    }
}

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let profile = if std::env::var("DEBUG").is_ok() {
        "debug"
    } else {
        "release"
    };

    let run_id = rand::thread_rng().next_u32();

    let c = DatabaseConfig::load();
    let mut config = tokio_postgres::Config::new();
    config.dbname(&c.name);
    config.host(&c.host);
    config.port(c.port);
    config.user(&c.user);
    config.password(&c.password);

    let (mut client, connection) = config.connect(NoTls).await.unwrap();

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            warn!("Connection error: {}", error);
        }
    });

    info!("Running migrations on {}", c.name);
    embedded::migrations::runner()
        .run_async(&mut client)
        .await
        .unwrap();

    let output_dir = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/tmp/arroyo".to_string());

    info!("Starting services");
    run_service(format!("target/{}/arroyo-api", profile), &[], vec![]).expect("Failed to run api");
    run_service(
        format!("target/{}/arroyo-controller", profile),
        &[],
        vec![
            ("OUTPUT_DIR".to_string(), output_dir.clone()),
            (
                "REMOTE_COMPILER_ENDPOINT".to_string(),
                "http://localhost:9000".to_string(),
            ),
        ],
    )
    .expect("Failed to run controller");
    run_service(
        format!("target/{}/arroyo-compiler-service", profile),
        &["start"],
        vec![("OUTPUT_DIR".to_string(), output_dir.clone())],
    )
    .expect("Failed to run compiler service");

    let mut client = connect().await;

    // create a source
    let source_name = format!("source_{}", run_id);
    info!("Creating source {}", source_name);
    // client
    //     .create_source(CreateSourceReq {
    //         name: source_name.clone(),
    //         schema: None,
    //         type_oneof: Some(
    //             arroyo_rpc::grpc::api::create_source_req::TypeOneof::Nexmark(NexmarkSourceConfig {
    //                 events_per_second: 10,
    //                 runtime_micros: None,
    //             }),
    //         ),
    //     })
    //     .await
    //     .unwrap();
    // info!("Created source");

    todo!();

    // create a pipeline
    let pipeline_name = format!("pipeline_{}", run_id);
    info!("Creating pipeline {}", pipeline_name);
    let pipeline_id = client
        .create_pipeline(CreatePipelineReq {
            name: pipeline_name.clone(),
            config: Some(create_pipeline_req::Config::Sql(
                arroyo_rpc::grpc::api::CreateSqlJob {
                    query: format!(
                        "select count(*) from {} where auction is not null group \
                by hop(interval '2 seconds', interval '10 seconds')",
                        source_name
                    ),
                    parallelism: 1,
                    udfs: vec![],
                    preview: false,
                },
            )),
        })
        .await
        .unwrap()
        .into_inner()
        .pipeline_id;
    info!("Created pipeline {}", pipeline_id);

    // create a job
    info!("Creating job");
    let job_id = client
        .create_job(CreateJobReq {
            pipeline_id: pipeline_id.clone(),
            checkpoint_interval_micros: 2_000_000,
            preview: false,
        })
        .await
        .unwrap()
        .into_inner()
        .job_id;

    info!("Created job {}", job_id);

    // wait for job to enter running phase
    info!("Waiting until running");
    wait_for_state(&mut client, &job_id, "Running").await;

    // wait for a checkpoint
    info!("Waiting for 10 successful checkpoints");
    loop {
        let checkpoints = client
            .get_checkpoints(JobCheckpointsReq {
                job_id: job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner();

        if let Some(checkpoint) = checkpoints.checkpoints.iter().find(|c| c.epoch == 10) {
            if checkpoint.finish_time.is_some() {
                break;
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // stop job
    info!("Stopping job");
    client
        .update_job(UpdateJobReq {
            job_id: job_id.clone(),
            checkpoint_interval_micros: None,
            stop: Some(StopType::Checkpoint as i32),
            parallelism: None,
        })
        .await
        .unwrap();

    info!("Waiting for stop");
    wait_for_state(&mut client, &job_id, "Stopped").await;

    info!("Test successful ✅")
}
