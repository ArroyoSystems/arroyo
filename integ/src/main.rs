use std::time::{Duration, Instant};

use crate::apis::configuration::Configuration;
use crate::apis::jobs_api::get_job_checkpoints;
use crate::apis::pipelines_api::{get_pipeline_jobs, patch_pipeline, post_pipeline};
use crate::models::{PipelinePatch, PipelinePost, StopType};

use anyhow::Result;
use arroyo_rpc::grpc::api::{
    api_grpc_client::ApiGrpcClient, CreateConnectionTableReq, GetConnectionTablesReq,
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

include!("../../arroyo-openapi/client/src/lib.rs");

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

async fn wait_for_state(api_conf: &Configuration, pipeline_id: &str, expected_state: &str) {
    let mut last_state = "None".to_string();
    while last_state != expected_state {
        let jobs = get_pipeline_jobs(&api_conf, &pipeline_id).await.unwrap();
        let job = jobs.data.first().unwrap();

        let state = job.state.clone();
        if last_state != state {
            info!("Job transitioned to {}", state);
            last_state = state;
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

        if client
            .get_connection_tables(GetConnectionTablesReq {})
            .await
            .is_ok()
        {
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
    let api_conf = Configuration {
        base_path: "http://localhost:8000/api".to_string(),
        user_agent: None,
        client: Default::default(),
        basic_auth: None,
        oauth_access_token: None,
        bearer_access_token: None,
        api_key: None,
    };

    // create a source
    let source_name = format!("source_{}", run_id);
    info!("Creating source {}", source_name);
    client
        .create_connection_table(CreateConnectionTableReq {
            name: source_name.clone(),
            schema: None,
            connector: "nexmark".to_string(),
            connection_id: None,
            config: "{\"event_rate\": 10.0}".to_string(),
        })
        .await
        .unwrap();
    info!("Created connection table");

    // create a pipeline
    let pipeline_name = format!("pipeline_{}", run_id);
    info!("Creating pipeline {}", pipeline_name);

    let pipeline_id = post_pipeline(
        &api_conf,
        PipelinePost {
            name: pipeline_name,
            parallelism: 1,
            preview: None,
            query: format!(
                "select count(*) from {} where auction is not null group \
                by hop(interval '2 seconds', interval '10 seconds')",
                source_name
            ),
            udfs: vec![],
        },
    )
    .await
    .unwrap()
    .id;

    info!("Created pipeline {}", pipeline_id);

    // wait for job to enter running phase
    info!("Waiting until running");
    wait_for_state(&api_conf, &pipeline_id, "Running").await;

    let jobs = get_pipeline_jobs(&api_conf, &pipeline_id).await.unwrap();
    let job = jobs.data.first().unwrap();

    // wait for a checkpoint
    info!("Waiting for 10 successful checkpoints");
    loop {
        let checkpoints = get_job_checkpoints(&api_conf, &pipeline_id, &job.id)
            .await
            .unwrap();

        if let Some(checkpoint) = checkpoints.data.iter().find(|c| c.epoch == 10) {
            if checkpoint.finish_time.is_some() {
                break;
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // stop job
    info!("Stopping job");
    patch_pipeline(
        &api_conf,
        &pipeline_id,
        PipelinePatch {
            checkpoint_interval_micros: None,
            parallelism: None,
            stop: Some(Some(StopType::Checkpoint)),
        },
    )
    .await
    .unwrap();

    info!("Waiting for stop");
    wait_for_state(&api_conf, &pipeline_id, "Stopped").await;

    info!("Test successful ✅")
}
