use std::env;
use std::time::{Duration, Instant};

use anyhow::Result;
use arroyo_openapi::types::{ConnectionTablePost, PipelinePatch, PipelinePost, StopType, Udf};
use arroyo_openapi::Client;
use rand::RngCore;
use serde_json::json;
use tracing::{info, warn};

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

async fn wait_for_state(client: &Client, pipeline_id: &str, expected_state: &str) {
    let mut last_state = "None".to_string();
    while last_state != expected_state {
        let jobs = client
            .get_pipeline_jobs()
            .id(pipeline_id)
            .send()
            .await
            .unwrap();
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

async fn connect() -> Client {
    let start = Instant::now();
    
    let client = Client::new(format!("{}/api", env::var("API_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:8000".to_string())));

    loop {
        if start.elapsed() > CONNECT_TIMEOUT {
            panic!(
                "Failed to connect to API server after {:?}",
                CONNECT_TIMEOUT
            );
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        if client.ping().send().await.is_ok() {
            info!("Connected to API server");
            return client;
        }
    }
}

async fn start_pipeline(client: &Client, run_id: u32, query: &str, udfs: &[&str]) -> anyhow::Result<String> {
    let pipeline_name = format!("pipeline_{}", run_id);
    info!("Creating pipeline {}", pipeline_name);

    let pipeline_id = client
        .post_pipeline()
        .body(
            PipelinePost::builder()
                .name(pipeline_name)
                .parallelism(1)
                .checkpoint_interval_micros(1_000_000)
                .query(query)
                .udfs(Some(udfs.iter().map(|udf| Udf::builder().definition(*udf).into()).collect())),
        )
        .send()
        .await?
        .into_inner()
        .id;

    info!("Created pipeline {}", pipeline_id);
    Ok(pipeline_id)
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

    let output_dir = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/tmp/arroyo".to_string());

    info!("Starting services");
    run_service(
        format!("target/{}/arroyo-bin", profile),
        &["cluster"],
        vec![("CHECKPOINT_URL".to_string(), output_dir.clone())],
    )
    .expect("Failed to run cluster");

    let api_client = connect().await;

    // create a source
    let source_name = format!("source_{}", run_id);
    info!("Creating source {}", source_name);

    let _ = api_client
        .create_connection_table()
        .body(
            ConnectionTablePost::builder()
                .config(json!({"event_rate": 10}))
                .connector("impulse")
                .name(source_name.clone()),
        )
        .send()
        .await;

    info!("Created connection table");

    // create a pipeline

    // wait for job to enter running phase
    info!("Waiting until running");
    wait_for_state(&api_client, &pipeline_id, "Running").await;

    let jobs = api_client
        .get_pipeline_jobs()
        .id(&pipeline_id)
        .send()
        .await
        .unwrap();
    let job = jobs.data.first().unwrap();

    // wait for a checkpoint
    info!("Waiting for 3 successful checkpoints");
    loop {
        let checkpoints = api_client
            .get_job_checkpoints()
            .pipeline_id(&pipeline_id)
            .job_id(&job.id)
            .send()
            .await
            .unwrap();

        if let Some(checkpoint) = checkpoints.data.iter().find(|c| c.epoch == 3) {
            if checkpoint.finish_time.is_some() {
                break;
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // stop job
    info!("Stopping job");
    api_client
        .patch_pipeline()
        .id(&pipeline_id)
        .body(PipelinePatch::builder().stop(StopType::Checkpoint))
        .send()
        .await
        .unwrap();

    info!("Waiting for stop");
    wait_for_state(&api_client, &pipeline_id, "Stopped").await;

    info!("Test successful ✅")
}
