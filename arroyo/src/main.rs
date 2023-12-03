use crate::query::start_query;
use anyhow::{bail, Context, Result};
use bollard::container::{CreateContainerOptions, LogOutput, LogsOptions, StartContainerOptions};
use bollard::image::CreateImageOptions;
use bollard::models::{ContainerStateStatusEnum, HostConfig, PortBinding};
use bollard::{container, Docker};
use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::process::exit;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::signal::unix::{signal, SignalKind};
use tokio_stream::StreamExt;

const CONTAINER_NAME: &str = "arroyo-cli-single";
pub static VERSION: &str = "0.7.0";

mod query;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Starts an Arroyo cluster in Docker
    Start {
        /// Set the tag to run (defaults to `latest`, the most recent release version)
        #[arg(short, long)]
        tag: Option<String>,

        /// If set, will run in the background
        #[arg(short, long)]
        daemon: bool,
    },

    /// Stops a running Arroyo cluster
    Stop {},

    Connect {
        endpoint: Option<String>,
    },
}

#[tokio::main]
pub async fn main() {
    let cli = Cli::parse();

    let result = match &cli.command {
        Commands::Start { tag, daemon } => start(tag.clone(), *daemon).await,
        Commands::Stop {} => stop().await,
        Commands::Connect { endpoint } => start_query(endpoint.clone()).await,
    };

    if let Err(e) = result {
        eprintln!("\n-------------------------------\n{:?}", e);
        exit(1);
    } else {
        exit(0);
    }
}

async fn get_docker() -> anyhow::Result<Docker> {
    Ok(Docker::connect_with_local_defaults()
        .context("Failed to connect to docker -- is it running?")?)
}

async fn create_image(docker: &Docker, image: &str) -> Result<String> {
    docker
        .create_image(
            Some(CreateImageOptions {
                from_image: image,
                ..Default::default()
            }),
            None,
            None,
        )
        .next()
        .await
        .unwrap()
        .context("Failed to pull image")?;

    // wait for the image to be available

    println!("Waiting for image to be available...");
    loop {
        match docker.inspect_image(&image).await {
            Ok(metadata) => {
                println!();
                return Ok(metadata.id.unwrap());
            }
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => {
                // wait
            }
            Err(e) => {
                bail!("Failed while fetching image metadata from docker: {:?}", e);
            }
        }

        print!(".");
        io::stdout().flush().unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn create_container(docker: &Docker, image: &str) -> Result<bool> {
    let mut ports = HashMap::new();
    ports.insert("8000/tcp", HashMap::new());

    let mut port_map = HashMap::new();
    port_map.insert(
        "8000/tcp".to_string(),
        Some(vec![PortBinding {
            host_ip: Some("0.0.0.0".to_string()),
            host_port: Some("8000".to_string()),
        }]),
    );

    let config = container::Config {
        image: Some(image),
        exposed_ports: Some(ports),
        host_config: Some(HostConfig {
            port_bindings: Some(port_map),
            ..Default::default()
        }),
        ..Default::default()
    };

    match docker
        .create_container(
            Some(CreateContainerOptions {
                name: CONTAINER_NAME,
                platform: None,
            }),
            config,
        )
        .await
    {
        Ok(_) => {}
        Err(bollard::errors::Error::DockerResponseServerError {
            status_code: 409, ..
        }) => {
            // if the container already exists, check if it's running
            if docker
                .inspect_container(CONTAINER_NAME, None)
                .await
                .context("Failed to inspect container")?
                .state
                .unwrap()
                .status
                .unwrap()
                == ContainerStateStatusEnum::RUNNING
            {
                println!("Container already running");
                return Ok(false);
            }
        }
        Err(e) => {
            bail!("Failed to create container: {:?}", e);
        }
    }

    Ok(true)
}

async fn read_logs(docker: &Docker, start_time: SystemTime, tail: bool) -> Result<()> {
    println!("Reading logs");
    let opts: LogsOptions<String> = LogsOptions {
        follow: tail,
        stdout: true,
        stderr: true,
        since: start_time.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        ..Default::default()
    };

    let mut tail = docker.logs(CONTAINER_NAME, Some(opts.clone()));

    while let Some(log) = tail.next().await {
        match log.context("Failed while tailing logs")? {
            LogOutput::StdErr { message } => {
                eprint!("> {}", String::from_utf8_lossy(&message));
            }
            LogOutput::StdOut { message } => {
                print!("> {}", String::from_utf8_lossy(&message));
            }
            LogOutput::StdIn { .. } => {}
            LogOutput::Console { .. } => {}
        }
    }

    Ok(())
}

pub struct ContainerStatus {
    state: ContainerStateStatusEnum,
    api_available: bool,
}

async fn get_status(docker: &Docker) -> anyhow::Result<ContainerStatus> {
    let state = docker
        .inspect_container(CONTAINER_NAME, None)
        .await
        .context("Failed talking to the docker daemon while inspecting")?
        .state
        .unwrap()
        .status
        .unwrap();

    let api_available = reqwest::get("http://localhost:8000").await.is_ok();

    Ok(ContainerStatus {
        state,
        api_available,
    })
}

pub async fn start(tag: Option<String>, damon: bool) -> Result<()> {
    let start_time = SystemTime::now();

    let docker = get_docker().await?;

    let tag = tag.as_ref().map(|t| t.as_str()).unwrap_or("latest");
    let image = format!("ghcr.io/arroyosystems/arroyo-single:{}", tag);

    let image_id = create_image(&docker, &image).await?;
    println!("Pulled image {}", image_id);

    if !create_container(&docker, &image_id).await? {
        return Ok(());
    }

    docker
        .start_container(CONTAINER_NAME, None::<StartContainerOptions<String>>)
        .await?;

    println!("Started container. Waiting for API to come up...");

    // wait for port
    tokio::time::sleep(Duration::from_secs(5)).await;
    loop {
        let status = get_status(&docker).await?;
        match status.state {
            ContainerStateStatusEnum::CREATED | ContainerStateStatusEnum::RUNNING => {}
            _ => {
                eprintln!("\nDocker container failed... see logs for details:");
                read_logs(&docker, start_time, false).await?;
                bail!("shutting down...");
            }
        }

        if status.api_available {
            break;
        }

        print!(".");
        io::stdout().flush().unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!();

    match open::that("http://localhost:8000") {
        Ok(_) => println!("Opened webui in browser"),
        Err(_) => println!("Failed to open browser... navigate to http://localhost:8000 for webui"),
    }

    if damon {
        return Ok(());
    }

    println!("Tailing logs...\n----------------------");

    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    {
        let docker = docker.clone();
        tokio::spawn(async move {
            match sigint.recv().await {
                None => {}
                Some(_) => {
                    print!("Stopping container...");
                    match docker.stop_container(CONTAINER_NAME, None).await {
                        Ok(_) => {
                            println!("Container stopped");
                        }
                        Err(e) => {
                            eprintln!("Failed to stop container: {:?}", e);
                        }
                    }
                    exit(0);
                }
            }
        });
    }

    read_logs(&docker, start_time, true).await?;

    println!("Container exited");

    Ok(())
}

async fn stop() -> anyhow::Result<()> {
    let docker = get_docker().await?;

    match docker.stop_container(CONTAINER_NAME, None).await {
        Ok(_) => {
            println!("Container stopped");
        }
        Err(bollard::errors::Error::DockerResponseServerError {
            status_code: 404, ..
        }) => {
            println!("Container does not exist")
        }
        Err(e) => {
            bail!("Encountered an error while stopping: {:?}", e);
        }
    }

    Ok(())
}
