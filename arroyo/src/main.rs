use anyhow::Result;
use bollard::container::{Config, CreateContainerOptions, StartContainerOptions};
use bollard::image::CreateImageOptions;
use bollard::Docker;
use clap::{Parser, Subcommand};
use tokio_stream::StreamExt;

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
    },
}

#[tokio::main]
pub async fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Start { tag } => {
            start(tag.clone()).await.unwrap();
            return;
        }
    }
}

pub async fn start(tag: Option<String>) -> Result<()> {
    let docker = Docker::connect_with_local_defaults()?;

    let tag = tag.as_ref().map(|t| t.as_str()).unwrap_or("latest");
    let image = format!("arroyo-single:{}", tag);

    docker
        .create_image(
            Some(CreateImageOptions {
                from_image: image.clone(),
                repo: "ghcr.io/arroyosystems".to_string(),
                ..Default::default()
            }),
            None,
            None,
        )
        .next()
        .await
        .unwrap()?;

    let config = Config {
        image: Some(image.clone()),
        ..Default::default()
    };

    docker
        .create_container(
            Some(CreateContainerOptions {
                name: "arroyo-single",
                platform: None,
            }),
            config,
        )
        .await?;

    docker
        .start_container("arroyo-single", None::<StartContainerOptions<String>>)
        .await?;

    println!("Started container");

    Ok(())
}
