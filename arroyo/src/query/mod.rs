mod model;

use crate::query::model::QueryModel;
use crate::VERSION;
use anyhow::{anyhow, bail, Context};
use arroyo_openapi::{Client, Error};
use eventsource_client::{Client as ESClient, SSE};
use reqwest::ClientBuilder;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::time::Duration;
use tokio_stream::StreamExt;
use url::Url;

const LOGO: &str = include_str!("../../logo.utf8ans");

pub async fn start_query(endpoint: Option<String>) -> anyhow::Result<()> {
    let logo = LOGO.replace("VERSION", VERSION);
    println!("{}", logo);

    let c = ClientBuilder::new()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let endpoint = endpoint
        .as_ref()
        .map(|t| t.as_str())
        .unwrap_or("http://localhost:8000/");

    let mut url =
        Url::parse(endpoint).map_err(|e| anyhow!("Invalid endpoint '{}': {:?}", endpoint, e))?;

    if url.cannot_be_a_base() {
        url = Url::parse(&format!("http://{}", endpoint)).unwrap();
    }

    let api_endpoint = url.join("api").unwrap();

    let client = Client::new_with_client(api_endpoint.as_str(), c);

    match client.ping().send().await {
        Ok(_) => {
            println!("\nConnected at {}\n\n\n", client.baseurl());
        }
        Err(e) => {
            bail!(
                "Failed to connect to Arroyo API at {}: {}",
                client.baseurl(),
                e.status()
                    .as_ref()
                    .map(|t| t.as_str())
                    .unwrap_or("unknown error")
            );
        }
    }

    let mut session = QuerySession::new(client)?;

    session.run().await?;

    Ok(())
}

struct QuerySession {
    client: Client,
    model: QueryModel,
    editor: DefaultEditor,
}

impl QuerySession {
    fn new(client: Client) -> anyhow::Result<Self> {
        Ok(Self {
            model: QueryModel::new(client.clone()),
            client,
            editor: DefaultEditor::new().context("Failed to construct line editor")?,
        })
    }

    async fn load_history(&mut self) -> anyhow::Result<()> {
        let config_dir = dirs::config_dir().unwrap_or_default().join("arroyo");
        tokio::fs::create_dir_all(&config_dir)
            .await
            .map_err(|_| anyhow!("failed to create config directory"))?;

        let _ = self.editor.load_history(&config_dir.join("history.txt"));

        Ok(())
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        if let Err(e) = self.load_history().await {
            eprintln!("Failed to load query history: {}", e);
        }

        loop {
            match self.editor.readline("> ") {
                Ok(line) => {
                    if self.model.push(&line) {
                        self.model.process_buffer().await?;
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("CTRL-C");
                    break;
                }
                Err(ReadlineError::Eof) => {
                    println!("CTRL-D");
                    break;
                }
                Err(err) => {
                    println!("Error: {:?}", err);
                    break;
                }
            }
        }

        Ok(())
    }
}
