use std::collections::HashMap;
use std::time::Duration;
use anyhow::{anyhow, bail, Context};
use reqwest::{ClientBuilder};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use arroyo_openapi::{Client, Error};
use url::Url;
use arroyo_openapi::types::{Job, OutputData, PipelinePost, ValidateQueryPost};
use crate::VERSION;
use eventsource_client::{Client as ESClient, SSE};
use futures::future::err;
use serde_json::Value;
use tokio::select;
use tokio_stream::StreamExt;
use arroyo_openapi::builder::ValidateQuery;


const LOGO: &str = include_str!("../logo.utf8ans");

pub async fn start_query(endpoint: Option<String>) -> anyhow::Result<()> {
    let logo = LOGO.replace("VERSION", VERSION);
    println!("{}", logo);

    let c = ClientBuilder::new().timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let endpoint = endpoint.as_ref().map(|t| t.as_str())
        .unwrap_or("http://localhost:8000/");

    let mut url = Url::parse(endpoint)
        .map_err(|e| anyhow!("Invalid endpoint '{}': {:?}", endpoint, e))?;

    if url.cannot_be_a_base() {
        url = Url::parse(&format!("http://{}", endpoint)).unwrap();
    }

    let api_endpoint = url.join("api").unwrap();

    let client = Client::new_with_client(
        api_endpoint.as_str(), c);

    match client.ping().send().await {
        Ok(_) => {
            println!("\nConnected at {}\n\n\n", client.baseurl());
        }
        Err(e) => {
            bail!("Failed to connect to Arroyo API at {}: {}", client.baseurl(), e.status()
                .as_ref().map(|t| t.as_str()).unwrap_or("unknown error"));
        }
    }

    let mut session = QuerySession::new(client)?;

    session.run().await?;

    Ok(())
}

struct QuerySession {
    client: Client,
    editor: DefaultEditor,

    current_line: String,
}

impl QuerySession {
    fn new(client: Client) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            current_line: String::new(),
            editor: DefaultEditor::new().context("Failed to construct line editor")?,
        })
    }

    async fn load_history(&mut self) -> anyhow::Result<()> {
        let config_dir = dirs::config_dir().unwrap_or_default()
            .join("arroyo");
        tokio::fs::create_dir_all(&config_dir).await
            .map_err(|_| anyhow!("failed to create config directory"))?;

        let _ = self.editor.load_history(&config_dir.join("history.txt"));

        Ok(())
    }

    async fn process_buffer(&mut self) -> anyhow::Result<bool> {
        if !self.current_line.trim_end().ends_with(';') {
            return Ok(false);
        }

        let _ = self.editor.add_history_entry(&self.current_line);

        if let Err(e) = self.run_query(&self.current_line).await {
            eprintln!("Failed to run query: {}", e);
        }

        self.current_line.clear();

        Ok(true)
    }

    async fn get_job(&self, pipeline_id: &str) -> anyhow::Result<Job> {
        self.client.get_pipeline_jobs()
            .id(pipeline_id)
            .send()
            .await?
            .into_inner()
            .data
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("No job found for pipeline"))
    }

    async fn start_pipeline(&self, query: &str) -> anyhow::Result<String> {
        let validation = self.client.validate_query()
            .body(ValidateQueryPost::builder().query(query))
            .send()
            .await?
            .into_inner();

        if let Some(errors) = validation.errors {
            eprintln!("Invalid query:");
            for error in errors {
                eprintln!("  * {}", error);
            }
            bail!("failed to run query");
        }


        let pipeline =
            self.client.post_pipeline()
                .body(PipelinePost::builder()
                    .name("cli-pipeline")
                    .query(query)
                    .parallelism(1)
                    .preview(true))
                .send()
                .await?
            .into_inner();

        Ok(pipeline.id)
    }

    async fn run_query(&self, query: &str) -> anyhow::Result<()> {
        println!("Starting pipeline...");
        let pipeline_id = self.start_pipeline(query).await?;

        let job = self.get_job(&pipeline_id).await?;

        let mut outputs = eventsource_client::ClientBuilder::for_url(
            &format!("{}/v1/pipelines/{}/jobs/{}/output", self.client.baseurl(), pipeline_id, job.id)
        ).unwrap().build().stream();

        let mut last_state = String::new();
        while last_state != "Running" {
            let job = self.get_job(&pipeline_id).await
                .context("waiting for job startup")?;

            if job.state != last_state {
                println!("Job entered {}", job.state);
                last_state = job.state;
            }

            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        loop {
            match outputs.next().await {
                Some(Ok(msg)) => {
                    match msg {
                        SSE::Event(e) => {
                            let Ok(data) = serde_json::from_str::<OutputData>(&e.data) else {
                                eprintln!("received invalid outputs from server");
                                continue;
                            };

                            let Ok(fields) = serde_json::from_str::<Value>(&data.value) else {
                                eprintln!("received invalid data from output operator {}", data.operator_id);
                                continue;
                            };

                            print!("| ");
                            for (k, value) in fields.as_object().unwrap() {
                                print!("{} | ", value);
                            }
                            print!("\n");

                        }
                        SSE::Comment(_) => {
                            // ignore
                        }
                    }
                }
                Some(Err(msg)) => {
                    bail!("error while reading output: {}", msg);
                }
                None => {
                    println!("output completed");
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        if let Err(e) = self.load_history().await {
            eprintln!("Failed to load query history: {}", e);
        }

        loop {
            match self.editor.readline("> ") {
                Ok(line) => {
                    self.current_line.push_str(&line);
                    self.current_line.push('\n');
                },
                Err(ReadlineError::Interrupted) => {
                    println!("CTRL-C");
                    break
                },
                Err(ReadlineError::Eof) => {
                    println!("CTRL-D");
                    break
                },
                Err(err) => {
                    println!("Error: {:?}", err);
                    break
                }
            }

            if self.process_buffer().await? {

            }
        }

        Ok(())
    }
}