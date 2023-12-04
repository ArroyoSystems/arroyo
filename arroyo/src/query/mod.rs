mod model;
mod runner;

use crate::query::model::{QueryError, QueryModel};
use crate::query::runner::{PipelineEvent, QueryRunner};
use crate::VERSION;
use anyhow::{anyhow, bail, Context};
use arroyo_openapi::{Client, Error};
use crossterm::event::Event::Key;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers};
use crossterm::style::{style, Stylize};
use crossterm::{cursor, execute, queue, style};
use crossterm::{terminal, terminal::ClearType};
use eventsource_client::{Client as ESClient, SSE};
use futures::future::FutureExt;
use reedline::{DefaultPrompt, FileBackedHistory, Reedline, Signal, ValidationResult, Validator};
use reqwest::ClientBuilder;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{stderr, stdout};
use std::time::Duration;
use tokio::select;
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

    let mut session = QuerySession::new(&endpoint, client)?;

    session.run().await?;

    Ok(())
}

pub struct SQLValidator;

impl Validator for SQLValidator {
    fn validate(&self, line: &str) -> ValidationResult {
        if !line.trim_end().ends_with(';') {
            ValidationResult::Incomplete
        } else {
            ValidationResult::Complete
        }
    }
}
struct OutputTable {
    columns: Vec<(String, usize)>,
    width: usize,
}

impl OutputTable {
    pub fn new() -> Self {
        OutputTable {
            columns: vec![],
            width: terminal::size().unwrap().0 as usize,
        }
    }

    pub fn add_data(&mut self, data: &HashMap<String, Value>) {
        if self.columns.is_empty() {
            self.columns = data
                .iter()
                .map(|(k, v)| (k.clone(), v.to_string().len()))
                .collect();
        }
    }
}

struct QuerySession {
    endpoint: Url,
    client: Client,
    model: QueryModel,
    editor: Reedline,
}

impl QuerySession {
    fn new(endpoint: &str, client: Client) -> anyhow::Result<Self> {
        let config_dir = dirs::config_dir().unwrap_or_default().join("arroyo");
        std::fs::create_dir_all(&config_dir)
            .map_err(|_| anyhow!("failed to create config directory"))?;

        let history = Box::new(
            FileBackedHistory::with_file(5, config_dir.join("history.txt").into())
                .expect("Error configuring history with file"),
        );

        Ok(Self {
            endpoint: Url::parse(&endpoint).unwrap(),
            model: QueryModel::new(client.clone()),
            editor: Reedline::create()
                .with_validator(Box::new(SQLValidator))
                .with_ansi_colors(true)
                .with_history(history),
            client,
        })
    }

    pub async fn edit(&mut self) -> anyhow::Result<Option<String>> {
        let prompt = DefaultPrompt::default();

        loop {
            match self.editor.read_line(&prompt) {
                Ok(Signal::Success(buffer)) => {
                    match self.model.process_buffer(buffer).await {
                        Ok(Some(query)) => {
                            return Ok(Some(query));
                        }
                        Ok(_) => {
                            // updated our model with new DDL, but no query to execute
                        }
                        Err(err) => {
                            let msg = match err {
                                QueryError::ClientError(err) => {
                                    format!("Error communicating with server: {}", err)
                                        .red()
                                        .to_string()
                                }
                                QueryError::InvalidQuery(errors) => {
                                    format!(
                                        "{}:\n{}",
                                        "ERROR".bold().red(),
                                        errors
                                            .into_iter()
                                            .map(|e| format!("• {}", e))
                                            .collect::<Vec<_>>()
                                            .join("\n")
                                            .red()
                                    )
                                }
                            };
                            eprintln!("{}", msg);
                        }
                    }
                }
                Ok(Signal::CtrlC) | Ok(Signal::CtrlD) => {
                    break;
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    break;
                }
            }
        }
        Ok(None)
    }

    async fn handle_event(event: PipelineEvent) -> bool {
        match event {
            PipelineEvent::Finish => {
                return false;
            }
            PipelineEvent::Error(e) => {
                queue!(
                    stderr(),
                    style::PrintStyledContent("Error: ".red().bold()),
                    style::PrintStyledContent(e.red())
                )
                .unwrap();
                return false;
            }
            PipelineEvent::Warning(e) => {
                queue!(
                    stderr(),
                    style::PrintStyledContent("Warning: ".yellow().bold()),
                    style::PrintStyledContent(e.yellow())
                )
                .unwrap();
            }
            PipelineEvent::StateChange(state) => {
                queue!(
                    stderr(),
                    cursor::MoveToColumn(0),
                    terminal::Clear(ClearType::CurrentLine),
                    style::PrintStyledContent("Job state: ".bold()),
                    style::Print(&state)
                )
                .unwrap();

                if state == "Running" {
                    println!();
                }
            }
            PipelineEvent::Output(data) => {
                print!("| ");
                println!("{:?}", data.value);
                for (_, v) in data.value.as_object().unwrap() {
                    print!(" {} |", v.to_string());
                }
                println!();
            }
        }

        true
    }

    pub async fn run_query(&mut self, query: String) -> anyhow::Result<()> {
        let mut runner = QueryRunner::run_query(self.client.clone(), &query).await?;
        let mut reader = EventStream::new();

        eprintln!("Running query as pipeline {}", runner.pipeline.id);

        let web_url = self
            .endpoint
            .join(&format!("pipelines/{}", runner.pipeline.id))
            .unwrap();
        eprintln!("Web UI: {}", web_url);

        loop {
            let mut reader_events = reader.next().fuse();
            select! {
                event = runner.rx.recv() => {
                    if let Some(event) = event {
                        if !Self::handle_event(event).await {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                event = reader_events => {
                    match event {
                        Some(Ok(Key(key))) => {
                            if key.code == KeyCode::Char('c') && key.modifiers == KeyModifiers::CONTROL {
                                // TODO: stop job
                                break;
                            }
                        }
                        _ => {
                            // ignore
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            let Some(query) = self.edit().await? else {
                eprintln!("Exiting...");
                break;
            };

            if let Err(e) = self.run_query(query).await {
                eprintln!("{}", format!("Error while running query:\n{}", e).red());
            }
        }

        Ok(())
    }
}
