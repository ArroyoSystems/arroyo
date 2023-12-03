use anyhow::{anyhow, bail, Context};
use arroyo_openapi::types::{Job, OutputData, Pipeline, PipelinePost, ValidateQueryPost};
use arroyo_openapi::Client;
use eventsource_client::{Client as ESClient, SSE};
use serde_json::Value;
use sqlparser::ast::{ObjectName, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tracing::warn;

struct RunningPipeline {
    pipeline: Pipeline,
    job: Job,
    stream: Receiver<String>,
}

pub struct QueryModel {
    ddl: HashMap<ObjectName, Statement>,
    current_line: String,
    client: Client,
    running: Option<RunningPipeline>,
}

impl QueryModel {
    pub fn new(client: Client) -> Self {
        Self {
            ddl: HashMap::new(),
            current_line: String::new(),
            client,
            running: None,
        }
    }

    pub fn push(&mut self, s: &str) -> bool {
        self.current_line.push_str(s);
        self.current_line.push('\n');

        return self.current_line.trim_end().ends_with(';');
    }

    fn build_query(queries: &Vec<String>, ddl: &HashMap<ObjectName, Statement>) -> String {
        let mut statements: Vec<_> = ddl.iter().map(|(_, t)| t.to_string()).collect();

        statements.extend(queries.iter().cloned());

        statements.join("\n")
    }

    async fn validate_query(&self, query: &str) -> anyhow::Result<Vec<String>> {
        let result = self
            .client
            .validate_query()
            .body(ValidateQueryPost::builder().query(query.to_string()))
            .send()
            .await?
            .into_inner();

        Ok(result.errors.unwrap_or_default())
    }

    async fn start_pipeline(&self, query: &str) -> anyhow::Result<String> {
        let validation = self
            .client
            .validate_query()
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

        let pipeline = self
            .client
            .post_pipeline()
            .body(
                PipelinePost::builder()
                    .name("cli-pipeline")
                    .query(query)
                    .parallelism(1)
                    .preview(true),
            )
            .send()
            .await?
            .into_inner();

        Ok(pipeline.id)
    }

    async fn get_job(&self, pipeline_id: &str) -> anyhow::Result<Job> {
        self.client
            .get_pipeline_jobs()
            .id(pipeline_id)
            .send()
            .await?
            .into_inner()
            .data
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("No job found for pipeline"))
    }

    async fn run_query(&self, query: &str) -> anyhow::Result<Sender<OutputData>> {
        println!("Starting pipeline...");
        let pipeline_id = self.start_pipeline(query).await?;

        let job = self.get_job(&pipeline_id).await?;

        let mut outputs = eventsource_client::ClientBuilder::for_url(&format!(
            "{}/v1/pipelines/{}/jobs/{}/output",
            self.client.baseurl(),
            pipeline_id,
            job.id
        ))
        .unwrap()
        .build()
        .stream();

        let mut last_state = String::new();
        while last_state != "Running" {
            let job = self
                .get_job(&pipeline_id)
                .await
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
                                eprintln!(
                                    "received invalid data from output operator {}",
                                    data.operator_id
                                );
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

    pub async fn process_buffer(&mut self) -> anyhow::Result<Option<Sender<OutputData>>> {
        let dialect = PostgreSqlDialect {};
        let ast = Parser::parse_sql(&dialect, self.current_line.clone())?;

        let mut queries = vec![];

        let mut new_ddl = self.ddl.clone();

        for statement in ast {
            match &statement {
                Statement::Query(_) | Statement::Insert { .. } => {
                    queries.push(statement.to_string())
                }
                Statement::CreateView { name, .. } | Statement::CreateTable { name, .. } => {
                    new_ddl.insert(name.to_owned(), statement.clone());
                }
                Statement::Drop {
                    if_exists, names, ..
                } => {
                    for name in names {
                        if new_ddl.remove(&name).is_none() {
                            if *if_exists {
                                warn!("Not dropping {}, which does not exist", name);
                            } else {
                                bail!("Failed to drop {}; table does not exist", name);
                            }
                        }
                    }
                }
                Statement::Copy { .. } => {
                    bail!("COPY is not supported");
                }
                Statement::Update { .. } => {
                    bail!("UPDATE is not supported");
                }
                Statement::Delete { .. } => {
                    bail!("DELETE is not supported");
                }
                Statement::AlterTable { .. } => {
                    bail!("ALTER is not supported; DROP and recreate instead")
                }
                Statement::StartTransaction { .. }
                | Statement::SetTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. } => {
                    bail!("Transactions are not supported");
                }
            }
        }

        let query = Self::build_query(&queries, &new_ddl);

        let errors = self.validate_query(&query).await?;

        if errors.len() == 1 && &errors[0] == "The provided SQL does not contain a query" {
            self.ddl = new_ddl;
            Ok(None)
        } else if !errors.is_empty() {
            bail!(
                "{}",
                errors
                    .iter()
                    .map(|e| format!("* {}", e))
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        } else {
            Ok(Some(query))
        }
    }
}
