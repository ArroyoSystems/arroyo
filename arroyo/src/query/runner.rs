use anyhow::{anyhow, bail, Context};
use arroyo_openapi::types::{Job, OutputData, Pipeline, PipelinePost};
use arroyo_openapi::Client;
use eventsource_client::{Client as ESClient, SSE};
use std::future::Future;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::StreamExt;

pub struct QueryRunner {
    client: Client,
    pub pipeline: Pipeline,
    pub job: Job,
    pub rx: Receiver<PipelineEvent>,
}

pub enum PipelineEvent {
    StateChange(String),
    Warning(String),
    Error(String),
    Output(OutputData),
    Finish,
}

impl QueryRunner {
    async fn start_pipeline(client: &Client, query: &str) -> anyhow::Result<Pipeline> {
        let pipeline = client
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

        Ok(pipeline)
    }

    async fn get_job(client: &Client, pipeline_id: &str) -> anyhow::Result<Job> {
        client
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

    pub async fn run_query(client: Client, query: &str) -> anyhow::Result<Self> {
        let (tx, rx) = channel(128);

        let pipeline = Self::start_pipeline(&client, query).await?;
        let job = Self::get_job(&client, &pipeline.id).await?;

        let pipeline_id = pipeline.id.clone();
        let job_id = job.id.clone();
        let inner_client = client.clone();

        tokio::spawn(async move {
            match Self::run_query_int(inner_client, pipeline_id, job_id, tx.clone()).await {
                Ok(_) => {
                    tx.send(PipelineEvent::Finish).await.unwrap();
                }
                Err(e) => {
                    tx.send(PipelineEvent::Error(e.to_string())).await.unwrap();
                }
            }
        });

        Ok(Self {
            client,
            pipeline,
            job,
            rx,
        })
    }

    async fn run_query_int(
        client: Client,
        pipeline_id: String,
        job_id: String,
        tx: Sender<PipelineEvent>,
    ) -> anyhow::Result<()> {
        let mut outputs = eventsource_client::ClientBuilder::for_url(&format!(
            "{}/v1/pipelines/{}/jobs/{}/output",
            client.baseurl(),
            pipeline_id,
            job_id
        ))
        .unwrap()
        .build()
        .stream();

        let mut last_state = String::new();
        while last_state != "Running" {
            let job = Self::get_job(&client, &pipeline_id)
                .await
                .context("waiting for job startup")?;

            if job.state != last_state {
                tx.send(PipelineEvent::StateChange(job.state.clone()))
                    .await
                    .unwrap();
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
                                tx.send(PipelineEvent::Warning(
                                    "received invalid outputs from server".to_string(),
                                ))
                                .await
                                .unwrap();
                                continue;
                            };

                            tx.send(PipelineEvent::Output(data)).await.unwrap();
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
                    break;
                }
            }
        }

        Ok(())
    }
}
