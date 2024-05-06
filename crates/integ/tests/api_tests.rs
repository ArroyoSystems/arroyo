use anyhow::bail;
use std::env;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arroyo_openapi::types::{
    builder, ConnectionProfilePost, ConnectionSchema, ConnectionTablePost, Format, JsonFormat,
    MetricNames, PipelinePatch, PipelinePost, SchemaDefinition, StopType, Udf, ValidateQueryPost,
    ValidateUdfPost,
};
use arroyo_openapi::Client;
use rand::random;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic};
use rdkafka::{ClientConfig, ClientContext};
use serde_json::json;
use tracing::info;

async fn wait_for_state(
    client: &Client,
    pipeline_id: &str,
    expected_state: &str,
) -> anyhow::Result<()> {
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
            bail!("Job transitioned to failed");
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}

fn get_client() -> Arc<Client> {
    static CLIENT: OnceLock<Arc<Client>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            let client = reqwest::ClientBuilder::new()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap();
            Arc::new(Client::new_with_client(
                &format!(
                    "{}/api",
                    env::var("API_ENDPOINT")
                        .unwrap_or_else(|_| "http://localhost:8000".to_string())
                ),
                client,
            ))
        })
        .clone()
}

async fn start_pipeline(run_id: u32, query: &str, udfs: &[&str]) -> anyhow::Result<String> {
    let pipeline_name = format!("pipeline_{}", run_id);
    info!("Creating pipeline {}", pipeline_name);

    let pipeline_id = get_client()
        .create_pipeline()
        .body(
            PipelinePost::builder()
                .name(pipeline_name)
                .parallelism(1)
                .checkpoint_interval_micros(1_000_000)
                .query(query)
                .udfs(Some(
                    udfs.iter()
                        .map(|udf| Udf::builder().definition(*udf).try_into().unwrap())
                        .collect(),
                )),
        )
        .send()
        .await?
        .into_inner()
        .id;

    info!("Created pipeline {}", pipeline_id);
    Ok(pipeline_id)
}

async fn start_and_monitor(
    run_id: u32,
    query: &str,
    udfs: &[&str],
    checkpoints_to_wait: u32,
) -> anyhow::Result<(String, String)> {
    let api_client = get_client();

    println!("Starting pipeline");
    let pipeline_id = start_pipeline(run_id, query, udfs)
        .await
        .expect("failed to start pipeline");

    // wait for job to enter running phase
    println!("Waiting until running");
    wait_for_state(&api_client, &pipeline_id, "Running")
        .await
        .unwrap();

    let jobs = api_client
        .get_pipeline_jobs()
        .id(&pipeline_id)
        .send()
        .await
        .unwrap();
    let job = jobs.data.first().unwrap();

    // wait for a checkpoint
    println!("Waiting for {checkpoints_to_wait} successful checkpoints");
    loop {
        let checkpoints = api_client
            .get_job_checkpoints()
            .pipeline_id(&pipeline_id)
            .job_id(&job.id)
            .send()
            .await
            .unwrap()
            .into_inner();

        if let Some(checkpoint) = checkpoints
            .data
            .iter()
            .find(|c| c.epoch == checkpoints_to_wait as i64)
        {
            if checkpoint.finish_time.is_some() {
                // get details
                let details = api_client
                    .get_checkpoint_details()
                    .pipeline_id(&pipeline_id)
                    .job_id(&job.id)
                    .epoch(checkpoint.epoch)
                    .send()
                    .await
                    .unwrap()
                    .into_inner();

                assert!(!details.data.is_empty());

                return Ok((pipeline_id, job.id.clone()));
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn patch_and_wait(
    pipeline_id: &str,
    body: builder::PipelinePatch,
    expected_state: &str,
) -> anyhow::Result<()> {
    println!("Patching with {:?}", body);
    get_client()
        .patch_pipeline()
        .id(pipeline_id)
        .body(body)
        .send()
        .await?;

    println!("Waiting for {}", expected_state);
    wait_for_state(&get_client(), &pipeline_id, expected_state).await?;

    Ok(())
}

#[tokio::test]
async fn basic_pipeline() {
    let api_client = get_client();

    // create a source
    println!("Creating source");
    let run_id: u32 = random();
    let source_name = format!("source_{}", run_id);

    let source_id = api_client
        .create_connection_table()
        .body(
            ConnectionTablePost::builder()
                .config(json!({"event_rate": 10}))
                .connector("impulse")
                .name(source_name.clone()),
        )
        .send()
        .await
        .expect("failed to create connection table")
        .into_inner()
        .id;

    // create a pipeline
    let query = format!(
        r#"
    select count(*) from {} where counter % 2 == 0
    group by hop(interval '2 seconds', interval '10 seconds');
    "#,
        source_name
    );

    // validate the pipeline
    let valid = api_client
        .validate_query()
        .body(ValidateQueryPost::builder().query(&query).udfs(vec![]))
        .send()
        .await
        .unwrap()
        .into_inner();

    assert_eq!(valid.errors, Vec::<String>::new());
    assert!(valid.graph.is_some());

    let (pipeline_id, job_id) = start_and_monitor(run_id, &query, &[], 10).await.unwrap();

    // get error messages
    let errors = api_client
        .get_job_errors()
        .pipeline_id(&pipeline_id)
        .job_id(&job_id)
        .send()
        .await
        .unwrap()
        .into_inner();
    assert_eq!(errors.data.len(), 0);

    // get metrics
    // (skip in CI for now, as we don't have prometheus available; re-enable when metrics are moved
    // off prometheus)
    if !env::var("CI").map(|ci| ci == "true").unwrap_or(false) {
        loop {
            let metrics = api_client
                .get_operator_metric_groups()
                .pipeline_id(&pipeline_id)
                .job_id(&job_id)
                .send()
                .await
                .unwrap()
                .into_inner();
            if metrics.data.len() == valid.graph.as_ref().unwrap().nodes.len() {
                for metric in metrics.data {
                    for group in metric.metric_groups {
                        if !metric.operator_id.contains("source")
                            && group.name == MetricNames::MessagesSent
                        {
                            assert!(group.subtasks[0].metrics.iter().last().unwrap().value > 0.0);
                        }
                    }
                }
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    // stop job
    patch_and_wait(
        &pipeline_id,
        PipelinePatch::builder().stop(StopType::Checkpoint),
        "Stopped",
    )
    .await
    .unwrap();

    // start job
    patch_and_wait(
        &pipeline_id,
        PipelinePatch::builder().stop(StopType::None),
        "Running",
    )
    .await
    .unwrap();

    // rescale job
    println!("Rescaling pipeline");
    patch_and_wait(
        &pipeline_id,
        PipelinePatch::builder().parallelism(2),
        "Running",
    )
    .await
    .unwrap();

    for node in api_client
        .get_pipeline()
        .id(&pipeline_id)
        .send()
        .await
        .unwrap()
        .into_inner()
        .graph
        .nodes
    {
        assert_eq!(node.parallelism, 2);
    }

    // restart job
    println!("Restarting pipeline");
    api_client
        .restart_pipeline()
        .id(&pipeline_id)
        .send()
        .await
        .unwrap();

    wait_for_state(&api_client, &pipeline_id, "Running")
        .await
        .unwrap();

    // stop job
    patch_and_wait(
        &pipeline_id,
        PipelinePatch::builder().stop(StopType::Immediate),
        "Stopped",
    )
    .await
    .unwrap();

    // delete pipeline
    println!("Deleting pipeline");
    api_client
        .delete_pipeline()
        .id(&pipeline_id)
        .send()
        .await
        .unwrap();

    // delete source
    println!("Deleting connection");
    api_client
        .delete_connection_table()
        .id(&source_id)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn udfs() {
    let udf = r#"
/*
[dependencies]
regex = "1"
*/

use arroyo_udf_plugin::udf;
use regex::Regex;

#[udf]
fn my_double(x: i64) -> i64 {
    x * 2
}"#;

    // validate UDF
    let valid = get_client()
        .validate_udf()
        .body(ValidateUdfPost::builder().definition(udf))
        .send()
        .await
        .unwrap()
        .into_inner();

    assert_eq!(valid.errors, Vec::<String>::new());

    let query = r#"
create table impulse with (
   connector = 'impulse',
   event_rate = '10'
);

select my_double(cast(counter as bigint)) from impulse;
"#;

    let run_id: u32 = random();

    let (pipeline_id, _job_id) = start_and_monitor(run_id, query, &[udf], 3).await.unwrap();

    // stop job
    patch_and_wait(
        &pipeline_id,
        PipelinePatch::builder().stop(StopType::Checkpoint),
        "Stopped",
    )
    .await
    .unwrap();

    // delete pipeline
    println!("Deleting pipeline");
    get_client()
        .delete_pipeline()
        .id(&pipeline_id)
        .send()
        .await
        .unwrap();
}

fn create_kafka_admin() -> AdminClient<impl ClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()
        .unwrap()
}

async fn create_topic(client: &AdminClient<impl ClientContext>, topic: &str) {
    client
        .create_topics(
            [&NewTopic::new(
                topic,
                1,
                rdkafka::admin::TopicReplication::Fixed(1),
            )],
            &AdminOptions::new(),
        )
        .await
        .expect("deletion should have worked");
}

async fn delete_topic(client: &AdminClient<impl ClientContext>, topic: &str) {
    client
        .delete_topics(&[topic], &AdminOptions::new())
        .await
        .expect("deletion should have worked");
}

#[tokio::test]
async fn connection_table() {
    let api_client = get_client();

    let connectors = api_client
        .get_connectors()
        .send()
        .await
        .unwrap()
        .into_inner();

    assert!(connectors.data.iter().find(|c| c.name == "Kafka").is_some());

    let run_id: u32 = random();
    let table_name = format!("kafka_table_{run_id}");
    let kafka_admin = create_kafka_admin();

    let kafka_topic = format!("kafka_test_{run_id}");
    create_topic(&kafka_admin, &kafka_topic).await;

    let schema = r#"
{
    "type": "object",
    "properties": {
        "a": {
            "type": "string"
        },
        "b": {
            "type": "number"
        },
        "c": {
            "type": "array",
            "items": {
                "type": "string"
            }
        }
    },
    "required": ["a"]
}"#;

    let connection_schema = ConnectionSchema::builder()
        .fields(vec![])
        .format(Format::Json(JsonFormat::builder().try_into().unwrap()))
        .definition(SchemaDefinition::JsonSchema(schema.to_string()));

    // create a kafka connection
    let profile_post = ConnectionProfilePost::builder()
        .name(format!("kafka_source_{}", run_id))
        .connector("kafka")
        .config(json!( {
            "authentication": {},
            "bootstrapServers": "localhost:9092",
            "schemaRegistryEnum": {}
        }));

    let valid = api_client
        .test_connection_profile()
        .body(profile_post.clone())
        .send()
        .await
        .unwrap()
        .into_inner();

    assert!(valid.done);
    assert!(!valid.error);

    let profile = api_client
        .create_connection_profile()
        .body(profile_post)
        .send()
        .await
        .unwrap()
        .into_inner();

    api_client
        .get_connection_profile_autocomplete()
        .id(&profile.id)
        .send()
        .await
        .unwrap()
        .into_inner()
        .values
        .get("topic")
        .unwrap()
        .iter()
        .find(|t| *t == &kafka_topic)
        .expect("autocomplete did not return kafka topic");

    api_client
        .test_schema()
        .body(connection_schema.clone())
        .send()
        .await
        .expect("valid schema");

    let connection_table = ConnectionTablePost::builder()
        .name(table_name.clone())
        .connector("kafka")
        .schema(Some(connection_schema.try_into().unwrap()))
        .config(json!({
            "type": {
                "offset": "latest",
                "read_mode": "read_uncommitted"
            },
            "topic": kafka_topic
        }))
        .connection_profile_id(Some(profile.id.clone()));

    let mut connection_table = api_client
        .create_connection_table()
        .body(connection_table)
        .send()
        .await
        .expect("failed to create table")
        .into_inner();

    connection_table
        .schema
        .fields
        .sort_by_key(|f| f.field_name.clone());

    assert_eq!(
        serde_json::to_value(connection_table.schema.fields).unwrap(),
        json!([
            {
                "fieldName": "a",
                "fieldType": {
                    "sqlName": "TEXT",
                    "type": {
                        "primitive": "String",
                    },
                },
                "nullable": false
            },
            {
                "fieldName": "b",
                "fieldType": {
                   "sqlName": "DOUBLE",
                    "type": {
                        "primitive": "F64",
                    },
                },
               "nullable": true,
            },
            {
                "fieldName": "c",
                "fieldType": {
                    "type": {
                        "list": {
                            "fieldName": "item",
                            "fieldType": {
                                "sqlName": "TEXT",
                                "type": {
                                    "primitive": "String",
                                }
                            },
                            "nullable": false
                        },
                    },
                },
                "nullable": true,
            }
        ])
    );

    let (pipeline_id, _) = start_and_monitor(
        run_id,
        &format!("select * from {};", connection_table.name),
        &[],
        5,
    )
    .await
    .unwrap();

    // stop job
    patch_and_wait(
        &pipeline_id,
        PipelinePatch::builder().stop(StopType::Immediate),
        "Stopped",
    )
    .await
    .unwrap();

    // delete pipeline
    println!("Deleting pipeline");
    api_client
        .delete_pipeline()
        .id(&pipeline_id)
        .send()
        .await
        .unwrap();

    // delete source
    println!("Deleting connection");
    api_client
        .delete_connection_table()
        .id(&connection_table.id)
        .send()
        .await
        .unwrap();

    // delete topic
    delete_topic(&kafka_admin, &kafka_topic).await;
}
