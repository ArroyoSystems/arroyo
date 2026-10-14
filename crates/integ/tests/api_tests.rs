use anyhow::bail;
use std::collections::HashSet;
use std::env;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arroyo_openapi::Client;
use arroyo_openapi::types::{
    ConnectionProfilePost, ConnectionSchema, ConnectionTablePost, ErrorDomain, Format, JsonType,
    MetricName, PipelinePatch, PipelinePost, SchemaDefinition, StopType, Udf, ValidateQueryPost,
    ValidateUdfPost, builder,
};
use rand::random;
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, ClientContext};
use serde_json::json;
use tracing::info;

async fn wait_for_state(
    client: &Client,
    run_id: Option<i64>,
    pipeline_id: &str,
    expected_state: &str,
) -> anyhow::Result<i64> {
    let mut last_state = "None".to_string();
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;

        let jobs = client
            .get_pipeline_jobs()
            .id(pipeline_id)
            .send()
            .await
            .unwrap();
        let job = jobs.data.first().unwrap();

        if Some(job.run_id) == run_id {
            continue;
        }

        let state = job.state.clone();
        if last_state != state {
            info!("Job transitioned to {}", state);
            last_state = state;
        }

        if last_state == "Failed" {
            bail!("Job transitioned to failed");
        }

        if last_state == expected_state {
            return Ok(job.run_id);
        }
    }
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
                        .unwrap_or_else(|_| "http://localhost:5115".to_string())
                ),
                client,
            ))
        })
        .clone()
}

async fn start_pipeline(test_id: u32, query: &str, udfs: &[&str]) -> anyhow::Result<String> {
    let pipeline_name = format!("pipeline_{test_id}");
    info!("Creating pipeline {}", pipeline_name);

    let pipeline_id = get_client()
        .create_pipeline()
        .body(
            PipelinePost::builder()
                .name(pipeline_name)
                .parallelism(1)
                .pipeline_config(Some(serde_json::json!({
                    "checkpoint": { "interval": "1s" }
                })))
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
    test_id: u32,
    query: &str,
    udfs: &[&str],
    checkpoints_to_wait: u32,
    verify_checkpoint_details: bool,
) -> anyhow::Result<(String, String, i64)> {
    let api_client = get_client();

    println!("Starting pipeline");
    let pipeline_id = start_pipeline(test_id, query, udfs)
        .await
        .expect("failed to start pipeline");

    // wait for job to enter running phase
    println!("Waiting until running");
    let run_id = wait_for_state(&api_client, None, &pipeline_id, "Running")
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
            && checkpoint.finish_time.is_some()
        {
            if verify_checkpoint_details {
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
            }

            return Ok((pipeline_id, job.id.clone(), run_id));
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn patch_and_wait(
    pipeline_id: &str,
    run_id: Option<i64>,
    body: builder::PipelinePatch,
    expected_state: &str,
) -> anyhow::Result<i64> {
    println!("Patching with {body:?}");
    get_client()
        .patch_pipeline()
        .id(pipeline_id)
        .body(body)
        .send()
        .await?;

    println!("Waiting for {expected_state}");
    wait_for_state(&get_client(), run_id, pipeline_id, expected_state).await
}

#[tokio::test]
async fn basic_pipeline() {
    let api_client = get_client();

    // create a source
    println!("Creating source");
    let test_id: u32 = random();
    let source_name = format!("source_{test_id}");

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
    select count(*) from {source_name} where counter % 2 == 0
    group by hop(interval '2 seconds', interval '10 seconds');
    "#
    );

    // validate the pipeline
    let valid = api_client
        .validate_query()
        .body(ValidateQueryPost::builder().query(&query).udfs(vec![]))
        .send()
        .await
        .unwrap()
        .into_inner();

    assert!(valid.errors.is_empty());
    assert!(valid.graph.is_some());

    let (pipeline_id, job_id, _) = start_and_monitor(test_id, &query, &[], 10, true)
        .await
        .unwrap();

    let sink_id = valid
        .graph
        .as_ref()
        .unwrap()
        .nodes
        .iter()
        .find(|n| n.description.contains("sink"))
        .unwrap()
        .node_id;

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

    loop {
        let metrics = api_client
            .get_operator_metric_groups()
            .pipeline_id(&pipeline_id)
            .job_id(&job_id)
            .send()
            .await
            .unwrap()
            .into_inner();
        if metrics.data.len() == valid.graph.as_ref().unwrap().nodes.len()
            && metrics
                .data
                .iter()
                .filter(|op| !op.node_id == sink_id)
                .map(|op| {
                    op.metric_groups
                        .iter()
                        .find(|t| t.name == MetricName::MessagesSent)
                })
                .all(|m| {
                    m.map(|m| {
                        !m.subtasks[0].metrics.is_empty()
                            && m.subtasks[0].metrics.iter().last().unwrap().value > 0.0
                    })
                    .unwrap_or(false)
                })
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // stop job
    let run_id = patch_and_wait(
        &pipeline_id,
        None,
        PipelinePatch::builder().stop(StopType::Checkpoint),
        "Stopped",
    )
    .await
    .unwrap();

    // start job
    let run_id = patch_and_wait(
        &pipeline_id,
        Some(run_id),
        PipelinePatch::builder().stop(StopType::None),
        "Running",
    )
    .await
    .unwrap();

    // rescale job
    println!("Rescaling pipeline");
    let run_id = patch_and_wait(
        &pipeline_id,
        Some(run_id),
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

    wait_for_state(&api_client, Some(run_id), &pipeline_id, "Running")
        .await
        .unwrap();

    // stop job
    patch_and_wait(
        &pipeline_id,
        None,
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

    let (pipeline_id, _job_id, _) = start_and_monitor(run_id, query, &[udf], 3, true)
        .await
        .unwrap();

    // stop job
    patch_and_wait(
        &pipeline_id,
        None,
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

fn create_kafka_consumer(topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("group.id", format!("integ-{}", random::<u32>()))
        .create()
        .expect("consumer creation should succeed");
    consumer
        .subscribe(&[topic])
        .expect("consumer subscription should succeed");
    consumer
}

async fn read_kafka_counters(consumer: &StreamConsumer) -> anyhow::Result<Vec<i64>> {
    let mut counters = vec![];

    loop {
        let wait = if counters.is_empty() {
            Duration::from_secs(10)
        } else {
            Duration::from_secs(1)
        };

        match tokio::time::timeout(wait, consumer.recv()).await {
            Ok(Ok(message)) => {
                let payload = message
                    .payload()
                    .ok_or_else(|| anyhow::anyhow!("Kafka record had no payload"))?;
                let value: serde_json::Value = serde_json::from_slice(payload)?;
                let counter = value
                    .get("counter")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| anyhow::anyhow!("Kafka record had no counter: {value}"))?;
                counters.push(counter);
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) if counters.is_empty() => bail!("timed out waiting for Kafka output"),
            Err(_) => return Ok(counters),
        }
    }
}

async fn wait_until_next_checkpoint_is_underway(
    pipeline_id: &str,
    job_id: &str,
    completed_epoch: i64,
) {
    loop {
        let checkpoints = get_client()
            .get_job_checkpoints()
            .pipeline_id(pipeline_id)
            .job_id(job_id)
            .send()
            .await
            .unwrap()
            .into_inner();

        if checkpoints.data.iter().any(|checkpoint| {
            checkpoint.epoch > completed_epoch && checkpoint.finish_time.is_none()
        }) {
            return;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn checkpoint_stop_does_not_duplicate_output() {
    let api_client = get_client();
    let test_id: u32 = random();
    let topic = format!("checkpoint_stop_{test_id}");
    let kafka_admin = create_kafka_admin();
    create_topic(&kafka_admin, &topic).await;
    let consumer = create_kafka_consumer(&topic);

    let query = format!(
        r#"
CREATE TABLE impulse WITH (
    connector = 'impulse',
    event_rate = '100'
);

CREATE TABLE output (
    counter BIGINT
) WITH (
    connector = 'kafka',
    type = 'sink',
    bootstrap_servers = 'localhost:9092',
    format = 'json',
    topic = '{topic}'
);

INSERT INTO output SELECT counter FROM impulse;
"#
    );

    let (pipeline_id, job_id, _) = start_and_monitor(test_id, &query, &[], 2, false)
        .await
        .unwrap();

    wait_until_next_checkpoint_is_underway(&pipeline_id, &job_id, 2).await;

    // The stopping checkpoint follows the in-progress scheduled checkpoint. Records covered by
    // the stopping checkpoint must not be emitted again after restart.
    let run_id = patch_and_wait(
        &pipeline_id,
        None,
        PipelinePatch::builder().stop(StopType::Checkpoint),
        "Stopped",
    )
    .await
    .unwrap();
    let first_run = read_kafka_counters(&consumer).await.unwrap();

    patch_and_wait(
        &pipeline_id,
        Some(run_id),
        PipelinePatch::builder().stop(StopType::None),
        "Running",
    )
    .await
    .unwrap();

    // Run long enough to replay every record between the scheduled and stopping checkpoints if
    // recovery incorrectly chose the scheduled checkpoint.
    tokio::time::sleep(Duration::from_secs(1)).await;
    patch_and_wait(
        &pipeline_id,
        None,
        PipelinePatch::builder().stop(StopType::Immediate),
        "Stopped",
    )
    .await
    .unwrap();

    let second_run = read_kafka_counters(&consumer).await.unwrap();
    let first_run: HashSet<_> = first_run.into_iter().collect();
    let mut duplicates: Vec<_> = second_run
        .iter()
        .copied()
        .filter(|counter| first_run.contains(counter))
        .collect();
    duplicates.sort_unstable();
    duplicates.dedup();

    api_client
        .delete_pipeline()
        .id(&pipeline_id)
        .send()
        .await
        .unwrap();
    drop(consumer);
    delete_topic(&kafka_admin, &topic).await;

    assert!(
        duplicates.is_empty(),
        "checkpoint-stop recovery duplicated counters: {duplicates:?}"
    );
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

    assert!(connectors.data.iter().any(|c| c.name == "Kafka"));

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
        .format(Format::Json {
            compression: None,
            confluent_schema_registry: None,
            debezium: None,
            decimal_encoding: None,
            include_schema: None,
            schema_id: None,
            timestamp_format: None,
            type_: JsonType::Json,
            unstructured: None,
        })
        .definition(SchemaDefinition::JsonSchema {
            schema: schema.to_string(),
        });

    // create a kafka connection
    let profile_post = ConnectionProfilePost::builder()
        .name(format!("kafka_source_{run_id}"))
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

    let connection_table = api_client
        .create_connection_table()
        .body(connection_table)
        .send()
        .await
        .expect("failed to create table")
        .into_inner();

    let mut v = serde_json::to_value(connection_table.schema.fields).unwrap();
    let a = v.as_array_mut().unwrap();
    a.sort_by_key(|v| v.get("name").unwrap().as_str().unwrap().to_string());

    assert_eq!(
        v,
        json!([
            {
                "name": "a",
                "type": "string",
                "sql_name": "TEXT",
                "required": true
            },
            {
                "name": "b",
                "type": "float64",
                "sql_name": "DOUBLE",
                "required": false,
            },
            {
                "name": "c",
                "type": "list",
                "sql_name": "TEXT[]",
                "items": {
                    "name": "item",
                    "type": "string",
                    "sql_name": "TEXT",
                    "required": true
                },
                "required": false,
            }
        ])
    );

    let (pipeline_id, _, _) = start_and_monitor(
        run_id,
        &format!("select * from {};", connection_table.name),
        &[],
        5,
        true,
    )
    .await
    .unwrap();

    // stop job
    patch_and_wait(
        &pipeline_id,
        None,
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

    // assert removal of pipeline
    assert_eq!(
        api_client
            .get_pipeline()
            .id(&pipeline_id)
            .send()
            .await
            .unwrap_err()
            .status()
            .unwrap(),
        reqwest::StatusCode::NOT_FOUND
    );

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

const AVRO_TEST_SCHEMA: &str = r#"
{
  "type": "record",
  "name": "IntegAvroTest",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "score", "type": "double"}
  ]
}
"#;

fn write_avro_test_file(path: &std::path::Path, records: &[(i64, &str, f64)]) {
    use apache_avro::Schema;
    use apache_avro::types::Value;

    let schema = Schema::parse_str(AVRO_TEST_SCHEMA).unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = apache_avro::Writer::new(&schema, file);
    for (id, name, score) in records {
        let record = Value::Record(vec![
            ("id".to_string(), Value::Long(*id)),
            ("name".to_string(), Value::String((*name).to_string())),
            ("score".to_string(), Value::Double(*score)),
        ]);
        writer.append(record).unwrap();
    }
    writer.flush().unwrap();
}

fn avro_source_table(dir: &std::path::Path, extra_options: &str) -> String {
    format!(
        r#"
CREATE TABLE avro_source (
    id BIGINT,
    name TEXT,
    score DOUBLE
) WITH (
    connector = 'filesystem',
    type = 'source',
    path = 'file://{}',
    format = 'avro'{extra_options}
);
"#,
        dir.display()
    )
}

async fn read_avro_test_rows(consumer: &StreamConsumer) -> anyhow::Result<Vec<(i64, String, f64)>> {
    let mut rows = vec![];
    loop {
        let wait = if rows.is_empty() {
            Duration::from_secs(10)
        } else {
            Duration::from_secs(1)
        };

        match tokio::time::timeout(wait, consumer.recv()).await {
            Ok(Ok(message)) => {
                let payload = message
                    .payload()
                    .ok_or_else(|| anyhow::anyhow!("Kafka record had no payload"))?;
                let value: serde_json::Value = serde_json::from_slice(payload)?;
                let id = value
                    .get("id")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| anyhow::anyhow!("record had no id: {value}"))?;
                let name = value
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("record had no name: {value}"))?
                    .to_string();
                let score = value
                    .get("score")
                    .and_then(|v| v.as_f64())
                    .ok_or_else(|| anyhow::anyhow!("record had no score: {value}"))?;
                rows.push((id, name, score));
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) if rows.is_empty() => bail!("timed out waiting for Kafka output"),
            Err(_) => return Ok(rows),
        }
    }
}

// Regression test for the FileSystem connector's Avro support (issue #1040): reads a small
// Avro object container file from local disk and asserts the exact records the sink
// received. Uses a Kafka sink in its default `at_least_once` commit mode, which writes
// directly with a plain (non-transactional) producer rather than a two-phase commit, so
// output is visible to a consumer as soon as it's produced instead of only after a
// checkpoint barrier — unlike a filesystem/Parquet sink or a Kafka sink in
// `exactly_once` mode, neither of which a fast-finishing bounded job is guaranteed to
// reach before completing naturally.
#[tokio::test]
async fn avro_filesystem_source() {
    let api_client = get_client();
    let test_id: u32 = random();
    let topic = format!("avro_fs_source_{test_id}");
    let kafka_admin = create_kafka_admin();
    create_topic(&kafka_admin, &topic).await;
    let consumer = create_kafka_consumer(&topic);

    let dir = std::env::temp_dir().join(format!("arroyo-integ-avro-{test_id}"));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir for avro test file");
    let records = [(1i64, "alice", 9.5), (2, "bob", 7.25), (3, "carol", 10.0)];
    write_avro_test_file(&dir.join("data.avro"), &records);

    let query = format!(
        r#"
{}
CREATE TABLE output (
    id BIGINT,
    name TEXT,
    score DOUBLE
) WITH (
    connector = 'kafka',
    type = 'sink',
    bootstrap_servers = 'localhost:9092',
    format = 'json',
    topic = '{topic}'
);

INSERT INTO output SELECT id, name, score FROM avro_source;
"#,
        avro_source_table(&dir, "")
    );

    let valid = api_client
        .validate_query()
        .body(ValidateQueryPost::builder().query(&query).udfs(vec![]))
        .send()
        .await
        .unwrap()
        .into_inner();
    assert!(valid.errors.is_empty());
    assert!(
        valid
            .graph
            .as_ref()
            .unwrap()
            .nodes
            .iter()
            .any(|n| n.description.contains("FileSystem")),
        "expected a FileSystem source node in the query plan"
    );

    let pipeline_id = start_pipeline(test_id, &query, &[])
        .await
        .expect("failed to start pipeline");

    wait_for_state(&api_client, None, &pipeline_id, "Finished")
        .await
        .expect("pipeline should finish reading the avro file without error");

    let mut rows = read_avro_test_rows(&consumer)
        .await
        .expect("failed to read expected rows back from kafka");
    rows.sort_by_key(|(id, _, _)| *id);

    let expected: Vec<_> = records
        .iter()
        .map(|(id, name, score)| (*id, name.to_string(), *score))
        .collect();
    assert_eq!(
        rows, expected,
        "records read back from kafka should exactly match the avro source file"
    );

    api_client
        .delete_pipeline()
        .id(&pipeline_id)
        .send()
        .await
        .unwrap();
    drop(consumer);
    delete_topic(&kafka_admin, &topic).await;
    std::fs::remove_dir_all(&dir).ok();
}

async fn assert_avro_source_rejected(extra_options: &str) {
    let api_client = get_client();
    let test_id: u32 = random();

    let dir = std::env::temp_dir().join(format!("arroyo-integ-avro-{test_id}"));
    std::fs::create_dir_all(&dir).expect("failed to create temp dir for avro test file");
    write_avro_test_file(&dir.join("data.avro"), &[(1, "alice", 9.5)]);

    let query = format!(
        r#"
{}
CREATE TABLE output (
    id BIGINT,
    name TEXT,
    score DOUBLE
) WITH (
    connector = 'blackhole'
);

INSERT INTO output SELECT id, name, score FROM avro_source;
"#,
        avro_source_table(&dir, extra_options)
    );

    let pipeline_id = start_pipeline(test_id, &query, &[])
        .await
        .expect("failed to start pipeline");

    let jobs = api_client
        .get_pipeline_jobs()
        .id(&pipeline_id)
        .send()
        .await
        .unwrap();
    let job_id = jobs.data.first().unwrap().id.clone();

    // The connector rejects these options immediately (see run() in filesystem/source.rs),
    // but the job-level restart supervisor doesn't respect a connector's NoRetry hint and
    // keeps retrying with backoff regardless, so the overall job can take a minute or more
    // to reach a terminal "Failed" state (a pre-existing behavior unrelated to this fix).
    // Polling for the error to appear is both faster and more direct than waiting for that.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let errors = loop {
        let errors = api_client
            .get_job_errors()
            .pipeline_id(&pipeline_id)
            .job_id(&job_id)
            .send()
            .await
            .unwrap()
            .into_inner();
        if !errors.data.is_empty() {
            break errors;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for a job error when {extra_options} is set"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    // The FileSystem-Avro source rejects these options with a User/NoRetry connector error.
    // Checking the typed error_domain rather than matching on message/details text keeps
    // this test independent of exactly which of those two fields the error text lands in.
    assert!(
        errors
            .data
            .iter()
            .any(|e| e.error_domain == Some(ErrorDomain::User)),
        "expected a User-domain job error, got: {:?}",
        errors.data
    );

    // The job is still mid-retry (not yet in a terminal state) at this point, and
    // delete_pipeline requires one. The job-level restart supervisor will give up and reach
    // "Failed" on its own once its retry backoff is exhausted; racing an explicit stop
    // request against that natural transition (wait_for_state treats "Failed" as a
    // hard-stop bail-out for any requested state, including "Stopped") is what made this
    // flaky, so just wait for whichever terminal state it naturally lands on.
    loop {
        let jobs = api_client
            .get_pipeline_jobs()
            .id(&pipeline_id)
            .send()
            .await
            .unwrap();
        let state = jobs.data.first().unwrap().state.clone();
        if matches!(state.as_str(), "Stopped" | "Finished" | "Failed") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    api_client
        .delete_pipeline()
        .id(&pipeline_id)
        .send()
        .await
        .unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

// Covers the P1 review finding on #1149: avro.raw_datums is a generic Avro format option
// accepted for any connector, but the FileSystem source always reads whole self-describing
// object container files, which is meaningless combined with raw_datums (single-record, no
// header) framing. The connector should reject this combination up front rather than
// silently misdecoding the container header as record data.
#[tokio::test]
async fn avro_filesystem_source_rejects_raw_datums() {
    assert_avro_source_rejected(", 'avro.raw_datums' = 'true'").await;
}

// Same as above (P1 finding on #1149) for the other option the deserializer treats
// specially: avro.confluent_schema_registry.
#[tokio::test]
async fn avro_filesystem_source_rejects_confluent_schema_registry() {
    assert_avro_source_rejected(", 'avro.confluent_schema_registry' = 'true'").await;
}
