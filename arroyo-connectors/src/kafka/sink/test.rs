#![allow(clippy::unnecessary_mut_passed)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::datatypes::Field;
use arrow::array::{RecordBatch, UInt32Array};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arroyo_rpc::formats::{Format, JsonFormat};
use arroyo_rpc::ArroyoSchema;
use arroyo_types::CheckpointBarrier;
use arroyo_types::*;
use itertools::Itertools;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::Producer;
use rdkafka::{ClientConfig, Message};
use serde::Deserialize;
use tokio::sync::mpsc::channel;
use arroyo_formats::serialize::ArrowSerializer;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::ArrowOperator;

use super::{ConsistencyMode, KafkaSinkFunc};

pub struct KafkaTopicTester {
    topic: String,
    server: String,
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt32,
        false,
    )]))
}

#[derive(Deserialize)]
struct TestData {
    value: u32,
}

impl KafkaTopicTester {
    async fn create_topic(&self, job_id: &str, num_partitions: i32) {
        let admin_client: AdminClient<_> = ClientConfig::new()
            .set("bootstrap.servers", self.server.to_string())
            .set("enable.auto.commit", "false")
            // TODO: parameterize group id
            .set("group.id", format!("{}-{}-creator", job_id, "operator_id"))
            .create()
            .unwrap();
        admin_client
            .delete_topics(&[&self.topic], &AdminOptions::new())
            .await
            .expect("deletion should have worked");
        tokio::time::sleep(Duration::from_secs(1)).await;
        admin_client
            .create_topics(
                [&NewTopic::new(
                    &self.topic,
                    num_partitions,
                    rdkafka::admin::TopicReplication::Fixed(1),
                )],
                &AdminOptions::new(),
            )
            .await
            .expect("new topic should be present");
    }

    async fn get_sink_with_writes(&self) -> KafkaSinkWithWrites {
        let mut kafka = KafkaSinkFunc {
            topic: self.topic.to_string(),
            bootstrap_servers: self.server.to_string(),
            producer: None,
            consistency_mode: ConsistencyMode::AtLeastOnce,
            write_futures: vec![],
            client_config: HashMap::new(),
            serializer: ArrowSerializer::new(Format::Json(JsonFormat::default())),
        };

        let (_, control_rx) = channel(128);
        let (command_tx, _) = channel(128);

        let task_info = get_test_task_info();

        let mut ctx = ArrowContext::new(
            task_info,
            None,
            control_rx,
            command_tx,
            1,
            vec![ArroyoSchema::new(schema(), 0, vec![])],
            None,
            None,
            vec![vec![]],
            HashMap::new(),
        )
        .await;

        kafka.on_start(&mut ctx).await;

        KafkaSinkWithWrites { sink: kafka, ctx }
    }

    fn get_consumer(&mut self, job_id: &str) -> StreamConsumer {
        let base_consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.server.to_string())
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            // TODO: parameterize group id
            .set("group.id", format!("{}-{}-consumer", job_id, "operator_id"))
            .set("group.instance.id", "0")
            .create()
            .expect("Consumer creation failed");

        base_consumer.subscribe(&[&self.topic]).expect("success");
        base_consumer
    }
}

async fn get_data(consumer: &mut StreamConsumer) -> Record<String, String> {
    let owned_message = consumer
        .recv()
        .await
        .expect("shouldn't have errored")
        .detach();
    let payload = owned_message.payload().unwrap();
    Record {
        timestamp: from_millis(owned_message.timestamp().to_millis().unwrap() as u64),
        key: owned_message
            .key()
            .map(|k| String::from_utf8(k.to_vec()).unwrap()),
        value: String::from_utf8(payload.to_vec()).unwrap(),
    }
}

struct KafkaSinkWithWrites {
    sink: KafkaSinkFunc,
    ctx: ArrowContext,
}

#[tokio::test]
async fn test_kafka_checkpoint_flushes() {
    let mut kafka_topic_tester = KafkaTopicTester {
        topic: "arroyo-sink-checkpoint".to_string(),
        server: "0.0.0.0:9092".to_string(),
    };

    kafka_topic_tester.create_topic("checkpoint", 1).await;
    let mut sink_with_writes = kafka_topic_tester.get_sink_with_writes().await;
    let mut consumer = kafka_topic_tester.get_consumer("0");

    for chunk in &(1u32..200).into_iter().chunks(7) {
        let array = UInt32Array::from_iter_values(chunk.into_iter());
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(array)]).unwrap();

        sink_with_writes
            .sink
            .process_batch(batch, &mut sink_with_writes.ctx)
            .await;
    }
    let barrier = CheckpointBarrier {
        epoch: 2,
        min_epoch: 0,
        timestamp: SystemTime::now(),
        then_stop: false,
    };
    sink_with_writes
        .sink
        .handle_checkpoint(barrier, &mut sink_with_writes.ctx)
        .await;

    for message in 1u32..200 {
        let record = get_data(&mut consumer).await.value;
        let result: TestData = serde_json::from_str(&record).unwrap();
        assert_eq!(message, result.value, "{} {:?}", message, record);
    }
}

#[tokio::test]
async fn test_kafka() {
    let mut kafka_topic_tester = KafkaTopicTester {
        topic: "arroyo-sink".to_string(),
        server: "0.0.0.0:9092".to_string(),
    };

    kafka_topic_tester.create_topic("basic", 2).await;
    let mut sink_with_writes = kafka_topic_tester.get_sink_with_writes().await;
    let mut consumer = kafka_topic_tester.get_consumer("1");

    for message in 1u32..20 {
        let data = UInt32Array::from_iter_values(vec![message].into_iter());
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(data)]).unwrap();

        sink_with_writes
            .sink
            .process_batch(batch, &mut sink_with_writes.ctx)
            .await;
        sink_with_writes
            .sink
            .producer
            .as_ref()
            .unwrap()
            .flush(Duration::from_secs(3))
            .unwrap();

        let result: TestData = serde_json::from_str(&get_data(&mut consumer).await.value).unwrap();
        assert_eq!(message, result.value);
    }
}
