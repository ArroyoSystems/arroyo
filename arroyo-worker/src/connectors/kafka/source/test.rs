use arrow::datatypes::{DataType, Field, Schema};
use arroyo_state::{BackingStore, StateBackend};
use rand::Rng;
use std::time::{Duration, SystemTime};

use crate::connectors::kafka::source;
use crate::engine::{Context, OutQueue, QueueItem};
use crate::SchemaData;
use arroyo_rpc::formats::{Format, JsonFormat};
use arroyo_rpc::grpc::{CheckpointMetadata, OperatorCheckpointMetadata};
use arroyo_rpc::{CheckpointCompleted, ControlMessage, ControlResp};
use arroyo_types::{to_micros, CheckpointBarrier, Message, TaskInfo};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic};
use rdkafka::producer::{BaseProducer, BaseRecord};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use super::KafkaSourceFunc;

#[derive(Debug, Clone, bincode::Encode, bincode::Decode, Serialize, Deserialize, PartialEq)]
struct TestData {
    i: u64,
}

impl SchemaData for TestData {
    fn name() -> &'static str {
        "test"
    }

    fn schema() -> Schema {
        Schema::new(vec![Field::new("i", DataType::UInt64, false)])
    }

    fn to_raw_string(&self) -> Option<Vec<u8>> {
        None
    }
}

pub struct KafkaTopicTester {
    topic: String,
    server: String,
    group_id: Option<String>,
}

impl KafkaTopicTester {
    async fn create_topic(&self) {
        let admin_client: AdminClient<_> = ClientConfig::new()
            .set("bootstrap.servers", self.server.to_string())
            .set("enable.auto.commit", "false")
            // TODO: parameterize group id
            .set(
                "group.id",
                format!("{}-{}-producer", "job_id", "operator_id"),
            )
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
                    2,
                    rdkafka::admin::TopicReplication::Fixed(1),
                )],
                &AdminOptions::new(),
            )
            .await
            .expect("deletion should have worked");
    }
    async fn get_source_with_reader(
        &self,
        task_info: TaskInfo,
        restore_from: Option<u32>,
    ) -> KafkaSourceWithReads {
        let mut kafka: KafkaSourceFunc<(), TestData> = KafkaSourceFunc::new(
            &self.server,
            &self.topic,
            self.group_id.clone(),
            crate::connectors::kafka::SourceOffset::Earliest,
            Format::Json(JsonFormat::default()),
            None,
            100,
            vec![],
        );
        let (to_control_tx, control_rx) = channel(128);
        let (command_tx, from_control_rx) = channel(128);
        let (data_tx, recv) = channel(128);

        let checkpoint_metadata = restore_from.map(|epoch| CheckpointMetadata {
            job_id: task_info.job_id.to_string(),
            epoch,
            min_epoch: 1,
            start_time: to_micros(SystemTime::now()),
            finish_time: to_micros(SystemTime::now()),
            operator_ids: vec![task_info.operator_id.clone()],
        });

        let mut ctx: Context<(), TestData> = Context::new(
            task_info,
            checkpoint_metadata,
            control_rx,
            command_tx,
            1,
            vec![vec![OutQueue::new(data_tx, false)]],
            source::tables(),
        )
        .await;

        tokio::spawn(async move {
            kafka.on_start(&mut ctx).await;
            kafka.run(&mut ctx).await;
        });
        KafkaSourceWithReads {
            to_control_tx,
            from_control_rx,
            data_recv: recv,
        }
    }

    fn get_producer(&mut self) -> KafkaTopicProducer {
        KafkaTopicProducer {
            base_producer: ClientConfig::new()
                .set("bootstrap.servers", self.server.to_string())
                .set("enable.auto.commit", "false")
                // TODO: parameterize group id
                .set(
                    "group.id",
                    format!("{}-{}-producer", "job_id", "operator_id"),
                )
                .create()
                .expect("Consumer creation failed"),
            topic: self.topic.to_string(),
        }
    }
}
struct KafkaTopicProducer {
    base_producer: BaseProducer,
    topic: String,
}

impl KafkaTopicProducer {
    fn send_data(&mut self, data: TestData) {
        let json = serde_json::to_string(&data).unwrap();
        self.base_producer
            .send(BaseRecord::<(), String>::to(&self.topic).payload(&json))
            .expect("could not send message")
    }
}

struct KafkaSourceWithReads {
    to_control_tx: Sender<ControlMessage>,
    from_control_rx: Receiver<ControlResp>,
    data_recv: Receiver<QueueItem>,
}

impl KafkaSourceWithReads {
    async fn assert_next_message_record_value(&mut self, expected_value: u64) {
        match self.data_recv.recv().await {
            Some(item) => {
                let msg: Message<(), TestData> = item.into();
                if let Message::Record(record) = msg {
                    assert_eq!(expected_value, record.value.i,);
                } else {
                    unreachable!("expected a record, got {:?}", msg);
                }
            }
            None => {
                unreachable!("option shouldn't be missing")
            }
        }
    }
    async fn assert_next_message_checkpoint(&mut self, expected_epoch: u32) {
        match self.data_recv.recv().await {
            Some(item) => {
                let msg: Message<(), TestData> = item.into();
                if let Message::Barrier(barrier) = msg {
                    assert_eq!(expected_epoch, barrier.epoch);
                } else {
                    unreachable!("expected a record, got {:?}", msg);
                }
            }
            None => {
                unreachable!("option shouldn't be missing")
            }
        }
    }

    async fn assert_control_checkpoint(&mut self, expected_epoch: u32) -> CheckpointCompleted {
        loop {
            let control_response = self
                .from_control_rx
                .recv()
                .await
                .expect("should be a valid message");

            if let ControlResp::CheckpointCompleted(checkpoint) = control_response {
                assert_eq!(expected_epoch, checkpoint.checkpoint_epoch);
                return checkpoint;
            }
        }
    }
}

#[tokio::test]
async fn test_kafka() {
    let mut kafka_topic_tester = KafkaTopicTester {
        topic: "arroyo-source".to_string(),
        server: "0.0.0.0:9092".to_string(),
        group_id: Some("test-consumer-group".to_string()),
    };

    let mut task_info = arroyo_types::get_test_task_info();
    task_info.job_id = format!("kafka-job-{}", rand::thread_rng().gen::<u64>());

    kafka_topic_tester.create_topic().await;
    let mut reader = kafka_topic_tester
        .get_source_with_reader(task_info.clone(), None)
        .await;
    let mut producer = kafka_topic_tester.get_producer();

    for message in 1u64..20 {
        let data = TestData { i: message };
        producer.send_data(data);
        reader.assert_next_message_record_value(message).await;
    }
    let barrier = ControlMessage::Checkpoint(CheckpointBarrier {
        epoch: 1,
        min_epoch: 0,
        timestamp: (SystemTime::now()),
        then_stop: false,
    });
    reader.to_control_tx.send(barrier).await.unwrap();
    let checkpoint_completed = reader.assert_control_checkpoint(1).await;
    producer.send_data(TestData { i: 20 });

    reader.assert_next_message_checkpoint(1).await;

    StateBackend::write_operator_checkpoint_metadata(OperatorCheckpointMetadata {
        job_id: task_info.job_id.clone(),
        operator_id: task_info.operator_id.clone(),
        epoch: 1,
        start_time: 0,
        finish_time: 0,
        min_watermark: Some(0),
        max_watermark: Some(0),
        has_state: true,
        tables: source::tables(),
        backend_data: checkpoint_completed.subtask_metadata.backend_data,
        bytes: checkpoint_completed.subtask_metadata.bytes,
        commit_data: None,
    })
    .await;

    StateBackend::write_checkpoint_metadata(CheckpointMetadata {
        job_id: task_info.job_id.clone(),
        epoch: 1,
        min_epoch: 1,
        start_time: 0,
        finish_time: 0,
        operator_ids: vec![task_info.operator_id.clone()],
    })
    .await;

    reader.assert_next_message_record_value(20).await;

    reader
        .to_control_tx
        .send(ControlMessage::Stop {
            mode: arroyo_rpc::grpc::StopMode::Graceful,
        })
        .await
        .unwrap();

    let mut reader = kafka_topic_tester
        .get_source_with_reader(task_info, Some(1))
        .await;

    // leftover metric
    reader.assert_next_message_record_value(20).await;
    producer.send_data(TestData { i: 21 });
    reader.assert_next_message_record_value(21).await;
}
