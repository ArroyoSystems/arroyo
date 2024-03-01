use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arroyo_formats::SchemaData;
use arroyo_rpc::formats::{Format, JsonFormat};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ControlMessage, ControlResp};
use arroyo_types::{Message, TaskInfo};
use rand::Rng;
use rumqttc::v5::mqttbytes::QoS;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::connectors::mqtt::source;
use crate::connectors::mqtt::{MqttConfig, Tls, Protocol};
use crate::engine::{Context, OutQueue, QueueItem};
use crate::RateLimiter;

use super::MqttSourceFunc;

#[derive(Debug, Clone, bincode::Encode, bincode::Decode, Serialize, Deserialize, PartialEq)]
struct TestData {
    s: String,
}

impl SchemaData for TestData {
    fn name() -> &'static str {
        "test"
    }

    fn schema() -> Schema {
        Schema::new(vec![Field::new("s", DataType::Utf8, false)])
    }

    fn to_raw_string(&self) -> Option<Vec<u8>> {
        None
    }

    fn to_avro(&self, _schema: &apache_avro::Schema) -> apache_avro::types::Value {
        todo!()
    }
}

struct MqttSourceWithReads {
    to_control_tx: Sender<ControlMessage>,
    #[allow(dead_code)]
    from_control_rx: Receiver<ControlResp>,
    data_recv: Receiver<QueueItem>,
    subscribed: Arc<AtomicBool>,
}

impl MqttSourceWithReads {
    async fn wait_for_subscription(&self, timeout: std::time::Duration) {
        let start = std::time::Instant::now();
        while !self.subscribed.load(Ordering::Relaxed) {
            if start.elapsed() > timeout {
                panic!("Timed out waiting for subscription");
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    async fn assert_next_message_record_value(&mut self, expected_value: u64) {
        match self.data_recv.recv().await {
            Some(item) => {
                let msg: Message<(), TestData> = item.into();
                if let Message::Record(record) = msg {
                    let s = format!("message-{}", expected_value);
                    assert_eq!(s, record.value.s);
                } else {
                    unreachable!("expected a record, got {:?}", msg);
                }
            }
            None => {
                unreachable!("option shouldn't be missing")
            }
        }
    }
}

pub struct MqttTopicTester {
    topic: String,
    port: u16,
    ca: Option<String>,
    cert: Option<String>,
    key: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

impl MqttTopicTester {
    fn get_config(&self) -> MqttConfig {
        MqttConfig {
            host: "localhost".to_string(),
            port: Some(self.port as i64),
            client_prefix: Some("test".to_string()),
            protocol: Protocol::Tcp,
            username: self.username.as_ref().map(|u| VarStr::new(u.clone())),
            password: self.password.as_ref().map(|p| VarStr::new(p.clone())),
            tls: Some(Tls {
                ca: self.ca.as_ref().map(|ca| VarStr::new(ca.clone())),
                cert: self.cert.as_ref().map(|ca| VarStr::new(ca.clone())),
                key: self.key.as_ref().map(|ca| VarStr::new(ca.clone())),
            }),
        }
    }

    async fn get_client(&self) -> rumqttc::v5::AsyncClient {
        let config = self.get_config();
        let (client, mut eventloop) = crate::connectors::mqtt::create_connection(config, 0)
            .expect("Failed to create connection");

        tokio::spawn(async move {
            loop {
                let event = eventloop.poll().await;
                if let Err(err) = event {
                    tracing::error!("Error in mqtt event loop: {:?}", err);
                    panic!("Error in mqtt event loop: {:?}", err);
                }
            }
        });

        client
    }

    async fn get_source_with_reader(&self, task_info: TaskInfo) -> MqttSourceWithReads {
        let config = self.get_config();

        let mut mqtt: MqttSourceFunc<(), TestData> = MqttSourceFunc::new(
            config,
            self.topic.clone(),
            QoS::AtLeastOnce,
            Format::Json(JsonFormat::default()),
            None,
            RateLimiter::new(),
            None,
            10,
        );

        let (to_control_tx, control_rx) = channel(128);
        let (command_tx, from_control_rx) = channel(128);
        let (data_tx, recv) = channel(128);

        let mut ctx: Context<(), TestData> = Context::new(
            task_info,
            None,
            control_rx,
            command_tx,
            1,
            vec![vec![OutQueue::new(data_tx, false)]],
            source::tables(),
        )
        .await;

        let subscribed = mqtt.subscribed();
        tokio::spawn(async move {
            mqtt.on_start(&mut ctx).await;
            mqtt.run(&mut ctx).await;
        });

        MqttSourceWithReads {
            to_control_tx,
            from_control_rx,
            data_recv: recv,
            subscribed,
        }
    }
}

#[tokio::test]
async fn test_mqtt() {
    let mqtt_tester = MqttTopicTester {
        topic: "mqtt-arroyo-test".to_string(),
        port: 1883,
        ca: None,
        cert: None,
        key: None,
        username: None,
        password: None,
    };

    let mut task_info = arroyo_types::get_test_task_info();
    task_info.job_id = format!("mqtt-job-{}", rand::thread_rng().gen::<u64>());

    let mut reader = mqtt_tester.get_source_with_reader(task_info.clone()).await;

    reader
        .wait_for_subscription(std::time::Duration::from_secs(5))
        .await;

    let client = mqtt_tester.get_client().await;

    for message in 1u64..20 {
        let payload = serde_json::to_vec(&TestData {
            s: format!("message-{}", message),
        })
        .unwrap();

        match client
            .publish(&mqtt_tester.topic, QoS::AtLeastOnce, false, payload)
            .await
        {
            Ok(_) => {
                reader.assert_next_message_record_value(message).await;
            }

            Err(e) => {
                panic!("Failed to publish message: {:?}", e);
            }
        }
    }

    reader
        .to_control_tx
        .send(ControlMessage::Stop {
            mode: arroyo_rpc::grpc::StopMode::Graceful,
        })
        .await
        .unwrap();
}
