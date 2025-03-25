use arrow::array::UInt64Array;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use crate::mqtt::{create_connection, MqttConfig, Tls};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arroyo_operator::context::{
    batch_bounded, ArrowCollector, BatchReceiver, OperatorContext, SourceCollector, SourceContext,
};
use arroyo_operator::operator::SourceOperator;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::formats::{Format, JsonFormat};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ControlMessage, ControlResp};
use arroyo_types::{to_nanos, ArrowMessage, ChainInfo, TaskInfo};
use rand::random;
use rumqttc::mqttbytes::QoS;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use super::MqttSourceFunc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestData {
    value: u64,
}

struct MqttSourceWithReads {
    to_control_tx: Sender<ControlMessage>,
    #[allow(dead_code)]
    from_control_rx: Receiver<ControlResp>,
    data_recv: BatchReceiver,
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

    async fn assert_next_message_record_value(&mut self, mut expected_values: VecDeque<u64>) {
        match self.data_recv.recv().await {
            Some(item) => {
                if let ArrowMessage::Data(record) = item {
                    let a = record.columns()[1]
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();

                    for v in a {
                        assert_eq!(
                            expected_values
                                .pop_front()
                                .expect("found more elements than expected"),
                            v.unwrap()
                        );
                    }
                } else {
                    unreachable!("expected data, got {:?}", item);
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
            url: format!("tcp://localhost:{}", self.port),
            client_prefix: Some("test".to_string()),
            username: self.username.as_ref().map(|u| VarStr::new(u.clone())),
            password: self.password.as_ref().map(|p| VarStr::new(p.clone())),
            tls: Some(Tls {
                ca: self.ca.as_ref().map(|ca| VarStr::new(ca.clone())),
                cert: self.cert.as_ref().map(|ca| VarStr::new(ca.clone())),
                key: self.key.as_ref().map(|ca| VarStr::new(ca.clone())),
            }),
        }
    }

    async fn get_client(&self) -> rumqttc::AsyncClient {
        let config = self.get_config();
        let (client, mut eventloop) = create_connection(
            &config,
            &format!("test-{}", to_nanos(SystemTime::now())),
            "source_0",
            0,
        )
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
        let task_info = Arc::new(task_info);

        let mut mqtt = MqttSourceFunc::new(
            config,
            self.topic.clone(),
            QoS::AtLeastOnce,
            Format::Json(JsonFormat::default()),
            None,
            None,
            10,
            vec![],
        );

        let (to_control_tx, control_rx) = channel(128);
        let (command_tx, from_control_rx) = channel(128);
        let (data_tx, recv) = batch_bounded(128);

        let ctx = OperatorContext::new(
            task_info.clone(),
            None.as_ref(),
            command_tx.clone(),
            1,
            vec![],
            Some(Arc::new(ArroyoSchema::new_unkeyed(
                Arc::new(Schema::new(vec![
                    Field::new(
                        "_timestamp",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        false,
                    ),
                    Field::new("value", DataType::UInt64, false),
                ])),
                0,
            ))),
            mqtt.tables(),
        )
        .await;

        let chain_info = Arc::new(ChainInfo {
            job_id: ctx.task_info.job_id.clone(),
            node_id: ctx.task_info.node_id,
            description: "mqtt source".to_string(),
            task_index: ctx.task_info.task_index,
        });

        let mut ctx = SourceContext::from_operator(ctx, chain_info.clone(), control_rx);
        let arrow_collector = ArrowCollector::new(
            chain_info.clone(),
            Some(ctx.out_schema.clone()),
            vec![vec![data_tx]],
        );
        let mut collector = SourceCollector::new(
            ctx.out_schema.clone(),
            arrow_collector,
            command_tx,
            &chain_info,
            &task_info,
        );

        let subscribed = mqtt.subscribed();
        tokio::spawn(async move {
            mqtt.on_start(&mut ctx).await;
            mqtt.run(&mut ctx, &mut collector).await;
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
    task_info.job_id = format!("mqtt-job-{}", random::<u64>());

    let mut reader = mqtt_tester.get_source_with_reader(task_info.clone()).await;

    reader
        .wait_for_subscription(std::time::Duration::from_secs(5))
        .await;

    let client = mqtt_tester.get_client().await;

    let mut expected = vec![];
    for message in 1u64..20 {
        let data = TestData { value: message };
        expected.push(message);
        client
            .publish(
                &mqtt_tester.topic,
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(&data).unwrap(),
            )
            .await
            .expect("Failed to publish message");
    }

    reader
        .assert_next_message_record_value(expected.into())
        .await;

    reader
        .to_control_tx
        .send(ControlMessage::Stop {
            mode: arroyo_rpc::grpc::rpc::StopMode::Graceful,
        })
        .await
        .unwrap();
}
