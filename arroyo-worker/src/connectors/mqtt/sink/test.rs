use std::time::SystemTime;

use arrow::datatypes::Field;
use arroyo_formats::SchemaData;
use arroyo_rpc::{
    formats::{Format, JsonFormat},
    var_str::VarStr,
};
use arroyo_types::Record;
use parquet::data_type::AsBytes;
use rumqttc::{
    v5::{mqttbytes::QoS, Event, Incoming},
    Outgoing,
};
use tokio::sync::mpsc::channel;

use crate::{
    connectors::mqtt::{MqttConfig, Tls, Protocol},
    engine::{Context, OutQueue},
};

use super::MqttSinkFunc;

#[derive(
    Clone,
    Debug,
    bincode::Encode,
    bincode::Decode,
    PartialEq,
    PartialOrd,
    serde::Serialize,
    serde::Deserialize,
)]
struct TestOutStruct {
    t: String,
}

impl From<String> for TestOutStruct {
    fn from(value: String) -> Self {
        TestOutStruct { t: value }
    }
}

impl SchemaData for TestOutStruct {
    fn name() -> &'static str {
        "test_out_struct"
    }
    fn schema() -> arrow::datatypes::Schema {
        arrow::datatypes::Schema::new(vec![Field::new(
            "t",
            arrow::datatypes::DataType::Utf8,
            false,
        )])
    }

    fn to_raw_string(&self) -> Option<Vec<u8>> {
        unimplemented!()
    }

    fn to_avro(&self, _schema: &apache_avro::Schema) -> apache_avro::types::Value {
        todo!()
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

    async fn get_client(&self) -> (rumqttc::v5::AsyncClient, rumqttc::v5::EventLoop) {
        let config = self.get_config();
        crate::connectors::mqtt::create_connection(config, 0).expect("Failed to create connection")
    }

    async fn get_sink_with_writes(&self) -> MqttSinkWithWrites {
        let config = self.get_config();
        let mut mqtt = MqttSinkFunc::new(
            config,
            QoS::AtLeastOnce,
            self.topic.clone(),
            false,
            Format::Json(JsonFormat::default()),
        );

        let (_, control_rx) = channel(128);
        let (command_tx, _) = channel(128);
        let (data_tx, _recv) = channel(128);

        let task_info = arroyo_types::get_test_task_info();

        let mut ctx: Context<(), ()> = Context::new(
            task_info,
            None,
            control_rx,
            command_tx,
            1,
            vec![vec![OutQueue::new(data_tx, false)]],
            vec![],
        )
        .await;
        mqtt.on_start(&mut ctx).await;

        MqttSinkWithWrites { sink: mqtt, ctx }
    }
}

struct MqttSinkWithWrites {
    sink: MqttSinkFunc<(), TestOutStruct>,
    ctx: Context<(), ()>,
}

#[tokio::test]
async fn test_mqtt() {
    let mqtt_tester = MqttTopicTester {
        topic: "mqtt-arroyo-test-sink".to_string(),
        port: 1883,
        ca: None,
        cert: None,
        key: None,
        username: None,
        password: None,
    };

    let mut sink_with_writes = mqtt_tester.get_sink_with_writes().await;
    let (client, mut eventloop) = mqtt_tester.get_client().await;

    client
        .subscribe(&mqtt_tester.topic, QoS::AtLeastOnce)
        .await
        .unwrap();
    let start = std::time::Instant::now();
    loop {
        match eventloop.poll().await {
            Ok(Event::Outgoing(Outgoing::Subscribe(_))) => {
                break;
            }
            _ => {
                if start.elapsed().as_secs() > 5 {
                    panic!("Failed to subscribe to topic");
                }
            }
        }
    }
    for message in 1u32..200 {
        let payload = message.to_string();
        let mut record = Record {
            timestamp: SystemTime::now(),
            key: Some(()),
            value: payload.into(),
        };

        sink_with_writes
            .sink
            .process_element(&mut record, &mut sink_with_writes.ctx)
            .await;
    }

    let mut message = 1u32;

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                let result: TestOutStruct = serde_json::from_slice(p.payload.as_bytes()).unwrap();
                assert_eq!(
                    message.to_string(),
                    result.t,
                    "{} {:?}",
                    message,
                    String::from_utf8_lossy(p.payload.as_bytes())
                );
                message += 1;
                if message >= 200 {
                    break;
                }
            }
            Ok(_) => (),
            Err(err) => {
                panic!("Error in mqtt event loop: {:?}", err);
            }
        }
    }
}
