use arrow::array::{RecordBatch, StringArray};
use std::collections::HashMap;
use std::sync::Arc;

use super::ZmqSinkFunc;
use crate::test::DummyCollector;
use crate::zmq::{ConnectionPattern, ZmqConfig};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arroyo_operator::context::OperatorContext;
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::{
    formats::{Format, JsonFormat},
    var_str::VarStr,
};
use arroyo_types::get_test_task_info;
use serde::Deserialize;
use tokio::sync::mpsc::channel;
use zmq::Context;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]))
}

#[derive(Deserialize)]
struct TestData {
    value: String,
}

pub struct ZmqTopicTester {
    socket: zmq::Socket,
    url: String,
}

impl ZmqTopicTester {
    async fn new() -> Self {
        let context = Context::new();
        let socket = context
            .socket(zmq::SocketType::PULL)
            .expect("Failed to create PULL socket");

        // Bind to port 0 to get an available port
        socket
            .bind("tcp://127.0.0.1:0")
            .expect("Failed to bind to available port");

        let endpoint = socket
            .get_last_endpoint()
            .expect("Failed to get endpoint")
            .expect("No endpoint found");

        let port = endpoint
            .split(':')
            .next_back()
            .expect("Invalid endpoint format")
            .parse::<u16>()
            .expect("Failed to parse port");

        let url = format!("tcp://127.0.0.1:{port}");

        Self { socket, url }
    }

    fn get_config(&self) -> ZmqConfig {
        ZmqConfig {
            connection_pattern: ConnectionPattern::Connect,
            url: VarStr::new(self.url.clone()),
        }
    }

    fn get_consumer_socket(&self) -> &zmq::Socket {
        &self.socket
    }

    async fn get_sink_with_writes(&self) -> ZmqSinkWithWrites {
        let config = self.get_config();
        let mut zmq_sink = ZmqSinkFunc::new(config, Format::Json(JsonFormat::default()));

        let (command_tx, _) = channel(128);

        let mut task_info = get_test_task_info();
        task_info.operator_id = "zmq_sink".to_string();
        let task_info = Arc::new(task_info);

        let mut ctx = OperatorContext::new(
            task_info,
            None,
            command_tx,
            1,
            vec![Arc::new(ArroyoSchema::new_unkeyed(schema(), 0))],
            None,
            HashMap::new(),
        )
        .await;

        zmq_sink.on_start(&mut ctx).await;

        ZmqSinkWithWrites {
            sink: zmq_sink,
            ctx,
        }
    }
}

struct ZmqSinkWithWrites {
    sink: ZmqSinkFunc,
    ctx: OperatorContext,
}

#[tokio::test]
async fn test_zmq() {
    const NUM_MESSAGES: u32 = 200;
    let zmq_tester = ZmqTopicTester::new().await;

    let consumer_socket = zmq_tester.get_consumer_socket();
    consumer_socket
        .set_rcvtimeo(1000)
        .expect("Failed to set receive timeout");

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut sink_with_writes = zmq_tester.get_sink_with_writes().await;

    for message in 1u32..NUM_MESSAGES {
        let data = StringArray::from_iter_values(vec![message.to_string()].into_iter());
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(data)]).unwrap();

        sink_with_writes
            .sink
            .process_batch(batch, &mut sink_with_writes.ctx, &mut DummyCollector {})
            .await;
    }

    let mut message = 1u32;
    let mut received_count = 0;

    while received_count < NUM_MESSAGES - 1 {
        match consumer_socket.recv_bytes(0) {
            Ok(bytes) => {
                let result: TestData =
                    serde_json::from_slice(&bytes).expect("Failed to deserialize message");

                assert_eq!(
                    message.to_string(),
                    result.value,
                    "Message {} mismatch: expected {}, got {}",
                    received_count + 1,
                    message,
                    result.value
                );

                message += 1;
                received_count += 1;
            }
            Err(zmq::Error::EAGAIN) => {
                if received_count > 0 {
                    continue;
                } else {
                    panic!("No messages received within timeout");
                }
            }
            Err(e) => {
                panic!("Error receiving ZMQ message: {e:?}");
            }
        }
    }

    println!("Successfully received and verified {received_count} messages");
}
