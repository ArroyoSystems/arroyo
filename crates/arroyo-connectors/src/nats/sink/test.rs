use super::{NatsSinkEncoder, NatsSinkFunc};
use crate::nats::{
    ConnectorType, NatsConfig, NatsConfigAuthentication, NatsTable, SinkType,
    encode_flatbuffers_message,
};
use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arroyo_rpc::var_str::VarStr;
use std::sync::Arc;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]))
}

#[test]
fn flatbuffers_sink_messages_roundtrip() {
    let sink = NatsSinkFunc {
        sink_type: SinkType::Subject("events".to_string()),
        servers: "nats://127.0.0.1:4222".to_string(),
        connection: NatsConfig {
            authentication: NatsConfigAuthentication::None {},
            servers: VarStr::new("nats://127.0.0.1:4222".to_string()),
        },
        table: NatsTable {
            connector_type: ConnectorType::Sink {
                sink_type: Some(SinkType::Subject("events".to_string())),
            },
        },
        publisher: None,
        encoder: NatsSinkEncoder::Flatbuffers,
    };

    assert!(matches!(sink.encoder, NatsSinkEncoder::Flatbuffers));

    let batch = RecordBatch::try_new(
        schema(),
        vec![Arc::new(StringArray::from(vec!["one", "two"]))],
    )
    .unwrap();

    let message = encode_flatbuffers_message(&batch).unwrap();
    let decoded = crate::nats::decode_flatbuffers_message(&message).unwrap();

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].schema(), batch.schema());

    let values = decoded[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(values, vec!["one", "two"]);
}
