use super::*;
use crate::source_field;
use arroyo_operator::connector::Connector;
use arroyo_rpc::api_types::connections::{ConnectionSchema, FieldType};
use arroyo_rpc::formats::{FlatbuffersFormat, Format};

fn schema() -> ConnectionSchema {
    ConnectionSchema {
        format: Some(Format::Flatbuffers(FlatbuffersFormat {})),
        bad_data: None,
        framing: None,
        fields: vec![source_field("value", FieldType::String)],
        definition: None,
        inferred: None,
        primary_keys: Default::default(),
    }
}

#[test]
fn sink_config_preserves_flatbuffers_format() {
    let connector = NatsConnector {};
    let connection = connector
        .from_config(
            None,
            "nats-flatbuffers",
            NatsConfig {
                authentication: NatsConfigAuthentication::None {},
                servers: VarStr::new("nats://127.0.0.1:4222".to_string()),
            },
            NatsTable {
                connector_type: ConnectorType::Sink {
                    sink_type: Some(SinkType::Subject("events".to_string())),
                },
            },
            Some(&schema()),
        )
        .unwrap();

    let config: arroyo_rpc::OperatorConfig = serde_json::from_str(&connection.config).unwrap();
    assert_eq!(
        config.format,
        Some(Format::Flatbuffers(FlatbuffersFormat {}))
    );
}
