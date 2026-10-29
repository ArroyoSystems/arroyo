use arroyo_operator::connector::ErasedConnector;
use arroyo_rpc::api_types::connections::{ConnectionSchema, ConnectionType};
use std::collections::HashMap;

#[cfg(test)]
use arroyo_connector_common::test;
pub use arroyo_connector_common::{EmptyConfig, header_map, render_schema};
use arroyo_connector_common::{construct_http_client, send, source_field};

pub mod blackhole;
pub mod confluent;
pub mod filesystem;
pub mod fluvio;
pub mod impulse;
pub mod kafka;
pub mod kinesis;
pub mod mqtt;
pub mod nats;
pub mod nexmark;
pub mod polling_http;
pub mod preview;
pub mod rabbitmq;
pub mod redis;
pub mod single_file;
pub mod sse;
pub mod stdout;
pub mod webhook;
pub mod websocket;

pub fn connectors() -> HashMap<&'static str, Box<dyn ErasedConnector>> {
    let connectors: Vec<Box<dyn ErasedConnector>> = vec![
        Box::new(blackhole::BlackholeConnector {}),
        Box::new(confluent::ConfluentConnector {}),
        Box::new(filesystem::delta::DeltaLakeConnector {}),
        Box::new(filesystem::FileSystemConnector {}),
        Box::new(fluvio::FluvioConnector {}),
        Box::new(filesystem::iceberg::IcebergConnector {}),
        Box::new(impulse::ImpulseConnector {}),
        Box::new(kafka::KafkaConnector {}),
        Box::new(kinesis::KinesisConnector {}),
        Box::new(mqtt::MqttConnector {}),
        Box::new(nats::NatsConnector {}),
        Box::new(nexmark::NexmarkConnector {}),
        Box::new(polling_http::PollingHTTPConnector {}),
        Box::new(preview::PreviewConnector {}),
        Box::new(rabbitmq::RabbitmqConnector {}),
        Box::new(redis::RedisConnector {}),
        Box::new(single_file::SingleFileConnector {}),
        Box::new(sse::SSEConnector {}),
        Box::new(stdout::StdoutConnector {}),
        Box::new(webhook::WebhookConnector {}),
        Box::new(websocket::WebsocketConnector {}),
    ];

    connectors.into_iter().map(|c| (c.name(), c)).collect()
}

pub fn connector_for_type(t: &str) -> Option<Box<dyn ErasedConnector>> {
    connectors().remove(t)
}
