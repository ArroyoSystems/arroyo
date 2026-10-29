use arroyo_operator::connector::ErasedConnector;
use arroyo_rpc::api_types::connections::{ConnectionSchema, ConnectionType};
use std::collections::HashMap;

#[cfg(test)]
use arroyo_connector_common::test;
pub use arroyo_connector_common::{EmptyConfig, header_map, render_schema};
use arroyo_connector_common::{construct_http_client, send};

#[cfg(feature = "blackhole")]
pub use arroyo_connector_blackhole as blackhole;
pub mod confluent;
pub mod filesystem;
pub mod fluvio;
#[cfg(feature = "impulse")]
pub use arroyo_connector_impulse as impulse;
pub mod kafka;
pub mod kinesis;
pub mod mqtt;
pub mod nats;
#[cfg(feature = "nexmark")]
pub use arroyo_connector_nexmark as nexmark;
pub mod polling_http;
#[cfg(feature = "preview")]
pub use arroyo_connector_preview as preview;
pub mod rabbitmq;
pub mod redis;
#[cfg(feature = "single-file")]
pub use arroyo_connector_single_file as single_file;
#[cfg(feature = "sse")]
pub use arroyo_connector_sse as sse;
#[cfg(feature = "stdout")]
pub use arroyo_connector_stdout as stdout;
#[cfg(feature = "webhook")]
pub use arroyo_connector_webhook as webhook;
pub mod websocket;

pub fn connectors() -> HashMap<&'static str, Box<dyn ErasedConnector>> {
    let connectors: Vec<Box<dyn ErasedConnector>> = vec![
        #[cfg(feature = "blackhole")]
        Box::new(blackhole::BlackholeConnector {}),
        Box::new(confluent::ConfluentConnector {}),
        Box::new(filesystem::delta::DeltaLakeConnector {}),
        Box::new(filesystem::FileSystemConnector {}),
        Box::new(fluvio::FluvioConnector {}),
        Box::new(filesystem::iceberg::IcebergConnector {}),
        #[cfg(feature = "impulse")]
        Box::new(impulse::ImpulseConnector {}),
        Box::new(kafka::KafkaConnector {}),
        Box::new(kinesis::KinesisConnector {}),
        Box::new(mqtt::MqttConnector {}),
        Box::new(nats::NatsConnector {}),
        #[cfg(feature = "nexmark")]
        Box::new(nexmark::NexmarkConnector {}),
        Box::new(polling_http::PollingHTTPConnector {}),
        #[cfg(feature = "preview")]
        Box::new(preview::PreviewConnector {}),
        Box::new(rabbitmq::RabbitmqConnector {}),
        Box::new(redis::RedisConnector {}),
        #[cfg(feature = "single-file")]
        Box::new(single_file::SingleFileConnector {}),
        #[cfg(feature = "sse")]
        Box::new(sse::SSEConnector {}),
        #[cfg(feature = "stdout")]
        Box::new(stdout::StdoutConnector {}),
        #[cfg(feature = "webhook")]
        Box::new(webhook::WebhookConnector {}),
        Box::new(websocket::WebsocketConnector {}),
    ];

    connectors.into_iter().map(|c| (c.name(), c)).collect()
}

pub fn connector_for_type(t: &str) -> Option<Box<dyn ErasedConnector>> {
    connectors().remove(t)
}
