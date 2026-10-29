use arroyo_operator::connector::ErasedConnector;
use arroyo_rpc::api_types::connections::ConnectionType;
use std::collections::HashMap;

use arroyo_connector_common::send;
#[cfg(test)]
use arroyo_connector_common::test;
pub use arroyo_connector_common::{EmptyConfig, header_map, render_schema};

#[cfg(feature = "blackhole")]
pub use arroyo_connector_blackhole as blackhole;
pub mod confluent;
pub mod filesystem;
#[cfg(feature = "fluvio")]
pub use arroyo_connector_fluvio as fluvio;
#[cfg(feature = "impulse")]
pub use arroyo_connector_impulse as impulse;
pub mod kafka;
#[cfg(feature = "kinesis")]
pub use arroyo_connector_kinesis as kinesis;
pub mod mqtt;
pub mod nats;
#[cfg(feature = "nexmark")]
pub use arroyo_connector_nexmark as nexmark;
#[cfg(feature = "polling-http")]
pub use arroyo_connector_polling_http as polling_http;
#[cfg(feature = "preview")]
pub use arroyo_connector_preview as preview;
#[cfg(feature = "rabbitmq")]
pub use arroyo_connector_rabbitmq as rabbitmq;
pub mod redis;
#[cfg(feature = "single-file")]
pub use arroyo_connector_single_file as single_file;
#[cfg(feature = "sse")]
pub use arroyo_connector_sse as sse;
#[cfg(feature = "stdout")]
pub use arroyo_connector_stdout as stdout;
#[cfg(feature = "webhook")]
pub use arroyo_connector_webhook as webhook;
#[cfg(feature = "websocket")]
pub use arroyo_connector_websocket as websocket;

pub fn connectors() -> HashMap<&'static str, Box<dyn ErasedConnector>> {
    let connectors: Vec<Box<dyn ErasedConnector>> = vec![
        #[cfg(feature = "blackhole")]
        Box::new(blackhole::BlackholeConnector {}),
        Box::new(confluent::ConfluentConnector {}),
        Box::new(filesystem::delta::DeltaLakeConnector {}),
        Box::new(filesystem::FileSystemConnector {}),
        #[cfg(feature = "fluvio")]
        Box::new(fluvio::FluvioConnector {}),
        Box::new(filesystem::iceberg::IcebergConnector {}),
        #[cfg(feature = "impulse")]
        Box::new(impulse::ImpulseConnector {}),
        Box::new(kafka::KafkaConnector {}),
        #[cfg(feature = "kinesis")]
        Box::new(kinesis::KinesisConnector {}),
        Box::new(mqtt::MqttConnector {}),
        Box::new(nats::NatsConnector {}),
        #[cfg(feature = "nexmark")]
        Box::new(nexmark::NexmarkConnector {}),
        #[cfg(feature = "polling-http")]
        Box::new(polling_http::PollingHTTPConnector {}),
        #[cfg(feature = "preview")]
        Box::new(preview::PreviewConnector {}),
        #[cfg(feature = "rabbitmq")]
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
        #[cfg(feature = "websocket")]
        Box::new(websocket::WebsocketConnector {}),
    ];

    connectors.into_iter().map(|c| (c.name(), c)).collect()
}

pub fn connector_for_type(t: &str) -> Option<Box<dyn ErasedConnector>> {
    connectors().remove(t)
}
