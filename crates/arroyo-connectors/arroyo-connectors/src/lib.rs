use arroyo_operator::connector::ErasedConnector;
use std::collections::HashMap;

pub use arroyo_connector_common::{EmptyConfig, header_map, render_schema};

#[cfg(feature = "blackhole")]
pub use arroyo_connector_blackhole as blackhole;
#[cfg(feature = "confluent")]
pub use arroyo_connector_confluent as confluent;
#[cfg(feature = "filesystem")]
pub use arroyo_connector_filesystem as filesystem;
#[cfg(feature = "fluvio")]
pub use arroyo_connector_fluvio as fluvio;
#[cfg(feature = "impulse")]
pub use arroyo_connector_impulse as impulse;
#[cfg(feature = "kafka")]
pub use arroyo_connector_kafka as kafka;
#[cfg(feature = "kinesis")]
pub use arroyo_connector_kinesis as kinesis;
#[cfg(feature = "mqtt")]
pub use arroyo_connector_mqtt as mqtt;
#[cfg(feature = "nats")]
pub use arroyo_connector_nats as nats;
#[cfg(feature = "nexmark")]
pub use arroyo_connector_nexmark as nexmark;
#[cfg(feature = "polling-http")]
pub use arroyo_connector_polling_http as polling_http;
#[cfg(feature = "preview")]
pub use arroyo_connector_preview as preview;
#[cfg(feature = "rabbitmq")]
pub use arroyo_connector_rabbitmq as rabbitmq;
#[cfg(feature = "redis")]
pub use arroyo_connector_redis as redis;
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
        #[cfg(feature = "confluent")]
        Box::new(confluent::ConfluentConnector {}),
        #[cfg(feature = "filesystem")]
        Box::new(filesystem::delta::DeltaLakeConnector {}),
        #[cfg(feature = "filesystem")]
        Box::new(filesystem::FileSystemConnector {}),
        #[cfg(feature = "fluvio")]
        Box::new(fluvio::FluvioConnector {}),
        #[cfg(feature = "filesystem")]
        Box::new(filesystem::iceberg::IcebergConnector {}),
        #[cfg(feature = "impulse")]
        Box::new(impulse::ImpulseConnector {}),
        #[cfg(feature = "kafka")]
        Box::new(kafka::KafkaConnector {}),
        #[cfg(feature = "kinesis")]
        Box::new(kinesis::KinesisConnector {}),
        #[cfg(feature = "mqtt")]
        Box::new(mqtt::MqttConnector {}),
        #[cfg(feature = "nats")]
        Box::new(nats::NatsConnector {}),
        #[cfg(feature = "nexmark")]
        Box::new(nexmark::NexmarkConnector {}),
        #[cfg(feature = "polling-http")]
        Box::new(polling_http::PollingHTTPConnector {}),
        #[cfg(feature = "preview")]
        Box::new(preview::PreviewConnector {}),
        #[cfg(feature = "rabbitmq")]
        Box::new(rabbitmq::RabbitmqConnector {}),
        #[cfg(feature = "redis")]
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
