use anyhow::{anyhow, bail};
use arroyo_operator::connector::ErasedConnector;
use arroyo_rpc::api_types::connections::{
    ConnectionType, FieldType, SourceField, SourceFieldType, TestSourceMessage,
};
use arroyo_rpc::primitive_to_sql;
use arroyo_rpc::var_str::VarStr;
use arroyo_types::string_to_map;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tracing::warn;

pub mod blackhole;
pub mod confluent;
pub mod filesystem;
#[cfg(feature = "fluvio")]
pub mod fluvio;
pub mod impulse;
pub mod kafka;
#[cfg(feature = "kinesis")]
pub mod kinesis;
#[cfg(feature = "mqtt")]
pub mod mqtt;
#[cfg(feature = "nats")]
pub mod nats;
pub mod nexmark;
pub mod polling_http;
pub mod preview;
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;
#[cfg(feature = "redis")]
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
        #[cfg(feature = "fluvio")]
        Box::new(fluvio::FluvioConnector {}),
        Box::new(impulse::ImpulseConnector {}),
        Box::new(kafka::KafkaConnector {}),
        #[cfg(feature = "kinesis")]
        Box::new(kinesis::KinesisConnector {}),
        #[cfg(feature = "mqtt")]
        Box::new(mqtt::MqttConnector {}),
        #[cfg(feature = "nats")]
        Box::new(nats::NatsConnector {}),
        Box::new(nexmark::NexmarkConnector {}),
        Box::new(polling_http::PollingHTTPConnector {}),
        Box::new(preview::PreviewConnector {}),
        #[cfg(feature = "rabbitmq")]
        Box::new(rabbitmq::RabbitmqConnector {}),
        #[cfg(feature = "redis")]
        Box::new(redis::RedisConnector {}),
        Box::new(single_file::SingleFileConnector {}),
        Box::new(sse::SSEConnector {}),
        Box::new(stdout::StdoutConnector {}),
        Box::new(webhook::WebhookConnector {}),
        Box::new(websocket::WebsocketConnector {}),
    ];

    connectors.into_iter().map(|c| (c.name(), c)).collect()
}

#[derive(Serialize, Deserialize)]
pub struct EmptyConfig {}

pub(crate) async fn send(tx: &mut Sender<TestSourceMessage>, msg: TestSourceMessage) {
    if tx.send(msg).await.is_err() {
        warn!("Test API rx closed while sending message");
    }
}

pub fn connector_for_type(t: &str) -> Option<Box<dyn ErasedConnector>> {
    connectors().remove(t)
}

pub(crate) fn source_field(name: &str, field_type: FieldType) -> SourceField {
    SourceField {
        field_name: name.to_string(),
        field_type: SourceFieldType {
            sql_name: match field_type.clone() {
                FieldType::Primitive(p) => Some(primitive_to_sql(p).to_string()),
                FieldType::Struct(_) => None,
                FieldType::List(_) => None,
            },
            r#type: field_type,
        },
        nullable: false,
        metadata_key: None,
    }
}

fn construct_http_client(endpoint: &str, headers: Option<String>) -> anyhow::Result<Client> {
    if let Err(e) = reqwest::Url::parse(endpoint) {
        bail!("invalid endpoint '{}': {:?}", endpoint, e)
    };

    let headers: anyhow::Result<HeaderMap> = string_to_map(headers.as_deref().unwrap_or(""), ':')
        .expect("Invalid header map")
        .into_iter()
        .map(|(k, v)| {
            Ok((
                TryInto::<HeaderName>::try_into(&k)
                    .map_err(|_| anyhow!("invalid header name {}", k))?,
                TryInto::<HeaderValue>::try_into(&v)
                    .map_err(|_| anyhow!("invalid header value {}", v))?,
            ))
        })
        .collect();

    let client = reqwest::ClientBuilder::new()
        .default_headers(headers?)
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| anyhow!("could not construct HTTP client: {:?}", e))?;

    Ok(client)
}

pub fn header_map(headers: Option<VarStr>) -> HashMap<String, String> {
    string_to_map(
        &headers
            .map(|t| t.sub_env_vars().expect("Failed to substitute env vars"))
            .unwrap_or("".to_string()),
        ':',
    )
    .expect("Invalid header map")
}

#[cfg(test)]
mod test {
    use arrow::array::RecordBatch;
    use arroyo_operator::context::Collector;
    use arroyo_types::Watermark;
    use async_trait::async_trait;

    pub struct DummyCollector {}

    #[async_trait]
    impl Collector for DummyCollector {
        async fn collect(&mut self, _: RecordBatch) {
            unreachable!()
        }

        async fn broadcast_watermark(&mut self, _: Watermark) {
            unreachable!()
        }
    }
}
