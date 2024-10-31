use crate::confluent::ConfluentConnector;
use crate::filesystem::delta::DeltaLakeConnector;
use crate::filesystem::FileSystemConnector;
use crate::kinesis::KinesisConnector;
use crate::mqtt::MqttConnector;
use crate::polling_http::PollingHTTPConnector;
use crate::preview::PreviewConnector;
use crate::redis::RedisConnector;
use crate::single_file::SingleFileConnector;
use crate::stdout::StdoutConnector;
use crate::webhook::WebhookConnector;
use anyhow::{anyhow, bail, Context};
use arroyo_operator::connector::ErasedConnector;
use arroyo_rpc::api_types::connections::{
    ConnectionSchema, ConnectionType, FieldType, SourceField, SourceFieldType, TestSourceMessage,
};
use arroyo_rpc::primitive_to_sql;
use arroyo_rpc::var_str::VarStr;
use arroyo_types::string_to_map;
use blackhole::BlackholeConnector;
use fluvio::FluvioConnector;
use impulse::ImpulseConnector;
use nats::NatsConnector;
use nexmark::NexmarkConnector;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sse::SSEConnector;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tracing::warn;
use websocket::WebsocketConnector;

use self::kafka::KafkaConnector;

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
pub mod redis;
pub mod single_file;
pub mod sse;
pub mod stdout;
pub mod webhook;
pub mod websocket;

pub fn connectors() -> HashMap<&'static str, Box<dyn ErasedConnector>> {
    let connectors: Vec<Box<dyn ErasedConnector>> = vec![
        Box::new(BlackholeConnector {}),
        Box::new(ConfluentConnector {}),
        Box::new(DeltaLakeConnector {}),
        Box::new(FileSystemConnector {}),
        Box::new(FluvioConnector {}),
        Box::new(ImpulseConnector {}),
        Box::new(KafkaConnector {}),
        Box::new(KinesisConnector {}),
        Box::new(MqttConnector {}),
        Box::new(NatsConnector {}),
        Box::new(NexmarkConnector {}),
        Box::new(PollingHTTPConnector {}),
        Box::new(PreviewConnector {}),
        Box::new(RedisConnector {}),
        Box::new(SingleFileConnector {}),
        Box::new(SSEConnector {}),
        Box::new(StdoutConnector {}),
        Box::new(WebhookConnector {}),
        Box::new(WebsocketConnector {}),
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

pub(crate) fn pull_opt(name: &str, opts: &mut HashMap<String, String>) -> anyhow::Result<String> {
    opts.remove(name)
        .ok_or_else(|| anyhow!("required option '{}' not set", name))
}

pub(crate) fn pull_option_to_i64(
    name: &str,
    opts: &mut HashMap<String, String>,
) -> anyhow::Result<Option<i64>> {
    opts.remove(name)
        .map(|value| {
            value.parse::<i64>().context(format!(
                "failed to parse {} as a number for option {}",
                value, name
            ))
        })
        .transpose()
}

pub(crate) fn pull_option_to_u64(
    name: &str,
    opts: &mut HashMap<String, String>,
) -> anyhow::Result<Option<u64>> {
    pull_option_to_i64(name, opts)?
        .map(|x| {
            if x < 0 {
                bail!("invalid valid for {}, must be greater than 0", name);
            } else {
                Ok(x as u64)
            }
        })
        .transpose()
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
