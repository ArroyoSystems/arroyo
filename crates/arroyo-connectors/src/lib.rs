use anyhow::{anyhow, bail};
use arroyo_operator::connector::ErasedConnector;
use arroyo_rpc::api_types::connections::{
    ConnectionSchema, ConnectionType, FieldType, SourceField, TestSourceMessage,
};
use arroyo_rpc::var_str::VarStr;
use arroyo_types::string_to_map;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client;
use schemars::generate::SchemaSettings;
use schemars::{JsonSchema, SchemaGenerator};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tracing::warn;

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
    let sql_name = field_type.sql_type();
    SourceField {
        name: name.to_string(),
        field_type,
        required: true,
        metadata_key: None,
        sql_name: Some(sql_name),
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

/// Rewrite the schema to be compatible with our hand-written schemas and JsonForm.tsx
fn sanitize_schema(mut schema: Value) -> Value {
    fn walk(v: &mut Value) {
        match v {
            Value::Object(obj) => {
                obj.remove("default");

                // collapse schemars's type | null types
                if let Some(Value::Array(types)) = obj.get_mut("type") {
                    if types.len() == 2
                        && types.iter().any(|t| t == "null")
                        && types.iter().all(|t| t.is_string())
                    {
                        let non_null = types
                            .iter()
                            .find_map(|t| t.as_str().filter(|s| *s != "null"))
                            .unwrap()
                            .to_owned();
                        *obj.get_mut("type").unwrap() = Value::String(non_null);
                    }
                }

                // remove null from enums
                if let Some(Value::Array(values)) = obj.get_mut("enum") {
                    if let Some(i) = values.iter().position(|v| v.is_null()) {
                        values.remove(i);
                    }
                }

                // change anyOf to oneOf
                if let Some(any_of) = obj.remove("anyOf") {
                    obj.insert("oneOf".to_string(), any_of);
                }

                // recurse
                for value in obj.values_mut() {
                    walk(value);
                }
            }
            Value::Array(arr) => arr.iter_mut().for_each(walk),
            _ => {}
        }
    }

    walk(&mut schema);
    schema
}

pub fn render_schema<T: ?Sized + JsonSchema>() -> String {
    let mut schema_settings = SchemaSettings::draft2019_09();
    schema_settings.inline_subschemas = true;
    //schema_settings.contract = Contract::Serialize;
    let schema = SchemaGenerator::new(schema_settings).into_root_schema_for::<T>();
    serde_json::to_string(&sanitize_schema(schema.to_value())).unwrap()
}

#[cfg(test)]
mod test {
    use arrow::array::RecordBatch;
    use arroyo_operator::context::Collector;
    use arroyo_rpc::errors::DataflowResult;
    use arroyo_types::Watermark;
    use async_trait::async_trait;

    pub struct DummyCollector {}

    #[async_trait]
    impl Collector for DummyCollector {
        async fn collect(&mut self, _: RecordBatch) -> DataflowResult<()> {
            unreachable!()
        }

        async fn broadcast_watermark(&mut self, _: Watermark) -> DataflowResult<()> {
            unreachable!()
        }
    }
}
