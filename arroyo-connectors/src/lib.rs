use anyhow::{anyhow, bail, Context};
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, FieldType, SourceField, SourceFieldType,
    TestSourceMessage,
};
use arroyo_rpc::primitive_to_sql;
use arroyo_types::string_to_map;
use axum::response::sse::Event;
use blackhole::BlackholeConnector;
use fluvio::FluvioConnector;
use impulse::ImpulseConnector;
use nexmark::NexmarkConnector;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sse::SSEConnector;
use std::collections::HashMap;
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tracing::warn;
use websocket::WebsocketConnector;

use self::kafka::KafkaConnector;

pub mod blackhole;
pub mod confluent;
pub mod delta;
pub mod filesystem;
pub mod fluvio;
pub mod impulse;
pub mod kafka;
pub mod kinesis;
pub mod mqtt;
pub mod nexmark;
pub mod polling_http;
pub mod redis;
pub mod single_file;
pub mod sse;
pub mod webhook;
pub mod websocket;

pub fn connectors() -> HashMap<&'static str, Box<dyn ErasedConnector>> {
    let mut m: HashMap<&'static str, Box<dyn ErasedConnector>> = HashMap::new();
    m.insert("blackhole", Box::new(BlackholeConnector {}));
    m.insert("confluent", Box::new(confluent::ConfluentConnector {}));
    m.insert("delta", Box::new(delta::DeltaLakeConnector {}));
    m.insert("filesystem", Box::new(filesystem::FileSystemConnector {}));
    m.insert("fluvio", Box::new(FluvioConnector {}));
    m.insert("impulse", Box::new(ImpulseConnector {}));
    m.insert("kafka", Box::new(KafkaConnector {}));
    m.insert("kinesis", Box::new(kinesis::KinesisConnector {}));
    m.insert("mqtt", Box::new(mqtt::MqttConnector {}));
    m.insert("nexmark", Box::new(NexmarkConnector {}));
    m.insert(
        "polling_http",
        Box::new(polling_http::PollingHTTPConnector {}),
    );
    m.insert("redis", Box::new(redis::RedisConnector {}));
    m.insert("single_file", Box::new(single_file::SingleFileConnector {}));
    m.insert("sse", Box::new(SSEConnector {}));
    m.insert("webhook", Box::new(webhook::WebhookConnector {}));
    m.insert("websocket", Box::new(WebsocketConnector {}));

    m
}

#[derive(Serialize, Deserialize)]
pub struct EmptyConfig {}

#[derive(Debug, Clone)]
pub struct Connection {
    pub id: Option<i64>,
    pub name: String,
    pub connection_type: ConnectionType,
    pub schema: ConnectionSchema,
    pub operator: String,
    pub config: String,
    pub description: String,
}

pub trait Connector: Send {
    type ProfileT: DeserializeOwned + Serialize;
    type TableT: DeserializeOwned + Serialize;

    fn name(&self) -> &'static str;

    #[allow(unused)]
    fn config_description(&self, config: Self::ProfileT) -> String {
        "".to_string()
    }

    fn parse_config(&self, s: &serde_json::Value) -> Result<Self::ProfileT, serde_json::Error> {
        serde_json::from_value(s.clone())
    }

    fn parse_table(&self, s: &serde_json::Value) -> Result<Self::TableT, serde_json::Error> {
        serde_json::from_value(s.clone())
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector;

    fn table_type(&self, config: Self::ProfileT, table: Self::TableT) -> ConnectionType;

    #[allow(unused)]
    fn get_schema(
        &self,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        schema.cloned()
    }

    #[allow(unused)]
    fn test_profile(
        &self,
        profile: Self::ProfileT,
    ) -> Option<tokio::sync::oneshot::Receiver<TestSourceMessage>> {
        None
    }

    #[allow(unused)]
    fn get_autocomplete(
        &self,
        profile: Self::ProfileT,
    ) -> oneshot::Receiver<anyhow::Result<HashMap<String, Vec<String>>>> {
        let (tx, rx) = oneshot::channel();
        tx.send(Ok(HashMap::new())).unwrap();
        rx
    }

    fn test(
        &self,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
        tx: Sender<Result<Event, Infallible>>,
    );

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection>;

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection>;
}

pub trait ErasedConnector: Send {
    fn name(&self) -> &'static str;

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector;

    fn validate_config(&self, s: &serde_json::Value) -> Result<(), serde_json::Error>;

    fn validate_table(&self, s: &serde_json::Value) -> Result<(), serde_json::Error>;

    fn table_type(
        &self,
        config: &serde_json::Value,
        table: &serde_json::Value,
    ) -> Result<ConnectionType, serde_json::Error>;

    fn config_description(&self, s: &serde_json::Value) -> Result<String, serde_json::Error>;

    fn get_schema(
        &self,
        config: &serde_json::Value,
        table: &serde_json::Value,
        schema: Option<&ConnectionSchema>,
    ) -> Result<Option<ConnectionSchema>, serde_json::Error>;

    /// Returns a map of autocomplete values from key names (with paths separated by dots) to values that should
    /// be used to autocomplete them.
    #[allow(unused)]
    fn get_autocomplete(
        &self,
        profile: &serde_json::Value,
    ) -> Result<oneshot::Receiver<anyhow::Result<HashMap<String, Vec<String>>>>, serde_json::Error>;

    fn test_profile(
        &self,
        profile: &serde_json::Value,
    ) -> Result<Option<oneshot::Receiver<TestSourceMessage>>, serde_json::Error>;

    fn test(
        &self,
        name: &str,
        config: &serde_json::Value,
        table: &serde_json::Value,
        schema: Option<&ConnectionSchema>,
        tx: Sender<Result<Event, Infallible>>,
    ) -> Result<(), serde_json::Error>;

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection>;

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: &serde_json::Value,
        table: &serde_json::Value,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection>;
}

impl<C: Connector> ErasedConnector for C {
    fn name(&self) -> &'static str {
        self.name()
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        self.metadata()
    }

    fn config_description(&self, s: &serde_json::Value) -> Result<String, serde_json::Error> {
        Ok(self.config_description(self.parse_config(s)?))
    }

    fn validate_config(&self, config: &serde_json::Value) -> Result<(), serde_json::Error> {
        self.parse_config(config)?;
        Ok(())
    }

    fn validate_table(&self, table: &serde_json::Value) -> Result<(), serde_json::Error> {
        self.parse_table(table)?;
        Ok(())
    }

    fn table_type(
        &self,
        config: &serde_json::Value,
        table: &serde_json::Value,
    ) -> Result<ConnectionType, serde_json::Error> {
        Ok(self.table_type(self.parse_config(config)?, self.parse_table(table)?))
    }

    fn get_schema(
        &self,
        config: &serde_json::Value,
        table: &serde_json::Value,
        schema: Option<&ConnectionSchema>,
    ) -> Result<Option<ConnectionSchema>, serde_json::Error> {
        Ok(self.get_schema(self.parse_config(config)?, self.parse_table(table)?, schema))
    }

    fn get_autocomplete(
        &self,
        profile: &Value,
    ) -> Result<oneshot::Receiver<anyhow::Result<HashMap<String, Vec<String>>>>, serde_json::Error>
    {
        Ok(self.get_autocomplete(self.parse_config(profile)?))
    }

    fn test_profile(
        &self,
        profile: &serde_json::Value,
    ) -> Result<Option<tokio::sync::oneshot::Receiver<TestSourceMessage>>, serde_json::Error> {
        Ok(self.test_profile(self.parse_config(profile)?))
    }

    fn test(
        &self,
        name: &str,
        config: &serde_json::Value,
        table: &serde_json::Value,
        schema: Option<&ConnectionSchema>,
        tx: Sender<Result<Event, Infallible>>,
    ) -> Result<(), serde_json::Error> {
        self.test(
            name,
            self.parse_config(config)?,
            self.parse_table(table)?,
            schema,
            tx,
        );

        Ok(())
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        self.from_options(name, options, schema, profile)
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: &serde_json::Value,
        table: &serde_json::Value,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        self.from_config(
            id,
            name,
            self.parse_config(config)?,
            self.parse_table(table)?,
            schema,
        )
    }
}

pub(crate) async fn send(tx: &mut Sender<Result<Event, Infallible>>, msg: impl Serialize) {
    if tx
        .send(Ok(Event::default().json_data(msg).unwrap()))
        .await
        .is_err()
    {
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
            },
            r#type: field_type,
        },
        nullable: false,
    }
}

pub(crate) fn nullable_field(name: &str, field_type: SourceFieldType) -> SourceField {
    SourceField {
        field_name: name.to_string(),
        field_type,
        nullable: true,
    }
}

fn construct_http_client(endpoint: &str, headers: Option<String>) -> anyhow::Result<Client> {
    if let Err(e) = reqwest::Url::parse(&endpoint) {
        bail!("invalid endpoint '{}': {:?}", endpoint, e)
    };

    let headers: anyhow::Result<HeaderMap> =
        string_to_map(headers.as_ref().map(|t| t.as_str()).unwrap_or(""), ':')
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
