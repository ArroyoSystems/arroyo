use anyhow::{anyhow, bail};
use apache_avro::Schema;
use async_trait::async_trait;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::warn;

#[async_trait]
pub trait SchemaResolver: Send {
    async fn resolve_schema(&self, id: u32) -> Result<Option<String>, String>;
}

/// A schema resolver that return errors when schemas are requested; this is intended
/// to be used when schemas are embedded into the message and we do not expect to
/// dynamically resolve them.
pub struct FailingSchemaResolver {}

impl FailingSchemaResolver {
    pub fn new() -> Self {
        FailingSchemaResolver {}
    }
}

#[async_trait]
impl SchemaResolver for FailingSchemaResolver {
    async fn resolve_schema(&self, id: u32) -> Result<Option<String>, String> {
        Err(format!(
            "Schema with id {} not available, and no schema registry configured",
            id
        ))
    }
}

pub struct FixedSchemaResolver {
    id: u32,
    schema: String,
}
impl FixedSchemaResolver {
    pub fn new(id: u32, schema: Schema) -> Self {
        FixedSchemaResolver {
            id,
            schema: schema.canonical_form(),
        }
    }
}

#[async_trait]
impl SchemaResolver for FixedSchemaResolver {
    async fn resolve_schema(&self, id: u32) -> Result<Option<String>, String> {
        if id == self.id {
            Ok(Some(self.schema.clone()))
        } else {
            Err(format!("Unexpected schema id {}, expected {}", id, self.id))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum ConfluentSchemaType {
    #[default]
    Avro,
    Json,
    Protobuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfluentSchemaResponse {
    pub id: u32,
    pub schema: String,
    #[serde(default)]
    pub schema_type: ConfluentSchemaType,
    pub subject: String,
    pub version: u32,
}

pub struct ConfluentSchemaResolver {
    endpoint: Url,
    topic: String,
    client: Client,
}

impl ConfluentSchemaResolver {
    pub fn new(endpoint: &str, topic: &str) -> anyhow::Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let endpoint: Url = format!("{}/subjects/{}-value/versions/", endpoint, topic)
            .as_str()
            .try_into()
            .map_err(|_| anyhow!("{} is not a valid url", endpoint))?;

        Ok(Self {
            client,
            topic: topic.to_string(),
            endpoint,
        })
    }

    pub async fn get_schema(
        &self,
        version: Option<u32>,
    ) -> anyhow::Result<ConfluentSchemaResponse> {
        let url = self
            .endpoint
            .join(
                &version
                    .map(|v| format!("{}", v))
                    .unwrap_or_else(|| "latest".to_string()),
            )
            .unwrap();

        let resp = self.client.get(url.clone()).send().await.map_err(|e| {
            warn!("Got error response from schema registry: {:?}", e);
            match e.status() {
                Some(StatusCode::NOT_FOUND) => {
                    anyhow!("Could not find value schema for topic '{}'", self.topic)
                }

                Some(code) => anyhow!("Schema registry returned error: {}", code),
                None => {
                    warn!(
                        "Unknown error connecting to schema registry {}: {:?}",
                        self.endpoint, e
                    );
                    anyhow!(
                        "Could not connect to Schema Registry at {}: unknown error",
                        self.endpoint
                    )
                }
            }
        })?;

        if !resp.status().is_success() {
            bail!(
                "Received an error status code from the provided endpoint: {} {}",
                resp.status().as_u16(),
                resp.bytes()
                    .await
                    .map(|bs| String::from_utf8_lossy(&bs).to_string())
                    .unwrap_or_else(|_| "<failed to read body>".to_string())
            );
        }

        resp.json().await.map_err(|e| {
            warn!(
                "Invalid json from schema registry: {:?} for request {:?}",
                e, url
            );
            anyhow!("Schema registry response could not be deserialized: {}", e)
        })
    }
}

#[async_trait]
impl SchemaResolver for ConfluentSchemaResolver {
    async fn resolve_schema(&self, id: u32) -> Result<Option<String>, String> {
        self.get_schema(Some(id))
            .await
            .map(|s| Some(s.schema))
            .map_err(|e| e.to_string())
    }
}
