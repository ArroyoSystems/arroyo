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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostSchemaRequest {
    pub schema: String,
    pub schema_type: ConfluentSchemaType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostSchemaResponse {
    pub id: i32,
}

pub struct ConfluentSchemaRegistry {
    endpoint: Url,
    topic: String,
    client: Client,
    api_key: Option<String>,
    api_secret: Option<String>,
}

impl ConfluentSchemaRegistry {
    pub fn new(
        endpoint: &str,
        topic: &str,
        api_key: Option<String>,
        api_secret: Option<String>,
    ) -> anyhow::Result<Self> {
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
            api_key,
            api_secret,
        })
    }

    pub async fn write_schema(
        &self,
        schema: impl Into<String>,
        schema_type: ConfluentSchemaType,
    ) -> anyhow::Result<i32> {
        let req = PostSchemaRequest {
            schema: schema.into(),
            schema_type,
        };

        let resp = self.client.post(self.endpoint.clone())
            .json(&req)
            .send()
            .await
            .map_err(|e| {
                warn!("Got error response writing to schema registry: {:?}", e);
                anyhow!(
                    "Could not connect to Schema Registry at {}: unknown error",
                    self.endpoint
                )
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body: serde_json::Value = resp.json().await.unwrap_or_default();

            let body = body.pointer("/message")
                .map(|m| m.to_string())
                .unwrap_or_else(|| body.to_string());

            match status {
                StatusCode::CONFLICT => {
                    bail!("Failed to register new schema for topic '{}': {}", self.topic, body)
                }
                StatusCode::UNPROCESSABLE_ENTITY => {
                    bail!("Invalid schema for topic '{}': {}", self.topic, body);
                }
                StatusCode::UNAUTHORIZED => {
                    bail!("Invalid credentials for schema registry");
                }
                code => {
                    bail!("Schema registry returned error {}: {}", code.as_u16(), body);
                }
            }
        }

        let resp: PostSchemaResponse = resp.json()
            .await
            .map_err(|e| anyhow!("could not parse response from schema registry: {}", e))?;

        Ok(resp.id)
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

        let mut get_call = self.client.get(url.clone());

        if let Some(api_key) = self.api_key.as_ref() {
            get_call = get_call.basic_auth(api_key, self.api_secret.as_ref());
        }

        let resp = get_call.send().await.map_err(|e| {
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
impl SchemaResolver for ConfluentSchemaRegistry {
    async fn resolve_schema(&self, id: u32) -> Result<Option<String>, String> {
        self.get_schema(Some(id))
            .await
            .map(|s| Some(s.schema))
            .map_err(|e| e.to_string())
    }
}
