use crate::var_str::VarStr;
use anyhow::{anyhow, bail};
use apache_avro::Schema;
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::write::EncoderWriter;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::Write;
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
pub struct ConfluentSchemaSubjectResponse {
    pub id: u32,
    pub schema: String,
    #[serde(default)]
    pub schema_type: ConfluentSchemaType,
    pub subject: String,
    pub version: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfluentSchemaIdResponse {
    pub schema: String,
    #[serde(default)]
    pub schema_type: ConfluentSchemaType,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RegistryErrorResponse {
    error_code: i32,
    message: String,
}

pub struct ConfluentSchemaRegistry {
    endpoint: Url,
    topic: String,
    client: Client,
}

impl ConfluentSchemaRegistry {
    pub fn new(
        endpoint: &str,
        topic: &str,
        api_key: Option<VarStr>,
        api_secret: Option<VarStr>,
    ) -> anyhow::Result<Self> {
        let mut client = Client::builder().timeout(Duration::from_secs(5));

        if let Some(api_key) = api_key {
            let mut buf = b"Basic ".to_vec();
            {
                let mut encoder = EncoderWriter::new(&mut buf, &BASE64_STANDARD);
                let _ = write!(encoder, "{}:", api_key.sub_env_vars()?);
                if let Some(password) = api_secret {
                    let _ = write!(encoder, "{}", password.sub_env_vars()?);
                }
            }
            let mut header =
                HeaderValue::from_bytes(&buf).expect("base64 is always valid HeaderValue");
            header.set_sensitive(true);
            let mut headers = HeaderMap::new();
            headers.append(reqwest::header::AUTHORIZATION, header);
            client = client.default_headers(headers);
        };

        let endpoint: Url = endpoint
            .try_into()
            .map_err(|_| anyhow!("{} is not a valid url", endpoint))?;

        Ok(Self {
            client: client.build()?,
            topic: topic.to_string(),
            endpoint,
        })
    }

    fn topic_endpoint(&self) -> Url {
        self.endpoint
            .join(&format!("subjects/{}-value/versions/", self.topic))
            .unwrap()
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

        let resp = self
            .client
            .post(self.topic_endpoint())
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

            let body = body
                .pointer("/message")
                .map(|m| m.to_string())
                .unwrap_or_else(|| body.to_string());

            match status {
                StatusCode::CONFLICT => {
                    bail!(
                        "Failed to register new schema for topic '{}': {}",
                        self.topic,
                        body
                    )
                }
                StatusCode::UNPROCESSABLE_ENTITY => {
                    bail!("Invalid schema for topic '{}': {}", self.topic, body);
                }
                StatusCode::UNAUTHORIZED => {
                    bail!("Invalid credentials for schema registry");
                }
                StatusCode::NOT_FOUND => {
                    bail!("Schema registry returned 404 for topic {}. Make sure that the topic exists.", self.topic)
                }
                code => {
                    bail!("Schema registry returned error {}: {}", code.as_u16(), body);
                }
            }
        }

        let resp: PostSchemaResponse = resp
            .json()
            .await
            .map_err(|e| anyhow!("could not parse response from schema registry: {}", e))?;

        Ok(resp.id)
    }

    pub async fn get_schema_for_id(
        &self,
        id: u32,
    ) -> anyhow::Result<Option<ConfluentSchemaIdResponse>> {
        let url = self.endpoint.join(&format!("/schemas/ids/{}", id)).unwrap();

        self.get_schema_for_url(url).await
    }

    pub async fn get_schema_for_version(
        &self,
        version: Option<u32>,
    ) -> anyhow::Result<Option<ConfluentSchemaSubjectResponse>> {
        let url = self
            .topic_endpoint()
            .join(
                &version
                    .map(|v| format!("{}", v))
                    .unwrap_or_else(|| "latest".to_string()),
            )
            .unwrap();

        self.get_schema_for_url(url).await
    }

    async fn get_schema_for_url<T: DeserializeOwned>(&self, url: Url) -> anyhow::Result<Option<T>> {
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

        let status = resp.status();
        if !status.is_success() {
            let bytes = resp
                .bytes()
                .await
                .map(|b| b.to_vec())
                .unwrap_or_else(|_| "<failed to read body>".to_string().into_bytes());
            let json = serde_json::from_slice::<RegistryErrorResponse>(&bytes);
            if status.as_u16() == 404 && json.is_ok() && json.as_ref().unwrap().error_code == 40401
            {
                // valid response, but schema was not found
                return Ok(None);
            }
            bail!(
                "Received an error status code from the schema endpoint while fetching {}: {} {}",
                url,
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
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
        self.get_schema_for_id(id)
            .await
            .map(|s| s.map(|r| r.schema))
            .map_err(|e| e.to_string())
    }
}
