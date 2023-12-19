use crate::var_str::VarStr;
use anyhow::{anyhow, bail, Context};
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

pub struct ConfluentSchemaRegistryClient {
    endpoint: Url,
    client: Client,
}

impl ConfluentSchemaRegistryClient {
    pub fn new(
        endpoint: &str,
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
            endpoint,
            client: client.build()?,
        })
    }

    async fn get_schema_for_url<T: DeserializeOwned>(&self, url: Url) -> anyhow::Result<Option<T>> {
        let resp = self.client.get(url.clone()).send().await.map_err(|e| {
            warn!("Got error response from schema registry: {:?}", e);
            match e.status() {
                Some(StatusCode::NOT_FOUND) => {
                    anyhow!("schema not found")
                }
                Some(code) => anyhow!("schema registry returned error: {}", code),
                None => {
                    warn!(
                        "unknown error connecting to schema registry {}: {:?}",
                        self.endpoint, e
                    );
                    anyhow!(
                        "could not connect to Schema Registry at {}: unknown error",
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
                "received an error status code from the schema endpoint while fetching {}: {} {}",
                url,
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            );
        }

        resp.json().await.map_err(|e| {
            warn!(
                "invalid json from schema registry: {:?} for request {:?}",
                e, url
            );
            anyhow!("schema registry response could not be deserialized: {}", e)
        })
    }

    async fn write_schema(
        &self,
        url: Url,
        schema: impl Into<String>,
        schema_type: ConfluentSchemaType,
    ) -> anyhow::Result<i32> {
        let req = PostSchemaRequest {
            schema: schema.into(),
            schema_type,
        };

        let resp = self.client.post(url).json(&req).send().await.map_err(|e| {
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
                        "there is already an existing schema for this topic which is \
                    incompatible with the new schema being registered:\n\n{}",
                        body
                    )
                }
                StatusCode::UNPROCESSABLE_ENTITY => {
                    bail!("invalid schema: {}", body);
                }
                StatusCode::UNAUTHORIZED => {
                    bail!("invalid credentials for schema registry");
                }
                StatusCode::NOT_FOUND => {
                    bail!("schema not found; make sure that the topic exists")
                }
                code => {
                    bail!("schema registry returned error {}: {}", code.as_u16(), body);
                }
            }
        }

        let resp: PostSchemaResponse = resp
            .json()
            .await
            .map_err(|e| anyhow!("could not parse response from schema registry: {}", e))?;

        Ok(resp.id)
    }

    pub async fn test(&self) -> anyhow::Result<()> {
        let resp = self
            .client
            .get(
                self.endpoint
                    .join("subjects")
                    .map_err(|_| anyhow!("invalid endpoint"))?,
            )
            .send()
            .await
            .map_err(|e| match e.status() {
                Some(code) => anyhow!("schema registry returned error: {}", code),
                None => {
                    warn!(
                        "unknown error connecting to schema registry {}: {:?}",
                        self.endpoint, e
                    );
                    anyhow!(
                        "could not connect to Schema Registry at {}: unknown error",
                        self.endpoint
                    )
                }
            })?;

        match resp.status() {
            StatusCode::OK => {
                return Ok(());
            }
            StatusCode::NOT_FOUND => {
                bail!("schema registry returned 404 Not Found; check the endpoint is correct")
            }
            StatusCode::UNAUTHORIZED => {
                bail!("schema registry returned 401 Unauthorized: check your credentials")
            }
            code => {
                bail!(
                    "schema registry returned error code {}; verify the endpoint is correct",
                    code
                );
            }
        }
    }
}

pub struct ConfluentSchemaRegistry {
    client: ConfluentSchemaRegistryClient,
    topic: String,
}

impl ConfluentSchemaRegistry {
    pub fn new(
        endpoint: &str,
        topic: &str,
        api_key: Option<VarStr>,
        api_secret: Option<VarStr>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client: ConfluentSchemaRegistryClient::new(endpoint, api_key, api_secret)?,
            topic: topic.to_string(),
        })
    }

    fn topic_endpoint(&self) -> Url {
        self.client
            .endpoint
            .join(&format!("subjects/{}-value/versions/", self.topic))
            .unwrap()
    }

    pub async fn write_schema(
        &self,
        schema: impl Into<String>,
        schema_type: ConfluentSchemaType,
    ) -> anyhow::Result<i32> {
        self.client
            .write_schema(self.topic_endpoint(), schema, schema_type)
            .await
            .context(format!("topic '{}'", self.topic))
    }

    pub async fn get_schema_for_id(
        &self,
        id: u32,
    ) -> anyhow::Result<Option<ConfluentSchemaIdResponse>> {
        let url = self
            .client
            .endpoint
            .join(&format!("/schemas/ids/{}", id))
            .unwrap();

        self.client
            .get_schema_for_url(url)
            .await
            .context(format!("failed to fetch schema for topic '{}'", self.topic))
    }

    pub async fn get_schema_for_version(
        &self,
        version: Option<u32>,
    ) -> anyhow::Result<Option<ConfluentSchemaSubjectResponse>> {
        let version = version
            .map(|v| format!("{}", v))
            .unwrap_or_else(|| "latest".to_string());

        let url = self.topic_endpoint().join(&version).unwrap();

        self.client.get_schema_for_url(url).await.context(format!(
            "failed to fetch schema for topic '{}' with version {}",
            self.topic, version
        ))
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
