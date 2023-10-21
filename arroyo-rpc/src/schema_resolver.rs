use std::time::Duration;
use anyhow::{anyhow, bail};
use arroyo_types::UserError;
use reqwest::{Client, StatusCode, Url};
use serde_json::Value;
use log::warn;
use serde::{Deserialize, Serialize};

pub trait SchemaResolver {
    fn resolve_schema(&self, id: [u8; 4]) -> Result<Option<String>, UserError>;
}

pub struct FailingSchemaResolver {
}

impl SchemaResolver for FailingSchemaResolver {
    fn resolve_schema(&self, id: [u8; 4]) -> Result<Option<String>, UserError> {
        Err(UserError {
            name: "Could not deserialize".to_string(),
            details: format!("Schema with id {:?} not available, and no schema registry configured", id),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum ConfluentSchemaType {
    Avro,
    Json,
    Protobuf
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfluentSchemaResponse {
    pub id: u32,
    pub schema: String,
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
            .build().unwrap();


        let endpoint: Url =
            format!("{}/subjects/{}-value/versions/", endpoint, topic).as_str().try_into()
            .map_err(|e| anyhow!("{} is not a valid url", endpoint))?;

        Ok(Self {
            client,
            topic: topic.to_string(),
            endpoint,
        })
    }


    pub async fn get_schema(&self, version: Option<u32>) -> anyhow::Result<ConfluentSchemaResponse> {
        let url = self.endpoint.join(
            &version.map(|v| format!("{}", v)).unwrap_or_else(|| "latest".to_string())).unwrap();

        let resp = reqwest::get(url).await.map_err(|e| {
            warn!("Got error response from schema registry: {:?}", e);
            match e.status() {
                Some(StatusCode::NOT_FOUND) => anyhow!(
                    "Could not find value schema for topic '{}'",
                    self.topic),

                Some(code) => anyhow!("Schema registry returned error: {}", code),
                None => {
                    warn!("Unknown error connecting to schema registry {}: {:?}",
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
            warn!("Invalid json from schema registry: {:?}", e);
            anyhow!(
                "Schema registry response could not be deserialied: {}", e
            )
        })
    }
}