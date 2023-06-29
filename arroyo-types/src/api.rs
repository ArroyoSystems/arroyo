use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HttpConnection {
    #[schema(example = "https://mstdn.social/api")]
    pub url: String,
    #[schema(example = "Content-Type: application/json")]
    pub headers: String,
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SaslAuth {
    pub protocol: String,
    pub mechanism: String,
    pub username: String,
    pub password: String,
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct KafkaAuthConfig {
    pub sasl_auth: Option<SaslAuth>,
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConnection {
    pub bootstrap_servers: String,
    pub auth_config: KafkaAuthConfig,
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionTypes {
    Http(HttpConnection),
    Kafka(KafkaConnection),
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PostConnections {
    #[schema(example = "mstdn")]
    pub name: String,
    pub config: ConnectionTypes,
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionCollection  {
    pub items: Vec<PostConnections>
}


