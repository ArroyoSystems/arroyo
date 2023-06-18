use arroyo_rpc::grpc::api::DeleteConnectionReq;
use arroyo_rpc::grpc::api::{
    connection::ConnectionType, create_connection_req::ConnectionType as ReqConnectionType,
    Connection, CreateConnectionReq, TestSourceMessage,
};
use cornucopia_async::GenericClient;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::channel;
use tonic::Status;
use utoipa::ToSchema;

use crate::queries::api_queries;
use crate::queries::api_queries::DbConnection;
use crate::rest::{log_and_map_rest, ErrorResp};
use crate::testers::HttpTester;
use crate::types::public;
use crate::{handle_db_error_rest, handle_delete, AuthData};
use crate::{log_and_map, required_field, testers::KafkaTester};

#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HttpConnection {
    #[schema(example = "https://mstdn.social/api")]
    url: String,
    #[schema(example = "Content-Type: application/json")]
    headers: String,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SaslAuth {
    protocol: String,
    mechanism: String,
    username: String,
    password: String,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct KafkaAuthConfig {
    sasl_auth: Option<SaslAuth>,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct KafkaConnection {
    bootstrap_servers: String,
    auth_config: KafkaAuthConfig,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ConnectionTypes {
    Http(HttpConnection),
    Kafka(KafkaConnection),
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PostConnections {
    #[schema(example = "mstdn")]
    name: String,
    config: ConnectionTypes,
}

pub(crate) async fn create_connection(
    req: PostConnections,
    auth: AuthData,
    client: impl GenericClient,
) -> Result<(), ErrorResp> {
    let (typ, v) = match req.config {
        ConnectionTypes::Http(c) => (
            public::ConnectionType::http,
            serde_json::to_value(c).map_err(log_and_map_rest)?,
        ),
        ConnectionTypes::Kafka(c) => (
            public::ConnectionType::kafka,
            serde_json::to_value(c).map_err(log_and_map_rest)?,
        ),
    };

    api_queries::create_connection()
        .bind(
            &client,
            &auth.organization_id,
            &auth.user_id,
            &req.name,
            &typ,
            &v,
        )
        .await
        .map_err(|e| handle_db_error_rest("connection", e))?;

    Ok(())
}

impl From<DbConnection> for Connection {
    fn from(val: DbConnection) -> Self {
        Connection {
            name: val.name,
            connection_type: Some(match val.r#type {
                public::ConnectionType::kafka => {
                    ConnectionType::Kafka(serde_json::from_value(val.config).unwrap())
                }
                public::ConnectionType::kinesis => {
                    ConnectionType::Kinesis(serde_json::from_value(val.config).unwrap())
                }
                public::ConnectionType::http => {
                    ConnectionType::Http(serde_json::from_value(val.config).unwrap())
                }
            }),
            sources: val.source_count as i32,
            sinks: val.sink_count as i32,
        }
    }
}

pub(crate) async fn get_connections(
    auth: &AuthData,
    client: &impl GenericClient,
) -> Result<Vec<Connection>, Status> {
    let res = api_queries::get_connections()
        .bind(client, &auth.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;

    Ok(res.into_iter().map(|rec| rec.into()).collect())
}

pub(crate) async fn get_connection<E: GenericClient>(
    auth: &AuthData,
    name: &str,
    client: &E,
) -> Result<Connection, Status> {
    api_queries::get_connection()
        .bind(client, &auth.organization_id, &name)
        .opt()
        .await
        .map_err(log_and_map)?
        .ok_or_else(|| Status::not_found(format!("No connection with name '{}'", name)))
        .map(|c| c.into())
}

pub(crate) async fn test_connection(req: CreateConnectionReq) -> Result<TestSourceMessage, Status> {
    let connection = req
        .connection_type
        .ok_or_else(|| required_field("connection_type"))?;

    let (tx, _rx) = channel(8);

    match connection {
        ReqConnectionType::Kafka(kafka) => Ok(KafkaTester::new(kafka, None, None, tx)
            .test_connection()
            .await),
        ReqConnectionType::Http(http) => Ok((HttpTester { connection: &http }).test().await),
        _ => Ok(TestSourceMessage {
            error: false,
            done: true,
            message: "Testing not yet supported for this connection type".to_string(),
        }),
    }
}

pub(crate) async fn delete_connection(
    req: DeleteConnectionReq,
    auth: AuthData,
    client: &impl GenericClient,
) -> Result<(), Status> {
    let deleted = api_queries::delete_connection()
        .bind(client, &auth.organization_id, &req.name)
        .await
        .map_err(|e| handle_delete("connection", "sources or sinks", e))?;

    if deleted == 0 {
        return Err(Status::not_found(format!(
            "Not connection with name {}",
            req.name
        )));
    }

    Ok(())
}
