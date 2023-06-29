use arroyo_rpc::grpc::api::{DeleteConnectionReq, TestSourceMessage};
use cornucopia_async::GenericClient;
use tokio::sync::mpsc::channel;
use tonic::Status;
use arroyo_types::api::{ConnectionTypes, PostConnections};

use crate::queries::api_queries;
use crate::queries::api_queries::DbConnection;
use crate::rest::{log_and_map_rest, ErrorResp};
use crate::testers::HttpTester;
use crate::types::public;
use crate::{handle_db_error_rest, handle_delete, AuthData};
use crate::{log_and_map, testers::KafkaTester};

pub(crate) async fn create_connection(
    req: PostConnections,
    auth: AuthData,
    client: impl GenericClient,
) -> Result<(), ErrorResp> {
    let (typ, v) = match req.config {
        ConnectionTypes::Kafka(c) => (
            public::ConnectionType::kafka,
            serde_json::to_value(c).map_err(log_and_map_rest)?,
        ),
        ConnectionTypes::Http(c) => (
            public::ConnectionType::http,
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

impl From<DbConnection> for PostConnections {
    fn from(val: DbConnection) -> Self {
        PostConnections {
            name: val.name,
            config: match val.r#type {
                public::ConnectionType::kafka => {
                    ConnectionTypes::Kafka(serde_json::from_value(val.config).unwrap())
                }
                public::ConnectionType::kinesis => {panic!()}
                public::ConnectionType::http => {
                    ConnectionTypes::Http(serde_json::from_value(val.config).unwrap())
                }
            },
        }
    }
}

pub(crate) async fn get_connections(
    auth: &AuthData,
    client: &impl GenericClient,
) -> Result<Vec<PostConnections>, ErrorResp> {
    let res = api_queries::get_connections()
        .bind(client, &auth.organization_id)
        .all()
        .await
        .map_err(log_and_map_rest)?;

    Ok(res.into_iter().map(|rec| rec.into()).collect())
}

pub(crate) async fn get_connection<E: GenericClient>(
    auth: &AuthData,
    name: &str,
    client: &E,
) -> Result<PostConnections, Status> {
    api_queries::get_connection()
        .bind(client, &auth.organization_id, &name)
        .opt()
        .await
        .map_err(log_and_map)?
        .ok_or_else(|| Status::not_found(format!("No connection with name '{}'", name)))
        .map(|c| c.into())
}

pub(crate) async fn test_connection(req: PostConnections) -> Result<TestSourceMessage, ErrorResp> {
    let (tx, _rx) = channel(8);

    match req.config {
        ConnectionTypes::Kafka(kafka) =>
            Ok(KafkaTester::new(kafka, None, None, tx)
            .test_connection()
            .await),
        ConnectionTypes::Http(http) => Ok((HttpTester { connection: &http }).test().await),
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
