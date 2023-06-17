use arroyo_rpc::grpc::api::{Connection, CreateConnectionReq, TestSourceMessage};
use arroyo_rpc::grpc::api::{CreateConnectionResp, DeleteConnectionReq, GetConnectorsResp};
use cornucopia_async::GenericClient;
use tokio::sync::mpsc::channel;
use tonic::Status;

use crate::connectors::connector_for_type;
use crate::handle_delete;
use crate::queries::api_queries;
use crate::queries::api_queries::DbConnection;
use crate::{handle_db_error, log_and_map, required_field, AuthData};

pub(crate) async fn create_connection(
    req: CreateConnectionReq,
    auth: AuthData,
    client: &impl GenericClient,
) -> Result<CreateConnectionResp, Status> {
    {
        let connector = connector_for_type(&req.connector).ok_or_else(|| {
            Status::invalid_argument(format!("Unknown connection type '{}'", req.connector))
        })?;

        (*connector)
            .validate_schema(&req.config)
            .map_err(|e| Status::invalid_argument(&format!("Failed to parse config: {:?}", e)))?;
    }

    let config: serde_json::Value = serde_json::from_str(&req.config).unwrap();

    let id = api_queries::create_connection()
        .bind(
            client,
            &auth.organization_id,
            &auth.user_id,
            &req.name,
            &req.connector,
            &serde_json::to_value(&config).unwrap(),
        )
        .one()
        .await
        .map_err(|e| handle_db_error("connection", e))?;

    Ok(CreateConnectionResp {
        connection: Some(Connection {
            id: id.to_string(),
            name: req.name,
            connector: req.connector,
            config: serde_json::to_string(&config).unwrap(),
        }),
    })
}

impl From<DbConnection> for Connection {
    fn from(val: DbConnection) -> Self {
        Connection {
            id: val.id.to_string(),
            name: val.name,
            connector: val.r#type,
            config: serde_json::to_string(&val.config).unwrap(),
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
    // let connection = req
    //     .connection_type
    //     .ok_or_else(|| required_field("connection_type"))?;

    // let (tx, _rx) = channel(8);

    // match connection {
    //     ReqConnectionType::Kafka(kafka) => Ok(KafkaTester::new(kafka, None, None, tx)
    //         .test_connection()
    //         .await),
    //     ReqConnectionType::Http(http) => Ok((HttpTester { connection: &http }).test().await),
    //     _ => Ok(TestSourceMessage {
    //         error: false,
    //         done: true,
    //         message: "Testing not yet supported for this connection type".to_string(),
    //     }),
    // }
    todo!()
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
