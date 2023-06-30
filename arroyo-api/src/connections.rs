use arroyo_connectors::connector_for_type;
use arroyo_rpc::grpc::api::{Connection, CreateConnectionReq};
use arroyo_rpc::grpc::api::{CreateConnectionResp, DeleteConnectionReq};
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use cornucopia_async::GenericClient;
use tonic::Status;
use tracing::warn;

use crate::handle_delete;
use crate::queries::api_queries;
use crate::queries::api_queries::DbConnection;
use crate::{handle_db_error, log_and_map, AuthData};

pub(crate) async fn create_connection(
    req: CreateConnectionReq,
    auth: AuthData,
    client: &impl GenericClient,
) -> Result<CreateConnectionResp, Status> {
    let description = {
        let connector = connector_for_type(&req.connector).ok_or_else(|| {
            Status::invalid_argument(format!("Unknown connection type '{}'", req.connector))
        })?;

        (*connector)
            .validate_config(&req.config)
            .map_err(|e| Status::invalid_argument(&format!("Failed to parse config: {:?}", e)))?;

        (*connector)
            .config_description(&req.config)
            .map_err(|e| Status::invalid_argument(&format!("Failed to parse config: {:?}", e)))?
    };

    let config: serde_json::Value = serde_json::from_str(&req.config).unwrap();

    let id = api_queries::create_connection()
        .bind(
            client,
            &generate_id(IdTypes::Connection),
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
            description,
        }),
    })
}

impl TryFrom<DbConnection> for Connection {
    type Error = String;

    fn try_from(val: DbConnection) -> Result<Self, String> {
        let connector = connector_for_type(&val.r#type)
            .ok_or_else(|| format!("Unknown connection type '{}'", val.name))?;

        let description = (*connector)
            .config_description(&serde_json::to_string(&val.config).unwrap())
            .map_err(|e| format!("Failed to parse config: {:?}", e))?;

        Ok(Connection {
            id: val.id.to_string(),
            name: val.name,
            connector: val.r#type,
            config: serde_json::to_string(&val.config).unwrap(),
            description,
        })
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

    Ok(res
        .into_iter()
        .filter_map(|rec| {
            let id = rec.id;
            match rec.try_into() {
                Ok(c) => Some(c),
                Err(e) => {
                    warn!("Invalid connection {}: {}", id, e);
                    None
                }
            }
        })
        .collect())
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
            "No connection with name {}",
            req.name
        )));
    }

    Ok(())
}
