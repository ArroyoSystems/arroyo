use axum::extract::State;
use axum::Json;
use axum_extra::extract::WithRejection;
use tonic::Status;
use tracing::warn;

use arroyo_connectors::connector_for_type;
use arroyo_rpc::public_ids::{generate_id, IdTypes};
use arroyo_rpc::types::{ConnectionProfile, ConnectionProfileCollection, ConnectionProfilePost};

use crate::queries::api_queries;
use crate::queries::api_queries::DbConnection;
use crate::rest::AppState;
use crate::rest_utils::{authenticate, client, ApiError, BearerAuth, ErrorResp};
use crate::{handle_db_error, log_and_map};

impl TryFrom<DbConnection> for ConnectionProfile {
    type Error = String;

    fn try_from(val: DbConnection) -> Result<Self, String> {
        let connector = connector_for_type(&val.r#type)
            .ok_or_else(|| format!("Unknown connection type '{}'", val.name))?;

        let description = (*connector)
            .config_description(&serde_json::to_string(&val.config).unwrap())
            .map_err(|e| format!("Failed to parse config: {:?}", e))?;

        Ok(ConnectionProfile {
            id: val.pub_id,
            name: val.name,
            connector: val.r#type,
            config: serde_json::to_string(&val.config).unwrap(),
            description,
        })
    }
}

/// Create connection profile
#[utoipa::path(
    post,
    path = "/v1/connection_profiles",
    tag = "connection_profiles",
    request_body = ConnectionProfilePost,
    responses(
        (status = 200, description = "Created connection profile", body = ConnectionProfile),
    ),
)]
pub async fn create_connection_profile(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    WithRejection(Json(req), _): WithRejection<Json<ConnectionProfilePost>, ApiError>,
) -> Result<Json<ConnectionProfile>, ErrorResp> {
    let client = client(&state.pool).await.unwrap();
    let auth_data = authenticate(&state.pool, bearer_auth).await.unwrap();

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

    let pub_id = generate_id(IdTypes::Connection);
    api_queries::create_connection()
        .bind(
            &client,
            &pub_id,
            &auth_data.organization_id,
            &auth_data.user_id,
            &req.name,
            &req.connector,
            &serde_json::to_value(&config).unwrap(),
        )
        .one()
        .await
        .map_err(|e| handle_db_error("connection", e))?;

    // TODO: query db
    Ok(Json(ConnectionProfile {
        id: pub_id,
        name: req.name,
        connector: req.connector,
        config: serde_json::to_string(&config).unwrap(),
        description,
    }))
}

/// List all connection profiles
#[utoipa::path(
    get,
    path = "/v1/connection_profiles",
    tag = "connection_profiles",
    responses(
        (status = 200, description = "Got connections collection", body = ConnectionProfileCollection),
    ),
)]
pub async fn get_connection_profiles(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
) -> Result<Json<ConnectionProfileCollection>, ErrorResp> {
    let client = client(&state.pool).await?;
    let auth_data = authenticate(&state.pool, bearer_auth).await?;

    let res: Vec<DbConnection> = api_queries::get_connections()
        .bind(&client, &auth_data.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;

    let data = res
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
        .collect();

    Ok(Json(ConnectionProfileCollection {
        data,
        has_more: false,
    }))
}
