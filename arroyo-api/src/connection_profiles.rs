use axum::extract::State;
use axum::Json;
use axum_extra::extract::WithRejection;

use arroyo_connectors::connector_for_type;
use tracing::warn;

use arroyo_rpc::public_ids::{generate_id, IdTypes};
use arroyo_rpc::types::{ConnectionProfile, ConnectionProfileCollection, ConnectionProfilePost};

use crate::handle_db_error;
use crate::queries::api_queries;
use crate::queries::api_queries::DbConnectionProfile;
use crate::rest::AppState;
use crate::rest_utils::{
    authenticate, bad_request, client, log_and_map, ApiError, BearerAuth, ErrorResp,
};

impl TryFrom<DbConnectionProfile> for ConnectionProfile {
    type Error = String;

    fn try_from(val: DbConnectionProfile) -> Result<Self, String> {
        let connector = connector_for_type(&val.r#type).ok_or_else(|| {
            format!(
                "Connection profile '{}' has unknown type '{}'",
                val.name, val.r#type
            )
        })?;

        let description = (*connector)
            .config_description(&val.config)
            .map_err(|e| format!("Failed to parse config: {:?}", e))?;

        Ok(ConnectionProfile {
            id: val.pub_id,
            name: val.name,
            connector: val.r#type,
            config: val.config,
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

    connector_for_type(&req.connector)
        .ok_or_else(|| bad_request("Unknown connector type".to_string()))?
        .validate_config(&req.config)
        .map_err(|e| bad_request(format!("Invalid config: {:?}", e)))?;

    let pub_id = generate_id(IdTypes::ConnectionProfile);
    api_queries::create_connection_profile()
        .bind(
            &client,
            &pub_id,
            &auth_data.organization_id,
            &auth_data.user_id,
            &req.name,
            &req.connector,
            &req.config,
        )
        .one()
        .await
        .map_err(|e| handle_db_error("connection_profile", e))?;

    let connection_profile = api_queries::get_connection_profile_by_pub_id()
        .bind(&client, &auth_data.organization_id, &pub_id)
        .one()
        .await
        .map_err(log_and_map)?
        .try_into()
        .map_err(log_and_map)?;

    Ok(Json(connection_profile))
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

    let res: Vec<DbConnectionProfile> = api_queries::get_connection_profiles()
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
                    warn!("Invalid connection profile {}: {}", id, e);
                    None
                }
            }
        })
        .collect();

    Ok(Json(ConnectionProfileCollection { data }))
}
