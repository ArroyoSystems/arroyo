use axum::extract::{Path, State};
use axum::Json;
use axum_extra::extract::WithRejection;
use std::collections::BTreeMap;

use arroyo_connectors::connector_for_type;
use arroyo_rpc::api_types::connections::{
    ConnectionAutocompleteResp, ConnectionProfile, ConnectionProfilePost, TestSourceMessage,
};
use arroyo_rpc::api_types::ConnectionProfileCollection;
use tracing::warn;

use arroyo_rpc::public_ids::{generate_id, IdTypes};

use crate::queries::api_queries;
use crate::queries::api_queries::DbConnectionProfile;
use crate::rest::AppState;
use crate::rest_utils::{
    authenticate, bad_request, log_and_map, map_delete_err, not_found, ApiError, BearerAuth,
    ErrorResp,
};
use crate::AuthData;
use cornucopia_async::Database;

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

/// Test connection profile
#[utoipa::path(
    post,
    path = "/v1/connection_profiles/test",
    tag = "connection_profiles",
    request_body = ConnectionProfilePost,
    responses(
        (status = 200, description = "Result of testing connection profile", body = TestSourceMessage),
    ),
)]
pub async fn test_connection_profile(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    WithRejection(Json(req), _): WithRejection<Json<ConnectionProfilePost>, ApiError>,
) -> Result<Json<TestSourceMessage>, ErrorResp> {
    let _auth_data = authenticate(&state.database, bearer_auth).await.unwrap();

    let connector = connector_for_type(&req.connector)
        .ok_or_else(|| bad_request("Unknown connector type".to_string()))?;

    let Some(rx) = connector
        .test_profile(&req.config)
        .map_err(|e| bad_request(format!("Invalid config: {:?}", e)))?
    else {
        return Ok(Json(TestSourceMessage::done(
            "This connector does not support testing",
        )));
    };

    let result = rx.await.map_err(log_and_map)?;

    Ok(Json(result))
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
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    connector_for_type(&req.connector)
        .ok_or_else(|| bad_request("Unknown connector type".to_string()))?
        .validate_config(&req.config)
        .map_err(|e| bad_request(format!("Invalid config: {:?}", e)))?;

    let pub_id = generate_id(IdTypes::ConnectionProfile);
    api_queries::execute_create_connection_profile(
        &state.database.client().await?,
        &pub_id,
        &auth_data.organization_id,
        &auth_data.user_id,
        &req.name,
        &req.connector,
        &req.config,
    )
    .await?;

    let connection_profile = api_queries::fetch_get_connection_profile_by_pub_id(
        &state.database.client().await?,
        &auth_data.organization_id,
        &pub_id,
    )
    .await?
    .into_iter()
    .next()
    .unwrap()
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
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    let data = get_all_connection_profiles(&auth_data, &state.database.client().await?).await?;

    Ok(Json(ConnectionProfileCollection { data }))
}

/// Delete a Connection Profile
#[utoipa::path(
    delete,
    path = "/v1/connection_profiles/{id}",
    tag = "connection_profiles",
    params(
       ("id" = String, Path, description = "Connection Profile id")
    ),
    responses(
       (status = 200, description = "Deleted connection profile"),
    ),
)]
pub(crate) async fn delete_connection_profile(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path(pub_id): Path<String>,
) -> Result<(), ErrorResp> {
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    let deleted = api_queries::execute_delete_connection_profile(
        &state.database.client().await?,
        &auth_data.organization_id,
        &pub_id,
    )
    .await
    .map_err(|e| map_delete_err("connection_profile", "connection tables", e))?;

    if deleted == 0 {
        return Err(not_found("Connection profile"));
    }

    Ok(())
}

/// Get autocomplete suggestions for a connection profile
#[utoipa::path(
    get,
    path = "/v1/connection_profiles/{id}/autocomplete",
    tag = "connection_profiles",
    params(
       ("id" = String, Path, description = "Connection Profile id")
    ),
    responses(
       (status = 200, description = "Autocomplete suggestions for connection profile", body = ConnectionAutocompleteResp),
    ),
)]
pub(crate) async fn get_connection_profile_autocomplete(
    State(state): State<AppState>,
    bearer_auth: BearerAuth,
    Path(pub_id): Path<String>,
) -> Result<Json<ConnectionAutocompleteResp>, ErrorResp> {
    let auth_data = authenticate(&state.database, bearer_auth).await?;

    let connection_profile = api_queries::fetch_get_connection_profile_by_pub_id(
        &state.database.client().await?,
        &auth_data.organization_id,
        &pub_id,
    )
    .await?
    .into_iter()
    .next()
    .ok_or_else(|| not_found("Connection profile"))?;

    let connector = connector_for_type(&connection_profile.r#type).unwrap();

    let result = connector
        .get_autocomplete(&connection_profile.config)
        .unwrap()
        .await
        .map_err(log_and_map)?
        .map_err(|e| bad_request(format!("Failed to get autocomplete suggestions: {}", e)))?;

    Ok(Json(ConnectionAutocompleteResp {
        values: BTreeMap::from_iter(result.into_iter()),
    }))
}

pub(crate) async fn get_all_connection_profiles<'a>(
    auth: &AuthData,
    db: &Database<'a>,
) -> Result<Vec<ConnectionProfile>, ErrorResp> {
    let res: Vec<DbConnectionProfile> =
        api_queries::fetch_get_connection_profiles(db, &auth.organization_id).await?;

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

    Ok(data)
}
