use crate::rest_utils::ErrorResp;
use arroyo_connectors::connectors;
use arroyo_rpc::types::ConnectorCollection;
use axum::Json;

/// List all connectors
#[utoipa::path(
    get,
    path = "/v1/connectors",
    tag = "connectors",
    responses(
        (status = 200, description = "Got connectors collection", body = ConnectorCollection),
    ),
)]
pub async fn get_connectors() -> Result<Json<ConnectorCollection>, ErrorResp> {
    let mut connectors: Vec<_> = connectors()
        .values()
        .map(|c| c.metadata())
        .filter(|metadata| !metadata.hidden)
        .collect();

    connectors.sort_by_cached_key(|c| c.name.clone());
    Ok(Json(ConnectorCollection { data: connectors }))
}
