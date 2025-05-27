use crate::{rest_utils::ErrorResp, AuthData, OrgMetadata, DEFAULT_ORG};
use axum_extra::headers::authorization::Bearer;
use axum_extra::headers::Authorization;
use axum_extra::TypedHeader;
use cornucopia_async::Database;

pub(crate) async fn authenticate(
    _client: &Database<'_>,
    _bearer_auth: Option<TypedHeader<Authorization<Bearer>>>,
) -> Result<AuthData, ErrorResp> {
    Ok(AuthData {
        user_id: "user".to_string(),
        organization_id: DEFAULT_ORG.to_string(),
        role: "admin".to_string(),
        org_metadata: OrgMetadata {
            can_create_programs: true,
            max_nexmark_qps: f64::MAX,
            max_impulse_qps: f64::MAX,
            max_parallelism: u32::MAX,
            max_operators: u32::MAX,
            max_running_jobs: u32::MAX,
            kafka_qps: u32::MAX,
        },
    })
}
