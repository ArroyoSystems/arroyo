use crate::{
    AuthData, DEFAULT_ORG, OrgMetadata,
    rest_utils::{AuthRequest, ErrorResp},
};
use axum::async_trait;
use axum::extract::{FromRequestParts, Path};
use axum::http::request::Parts;
use axum_extra::TypedHeader;
use axum_extra::headers::Authorization as HeaderAuthorization;
use axum_extra::headers::authorization::Bearer;
use cornucopia_async::Database;
use std::collections::HashMap;
use std::convert::Infallible;

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct ApiAuthorization {
    bearer: Option<TypedHeader<HeaderAuthorization<Bearer>>>,
    impersonated_organization_id: Option<String>,
}

pub type BearerAuth = ApiAuthorization;

#[async_trait]
impl<S> FromRequestParts<S> for ApiAuthorization
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let bearer =
            Option::<TypedHeader<HeaderAuthorization<Bearer>>>::from_request_parts(parts, state)
                .await
                .unwrap_or(None);
        let impersonated_organization_id =
            Path::<HashMap<String, String>>::from_request_parts(parts, state)
                .await
                .ok()
                .and_then(|Path(params)| params.get("organization_id").cloned());

        Ok(Self {
            bearer,
            impersonated_organization_id,
        })
    }
}

impl AuthRequest for ApiAuthorization {
    fn impersonated_organization_id(&self) -> Option<&str> {
        self.impersonated_organization_id.as_deref()
    }
}

pub(crate) async fn authenticate(
    _client: &Database<'_>,
    _bearer_auth: BearerAuth,
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
