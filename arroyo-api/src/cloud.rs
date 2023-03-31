use crate::{AuthData, OrgMetadata};
use cornucopia_async::GenericClient;
use tonic::{Request, Status};

pub(crate) async fn authenticate<T>(
    _client: impl GenericClient,
    request: Request<T>,
) -> Result<(Request<T>, AuthData), Status> {
    Ok((
        request,
        AuthData {
            user_id: "user".to_string(),
            organization_id: "org".to_string(),
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
        },
    ))
}
