//! Identity validation for worker-targeted gRPC requests.
//!
//! Every request addressed to a worker carries `x-target-worker-id` in
//! metadata; the receiving server rejects mismatches with `PermissionDenied`.
//! This guards against stale channels delivering to the wrong worker when
//! network endpoints are recycled across worker lifetimes. Worker IDs are
//! 64-bit random values in production, so they are effectively globally
//! unique across jobs, runs, and clusters.

use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Request, Status};

use crate::grpc::rpc::worker_grpc_client::WorkerGrpcClient;

/// Target worker_id header, ASCII decimal.
pub const WORKER_ID_HEADER: &str = "x-target-worker-id";

/// Worker client with identity interceptor attached.
pub type WorkerClient = WorkerGrpcClient<InterceptedService<Channel, InjectWorkerId>>;

/// Construct a worker client that injects `target` as `x-target-worker-id`.
pub fn worker_client(channel: Channel, target: u64) -> WorkerClient {
    WorkerGrpcClient::with_interceptor(channel, InjectWorkerId::new(target))
}

/// Client interceptor: injects target worker_id into request metadata.
#[derive(Clone, Debug)]
pub struct InjectWorkerId(MetadataValue<Ascii>);

impl InjectWorkerId {
    pub fn new(target: u64) -> Self {
        // u64 decimal is always ASCII; parse cannot fail.
        Self(target.to_string().parse().unwrap())
    }
}

impl Interceptor for InjectWorkerId {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        // MetadataValue clone is a Bytes refcount bump, not an allocation.
        req.metadata_mut().insert(WORKER_ID_HEADER, self.0.clone());
        Ok(req)
    }
}

/// Server interceptor: rejects requests whose `x-target-worker-id` header
/// does not match the server's own worker_id.
#[derive(Clone, Debug)]
pub struct VerifyWorkerId(pub u64);

impl Interceptor for VerifyWorkerId {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let received: u64 = req
            .metadata()
            .get(WORKER_ID_HEADER)
            .ok_or_else(|| Status::permission_denied("missing x-target-worker-id header"))?
            .to_str()
            .map_err(|_| Status::permission_denied("x-target-worker-id is not ASCII"))?
            .parse()
            .map_err(|_| Status::permission_denied("x-target-worker-id is not a valid u64"))?;

        if received != self.0 {
            return Err(Status::permission_denied(format!(
                "request targets worker {received}, but this worker is {}",
                self.0
            )));
        }

        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn inject_writes_decimal_metadata() {
        for id in [0, 42, 1234567890, u64::MAX] {
            let req = InjectWorkerId::new(id).call(Request::new(())).unwrap();
            let parsed: u64 = req
                .metadata()
                .get(WORKER_ID_HEADER)
                .unwrap()
                .to_str()
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(parsed, id);
        }
    }

    #[test]
    fn verify_behavior() {
        // (expected_own_id, incoming_header, should_pass)
        let cases: &[(u64, Option<&str>, bool)] = &[
            (0, Some("0"), true),
            (42, Some("42"), true),
            (42, Some("43"), false),
            (42, None, false),
            (42, Some("not-a-number"), false),
            (42, Some("-1"), false),
        ];

        for &(expected, header, should_pass) in cases {
            let mut req = Request::new(());
            if let Some(h) = header {
                req.metadata_mut()
                    .insert(WORKER_ID_HEADER, h.parse().unwrap());
            }
            let result = VerifyWorkerId(expected).call(req);
            assert_eq!(
                result.is_ok(),
                should_pass,
                "case: expected={expected}, header={header:?}"
            );
            if let Err(s) = result {
                assert_eq!(s.code(), Code::PermissionDenied);
            }
        }
    }

    #[test]
    fn round_trip_through_both_interceptors() {
        let req = InjectWorkerId::new(1234567890)
            .call(Request::new(()))
            .unwrap();
        VerifyWorkerId(1234567890).call(req).unwrap();
    }
}
