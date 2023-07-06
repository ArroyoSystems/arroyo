use axum::body::BoxBody;
use http::{Request, Response, StatusCode};
use tower_http::validate_request::ValidateRequest;

#[derive(Clone)]
pub(crate) struct RestAuth;

impl<B> ValidateRequest<B> for RestAuth {
    type ResponseBody = BoxBody;

    fn validate(&mut self, request: &mut Request<B>) -> Result<(), Response<Self::ResponseBody>> {
        // let Ok(unauthorized_response) = Response::builder()
        //     .status(StatusCode::UNAUTHORIZED)
        //     .body(Default::default()) else {
        //     tracing::error!("Couldn't create unauthorized_response");
        //     return Ok(())
        // };

        Ok(())
    }
}
