use axum::{http::StatusCode, Json, Router, routing::post};
use axum::extract::State;
use serde::{Deserialize, Serialize};
use tonic::Request;

use arroyo_rpc::grpc::api::CreateConnectionReq;
use arroyo_rpc::grpc::api::api_grpc_server::ApiGrpc;

use crate::api_server::ApiServer;

#[derive(Debug, Deserialize, Serialize)]
struct User {
    username: String,
    email: String,
}

#[derive(Clone)]
struct AppState {
    api_server: ApiServer
}

async fn create_user(
        State(state): State<AppState>,
        create_connection_req: Json<CreateConnectionReq>)
    -> StatusCode {

    let req = Request::new(create_connection_req.0);
    match state.api_server.create_connection(req).await {
        Ok(_) => {
            StatusCode::CREATED
        }
        Err(err) => {
            println!("err: {}", err);
            StatusCode::BAD_REQUEST
        }
    }
}


pub(crate) fn create_rest_app(api_server: ApiServer) -> Router {
    Router::new()
        .route("/connections", post(create_user))
        .with_state(AppState{api_server})
}
