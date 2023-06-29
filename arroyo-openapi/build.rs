use arroyo_api;
use std::fs;
use utoipa::OpenApi;

fn main() {
    let doc = arroyo_api::rest::ApiDoc::openapi()
        .to_pretty_json()
        .unwrap();
    fs::write("./api-spec.json", doc).unwrap();
}
