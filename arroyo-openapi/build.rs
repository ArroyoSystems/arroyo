use arroyo_api;
use arroyo_api::rest::ApiDoc;
use std::fs;
use utoipa::OpenApi;

fn main() {
    let doc = ApiDoc::openapi().to_pretty_json().unwrap();
    fs::write("./api-spec.json", doc).unwrap();
}
