use arroyo_api::ApiDoc;
use std::fs;
use utoipa::OpenApi;

fn main() {
    let doc = ApiDoc::openapi().to_pretty_json().unwrap();
    fs::write("./api-spec.json", doc).unwrap();
}
