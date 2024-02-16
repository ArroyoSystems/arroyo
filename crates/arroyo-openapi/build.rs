use arroyo_api::ApiDoc;
use std::fs;
use std::process::Command;
use utoipa::OpenApi;

fn main() {
    // Generate the OpenAPI spec

    let doc = ApiDoc::openapi().to_pretty_json().unwrap();
    fs::write("./api-spec.json", doc).unwrap();

    // Generate the API client

    let output = Command::new("openapi-generator-cli")
        .arg("generate")
        .arg("-g")
        .arg("rust")
        .arg("-i")
        .arg("api-spec.json")
        .arg("-o")
        .arg("client")
        .output()
        .expect("Failed to run OpenAPI Generator, have you installed openapi-generator-cli?");

    if !output.status.success() {
        let error_message = String::from_utf8_lossy(&output.stderr);
        panic!("OpenAPI Generator failed with error: {}", error_message);
    }
}
