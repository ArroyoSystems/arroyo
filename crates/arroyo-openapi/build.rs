use arroyo_api::ApiDoc;
use progenitor::{GenerationSettings, InterfaceStyle};
use std::path::PathBuf;
use std::{env, fs};
use utoipa::OpenApi;

fn main() {
    // Generate the OpenAPI spec
    let api_spec = "../../target/api-spec.json";

    let doc = ApiDoc::openapi().to_pretty_json().unwrap();
    fs::write(&api_spec, &doc).unwrap();

    println!("cargo:rerun-if-changed={}", api_spec);

    let spec = serde_json::from_str(&doc).unwrap();
    let mut settings = GenerationSettings::new();
    settings.with_interface(InterfaceStyle::Builder);
    let mut generator = progenitor::Generator::new(&settings);

    let tokens = generator.generate_tokens(&spec).unwrap();
    let ast = syn::parse2(tokens).unwrap();
    let content = prettyplease::unparse(&ast);

    let generated = PathBuf::from(env::var("OUT_DIR").unwrap()).join("generated");
    fs::create_dir_all(&generated).unwrap();
    fs::write(generated.join("api-client.rs"), content).unwrap();
}
