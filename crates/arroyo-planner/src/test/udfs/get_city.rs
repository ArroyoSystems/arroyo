/*
[dependencies]
reqwest = { version = "0.11.23", features = ["json"] }
serde_json = "1" 
*/

use arroyo_udf_plugin::udf;
use reqwest::Client;

#[udf(allowed_in_flight=100000, timeout="180s")]
pub async fn get_city(ip: String) -> Option<String> {
    use std::sync::OnceLock;
    static CLIENT: OnceLock<Client> = OnceLock::new();
    let client = CLIENT.get_or_init(|| {
        Client::new()
    });


    let body: serde_json::Value =
        client.get(
            format!("http://localhost:6006/{ip}"))
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;

    body.pointer("/names/en").and_then(|t|
        t.as_str()
    ).map(|t| t.to_string())
}