use arroyo_rpc::grpc::{self, api::ConnectionSchema};

use super::Connector;

pub struct SSEConnector {}

impl Connector for SSEConnector {
    type ConfigT = ();

    type TableT = ();

    fn name(&self) -> &'static str {
        "sse"
    }

    fn metadata(&self) -> grpc::api::Connector {
        grpc::api::Connector {
            id: "sse".to_string(),
            name: "Server-Sent Events".to_string(),
            icon: "FaGlobeAmericas".to_string(),
            description: "Connect to a SSE/EventSource server".to_string(),
            enabled: true,
            source: true,
            sink: false,
            custom_schemas: true,
            connection_config: None,
            table_config: "{}".to_string(),
        }
    }

    fn register(
        &self,
        name: &str,
        config: Self::ConfigT,
        table: Self::TableT,
        schema: Option<ConnectionSchema>,
        schema_provider: &mut arroyo_sql::ArroyoSchemaProvider,
    ) {
        todo!()
    }
}

// pub struct HttpTester<'a> {
//     pub connection: &'a HttpConnection,
// }

// impl<'a> HttpTester<'a> {
//     pub async fn test(&self) -> TestSourceMessage {
//         match self.test_internal().await {
//             Ok(_) => TestSourceMessage {
//                 error: false,
//                 done: true,
//                 message: "HTTP endpoint is valid".to_string(),
//             },
//             Err(e) => TestSourceMessage {
//                 error: true,
//                 done: true,
//                 message: e,
//             },
//         }
//     }

//     async fn test_internal(&self) -> Result<(), String> {
//         let headers = string_to_map(&self.connection.headers)
//             .ok_or_else(|| "Headers are invalid; should be comma-separated pairs".to_string())?;

//         let mut header_map = HeaderMap::new();

//         for (k, v) in headers {
//             header_map.append(
//                 HeaderName::from_str(&k).map_err(|s| format!("Invalid header name: {:?}", s))?,
//                 HeaderValue::from_str(&v).map_err(|s| format!("Invalid header value: {:?}", s))?,
//             );
//         }

//         let client = reqwest::Client::builder()
//             .default_headers(header_map)
//             .build()
//             .unwrap();

//         let req = client
//             .head(&self.connection.url)
//             .build()
//             .map_err(|e| format!("Invalid URL: {:?}", e))?;

//         client
//             .execute(req)
//             .await
//             .map_err(|e| format!("HEAD request failed with: {:?}", e.status()))?;

//         Ok(())
//     }
// }
