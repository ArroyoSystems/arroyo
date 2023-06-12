use arroyo_rpc::grpc::api::TestSourceMessage;
use http::{HeaderMap, HeaderName, HeaderValue};

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
