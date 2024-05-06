// build.rs generates the OpenAPI spec
#![allow(non_camel_case_types)]

include!(concat!(env!("OUT_DIR"), "/generated/api-client.rs"));
