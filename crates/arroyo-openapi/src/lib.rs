// build.rs generates the OpenAPI spec
#![allow(non_camel_case_types, mismatched_lifetime_syntaxes)]

include!(concat!(env!("OUT_DIR"), "/generated/api-client.rs"));
