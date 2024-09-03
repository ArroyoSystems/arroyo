use arroyo_rpc::config::config;
use serde_json::{json, Value};
use std::time::Instant;

pub mod avro;
pub mod json;

pub mod de;
pub mod proto;
pub mod ser;

pub fn should_flush(size: usize, time: Instant) -> bool {
    size > 0
        && (size >= config().pipeline.source_batch_size
            || time.elapsed() >= *config().pipeline.source_batch_linger)
}

pub(crate) fn float_to_json(f: f64) -> Value {
    match serde_json::Number::from_f64(f) {
        Some(n) => Value::Number(n),
        None => Value::String(
            (if f.is_infinite() && f.is_sign_positive() {
                "+Inf"
            } else if f.is_infinite() {
                "-Inf"
            } else {
                "NaN"
            })
            .to_string(),
        ),
    }
}
