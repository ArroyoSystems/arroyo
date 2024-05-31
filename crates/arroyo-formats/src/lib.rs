extern crate core;

use arroyo_rpc::config::config;
use serde_json::json;
use std::time::Instant;

pub mod avro;
pub mod json;

pub mod de;
pub mod ser;

pub fn should_flush(size: usize, time: Instant) -> bool {
    size > 0
        && (size >= config().pipeline.source_batch_size
            || time.elapsed() >= *config().pipeline.source_batch_linger)
}
