use arroyo_rpc::config::config;
use serde_json::{Value, json};
use std::time::Instant;

pub mod avro;
pub mod json;

pub mod de;
pub mod proto;
pub mod ser;

pub fn should_flush(buffered_count: usize, buffered_bytes: usize, time: Instant) -> bool {
    if buffered_count == 0 {
        return false;
    }
    let cfg = &config().pipeline;
    buffered_count >= cfg.source_batch_size
        || time.elapsed() >= *cfg.source_batch_linger
        || cfg
            .source_batch_max_bytes
            .is_some_and(|max| buffered_bytes >= max)
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

#[cfg(test)]
mod tests {
    use super::*;
    use arroyo_rpc::config::update;
    use std::time::Duration;

    /// Set all flush-related config fields so each test is self-contained
    /// even if tests run in parallel.
    fn set_flush_config(batch_size: usize, linger_ms: u64, max_bytes: Option<usize>) {
        // config() auto-initialises from defaults; update() panics without it
        let _ = config();
        update(|c| {
            c.pipeline.source_batch_size = batch_size;
            c.pipeline.source_batch_linger = Duration::from_millis(linger_ms).into();
            c.pipeline.source_batch_max_bytes = max_bytes;
        });
    }

    #[test]
    fn bytes_threshold_triggers_flush() {
        set_flush_config(512, 10_000, Some(100));
        assert!(should_flush(1, 100, Instant::now()));
    }

    #[test]
    fn below_bytes_threshold_no_flush() {
        set_flush_config(512, 10_000, Some(100));
        assert!(!should_flush(1, 99, Instant::now()));
    }

    #[test]
    fn bytes_threshold_disabled() {
        set_flush_config(512, 10_000, None);
        assert!(!should_flush(1, 1_000_000, Instant::now()));
        assert!(!should_flush(1, usize::MAX, Instant::now()));
    }

    #[test]
    fn count_threshold_triggers_flush() {
        set_flush_config(10, 10_000, None);
        assert!(should_flush(10, 0, Instant::now()));
    }

    #[test]
    fn linger_triggers_flush() {
        set_flush_config(512, 100, None);
        assert!(should_flush(1, 0, Instant::now() - Duration::from_secs(1)));
    }
}
