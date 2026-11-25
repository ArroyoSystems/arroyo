use arroyo_rpc::analytics::{self, AnalyticsEvent};
use arroyo_rpc::{EventLevel, EventLogger, init_event_logger, log_event, log_trace_event};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[test]
fn trace_gating_precedes_event_construction() {
    #[derive(Default)]
    struct Logger {
        trace_enabled: AtomicBool,
        events: Mutex<Vec<(&'static str, EventLevel, serde_json::Value)>>,
    }

    impl EventLogger for Logger {
        fn log_event(&self, event: &dyn AnalyticsEvent, level: EventLevel) {
            self.events
                .lock()
                .unwrap()
                .push((event.event_type(), level, event.to_json().unwrap()));
        }

        fn trace_enabled(&self) -> bool {
            self.trace_enabled.load(Ordering::Relaxed)
        }
    }

    let logger = Arc::new(Logger::default());
    let constructions = AtomicUsize::new(0);
    log_event!(analytics::ApiError {
        error: &{
            constructions.fetch_add(1, Ordering::Relaxed);
            "uninstalled".to_string()
        },
    });
    assert_eq!(constructions.load(Ordering::Relaxed), 0);
    init_event_logger(logger.clone());

    for enabled in [false, true] {
        logger.trace_enabled.store(enabled, Ordering::Relaxed);
        log_trace_event!(analytics::Checkpoint {
            operator_id: None,
            operator_name: None,
            subtask_idx: "",
            epoch: &{
                constructions.fetch_add(1, Ordering::Relaxed);
                "7".to_string()
            },
            size_bytes: 1.0,
            total_time: 2.0,
            alignment_time: 3.0,
            sync_time: 4.0,
            async_time: 5.0,
            commit_time: 6.0,
        });
        assert_eq!(constructions.load(Ordering::Relaxed), usize::from(enabled));
    }

    logger.trace_enabled.store(false, Ordering::Relaxed);
    log_event!(analytics::ApiError { error: "failed" });

    let events = logger.events.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(
        (events[0].0, events[0].1),
        ("checkpoint", EventLevel::Trace)
    );
    assert_eq!(events[0].2["epoch"], "7");
    assert_eq!(events[0].2["size_bytes"], 1.0);
    assert_eq!(
        (events[1].0, events[1].1),
        ("api_error", EventLevel::Analytics)
    );
    assert_eq!(events[1].2, serde_json::json!({ "error": "failed" }));
}
