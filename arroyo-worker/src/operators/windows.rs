use std::{marker::PhantomData, time::SystemTime};

use crate::engine::{Context, StreamNode};
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_state::tables::{KeyTimeMultiMap, KeyedState};
use arroyo_types::*;
use std::time::Duration;
use arroyo_operator::operators::{InstantWindowAssigner, SlidingWindowAssigner, TimeWindowAssigner, TumblingWindowAssigner};


// Enforce a maximum session size to prevent unbounded state growth until we are able to
// delete from parquet state.
const MAX_SESSION_SIZE: Duration = Duration::from_secs(24 * 60 * 60); // 1 day

pub mod aggregators {
    use std::ops::Add;

    use arroyo_types::{Data, Key, Window};

    pub fn vec_aggregator<K: Key, T: Data>(_: &K, _: Window, vs: Vec<&T>) -> Vec<T> {
        vs.into_iter().cloned().collect()
    }

    pub fn count_aggregator<K: Key, T: Data>(_: &K, _: Window, vs: Vec<&T>) -> usize {
        vs.len()
    }

    pub fn max_aggregator<K: Key, N: Ord + Copy + Data>(
        _: &K,
        _: Window,
        vs: Vec<&N>,
    ) -> Option<N> {
        vs.into_iter().max().copied()
    }

    pub fn min_aggregator<K: Key, N: Ord + Copy + Data>(
        _: &K,
        _: Window,
        vs: Vec<&N>,
    ) -> Option<N> {
        vs.into_iter().min().copied()
    }

    pub fn sum_aggregator<K: Key, N: Add<Output = N> + Ord + Copy + Data>(
        _: &K,
        _: Window,
        vs: Vec<&N>,
    ) -> Option<N> {
        if vs.is_empty() {
            None
        } else {
            Some(vs[1..].iter().fold(*vs[0], |s, t| s + **t))
        }
    }
}

pub enum WindowOperation<K: Key, T: Data, OutT: Data> {
    Aggregate(fn(&K, Window, Vec<&T>) -> OutT),
    Flatten(fn(&K, Window, Vec<&T>) -> Vec<OutT>),
}

impl<K: Key, T: Data, OutT: Data> WindowOperation<K, T, OutT> {
    async fn operate(&self, key: &mut K, window: Window, table: char, ctx: &mut Context<K, OutT>) {
        let mut state = ctx.state.get_key_time_multi_map(table).await;

        match self {
            WindowOperation::Aggregate(aggregator) => {
                let value = {
                    let vs: Vec<&T> = state.get_time_range(key, window.start, window.end).await;
                    (aggregator)(key, window, vs)
                };

                let record = Record {
                    timestamp: window.end - Duration::from_nanos(1),
                    key: Some(key.clone()),
                    value,
                };

                ctx.collect(record).await;
            }
            WindowOperation::Flatten(flatten) => {
                let values = {
                    let vs: Vec<&T> = state.get_time_range(key, window.start, window.end).await;
                    (flatten)(&key, window, vs)
                };

                for v in values {
                    let record = Record {
                        timestamp: window.end - Duration::from_nanos(1),
                        key: Some(key.clone()),
                        value: v,
                    };
                    ctx.collect(record).await;
                }
            }
        }
    }
}

#[derive(StreamNode)]
pub struct KeyedWindowFunc<K: Key, T: Data, OutT: Data, W: TimeWindowAssigner> {
    assigner: W,
    operation: WindowOperation<K, T, OutT>,
    _phantom: PhantomData<(K, T, OutT)>,
}

#[process_fn(in_k = K, in_t = T, out_k = K, out_t = OutT, timer_t = Window)]
impl<K: Key, T: Data, OutT: Data, W: TimeWindowAssigner> KeyedWindowFunc<K, T, OutT, W> {
    pub fn tumbling_window(
        size: Duration,
        operation: WindowOperation<K, T, OutT>,
    ) -> KeyedWindowFunc<K, T, OutT, TumblingWindowAssigner> {
        KeyedWindowFunc {
            assigner: TumblingWindowAssigner { size },
            operation,
            _phantom: PhantomData,
        }
    }

    pub fn sliding_window(
        size: Duration,
        slide: Duration,
        operation: WindowOperation<K, T, OutT>,
    ) -> KeyedWindowFunc<K, T, OutT, SlidingWindowAssigner> {
        KeyedWindowFunc {
            assigner: SlidingWindowAssigner { size, slide },
            operation,
            _phantom: PhantomData,
        }
    }
    pub fn instant_window(
        operation: WindowOperation<K, T, OutT>,
    ) -> KeyedWindowFunc<K, T, OutT, InstantWindowAssigner> {
        KeyedWindowFunc {
            assigner: InstantWindowAssigner {},
            operation,
            _phantom: PhantomData,
        }
    }

    fn name(&self) -> String {
        "KeyWindow".to_string()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![TableDescriptor {
            name: "w".to_string(),
            description: "window state".to_string(),
            table_type: TableType::KeyTimeMultiMap as i32,
            delete_behavior: TableDeleteBehavior::NoReadsBeforeWatermark as i32,
            write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
            retention_micros: self.assigner.safe_retention_duration().unwrap().as_micros() as u64,
        }]
    }

    async fn process_element(&mut self, record: &Record<K, T>, ctx: &mut Context<K, OutT>) {
        let windows = self.assigner.windows(record.timestamp);
        let watermark = ctx
            .last_present_watermark()
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let mut has_window = false;
        let mut key = record.key.clone().unwrap();
        for w in windows {
            if w.end > watermark {
                has_window = true;

                ctx.schedule_timer(&mut key, w.end, w).await;
            }
        }

        if has_window {
            let key = record.key.as_ref().unwrap().clone();
            let value = record.value.clone();
            ctx.state
                .get_key_time_multi_map('w')
                .await
                .insert(record.timestamp, key, value)
                .await;
        }
    }

    async fn handle_timer(&mut self, mut key: K, window: Window, ctx: &mut Context<K, OutT>) {
        self.operation.operate(&mut key, window, 'w', ctx).await;

        // clear everything before our start time (we're guaranteed that timers execute in order,
        // so with fixed-width windows there won't be any earlier data)
        let next = self.assigner.next(window);
        let mut state: KeyTimeMultiMap<K, T, _> = ctx.state.get_key_time_multi_map('w').await;

        state
            .clear_time_range(&mut key, SystemTime::UNIX_EPOCH, next.start)
            .await;
    }
}

#[derive(StreamNode)]
pub struct SessionWindowFunc<K: Key, T: Data, OutT: Data> {
    operation: WindowOperation<K, T, OutT>,
    gap_size: Duration,
    _t: PhantomData<K>,
}

struct WindowGroup {
    windows: Vec<Window>,
    gap_size: Duration,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct HandleResult {
    remove: Option<SystemTime>,
    add: Option<SystemTime>,
}

impl WindowGroup {
    fn handle_new_window(&mut self, mut new: Window) -> Option<SystemTime> {
        // look for an existing window to extend forward, unless it would be too long
        if let Some(w) = self.windows.iter_mut().find(|w| w.contains(new.end)) {
            if Window::new(new.start, w.end).size() < MAX_SESSION_SIZE {
                w.start = new.start;
                None
            } else {
                // if the merged window would be too long, we will extend the new window to MAX_SESSION_SIZE
                // and shorten the overlapping one
                new.end = new.start + MAX_SESSION_SIZE;
                w.start = new.end;
                self.windows.push(new);
                Some(new.end)
            }
        } else {
            // otherwise we're going to insert and schedule a new window
            self.windows.push(new);
            Some(new.end)
        }
    }

    fn handle_event(&mut self, timestamp: SystemTime) -> HandleResult {
        let (remove, add) = if !self.windows.is_empty() {
            if let Some((i, window)) = self
                .windows
                .iter()
                .enumerate()
                .find(|(_, w)| w.contains(timestamp))
                .map(|(i, w)| (i, *w))
            {
                // there's an existing window this record falls into
                let new_window = window.extend(timestamp + self.gap_size, MAX_SESSION_SIZE);
                if window != new_window {
                    // we're extending an existing window
                    self.windows.remove(i);
                    (Some(window.end), self.handle_new_window(new_window))
                } else {
                    (None, None)
                }
                // otherwise the window is unchanged, we don't need to do anything
            } else {
                // there's no existing window, we need to add one
                (
                    None,
                    self.handle_new_window(Window::session(timestamp, self.gap_size)),
                )
            }
        } else {
            // no existing window, create one
            let window = Window::session(timestamp, self.gap_size);
            let t = self.handle_new_window(window);
            (None, t)
        };

        // sort the windows by start time
        self.windows.sort_by_key(|w| w.start);

        assert!(
            self.is_valid(),
            "invalid session window state: {:?}",
            self.windows
        );

        HandleResult { remove, add }
    }

    fn max_end(&self) -> SystemTime {
        // windows are sorted by start time and do not overlap
        self.windows
            .iter()
            .last()
            .expect("this must be non-empty when called")
            .end
    }

    // ensures we've followed the two rules of session windows:
    //  1. Windows must not overlap
    //  2. Windows must not be longer than MAX_SESSION_SIZE
    fn is_valid(&self) -> bool {
        self.windows
            .windows(2)
            .all(|w| w[0].end <= w[1].start && w[0].size() <= MAX_SESSION_SIZE)
    }
}

#[process_fn(in_k = K, in_t = T, out_k = K, out_t = OutT, time_t = Window)]
impl<K: Key, T: Data, OutT: Data> SessionWindowFunc<K, T, OutT> {
    fn name(&self) -> String {
        "SessionWindow".to_string()
    }

    pub fn new(operation: WindowOperation<K, T, OutT>, gap_size: Duration) -> Self {
        Self {
            operation,
            gap_size,
            _t: PhantomData,
        }
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![
            TableDescriptor {
                name: "w".to_string(),
                description: "window state".to_string(),
                table_type: TableType::KeyTimeMultiMap as i32,
                delete_behavior: TableDeleteBehavior::NoReadsBeforeWatermark as i32,
                write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
                // we always write the largest end in the list of windows
                retention_micros: 0 as u64,
            },
            TableDescriptor {
                name: "s".to_string(),
                description: "sessions".to_string(),
                table_type: TableType::TimeKeyMap as i32,
                delete_behavior: TableDeleteBehavior::NoReadsBeforeWatermark as i32,
                write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
                retention_micros: MAX_SESSION_SIZE.as_micros() as u64,
            },
        ]
    }

    async fn process_element(&mut self, record: &Record<K, T>, ctx: &mut Context<K, OutT>) {
        let watermark = ctx
            .last_present_watermark()
            .unwrap_or(SystemTime::UNIX_EPOCH);

        if watermark >= record.timestamp {
            // drop late data
            return;
        }

        let mut key = record.key.as_ref().unwrap().clone();
        let value = record.value.clone();
        let timestamp = record.timestamp;

        let mut windows = WindowGroup {
            windows: {
                let t: KeyedState<'_, K, Vec<Window>, _> = ctx.state.get_key_state('s').await;
                t.get(&key).map(|t| t.iter().map(|w| *w).collect())
            }
            .unwrap_or_default(),
            gap_size: self.gap_size,
        };

        let result = windows.handle_event(timestamp);

        if let Some(remove) = result.remove {
            let _: Option<Window> = ctx.cancel_timer(&mut key, remove).await;
        }
        if let Some(add) = result.add {
            // we use UNIX_EPOCH as the start of our windows to avoid having to update them when we extend
            // the beginning; this works because windows are always handled in order
            ctx.schedule_timer(&mut key, add, Window::new(SystemTime::UNIX_EPOCH, add))
                .await;
        }

        if result.add.is_some() || result.remove.is_some() {
            let key = key.clone();
            ctx.state
                .get_key_state('s')
                .await
                .insert(windows.max_end(), key, windows.windows)
                .await;
        }

        ctx.state
            .get_key_time_multi_map('w')
            .await
            .insert(timestamp, key, value)
            .await;
    }

    async fn handle_timer(&mut self, mut key: K, window: Window, ctx: &mut Context<K, OutT>) {
        let window = {
            // get the actual window (as the timer one doesn't have the actual start time)
            let mut t: KeyedState<'_, K, Vec<Window>, _> = ctx.state.get_key_state('s').await;
            let mut windows: Vec<Window> = t
                .get(&key)
                .map(|t| t.iter().map(|w| *w).collect())
                .expect("there must be a window for this key in state");

            let window = *windows
                .iter()
                .find(|w| w.end == window.end)
                .expect("this window must be in state");

            windows.retain(|w| w.end != window.end);

            if windows.is_empty() {
                t.remove(&mut key).await;
            } else {
                let key = key.clone();
                t.insert(windows.iter().map(|w| w.end).max().unwrap(), key, windows)
                    .await;
            }

            window
        };

        self.operation.operate(&mut key, window, 'w', ctx).await;

        // clear this window and everything before it -- we're guaranteed that windows are executed in order and are
        // non-overlapping so we will never need to reprocess this data
        let mut state: KeyTimeMultiMap<K, T, _> = ctx.state.get_key_time_multi_map('w').await;

        state
            .clear_time_range(&mut key, window.start, window.end)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use arroyo_types::{from_millis, Window};

    use crate::operators::windows::{HandleResult, MAX_SESSION_SIZE};

    use super::WindowGroup;

    #[test]
    fn test_no_windows() {
        let mut wg = WindowGroup {
            windows: vec![],
            gap_size: Duration::from_millis(100),
        };

        // no existing windows, so we should create one
        assert_eq!(
            wg.handle_event(from_millis(0)),
            HandleResult {
                remove: None,
                add: Some(from_millis(100))
            }
        );

        assert_eq!(
            wg.windows,
            vec![Window {
                start: from_millis(0),
                end: from_millis(100)
            }]
        );

        assert_eq!(from_millis(100), wg.max_end());
    }

    #[test]
    fn test_extend_window() {
        let mut wg = WindowGroup {
            windows: vec![Window::session(from_millis(0), Duration::from_millis(100))],
            gap_size: Duration::from_millis(100),
        };

        // we should extend the existing window which involves removing the timer and re-adding it
        // at the new stop time
        assert_eq!(
            wg.handle_event(from_millis(50)),
            HandleResult {
                remove: Some(from_millis(100)),
                add: Some(from_millis(150))
            }
        );

        assert_eq!(
            wg.windows,
            vec![Window {
                start: from_millis(0),
                end: from_millis(150)
            }]
        );

        assert_eq!(from_millis(150), wg.max_end());
    }

    #[test]
    fn test_add_window() {
        let mut wg = WindowGroup {
            windows: vec![Window::session(from_millis(0), Duration::from_millis(100))],
            gap_size: Duration::from_millis(100),
        };

        // this doesn't overlap with the existing window, so we should add a new one
        assert_eq!(
            wg.handle_event(from_millis(150)),
            HandleResult {
                remove: None,
                add: Some(from_millis(250))
            }
        );

        assert_eq!(
            wg.windows,
            vec![
                Window {
                    start: from_millis(0),
                    end: from_millis(100)
                },
                Window {
                    start: from_millis(150),
                    end: from_millis(250)
                }
            ]
        );

        assert_eq!(from_millis(250), wg.max_end());
    }

    #[test]
    fn test_merge_windows() {
        let mut wg: WindowGroup = WindowGroup {
            windows: vec![
                Window {
                    start: from_millis(0),
                    end: from_millis(100),
                },
                Window {
                    start: from_millis(150),
                    end: from_millis(250),
                },
            ],
            gap_size: Duration::from_millis(100),
        };

        // this extends one window far enough that it merges with the next one
        assert_eq!(
            wg.handle_event(from_millis(80)),
            HandleResult {
                remove: Some(from_millis(100)),
                add: None
            }
        );

        assert_eq!(
            wg.windows,
            vec![Window {
                start: from_millis(0),
                end: from_millis(250)
            }]
        );

        assert_eq!(from_millis(250), wg.max_end());
    }

    #[test]
    fn test_dont_extend_past_max_size() {
        let mut wg = WindowGroup {
            windows: vec![Window::session(
                from_millis(0),
                MAX_SESSION_SIZE - Duration::from_secs(3),
            )],
            gap_size: Duration::from_secs(10),
        };

        // this would extend the window past the max size, so it should only be extended up to the max size
        let start = from_millis(MAX_SESSION_SIZE.as_millis() as u64 - 5000);
        assert_eq!(
            wg.handle_event(start),
            HandleResult {
                remove: Some(from_millis(0) + (MAX_SESSION_SIZE - Duration::from_secs(3))),
                add: Some(SystemTime::UNIX_EPOCH + MAX_SESSION_SIZE)
            }
        );

        assert_eq!(
            wg.windows,
            vec![Window {
                start: from_millis(0),
                end: SystemTime::UNIX_EPOCH + MAX_SESSION_SIZE
            },]
        );
    }

    #[test]
    fn dont_merge_past_max_size() {
        // we have two windows, each slightly less than half of the max session size, and separated by 2 seconds
        // adding an event at the end of the first window would normally cause them to be merged into one
        // however, because that would violate the max session window, the first window should be extended
        // to the max session size and the second one should follow immediately after it
        let half_millis = MAX_SESSION_SIZE.as_millis() as u64 / 2;
        let mut wg: WindowGroup = WindowGroup {
            windows: vec![
                Window::session(from_millis(0), Duration::from_millis(half_millis - 1000)),
                Window::session(
                    from_millis(half_millis + 1000),
                    Duration::from_millis(half_millis + 5000),
                ),
            ],
            gap_size: Duration::from_secs(10),
        };

        let start = from_millis(half_millis - 2000);

        assert_eq!(
            wg.handle_event(start),
            HandleResult {
                remove: Some(from_millis(half_millis - 1000)),
                add: Some(from_millis(MAX_SESSION_SIZE.as_millis() as u64))
            }
        );

        assert_eq!(
            wg.windows,
            vec![
                Window {
                    start: from_millis(0),
                    end: from_millis(MAX_SESSION_SIZE.as_millis() as u64)
                },
                Window {
                    start: from_millis(MAX_SESSION_SIZE.as_millis() as u64),
                    end: from_millis(half_millis + 1000)
                        + Duration::from_millis(half_millis + 5000)
                }
            ]
        );
    }
}
