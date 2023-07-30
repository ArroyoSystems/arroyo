use std::{marker::PhantomData, time::SystemTime, ops::Range, collections::HashMap};

use crate::engine::{Context, StreamNode, Collector};
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_state::tables::{KeyTimeMultiMap, TimeKeyMap};
use arroyo_types::*;
use std::time::Duration;

use super::{
    InstantWindowAssigner, SlidingWindowAssigner, TimeWindowAssigner, TumblingWindowAssigner,
};

pub mod aggregators {
    use std::ops::Add;

    use arroyo_types::Data;

    pub fn vec_aggregator<T: Data>(vs: Vec<&T>) -> Vec<T> {
        vs.into_iter().cloned().collect()
    }

    pub fn count_aggregator<T: Data>(vs: Vec<&T>) -> usize {
        vs.len()
    }

    pub fn max_aggregator<N: Ord + Copy + Data>(vs: Vec<&N>) -> Option<N> {
        vs.into_iter().max().copied()
    }

    pub fn min_aggregator<N: Ord + Copy + Data>(vs: Vec<&N>) -> Option<N> {
        vs.into_iter().min().copied()
    }

    pub fn sum_aggregator<N: Add<Output = N> + Ord + Copy + Data>(vs: Vec<&N>) -> Option<N> {
        if vs.is_empty() {
            None
        } else {
            Some(vs[1..].iter().fold(*vs[0], |s, t| s + **t))
        }
    }
}

pub enum WindowOperation<T: Data, OutT: Data> {
    Aggregate(fn(Vec<&T>) -> OutT),
    Flatten(fn(Vec<&T>) -> Vec<OutT>),
}

#[derive(StreamNode)]
pub struct KeyedWindowFunc<K: Key, T: Data, OutT: Data, W: TimeWindowAssigner<K, T>> {
    assigner: W,
    operation: WindowOperation<T, OutT>,
    _phantom: PhantomData<(K, T, OutT)>,
}

#[process_fn(in_k = K, in_t = T, out_k = K, out_t = OutT, timer_t = Window)]
impl<K: Key, T: Data, OutT: Data, W: TimeWindowAssigner<K, T>> KeyedWindowFunc<K, T, OutT, W> {
    pub fn tumbling_window(
        size: Duration,
        operation: WindowOperation<T, OutT>,
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
        operation: WindowOperation<T, OutT>,
    ) -> KeyedWindowFunc<K, T, OutT, SlidingWindowAssigner> {
        KeyedWindowFunc {
            assigner: SlidingWindowAssigner { size, slide },
            operation,
            _phantom: PhantomData,
        }
    }
    pub fn instant_window(
        operation: WindowOperation<T, OutT>,
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
            if w.end_time > watermark {
                has_window = true;

                ctx.schedule_timer(&mut key, w.end_time, w).await;
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
        let mut state = ctx.state.get_key_time_multi_map('w').await;

        match self.operation {
            WindowOperation::Aggregate(aggregator) => {
                let value = {
                    let vs: Vec<&T> = state
                        .get_time_range(&mut key, window.start_time, window.end_time)
                        .await;
                    (aggregator)(vs)
                };

                let record = Record {
                    timestamp: window.end_time - Duration::from_nanos(1),
                    key: Some(key.clone()),
                    value,
                };

                ctx.collect(record).await;
            }
            WindowOperation::Flatten(flatten) => {
                let values = {
                    let vs: Vec<&T> = state
                        .get_time_range(&mut key, window.start_time, window.end_time)
                        .await;
                    (flatten)(vs)
                };

                for v in values {
                    let record = Record {
                        timestamp: window.end_time - Duration::from_nanos(1),
                        key: Some(key.clone()),
                        value: v,
                    };
                    ctx.collect(record).await;
                }
            }
        }


        // clear everything before our start time (we're guaranteed that timers execute in order,
        // so with fixed-width windows there won't be any earlier data)
        let next = self.assigner.next(window);
        let mut state: KeyTimeMultiMap<K, T, _> = ctx.state.get_key_time_multi_map('w').await;

        state
            .clear_time_range(&mut key, SystemTime::UNIX_EPOCH, next.start_time)
            .await;
    }
}


#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct SessionWindow {
    start: SystemTime,
    end: SystemTime
}

impl SessionWindow {
    fn new(start: SystemTime, gap: Duration) -> Self {
        Self {
            start,
            end: start + gap,
        }
    }

    fn contains(&self, t: SystemTime) -> bool {
        self.start >= t && t < self.end
    }
}

impl From<Range<SystemTime>> for SessionWindow {
    fn from(value: Range<SystemTime>) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}

#[derive(StreamNode)]
pub struct SessionWindowFunc<K: Key, T: Data, OutT: Data> {
    operation: WindowOperation<T, OutT>,
    gap_size: Duration,

    windows: HashMap<K, Vec<SessionWindow>>,
}

#[process_fn(in_k = K, in_t = T, out_k = K, out_t = OutT, time_t = SystemTime)]
impl<K: Key, T: Data, OutT: Data> SessionWindowFunc<K, T, OutT> {
    fn name(&self) -> String {
        "SessionWindow".to_string()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![TableDescriptor {
            name: "w".to_string(),
            description: "window state".to_string(),
            table_type: TableType::KeyTimeMultiMap as i32,
            delete_behavior: TableDeleteBehavior::NoReadsBeforeWatermark as i32,
            write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
            retention_micros: self.gap_size.as_micros() as u64,
        }]
    }

    fn handle_new_window(windows: &mut Vec<SessionWindow>, key: &mut K, new: SessionWindow) -> Option<SystemTime> {
        // look for an existing window to extend forward
        if let Some(w) = windows.iter_mut().find(|w| w.contains(new.end)) {
            w.start = new.start;
            None
        } else {
            // otherwise we're going to insert and schedule a new window
            windows.push(new.clone());
            Some(new.end)
        }
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

        let (remove, add) = if let Some(windows) = self.windows.get_mut(&key) {
            if let Some((i, window)) = windows.iter().enumerate()
                .find(|(_, w)| w.contains(timestamp))
                .map(|(i, w)| (i, *w)) {
                // there's an existing window this record falls into
                let our_end = timestamp + self.gap_size;
                if our_end > window.end {
                    // we're extending an existing window
                    windows.remove(i);
                    let new = (window.start..our_end).into();
                    (Some(window.end), Self::handle_new_window(windows, &mut key, new))
                } else {
                    (None, None)
                }
                // otherwise the window is unchanged, we don't need to do anything
            } else {
                // there's no existing window, we need to add one
                (None, Self::handle_new_window(windows, &mut key, SessionWindow::new(timestamp, self.gap_size)))
            }
        } else {
            // no existing window, create one
            let window = SessionWindow::new(timestamp, self.gap_size);
            let mut windows = vec![];
            let t = Self::handle_new_window(&mut windows, &mut key, window);
            assert!(self.windows.insert(key.clone(), windows).is_none());
            (None, t)
        };

        if let Some(remove) = remove {
            let _: Option<SystemTime> = ctx.cancel_timer(&mut key, remove).await;
        }
        if let Some(add) = add {
            ctx.schedule_timer(&mut key, add, add).await;
        }

        ctx.state
            .get_key_time_multi_map('w')
            .await
            .insert(timestamp, key, value)
            .await;
    }

    async fn handle_timer(&mut self, mut key: K, t: SystemTime, ctx: &mut Context<K, OutT>) {
        let mut state = ctx.state.get_key_time_multi_map('w').await;

        let vs: Vec<&T> = state.get_time_range(&mut key, SystemTime::UNIX_EPOCH, t).await;
    }
}
