use std::{marker::PhantomData, time::SystemTime};

use crate::engine::{Context, StreamNode};
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_state::tables::{KeyTimeMultiMap, KeyedState};
use arroyo_types::*;
use std::time::Duration;

use super::{
    InstantWindowAssigner, SlidingWindowAssigner, TimeWindowAssigner, TumblingWindowAssigner,
};

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
pub struct KeyedWindowFunc<K: Key, T: Data, OutT: Data, W: TimeWindowAssigner<K, T>> {
    assigner: W,
    operation: WindowOperation<K, T, OutT>,
    _phantom: PhantomData<(K, T, OutT)>,
}

#[process_fn(in_k = K, in_t = T, out_k = K, out_t = OutT, timer_t = Window)]
impl<K: Key, T: Data, OutT: Data, W: TimeWindowAssigner<K, T>> KeyedWindowFunc<K, T, OutT, W> {
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

#[process_fn(in_k = K, in_t = T, out_k = K, out_t = OutT, time_t = Window)]
impl<K: Key, T: Data, OutT: Data> SessionWindowFunc<K, T, OutT> {
    fn name(&self) -> String {
        "SessionWindow".to_string()
    }

    pub async fn new(operation: WindowOperation<K, T, OutT>, gap_size: Duration) -> Self {
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
                retention_micros: self.gap_size.as_micros() as u64,
            },
            TableDescriptor {
                name: "s".to_string(),
                description: "sessions".to_string(),
                table_type: TableType::TimeKeyMap as i32,
                delete_behavior: TableDeleteBehavior::NoReadsBeforeWatermark as i32,
                write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
                retention_micros: self.gap_size.as_micros() as u64,
            },
        ]
    }

    fn handle_new_window(windows: &mut Vec<Window>, new: Window) -> Option<SystemTime> {
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

        let mut windows: Option<Vec<Window>> = {
            let t: KeyedState<'_, K, Vec<Window>, _> = ctx.state.get_key_state('s').await;
            t.get(&key).map(|t| t.iter().map(|w| *w).collect())
        };

        let (remove, add) = if let Some(windows) = windows.as_mut() {
            if let Some((i, window)) = windows
                .iter()
                .enumerate()
                .find(|(_, w)| w.contains(timestamp))
                .map(|(i, w)| (i, *w))
            {
                // there's an existing window this record falls into
                let our_end = timestamp + self.gap_size;
                if our_end > window.end {
                    // we're extending an existing window
                    windows.remove(i);
                    let new = (window.start..our_end).into();
                    (Some(window.end), Self::handle_new_window(windows, new))
                } else {
                    (None, None)
                }
                // otherwise the window is unchanged, we don't need to do anything
            } else {
                // there's no existing window, we need to add one
                (
                    None,
                    Self::handle_new_window(windows, Window::session(timestamp, self.gap_size)),
                )
            }
        } else {
            // no existing window, create one
            let window = Window::session(timestamp, self.gap_size);
            windows = Some(vec![]);
            let t = Self::handle_new_window(windows.as_mut().unwrap(), window);
            (None, t)
        };

        if let Some(remove) = remove {
            let _: Option<SystemTime> = ctx.cancel_timer(&mut key, remove).await;
        }
        if let Some(add) = add {
            // we use UNIX_EPOCH as the start of our windows to aovid having to update them when we extend
            // the beginning; this works because windows are always handled in order
            ctx.schedule_timer(&mut key, add, Window::new(SystemTime::UNIX_EPOCH, add))
                .await;
        }

        if add.is_some() || remove.is_some() {
            let key = key.clone();
            ctx.state
                .get_key_state('s')
                .await
                // I have no idea if this timestamp is correct -- this datastructure does not make sense to me
                .insert(timestamp, key, windows.unwrap())
                .await;
        }

        ctx.state
            .get_key_time_multi_map('w')
            .await
            .insert(timestamp, key, value)
            .await;
    }

    async fn handle_timer(&mut self, mut key: K, window: Window, ctx: &mut Context<K, OutT>) {
        self.operation.operate(&mut key, window, 'w', ctx).await;

        // clear this window and everything before it -- we're guaranteed that windows are executed in order and are
        // non-overlapping so we will never need to reprocess this data
        let mut state: KeyTimeMultiMap<K, T, _> = ctx.state.get_key_time_multi_map('w').await;

        state
            .clear_time_range(&mut key, window.start, window.end)
            .await;

        let mut t: KeyedState<'_, K, Vec<Window>, _> = ctx.state.get_key_state('s').await;
        let mut windows: Vec<Window> = t
            .get(&key)
            .map(|t| t.iter().map(|w| *w).collect())
            .expect("there must be a window for this key in state");

        windows.retain(|w| w.end != window.end);

        if windows.is_empty() {
            t.remove(key).await;
        } else {
            t.insert(windows.iter().map(|w| w.end).max().unwrap(), key, windows)
                .await;
        }
    }
}
