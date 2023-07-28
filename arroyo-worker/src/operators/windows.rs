use std::{marker::PhantomData, time::SystemTime};

use crate::engine::{Context, StreamNode};
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_state::tables::KeyTimeMultiMap;
use arroyo_types::*;
use rand::{rngs::SmallRng, RngCore, SeedableRng};
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
    salt: u64,
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
            salt: SmallRng::from_entropy().next_u64(),
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
            salt: SmallRng::from_entropy().next_u64(),
            _phantom: PhantomData,
        }
    }
    pub fn instant_window(
        operation: WindowOperation<T, OutT>,
    ) -> KeyedWindowFunc<K, T, OutT, InstantWindowAssigner> {
        KeyedWindowFunc {
            assigner: InstantWindowAssigner {},
            operation,
            salt: SmallRng::from_entropy().next_u64(),
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
            self.salt = self.salt.wrapping_add(1);
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
