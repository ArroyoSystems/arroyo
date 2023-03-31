use std::time::SystemTime;
use std::{marker::PhantomData, time::Duration};

use arroyo_macro::{co_process_fn, StreamNode};
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_types::*;

use crate::engine::Context;

use super::{
    InstantWindowAssigner, SlidingWindowAssigner, TimeWindowAssigner, TumblingWindowAssigner,
};

#[derive(StreamNode)]
pub struct WindowedHashJoin<
    K: Key,
    T1: Data,
    T2: Data,
    W1: TimeWindowAssigner<K, T1>,
    W2: TimeWindowAssigner<K, T2>,
> {
    assigner1: W1,
    assigner2: W2,
    _t: PhantomData<(K, T1, T2)>,
}

#[co_process_fn(in_k1=K, in_t1=T1, in_k2=K, in_t2=T2, out_k=K, out_t=(Vec<T1>, Vec<T2>), timer_t=Window)]
impl<K: Key, T1: Data, T2: Data, W1: TimeWindowAssigner<K, T1>, W2: TimeWindowAssigner<K, T2>>
    WindowedHashJoin<K, T1, T2, W1, W2>
{
    pub fn tumbling_window(
        size: Duration,
    ) -> WindowedHashJoin<K, T1, T2, TumblingWindowAssigner, TumblingWindowAssigner> {
        WindowedHashJoin {
            assigner1: TumblingWindowAssigner { size },
            assigner2: TumblingWindowAssigner { size },
            _t: PhantomData,
        }
    }

    pub fn sliding_window(
        size: Duration,
        slide: Duration,
    ) -> WindowedHashJoin<K, T1, T2, SlidingWindowAssigner, SlidingWindowAssigner> {
        WindowedHashJoin {
            assigner1: SlidingWindowAssigner { size, slide },
            assigner2: SlidingWindowAssigner { size, slide },
            _t: PhantomData,
        }
    }

    pub fn instant_window(
    ) -> WindowedHashJoin<K, T1, T2, InstantWindowAssigner, InstantWindowAssigner> {
        WindowedHashJoin {
            assigner1: InstantWindowAssigner {},
            assigner2: InstantWindowAssigner {},
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        "WindowedHashJoin".to_string()
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![
            TableDescriptor {
                name: "l".to_string(),
                description: "join left state".to_string(),
                table_type: TableType::KeyTimeMultiMap as i32,
                delete_behavior: TableDeleteBehavior::NoReadsBeforeWatermark as i32,
                write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
                retention_micros: self
                    .assigner1
                    .safe_retention_duration()
                    .unwrap()
                    .as_micros() as u64,
            },
            TableDescriptor {
                name: "r".to_string(),
                description: "join right state".to_string(),
                table_type: TableType::KeyTimeMultiMap as i32,
                delete_behavior: TableDeleteBehavior::NoReadsBeforeWatermark as i32,
                write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
                retention_micros: self
                    .assigner2
                    .safe_retention_duration()
                    .unwrap()
                    .as_micros() as u64,
            },
        ]
    }

    async fn store<T: Data, W: TimeWindowAssigner<K, T>>(
        record: &Record<K, T>,
        assigner: W,
        table: char,
        ctx: &mut Context<K, (Vec<T1>, Vec<T2>)>,
    ) {
        let windows = assigner.windows(record.timestamp);
        let watermark = ctx.watermark().unwrap_or(SystemTime::UNIX_EPOCH);
        let mut has_window = false;
        for w in windows {
            if w.end_time > watermark {
                has_window = true;
                let mut key = record.key.as_ref().unwrap().clone();
                ctx.schedule_timer(&mut key, w.end_time, w).await;
            }
        }

        if has_window {
            let key = record.key.clone().unwrap();
            let value = record.value.clone();
            ctx.state
                .get_key_time_multi_map(table)
                .await
                .insert(record.timestamp, key, value)
                .await;
        }
    }

    async fn handle_timer(
        &mut self,
        mut key: K,
        window: Window,
        ctx: &mut Context<K, (Vec<T1>, Vec<T2>)>,
    ) {
        let record = {
            let mut left_state = ctx.state.get_key_time_multi_map('l').await;
            let left: Vec<T1> = {
                let left: Vec<&T1> = left_state
                    .get_time_range(&mut key, window.start_time, window.end_time)
                    .await;
                left.into_iter().cloned().collect()
            };

            let next = self.assigner1.next(window);
            left_state
                .clear_time_range(&mut key, SystemTime::UNIX_EPOCH, next.start_time)
                .await;

            let mut right_state = ctx.state.get_key_time_multi_map('l').await;
            let right: Vec<T2> = {
                let right: Vec<&T2> = right_state
                    .get_time_range(&mut key, window.start_time, window.end_time)
                    .await;
                right.into_iter().cloned().collect()
            };
            let next = self.assigner2.next(window);
            right_state
                .clear_time_range(&mut key, SystemTime::UNIX_EPOCH, next.start_time)
                .await;

            Record {
                timestamp: window.end_time - Duration::from_nanos(1),
                key: Some(key),
                value: (left, right),
            }
        };

        ctx.collector.collect(record).await;
    }

    async fn process_left(
        &mut self,
        record: &Record<K, T1>,
        ctx: &mut Context<K, (Vec<T1>, Vec<T2>)>,
    ) {
        Self::store(record, self.assigner1, 'l', ctx).await;
    }

    async fn process_right(
        &mut self,
        record: &Record<K, T2>,
        ctx: &mut Context<K, (Vec<T1>, Vec<T2>)>,
    ) {
        Self::store(record, self.assigner2, 'r', ctx).await;
    }
}
