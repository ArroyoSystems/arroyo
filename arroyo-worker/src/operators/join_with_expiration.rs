use std::{marker::PhantomData, time::Duration};

use arroyo_macro::{co_process_fn, StreamNode};
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_state::tables::KeyTimeMultiMap;
use arroyo_types::*;

use crate::engine::Context;

#[derive(StreamNode)]
pub struct JoinWithExpiration<K: Key, T1: Data, T2: Data> {
    left_expiration: Duration,
    right_expiration: Duration,
    _t: PhantomData<(K, T1, T2)>,
}

#[co_process_fn(in_k1=K, in_t1=T1, in_k2=K, in_t2=T2, out_k=K, out_t=(T1,T2))]
impl<K: Key, T1: Data, T2: Data> JoinWithExpiration<K, T1, T2> {
    fn name(&self) -> String {
        "JoinWithExperiation".to_string()
    }

    pub fn new(left_expiration: Duration, right_expiration: Duration) -> Self {
        Self {
            left_expiration,
            right_expiration,
            _t: PhantomData,
        }
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![
            TableDescriptor {
                name: "l".to_string(),
                description: "join left state".to_string(),
                table_type: TableType::KeyTimeMultiMap as i32,
                delete_behavior: TableDeleteBehavior::NoReadsBeforeWatermark as i32,
                write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
                retention_micros: self.left_expiration.as_micros() as u64,
            },
            TableDescriptor {
                name: "r".to_string(),
                description: "join right state".to_string(),
                table_type: TableType::KeyTimeMultiMap as i32,
                delete_behavior: TableDeleteBehavior::NoReadsBeforeWatermark as i32,
                write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
                retention_micros: self.right_expiration.as_micros() as u64,
            },
        ]
    }

    async fn process_left(&mut self, record: &Record<K, T1>, ctx: &mut Context<K, (T1, T2)>) {
        if let Some(watermark) = ctx.watermark() {
            if record.timestamp < watermark {
                return;
            }
        };
        let mut right_state: KeyTimeMultiMap<K, T2, _> =
            ctx.state.get_key_time_multi_map('r').await;
        let mut key = record.key.clone().unwrap();
        let value = record.value.clone();
        let records = {
            let right_rows = right_state.get_all_values_with_timestamps(&mut key).await;
            let mut records = vec![];
            for (timestamp, right_value) in right_rows {
                records.push(Record {
                    timestamp: record.timestamp.max(timestamp),
                    key: Some(key.clone()),
                    value: (value.clone(), right_value.clone()),
                });
            }
            records
        };
        for record in records {
            ctx.collect(record).await;
        }
        let mut left_state = ctx.state.get_key_time_multi_map('l').await;
        left_state.insert(record.timestamp, key, value).await;
    }

    async fn process_right(&mut self, record: &Record<K, T2>, ctx: &mut Context<K, (T1, T2)>) {
        if let Some(watermark) = ctx.watermark() {
            if record.timestamp < watermark {
                return;
            }
        };

        let mut left_state: KeyTimeMultiMap<K, T1, _> = ctx.state.get_key_time_multi_map('l').await;
        let mut key = record.key.clone().unwrap();
        let value = record.value.clone();
        let records = {
            let left_rows = left_state.get_all_values_with_timestamps(&mut key).await;
            let mut records = vec![];
            for (timestamp, left_value) in left_rows {
                records.push(Record {
                    timestamp: record.timestamp.max(timestamp),
                    key: Some(key.clone()),
                    value: (left_value.clone(), value.clone()),
                });
            }
            records
        };
        for record in records {
            ctx.collect(record).await;
        }
        let mut right_state = ctx.state.get_key_time_multi_map('r').await;
        right_state.insert(record.timestamp, key, value).await;
    }

    async fn handle_watermark(
        &mut self,
        _watermark: std::time::SystemTime,
        ctx: &mut Context<K, (T1, T2)>,
    ) {
        let Some(watermark) = ctx.watermark() else {return};
        let mut left_state: KeyTimeMultiMap<K, T1, _> = ctx.state.get_key_time_multi_map('l').await;
        left_state.expire_entries_before(watermark - self.left_expiration);
        let mut right_state: KeyTimeMultiMap<K, T2, _> =
            ctx.state.get_key_time_multi_map('r').await;
        right_state.expire_entries_before(watermark - self.right_expiration);
        ctx.broadcast(arroyo_types::Message::Watermark(watermark))
            .await;
    }
}
