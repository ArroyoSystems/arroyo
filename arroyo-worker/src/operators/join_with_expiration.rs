use std::{
    marker::PhantomData,
    time::{Duration, SystemTime},
};

use arroyo_macro::{co_process_fn, StreamNode};
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_state::tables::KeyTimeMultiMap;
use arroyo_types::*;

use crate::engine::Context;

#[derive(StreamNode)]
pub struct JoinWithExpiration<
    K: Key,
    T1: Data,
    T2: Data,
    Output: Data,
    P: JoinProcessor<K, T1, T2, Output>,
> {
    left_expiration: Duration,
    right_expiration: Duration,
    processor: P,
    _t: PhantomData<(K, T1, T2, Output)>,
}

pub trait JoinProcessor<K: Key, T1: Data, T2: Data, Output: Data>: Send + 'static {
    fn left_join(
        &self,
        key: K,
        left_timestamp: SystemTime,
        left_value: T1,
        right: Option<(SystemTime, &T2)>,
        evict_prior: bool,
    ) -> Option<(SystemTime, Output)>;
    fn right_join(
        &self,
        key: K,
        right_timestamp: SystemTime,
        right_value: T2,
        left: Option<(SystemTime, &T1)>,
        evict_prior: bool,
    ) -> Option<(SystemTime, Output)>;
}

pub struct LeftJoinProcessor<K: Key, T1: Data, T2: Data> {
    _t: PhantomData<(K, T1, T2)>,
}

impl<K: Key, T1: Data, T2: Data> JoinProcessor<K, T1, T2, UpdatingData<(T1, Option<T2>)>>
    for LeftJoinProcessor<K, T1, T2>
{
    fn left_join(
        &self,
        _key: K,
        left_timestamp: SystemTime,
        left_value: T1,
        right: Option<(SystemTime, &T2)>,
        evict_prior: bool,
    ) -> Option<(SystemTime, UpdatingData<(T1, Option<T2>)>)> {
        match right {
            Some((right_timestamp, right_value)) => {
                if evict_prior {
                    Some((
                        left_timestamp.max(right_timestamp),
                        UpdatingData::Update {
                            old: (left_value.clone(), None),
                            new: (left_value, Some(right_value.clone())),
                        },
                    ))
                } else {
                    Some((
                        left_timestamp,
                        UpdatingData::Append((left_value, Some(right_value.clone()))),
                    ))
                }
            }
            None => Some((left_timestamp, UpdatingData::Append((left_value, None)))),
        }
    }

    fn right_join(
        &self,
        _key: K,
        right_timestamp: SystemTime,
        right_value: T2,
        left: Option<(SystemTime, &T1)>,
        _evict_prior: bool,
    ) -> Option<(SystemTime, UpdatingData<(T1, Option<T2>)>)> {
        left.map(|(left_timestamp, left_value)| {
            (
                right_timestamp.max(left_timestamp),
                UpdatingData::Append((left_value.clone(), Some(right_value))),
            )
        })
    }
}

pub struct RightJoinProcessor<K: Key, T1: Data, T2: Data> {
    _t: PhantomData<(K, T1, T2)>,
}

impl<K: Key, T1: Data, T2: Data> JoinProcessor<K, T1, T2, UpdatingData<(Option<T1>, T2)>>
    for RightJoinProcessor<K, T1, T2>
{
    fn left_join(
        &self,
        _key: K,
        left_timestamp: SystemTime,
        left_value: T1,
        right: Option<(SystemTime, &T2)>,
        _evict_prior: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, T2)>)> {
        right.map(|(right_timestamp, right_value)| {
            (
                left_timestamp.max(right_timestamp),
                UpdatingData::Append((Some(left_value), right_value.clone())),
            )
        })
    }

    fn right_join(
        &self,
        _key: K,
        right_timestamp: SystemTime,
        right_value: T2,
        left: Option<(SystemTime, &T1)>,
        evict_prior: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, T2)>)> {
        match left {
            Some((left_timestamp, left_value)) => {
                if evict_prior {
                    Some((
                        left_timestamp.max(right_timestamp),
                        UpdatingData::Update {
                            old: (Some(left_value.clone()), right_value.clone()),
                            new: (None, right_value),
                        },
                    ))
                } else {
                    Some((
                        right_timestamp,
                        UpdatingData::Append((Some(left_value.clone()), right_value)),
                    ))
                }
            }
            None => Some((right_timestamp, UpdatingData::Append((None, right_value)))),
        }
    }
}

pub struct FullJoinProcessor<K: Key, T1: Data, T2: Data> {
    _t: PhantomData<(K, T1, T2)>,
}

impl<K: Key, T1: Data, T2: Data> JoinProcessor<K, T1, T2, UpdatingData<(Option<T1>, Option<T2>)>>
    for FullJoinProcessor<K, T1, T2>
{
    fn left_join(
        &self,
        _key: K,
        left_timestamp: SystemTime,
        left_value: T1,
        right: Option<(SystemTime, &T2)>,
        evict_prior: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, Option<T2>)>)> {
        match right {
            Some((right_timestamp, right_value)) => {
                if evict_prior {
                    Some((
                        left_timestamp.max(right_timestamp),
                        UpdatingData::Update {
                            old: (Some(left_value.clone()), None),
                            new: (Some(left_value), Some(right_value.clone())),
                        },
                    ))
                } else {
                    Some((
                        left_timestamp.max(right_timestamp),
                        UpdatingData::Append((Some(left_value), Some(right_value.clone()))),
                    ))
                }
            }
            None => Some((
                left_timestamp,
                UpdatingData::Append((Some(left_value), None)),
            )),
        }
    }

    fn right_join(
        &self,
        _key: K,
        right_timestamp: SystemTime,
        right_value: T2,
        left: Option<(SystemTime, &T1)>,
        evict_prior: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, Option<T2>)>)> {
        match left {
            Some((left_timestamp, left_value)) => {
                if evict_prior {
                    Some((
                        left_timestamp.max(right_timestamp),
                        UpdatingData::Update {
                            old: (None, Some(right_value.clone())),
                            new: (Some(left_value.clone()), Some(right_value)),
                        },
                    ))
                } else {
                    Some((
                        left_timestamp.max(right_timestamp),
                        UpdatingData::Append((Some(left_value.clone()), Some(right_value))),
                    ))
                }
            }
            None => Some((
                right_timestamp,
                UpdatingData::Append((None, Some(right_value))),
            )),
        }
    }
}

pub struct InnerJoinProcessor<K: Key, T1: Data, T2: Data> {
    _t: PhantomData<(K, T1, T2)>,
}

impl<K: Key, T1: Data, T2: Data> JoinProcessor<K, T1, T2, (T1, T2)>
    for InnerJoinProcessor<K, T1, T2>
{
    fn left_join(
        &self,
        _key: K,
        left_timestamp: SystemTime,
        left_value: T1,
        right: Option<(SystemTime, &T2)>,
        _evict_prior: bool,
    ) -> Option<(SystemTime, (T1, T2))> {
        right.map(|(right_timestamp, right_value)| {
            (
                left_timestamp.max(right_timestamp),
                (left_value, right_value.clone()),
            )
        })
    }

    fn right_join(
        &self,
        _key: K,
        right_timestamp: SystemTime,
        right_value: T2,
        left: Option<(SystemTime, &T1)>,
        _evict_prior: bool,
    ) -> Option<(SystemTime, (T1, T2))> {
        left.map(|(left_timestamp, left_value)| {
            (
                left_timestamp.max(right_timestamp),
                (left_value.clone(), right_value),
            )
        })
    }
}

// Return left JoinWithExpiration
pub fn left_join<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<K, T1, T2, UpdatingData<(T1, Option<T2>)>, LeftJoinProcessor<K, T1, T2>> {
    JoinWithExpiration::new(
        left_expiration,
        right_expiration,
        LeftJoinProcessor { _t: PhantomData },
    )
}

// Return right JoinWithExpiration
pub fn right_join<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<K, T1, T2, UpdatingData<(Option<T1>, T2)>, RightJoinProcessor<K, T1, T2>> {
    JoinWithExpiration::new(
        left_expiration,
        right_expiration,
        RightJoinProcessor { _t: PhantomData },
    )
}

// Return full JoinWithExpiration
pub fn full_join<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    T1,
    T2,
    UpdatingData<(Option<T1>, Option<T2>)>,
    FullJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(
        left_expiration,
        right_expiration,
        FullJoinProcessor { _t: PhantomData },
    )
}

// return inner JoinWithExpiration
pub fn inner_join<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<K, T1, T2, (T1, T2), InnerJoinProcessor<K, T1, T2>> {
    JoinWithExpiration::new(
        left_expiration,
        right_expiration,
        InnerJoinProcessor { _t: PhantomData },
    )
}

#[co_process_fn(in_k1=K, in_t1=T1, in_k2=K, in_t2=T2, out_k=K, out_t=Output)]
impl<K: Key, T1: Data, T2: Data, Output: Data, P: JoinProcessor<K, T1, T2, Output>>
    JoinWithExpiration<K, T1, T2, Output, P>
{
    fn name(&self) -> String {
        "JoinWithExpiration".to_string()
    }

    pub fn new(left_expiration: Duration, right_expiration: Duration, processor: P) -> Self {
        Self {
            left_expiration,
            right_expiration,
            processor,
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

    async fn process_left(&mut self, record: &Record<K, T1>, ctx: &mut Context<K, Output>) {
        if let Some(watermark) = ctx.watermark() {
            if record.timestamp < watermark {
                return;
            }
        };
        let mut key = record.key.clone().unwrap();
        let value = record.value.clone();

        let mut left_state: KeyTimeMultiMap<K, T1, _> = ctx.state.get_key_time_multi_map('l').await;
        let evict = left_state
            .get_all_values_with_timestamps(&mut key)
            .await
            .is_none();
        let mut right_state: KeyTimeMultiMap<K, T2, _> =
            ctx.state.get_key_time_multi_map('r').await;
        let records = {
            let mut records = vec![];
            if let Some(right_rows) = right_state.get_all_values_with_timestamps(&mut key).await {
                for right in right_rows {
                    if let Some((timestamp, value)) = self.processor.left_join(
                        key.clone(),
                        record.timestamp,
                        value.clone(),
                        Some(right),
                        evict,
                    ) {
                        records.push(Record {
                            timestamp,
                            key: Some(key.clone()),
                            value,
                        });
                    }
                }
            } else if let Some((timestamp, value)) =
                self.processor
                    .left_join(key.clone(), record.timestamp, value.clone(), None, evict)
            {
                records.push(Record {
                    timestamp,
                    key: Some(key.clone()),
                    value,
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

    async fn process_right(&mut self, record: &Record<K, T2>, ctx: &mut Context<K, Output>) {
        if let Some(watermark) = ctx.watermark() {
            if record.timestamp < watermark {
                return;
            }
        };
        let mut key = record.key.clone().unwrap();
        let value = record.value.clone();
        let mut right_state = ctx.state.get_key_time_multi_map('r').await;
        let evict = right_state
            .get_all_values_with_timestamps(&mut key)
            .await
            .is_none();
        let key_to_insert = key.clone();
        let value_to_insert = value.clone();
        right_state
            .insert(record.timestamp, key_to_insert, value_to_insert)
            .await;

        let mut left_state: KeyTimeMultiMap<K, T1, _> = ctx.state.get_key_time_multi_map('l').await;
        let records = {
            let mut records = vec![];
            if let Some(left_rows) = left_state.get_all_values_with_timestamps(&mut key).await {
                for left in left_rows {
                    if let Some((timestamp, value)) = self.processor.right_join(
                        key.clone(),
                        record.timestamp,
                        value.clone(),
                        Some(left),
                        evict,
                    ) {
                        records.push(Record {
                            timestamp,
                            key: Some(key.clone()),
                            value,
                        });
                    }
                }
            } else if let Some((timestamp, value)) =
                self.processor
                    .right_join(key.clone(), record.timestamp, value.clone(), None, evict)
            {
                records.push(Record {
                    timestamp,
                    key: Some(key.clone()),
                    value,
                });
            }
            records
        };
        for record in records {
            ctx.collect(record).await;
        }
    }

    async fn handle_watermark(
        &mut self,
        _watermark: std::time::SystemTime,
        ctx: &mut Context<K, Output>,
    ) {
        let Some(watermark) = ctx.watermark() else {return};
        let mut left_state: KeyTimeMultiMap<K, T1, _> = ctx.state.get_key_time_multi_map('l').await;
        left_state.expire_entries_before(watermark - self.left_expiration);
        let mut right_state: KeyTimeMultiMap<K, T2, _> =
            ctx.state.get_key_time_multi_map('r').await;
        right_state.expire_entries_before(watermark - self.right_expiration);
        ctx.broadcast(arroyo_types::Message::Watermark(Watermark::EventTime(
            watermark,
        )))
        .await;
    }
}
