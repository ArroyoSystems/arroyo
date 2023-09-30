use std::{
    borrow::Cow,
    marker::PhantomData,
    time::{Duration, SystemTime},
};

use arroyo_macro::{co_process_fn, StreamNode};
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_state::{
    judy::tables::key_time_multimap::JudyKeyTimeMultiMap,
    tables::key_time_multi_map::KeyTimeMultiMap,
};
use arroyo_types::*;

use crate::engine::Context;

#[derive(StreamNode)]
pub struct JoinWithExpiration<
    K: Key + Sync,
    T1: Data + Sync,
    T2: Data + Sync,
    Output: Data,
    P: JoinProcessor<K, T1, T2, Output>,
> {
    left_expiration: Duration,
    right_expiration: Duration,
    processor: P,
    _t: PhantomData<(K, T1, T2, Output)>,
}

pub trait JoinProcessor<K: Key + Sync, T1: Data + Sync, T2: Data + Sync, Output: Data>:
    Send + 'static
{
    fn left_join(
        &self,
        key: K,
        left_timestamp: SystemTime,
        left_value: T1,
        right: Option<(SystemTime, Cow<T2>)>,
        first_left: bool,
    ) -> Option<(SystemTime, Output)>;
    fn right_join(
        &self,
        key: K,
        right_timestamp: SystemTime,
        right_value: T2,
        left: Option<(SystemTime, Cow<T1>)>,
        first_right: bool,
    ) -> Option<(SystemTime, Output)>;
}

pub struct LeftJoinProcessor<K: Key + Sync, T1: Data + Sync, T2: Data + Sync> {
    _t: PhantomData<(K, T1, T2)>,
}

impl<K: Key + Sync, T1: Data + Sync, T2: Data + Sync>
    JoinProcessor<K, T1, T2, UpdatingData<(T1, Option<T2>)>> for LeftJoinProcessor<K, T1, T2>
{
    fn left_join(
        &self,
        _key: K,
        left_timestamp: SystemTime,
        left_value: T1,
        right: Option<(SystemTime, Cow<T2>)>,
        _first_left: bool,
    ) -> Option<(SystemTime, UpdatingData<(T1, Option<T2>)>)> {
        match right {
            Some((right_timestamp, right_value)) => Some((
                left_timestamp.max(right_timestamp),
                UpdatingData::Append((left_value, Some(right_value.into_owned()))),
            )),
            None => Some((left_timestamp, UpdatingData::Append((left_value, None)))),
        }
    }

    fn right_join(
        &self,
        _key: K,
        right_timestamp: SystemTime,
        right_value: T2,
        left: Option<(SystemTime, Cow<T1>)>,
        first_right: bool,
    ) -> Option<(SystemTime, UpdatingData<(T1, Option<T2>)>)> {
        left.map(|(left_timestamp, left_value)| {
            if first_right {
                let left_value = left_value.into_owned();
                (
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Update {
                        old: (left_value.clone(), None),
                        new: (left_value, Some(right_value.clone())),
                    },
                )
            } else {
                (
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Append((left_value.into_owned(), Some(right_value))),
                )
            }
        })
    }
}

pub struct RightJoinProcessor<K: Key + Sync, T1: Data + Sync, T2: Data + Sync> {
    _t: PhantomData<(K, T1, T2)>,
}

impl<K: Key + Sync, T1: Data + Sync, T2: Data + Sync>
    JoinProcessor<K, T1, T2, UpdatingData<(Option<T1>, T2)>> for RightJoinProcessor<K, T1, T2>
{
    fn left_join(
        &self,
        _key: K,
        left_timestamp: SystemTime,
        left_value: T1,
        right: Option<(SystemTime, Cow<T2>)>,
        first_left: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, T2)>)> {
        right.map(|(right_timestamp, right_value)| {
            if first_left {
                let right = right_value.into_owned();
                (
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Update {
                        old: (None, right.clone()),
                        new: (Some(left_value.clone()), right),
                    },
                )
            } else {
                (
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Append((Some(left_value), right_value.into_owned())),
                )
            }
        })
    }

    fn right_join(
        &self,
        _key: K,
        right_timestamp: SystemTime,
        right_value: T2,
        left: Option<(SystemTime, Cow<T1>)>,
        _first_right: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, T2)>)> {
        match left {
            Some((left_timestamp, left_value)) => Some((
                left_timestamp.max(right_timestamp),
                UpdatingData::Append((Some(left_value.into_owned()), right_value)),
            )),
            None => Some((right_timestamp, UpdatingData::Append((None, right_value)))),
        }
    }
}

pub struct FullJoinProcessor<K: Key + Sync, T1: Data + Sync, T2: Data + Sync> {
    _t: PhantomData<(K, T1, T2)>,
}

impl<K: Key + Sync, T1: Data + Sync, T2: Data + Sync>
    JoinProcessor<K, T1, T2, UpdatingData<(Option<T1>, Option<T2>)>>
    for FullJoinProcessor<K, T1, T2>
{
    fn left_join(
        &self,
        _key: K,
        left_timestamp: SystemTime,
        left_value: T1,
        right: Option<(SystemTime, Cow<T2>)>,
        first_left: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, Option<T2>)>)> {
        match right {
            Some((right_timestamp, right_value)) => {
                if first_left {
                    let right = right_value.into_owned();
                    Some((
                        left_timestamp.max(right_timestamp),
                        UpdatingData::Update {
                            old: (None, Some(right.clone())),
                            new: (Some(left_value), Some(right)),
                        },
                    ))
                } else {
                    Some((
                        left_timestamp.max(right_timestamp),
                        UpdatingData::Append((Some(left_value), Some(right_value.into_owned()))),
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
        left: Option<(SystemTime, Cow<T1>)>,
        first_right: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, Option<T2>)>)> {
        match left {
            Some((left_timestamp, left_value)) => {
                if first_right {
                    let left_value = left_value.into_owned();
                    Some((
                        left_timestamp.max(right_timestamp),
                        UpdatingData::Update {
                            old: (Some(left_value.clone()), None),
                            new: (Some(left_value), Some(right_value)),
                        },
                    ))
                } else {
                    Some((
                        left_timestamp.max(right_timestamp),
                        UpdatingData::Append((Some(left_value.into_owned()), Some(right_value))),
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

pub struct InnerJoinProcessor<K: Key + Sync, T1: Data + Sync, T2: Data + Sync> {
    _t: PhantomData<(K, T1, T2)>,
}

impl<K: Key + Sync, T1: Data + Sync, T2: Data + Sync> JoinProcessor<K, T1, T2, (T1, T2)>
    for InnerJoinProcessor<K, T1, T2>
{
    fn left_join(
        &self,
        _key: K,
        left_timestamp: SystemTime,
        left_value: T1,
        right: Option<(SystemTime, Cow<T2>)>,
        _evict_prior: bool,
    ) -> Option<(SystemTime, (T1, T2))> {
        right.map(|(right_timestamp, right_value)| {
            (
                left_timestamp.max(right_timestamp),
                (left_value, right_value.into_owned()),
            )
        })
    }

    fn right_join(
        &self,
        _key: K,
        right_timestamp: SystemTime,
        right_value: T2,
        left: Option<(SystemTime, Cow<T1>)>,
        _evict_prior: bool,
    ) -> Option<(SystemTime, (T1, T2))> {
        left.map(|(left_timestamp, left_value)| {
            (
                left_timestamp.max(right_timestamp),
                (left_value.into_owned(), right_value),
            )
        })
    }
}

// Return left JoinWithExpiration
pub fn left_join<K: Key + Sync, T1: Data + Sync, T2: Data + Sync>(
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
pub fn right_join<K: Key + Sync, T1: Data + Sync, T2: Data + Sync>(
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
pub fn full_join<K: Key + Sync, T1: Data + Sync, T2: Data + Sync>(
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
pub fn inner_join<K: Key + Sync, T1: Data + Sync, T2: Data + Sync>(
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
impl<
        K: Key + Sync,
        T1: Data + Sync,
        T2: Data + Sync,
        Output: Data,
        P: JoinProcessor<K, T1, T2, Output>,
    > JoinWithExpiration<K, T1, T2, Output, P>
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
        if let Some(watermark) = ctx.last_present_watermark() {
            if record.timestamp < watermark {
                return;
            }
        };
        let mut key = record.key.clone().unwrap();
        let value = record.value.clone();

        let mut left_state: &mut JudyKeyTimeMultiMap<K, T1> =
            ctx.state.get_key_time_multi_map('l').await;
        // TODO: this could be faster
        let first_left = left_state
            .get_all_values_with_timestamps(&mut key)
            .is_empty();
        let mut right_state: &mut JudyKeyTimeMultiMap<K, T2> =
            ctx.state.get_key_time_multi_map('r').await;
        let records = {
            let mut records = vec![];
            let right_rows = right_state.get_all_values_with_timestamps(&mut key);
            if !right_rows.is_empty() {
                for right in right_rows {
                    if let Some((timestamp, value)) = self.processor.left_join(
                        key.clone(),
                        record.timestamp,
                        value.clone(),
                        Some(right),
                        first_left,
                    ) {
                        records.push(Record {
                            timestamp,
                            key: Some(key.clone()),
                            value,
                        });
                    }
                }
            } else if let Some((timestamp, value)) = self.processor.left_join(
                key.clone(),
                record.timestamp,
                value.clone(),
                None,
                first_left,
            ) {
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
        left_state.insert(record.timestamp, key, value);
    }

    async fn process_right(&mut self, record: &Record<K, T2>, ctx: &mut Context<K, Output>) {
        if let Some(watermark) = ctx.last_present_watermark() {
            if record.timestamp < watermark {
                return;
            }
        };
        let mut key = record.key.clone().unwrap();
        let value = record.value.clone();
        let mut right_state = ctx.state.get_key_time_multi_map('r').await;
        let first_right = right_state
            .get_all_values_with_timestamps(&mut key)
            .is_empty();
        let key_to_insert = key.clone();
        let value_to_insert = value.clone();
        right_state.insert(record.timestamp, key_to_insert, value_to_insert);

        let mut left_state = ctx.state.get_key_time_multi_map('l').await;
        let records = {
            let mut records = vec![];
            let left_rows = left_state.get_all_values_with_timestamps(&mut key);
            if !left_rows.is_empty() {
                for left in left_rows {
                    if let Some((timestamp, value)) = self.processor.right_join(
                        key.clone(),
                        record.timestamp,
                        value.clone(),
                        Some(left),
                        first_right,
                    ) {
                        records.push(Record {
                            timestamp,
                            key: Some(key.clone()),
                            value,
                        });
                    }
                }
            } else if let Some((timestamp, value)) = self.processor.right_join(
                key.clone(),
                record.timestamp,
                value.clone(),
                None,
                first_right,
            ) {
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

    async fn handle_watermark(&mut self, watermark: Watermark, ctx: &mut Context<K, Output>) {
        match watermark {
            Watermark::EventTime(watermark) => {
                ctx.state.handle_watermark(watermark);
            }
            Watermark::Idle => (),
        };

        ctx.broadcast(arroyo_types::Message::Watermark(watermark))
            .await;
    }
}
