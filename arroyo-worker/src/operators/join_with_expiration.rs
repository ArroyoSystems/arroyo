use std::{
    marker::PhantomData,
    time::{Duration, SystemTime},
};

use arroyo_macro::{co_process_fn, StreamNode};
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_state::tables::key_time_multi_map::KeyTimeMultiMap;
use arroyo_types::*;

use crate::engine::Context;

#[derive(StreamNode)]
pub struct JoinWithExpiration<
    K: Key,
    InT1: Data,
    InT2: Data,
    P1: IncomingDataProcessor<InT1, T1>,
    P2: IncomingDataProcessor<InT2, T2>,
    T1: Data,
    T2: Data,
    Output: Data,
    P: JoinProcessor<K, T1, T2, Output>,
> {
    left_expiration: Duration,
    right_expiration: Duration,
    processor: P, 
    _t: PhantomData<(K, P1, P2, InT1, InT2, T1, T2, Output)>,
}

pub trait IncomingDataProcessor<In: Data, Out: Data> : Send + 'static {
    fn process_incoming(incoming: In) -> UpdatingData<Out>;
 }

 struct IdentityProcessor<T: Data> {
     _t: PhantomData<T>,
 }

impl<T: Data> IncomingDataProcessor<UpdatingData<T>, T> for IdentityProcessor<T> {
    fn process_incoming(incoming: UpdatingData<T>) -> UpdatingData<T> {
        incoming
    }
}

pub struct CoerceToUpdatingProcessor<T: Data> {
    _t: PhantomData<T>,
}

impl<T: Data> IncomingDataProcessor<T, T> for CoerceToUpdatingProcessor<T> {
    fn process_incoming(incoming: T) -> UpdatingData<T> {
        UpdatingData::Append(incoming)
    }
}
    

pub trait JoinProcessor<K: Key, T1: Data, T2: Data, Output: Data>: Send + 'static {
    fn left_join(
        &self,
        key: K,
        left_timestamp: SystemTime,
        left_value: T1,
        right: Option<(SystemTime, &T2)>,
        first_left: bool,
    ) -> Option<(SystemTime, Output)>;
    fn right_join(
        &self,
        key: K,
        right_timestamp: SystemTime,
        right_value: T2,
        left: Option<(SystemTime, &T1)>,
        first_right: bool,
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
        _first_left: bool,
    ) -> Option<(SystemTime, UpdatingData<(T1, Option<T2>)>)> {
        match right {
            Some((right_timestamp, right_value)) => Some((
                left_timestamp.max(right_timestamp),
                UpdatingData::Append((left_value, Some(right_value.clone()))),
            )),
            None => Some((left_timestamp, UpdatingData::Append((left_value, None)))),
        }
    }

    fn right_join(
        &self,
        _key: K,
        right_timestamp: SystemTime,
        right_value: T2,
        left: Option<(SystemTime, &T1)>,
        first_right: bool,
    ) -> Option<(SystemTime, UpdatingData<(T1, Option<T2>)>)> {
        left.map(|(left_timestamp, left_value)| {
            if first_right {
                (
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Update {
                        old: (left_value.clone(), None),
                        new: (left_value.clone(), Some(right_value.clone())),
                    },
                )
            } else {
                (
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Append((left_value.clone(), Some(right_value))),
                )
            }
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
        first_left: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, T2)>)> {
        right.map(|(right_timestamp, right_value)| {
            if first_left {
                (
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Update {
                        old: (None, right_value.clone()),
                        new: (Some(left_value.clone()), right_value.clone()),
                    },
                )
            } else {
                (
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Append((Some(left_value), right_value.clone())),
                )
            }
        })
    }

    fn right_join(
        &self,
        _key: K,
        right_timestamp: SystemTime,
        right_value: T2,
        left: Option<(SystemTime, &T1)>,
        _first_right: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, T2)>)> {
        match left {
            Some((left_timestamp, left_value)) => Some((
                left_timestamp.max(right_timestamp),
                UpdatingData::Append((Some(left_value.clone()), right_value)),
            )),
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
        first_left: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, Option<T2>)>)> {
        match right {
            Some((right_timestamp, right_value)) => {
                if first_left {
                    Some((
                        left_timestamp.max(right_timestamp),
                        UpdatingData::Update {
                            old: (None, Some(right_value.clone())),
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
        first_right: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, Option<T2>)>)> {
        match left {
            Some((left_timestamp, left_value)) => {
                if first_right {
                    Some((
                        left_timestamp.max(right_timestamp),
                        UpdatingData::Update {
                            old: (Some(left_value.clone()), None),
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
) -> JoinWithExpiration<K, T1, T2, CoerceToUpdatingProcessor<T1>, CoerceToUpdatingProcessor<T2>, T1, T2, UpdatingData<(T1, Option<T2>)>, LeftJoinProcessor<K, T1, T2>> {
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
) -> JoinWithExpiration<K, T1, T2, CoerceToUpdatingProcessor<T1>, CoerceToUpdatingProcessor<T2>,  T1, T2, UpdatingData<(Option<T1>, T2)>, RightJoinProcessor<K, T1, T2>> {
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
    K,  T1, T2, CoerceToUpdatingProcessor<T1>, CoerceToUpdatingProcessor<T2>, 
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
) -> JoinWithExpiration<K, T1, T2, CoerceToUpdatingProcessor<T1>, CoerceToUpdatingProcessor<T2>,  T1, T2, (T1, T2), InnerJoinProcessor<K, T1, T2>> {
    JoinWithExpiration::new(
        left_expiration,
        right_expiration,
        InnerJoinProcessor { _t: PhantomData },
    )
}

pub fn inner_join_left_updating<K: Key, T1: Data, T2: Data> (
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<K, UpdatingData<T1>, T2, IdentityProcessor<T1>, CoerceToUpdatingProcessor<T2>, T1, T2, UpdatingData<(T1, T2)>, LeftJoinProcessor<K, T1, T2>> {
    JoinWithExpiration::new(
        left_expiration,
        right_expiration,
        LeftJoinProcessor { _t: PhantomData },
    )
}

#[co_process_fn(in_k1=K, in_t1=InT1, in_k2=K, in_t2=InT2, out_k=K, out_t=Output)]
impl<K: Key,
InT1: Data,
InT2: Data,
P1: IncomingDataProcessor<InT1, T1>,
P2: IncomingDataProcessor<InT2, T2>,
 T1: Data, T2: Data, Output: Data, P: JoinProcessor<K, T1, T2, Output>>
    JoinWithExpiration<K, InT1, InT2, P1, P2,  T1, T2, Output, P>
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

    async fn process_left(&mut self, record: &Record<K, InT1>, ctx: &mut Context<K, Output>) {
        if let Some(watermark) = ctx.last_present_watermark() {
            if record.timestamp < watermark {
                return;
            }
        };
        let mut key = record.key.clone().unwrap();
        let value = P1::process_incoming(record.value.clone());
        let UpdatingData::Append(value) = value else {
            panic!()
        };

        let mut left_state: KeyTimeMultiMap<K, T1, _> = ctx.state.get_key_time_multi_map('l').await;
        let first_left = left_state
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
        left_state.insert(record.timestamp, key, value).await;
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
                let mut left_state: KeyTimeMultiMap<K, T1, _> =
                    ctx.state.get_key_time_multi_map('l').await;
                left_state
                    .expire_entries_before(watermark - self.left_expiration)
                    .await;

                let mut right_state: KeyTimeMultiMap<K, T2, _> =
                    ctx.state.get_key_time_multi_map('r').await;
                right_state
                    .expire_entries_before(watermark - self.right_expiration)
                    .await;
            }
            Watermark::Idle => (),
        };

        ctx.broadcast(arroyo_types::Message::Watermark(watermark))
            .await;
    }
}
