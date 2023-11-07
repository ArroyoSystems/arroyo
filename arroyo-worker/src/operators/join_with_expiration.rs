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
    fn process_left(
        &self,
        left_record: &Record<K, T1>,
        right: Option<(SystemTime, &T2)>,
        first_left: bool,
    ) -> Option<(SystemTime, Output)>;
    fn process_right(
        &self,
        right_record: &Record<K, T2>,
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
    fn process_left(
        &self,
        left_record: &Record<K, T1>,
        right: Option<(SystemTime, &T2)>,
        _first_left: bool,
    ) -> Option<(SystemTime, UpdatingData<(T1, Option<T2>)>)> {
        match right {
            Some((right_timestamp, right_value)) => Some((
                left_record.timestamp.max(right_timestamp),
                UpdatingData::Append((left_record.value.clone(), Some(right_value.clone()))),
            )),
            None => Some((
                left_record.timestamp,
                UpdatingData::Append((left_record.value.clone(), None)),
            )),
        }
    }

    fn process_right(
        &self,
        right_record: &Record<K, T2>,
        left: Option<(SystemTime, &T1)>,
        first_right: bool,
    ) -> Option<(SystemTime, UpdatingData<(T1, Option<T2>)>)> {
        left.map(|(left_timestamp, left_value)| {
            if first_right {
                (
                    left_timestamp.max(right_record.timestamp),
                    UpdatingData::Update {
                        old: (left_value.clone(), None),
                        new: (left_value.clone(), Some(right_record.value.clone())),
                    },
                )
            } else {
                (
                    left_timestamp.max(right_record.timestamp),
                    UpdatingData::Append((left_value.clone(), Some(right_record.value.clone()))),
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
    fn process_left(
        &self,
        left_record: &Record<K, T1>,
        right: Option<(SystemTime, &T2)>,
        first_left: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, T2)>)> {
        right.map(|(right_timestamp, right_value)| {
            if first_left {
                (
                    left_record.timestamp.max(right_timestamp),
                    UpdatingData::Update {
                        old: (None, right_value.clone()),
                        new: (Some(left_record.value.clone()), right_value.clone()),
                    },
                )
            } else {
                (
                    left_record.timestamp.max(right_timestamp),
                    UpdatingData::Append((Some(left_record.value.clone()), right_value.clone())),
                )
            }
        })
    }

    fn process_right(
        &self,
        right_record: &Record<K, T2>,
        left: Option<(SystemTime, &T1)>,
        _first_right: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, T2)>)> {
        match left {
            Some((left_timestamp, left_value)) => Some((
                left_timestamp.max(right_record.timestamp),
                UpdatingData::Append((Some(left_value.clone()), right_record.value.clone())),
            )),
            None => Some((
                right_record.timestamp,
                UpdatingData::Append((None, right_record.value.clone())),
            )),
        }
    }
}

pub struct FullJoinProcessor<K: Key, T1: Data, T2: Data> {
    _t: PhantomData<(K, T1, T2)>,
}

impl<K: Key, T1: Data, T2: Data> JoinProcessor<K, T1, T2, UpdatingData<(Option<T1>, Option<T2>)>>
    for FullJoinProcessor<K, T1, T2>
{
    fn process_left(
        &self,
        left_record: &Record<K, T1>,
        right: Option<(SystemTime, &T2)>,
        first_left: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, Option<T2>)>)> {
        let left_value = left_record.value.clone();
        match right {
            Some((right_timestamp, right_value)) => {
                if first_left {
                    Some((
                        left_record.timestamp.max(right_timestamp),
                        UpdatingData::Update {
                            old: (None, Some(right_value.clone())),
                            new: (Some(left_value), Some(right_value.clone())),
                        },
                    ))
                } else {
                    Some((
                        left_record.timestamp.max(right_timestamp),
                        UpdatingData::Append((Some(left_value), Some(right_value.clone()))),
                    ))
                }
            }
            None => Some((
                left_record.timestamp,
                UpdatingData::Append((Some(left_value), None)),
            )),
        }
    }

    fn process_right(
        &self,
        right_record: &Record<K, T2>,
        left: Option<(SystemTime, &T1)>,
        first_right: bool,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, Option<T2>)>)> {
        let right_value = right_record.value.clone();
        match left {
            Some((left_timestamp, left_value)) => {
                if first_right {
                    Some((
                        left_timestamp.max(right_record.timestamp),
                        UpdatingData::Update {
                            old: (Some(left_value.clone()), None),
                            new: (Some(left_value.clone()), Some(right_value)),
                        },
                    ))
                } else {
                    Some((
                        left_timestamp.max(right_record.timestamp),
                        UpdatingData::Append((Some(left_value.clone()), Some(right_value))),
                    ))
                }
            }
            None => Some((
                right_record.timestamp,
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
    fn process_left(
        &self,
        left_record: &Record<K, T1>,
        right: Option<(SystemTime, &T2)>,
        _evict_prior: bool,
    ) -> Option<(SystemTime, (T1, T2))> {
        right.map(|(right_timestamp, right_value)| {
            (
                left_record.timestamp.max(right_timestamp),
                (left_record.value.clone(), right_value.clone()),
            )
        })
    }

    fn process_right(
        &self,
        right_record: &Record<K, T2>,
        left: Option<(SystemTime, &T1)>,
        _evict_prior: bool,
    ) -> Option<(SystemTime, (T1, T2))> {
        left.map(|(left_timestamp, left_value)| {
            (
                left_timestamp.max(right_record.timestamp),
                (left_value.clone(), right_record.value.clone()),
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

    async fn process_left(&mut self, left_record: &Record<K, T1>, ctx: &mut Context<K, Output>) {
        if let Some(watermark) = ctx.last_present_watermark() {
            if left_record.timestamp < watermark {
                return;
            }
        };
        let mut key = left_record.key.clone().unwrap();
        let value = left_record.value.clone();

        let mut left_state: KeyTimeMultiMap<K, T1, _> = ctx.state.get_key_time_multi_map('l').await;
        let first_left = left_state
            .get_all_values_with_timestamps(&mut key)
            .await
            .is_none();
        let mut right_state: KeyTimeMultiMap<K, T2, _> =
            ctx.state.get_key_time_multi_map('r').await;

        let mut out_records = vec![];
        if let Some(right_rows) = right_state.get_all_values_with_timestamps(&mut key).await {
            out_records.extend(
                right_rows
                    .filter_map(|(timestamp, value)| {
                        self.processor.process_left(
                            &left_record,
                            Some((timestamp.clone(), value)),
                            first_left,
                        )
                    })
                    .map(|(timestamp, value)| Record {
                        timestamp,
                        key: Some(key.clone()),
                        value,
                    }),
            );
        } else {
            self.processor
                .process_left(&left_record, None, first_left)
                .map(|(timestamp, value)| {
                    out_records.push(Record {
                        timestamp,
                        key: Some(key.clone()),
                        value,
                    });
                });
        }

        for record in out_records {
            ctx.collect(record).await;
        }
        let mut left_state = ctx.state.get_key_time_multi_map('l').await;
        left_state.insert(left_record.timestamp, key, value).await;
    }

    async fn process_right(&mut self, right_record: &Record<K, T2>, ctx: &mut Context<K, Output>) {
        if let Some(watermark) = ctx.last_present_watermark() {
            if right_record.timestamp < watermark {
                return;
            }
        };
        let mut key = right_record.key.clone().unwrap();
        let value = right_record.value.clone();
        let mut right_state = ctx.state.get_key_time_multi_map('r').await;
        let first_right = right_state
            .get_all_values_with_timestamps(&mut key)
            .await
            .is_none();
        let key_to_insert = key.clone();
        let value_to_insert = value.clone();
        right_state
            .insert(right_record.timestamp, key_to_insert, value_to_insert)
            .await;

        let mut left_state: KeyTimeMultiMap<K, T1, _> = ctx.state.get_key_time_multi_map('l').await;

        let mut out_records = vec![];
        if let Some(left_rows) = left_state.get_all_values_with_timestamps(&mut key).await {
            out_records.extend(
                left_rows
                    .filter_map(|(timestamp, value)| {
                        self.processor.process_right(
                            &right_record,
                            Some((timestamp.clone(), value)),
                            first_right,
                        )
                    })
                    .map(|(timestamp, value)| Record {
                        timestamp,
                        key: Some(key.clone()),
                        value,
                    }),
            );
        } else {
            self.processor
                .process_right(&right_record, None, first_right)
                .map(|(timestamp, value)| {
                    out_records.push(Record {
                        timestamp,
                        key: Some(key.clone()),
                        value,
                    });
                });
        }

        for out_records in out_records {
            ctx.collect(out_records).await;
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
