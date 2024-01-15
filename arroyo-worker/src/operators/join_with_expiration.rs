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

pub trait JoinProcessor<K: Key, T1: Data, T2: Data, Output: Data>: Send + 'static {
    fn process_left(
        &self,
        left_timestamp: SystemTime,
        left_update: UpdatingData<T1>,
        right: Option<(SystemTime, &T2)>,
        left_count: usize,
    ) -> Option<(SystemTime, Output)>;
    fn process_right(
        &self,
        right_timestamp: SystemTime,
        right_update: UpdatingData<T2>,
        left: Option<(SystemTime, &T1)>,
        right_count: usize,
    ) -> Option<(SystemTime, Output)>;
}

pub trait IncomingDataProcessor<In: Data, Out: Data>: Send + 'static {
    fn ensure_updating(incoming: In) -> UpdatingData<Out>;
}

pub struct NoOpProcessor<T: Data> {
    _t: PhantomData<T>,
}

impl<T: Data> IncomingDataProcessor<UpdatingData<T>, T> for NoOpProcessor<T> {
    fn ensure_updating(incoming: UpdatingData<T>) -> UpdatingData<T> {
        incoming
    }
}

pub struct Coercer<T: Data> {
    _t: PhantomData<T>,
}

impl<T: Data> IncomingDataProcessor<T, T> for Coercer<T> {
    fn ensure_updating(incoming: T) -> UpdatingData<T> {
        UpdatingData::Append(incoming)
    }
}

pub struct LeftJoinProcessor<K: Key, T1: Data, T2: Data> {
    pub _t: PhantomData<(K, T1, T2)>,
}

impl<K: Key, T1: Data, T2: Data> LeftJoinProcessor<K, T1, T2> {
    pub fn new() -> Self {
        Self { _t: PhantomData }
    }
}

impl<K: Key, T1: Data, T2: Data> JoinProcessor<K, T1, T2, UpdatingData<(T1, Option<T2>)>>
    for LeftJoinProcessor<K, T1, T2>
{
    fn process_left(
        &self,
        left_timestamp: SystemTime,
        left_update: UpdatingData<T1>,
        right: Option<(SystemTime, &T2)>,
        _first_left: usize,
    ) -> Option<(SystemTime, UpdatingData<(T1, Option<T2>)>)> {
        match left_update {
            UpdatingData::Append(left) => match right {
                Some((right_timestamp, right)) => Some((
                    left_timestamp.max(right_timestamp).clone(),
                    UpdatingData::Append((left.clone(), Some(right.clone()))),
                )),
                None => Some((left_timestamp, UpdatingData::Append((left.clone(), None)))),
            },
            UpdatingData::Update { old, new } => match right {
                Some((right_timestamp, right)) => Some((
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Update {
                        old: (old.clone(), Some(right.clone())),
                        new: (new.clone(), Some(right.clone())),
                    },
                )),
                None => Some((
                    left_timestamp,
                    UpdatingData::Update {
                        old: (old.clone(), None),
                        new: (new.clone(), None),
                    },
                )),
            },
            UpdatingData::Retract(left) => match right {
                Some((right_timestamp, right)) => Some((
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Retract((left.clone(), Some(right.clone()))),
                )),
                None => Some((left_timestamp, UpdatingData::Retract((left.clone(), None)))),
            },
        }
    }

    fn process_right(
        &self,
        right_timestamp: SystemTime,
        right_update: UpdatingData<T2>,
        left: Option<(SystemTime, &T1)>,
        right_count: usize,
    ) -> Option<(SystemTime, UpdatingData<(T1, Option<T2>)>)> {
        let Some((left_timestamp, left)) = left else {
            return None;
        };

        let timestamp = left_timestamp.max(right_timestamp);

        match right_update {
            UpdatingData::Append(right) => {
                if right_count == 0 {
                    Some((
                        timestamp,
                        UpdatingData::Update {
                            old: (left.clone(), None),
                            new: (left.clone(), Some(right)),
                        },
                    ))
                } else {
                    Some((timestamp, UpdatingData::Append((left.clone(), Some(right)))))
                }
            }
            UpdatingData::Update { old, new } => Some((
                timestamp,
                UpdatingData::Update {
                    old: (left.clone(), Some(old)),
                    new: (left.clone(), Some(new)),
                },
            )),
            UpdatingData::Retract(right) => {
                if right_count == 1 {
                    Some((
                        timestamp,
                        UpdatingData::Update {
                            old: (left.clone(), Some(right.clone())),
                            new: (left.clone(), None),
                        },
                    ))
                } else {
                    Some((
                        timestamp,
                        UpdatingData::Retract((left.clone(), Some(right.clone()))),
                    ))
                }
            }
        }
    }
}

pub struct RightJoinProcessor<K: Key, T1: Data, T2: Data> {
    pub _t: PhantomData<(K, T1, T2)>,
}

impl<K: Key, T1: Data, T2: Data> RightJoinProcessor<K, T1, T2> {
    pub fn new() -> Self {
        Self { _t: PhantomData }
    }
}

impl<K: Key, T1: Data, T2: Data> JoinProcessor<K, T1, T2, UpdatingData<(Option<T1>, T2)>>
    for RightJoinProcessor<K, T1, T2>
{
    fn process_left(
        &self,
        left_timestamp: SystemTime,
        left_update: UpdatingData<T1>,
        right: Option<(SystemTime, &T2)>,
        left_count: usize,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, T2)>)> {
        let Some((right_timestamp, right)) = right else {
            return None;
        };

        let timestamp = left_timestamp.max(right_timestamp);

        match left_update {
            UpdatingData::Append(left) => {
                if left_count == 0 {
                    Some((
                        timestamp,
                        UpdatingData::Update {
                            old: (None, right.clone()),
                            new: (Some(left), right.clone()),
                        },
                    ))
                } else {
                    Some((
                        timestamp,
                        UpdatingData::Append((Some(left.clone()), right.clone())),
                    ))
                }
            }
            UpdatingData::Update { old, new } => Some((
                timestamp,
                UpdatingData::Update {
                    old: (Some(old), right.clone()),
                    new: (Some(new), right.clone()),
                },
            )),
            UpdatingData::Retract(left) => {
                if left_count == 1 {
                    Some((
                        timestamp,
                        UpdatingData::Update {
                            old: (Some(left.clone()), right.clone()),
                            new: (None, right.clone()),
                        },
                    ))
                } else {
                    Some((
                        timestamp,
                        UpdatingData::Retract((Some(left.clone()), right.clone())),
                    ))
                }
            }
        }
    }

    fn process_right(
        &self,
        right_timestamp: SystemTime,
        right_update: UpdatingData<T2>,
        left: Option<(SystemTime, &T1)>,
        _first_left: usize,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, T2)>)> {
        match right_update {
            UpdatingData::Append(right) => match left {
                Some((left_timestamp, left)) => Some((
                    left_timestamp.max(right_timestamp).clone(),
                    UpdatingData::Append((Some(left.clone()), right.clone())),
                )),
                None => Some((right_timestamp, UpdatingData::Append((None, right.clone())))),
            },
            UpdatingData::Update { old, new } => match left {
                Some((left_timestamp, left)) => Some((
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Update {
                        old: (Some(left.clone()), old.clone()),
                        new: (Some(left.clone()), new.clone()),
                    },
                )),
                None => Some((
                    right_timestamp,
                    UpdatingData::Update {
                        old: (None, old.clone()),
                        new: (None, new.clone()),
                    },
                )),
            },
            UpdatingData::Retract(right) => match left {
                Some((left_timestamp, left)) => Some((
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Retract((Some(left.clone()), right.clone())),
                )),
                None => Some((
                    right_timestamp,
                    UpdatingData::Retract((None, right.clone())),
                )),
            },
        }
    }
}

pub struct FullJoinProcessor<K: Key, T1: Data, T2: Data> {
    pub(crate) _t: PhantomData<(K, T1, T2)>,
}

impl<K: Key, T1: Data, T2: Data> FullJoinProcessor<K, T1, T2> {
    pub fn new() -> Self {
        Self { _t: PhantomData }
    }
}

impl<K: Key, T1: Data, T2: Data> JoinProcessor<K, T1, T2, UpdatingData<(Option<T1>, Option<T2>)>>
    for FullJoinProcessor<K, T1, T2>
{
    fn process_left(
        &self,
        left_timestamp: SystemTime,
        left_update: UpdatingData<T1>,
        right: Option<(SystemTime, &T2)>,
        left_count: usize,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, Option<T2>)>)> {
        match left_update {
            UpdatingData::Append(left) => match right {
                Some((right_timestamp, right)) => {
                    if left_count == 0 {
                        Some((
                            left_timestamp.max(right_timestamp),
                            UpdatingData::Update {
                                old: (None, Some(right.clone())),
                                new: (Some(left.clone()), Some(right.clone())),
                            },
                        ))
                    } else {
                        Some((
                            left_timestamp.max(right_timestamp),
                            UpdatingData::Append((Some(left.clone()), Some(right.clone()))),
                        ))
                    }
                }
                None => Some((
                    left_timestamp,
                    UpdatingData::Append((Some(left.clone()), None)),
                )),
            },
            UpdatingData::Update { old, new } => match right {
                Some((right_timestamp, right)) => Some((
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Update {
                        old: (Some(old.clone()), Some(right.clone())),
                        new: (Some(new.clone()), Some(right.clone())),
                    },
                )),
                None => Some((
                    left_timestamp,
                    UpdatingData::Update {
                        old: (Some(old.clone()), None),
                        new: (Some(new.clone()), None),
                    },
                )),
            },
            UpdatingData::Retract(left) => match right {
                Some((right_timestamp, right)) => {
                    if left_count == 1 {
                        Some((
                            left_timestamp.max(right_timestamp),
                            UpdatingData::Update {
                                old: (Some(left.clone()), Some(right.clone())),
                                new: (None, Some(right.clone())),
                            },
                        ))
                    } else {
                        Some((
                            left_timestamp.max(right_timestamp),
                            UpdatingData::Retract((Some(left.clone()), Some(right.clone()))),
                        ))
                    }
                }
                None => Some((
                    left_timestamp,
                    UpdatingData::Retract((Some(left.clone()), None)),
                )),
            },
        }
    }

    fn process_right(
        &self,
        right_timestamp: SystemTime,
        right_update: UpdatingData<T2>,
        left: Option<(SystemTime, &T1)>,
        right_count: usize,
    ) -> Option<(SystemTime, UpdatingData<(Option<T1>, Option<T2>)>)> {
        match right_update {
            UpdatingData::Append(right) => match left {
                Some((left_timestamp, left)) => {
                    if right_count == 0 {
                        Some((
                            right_timestamp.max(left_timestamp),
                            UpdatingData::Update {
                                old: (Some(left.clone()), None),
                                new: (Some(left.clone()), Some(right.clone())),
                            },
                        ))
                    } else {
                        Some((
                            right_timestamp.max(left_timestamp),
                            UpdatingData::Append((Some(left.clone()), Some(right.clone()))),
                        ))
                    }
                }
                None => Some((
                    right_timestamp,
                    UpdatingData::Append((None, Some(right.clone()))),
                )),
            },
            UpdatingData::Update { old, new } => match left {
                Some((left_timestamp, left)) => Some((
                    left_timestamp.max(right_timestamp),
                    UpdatingData::Update {
                        old: (Some(left.clone()), Some(old.clone())),
                        new: (Some(left.clone()), Some(new.clone())),
                    },
                )),
                None => Some((
                    right_timestamp,
                    UpdatingData::Update {
                        old: (None, Some(old.clone())),
                        new: (None, Some(new.clone())),
                    },
                )),
            },
            UpdatingData::Retract(right) => match left {
                Some((left_timestamp, left)) => {
                    if right_count == 1 {
                        Some((
                            left_timestamp.max(right_timestamp),
                            UpdatingData::Update {
                                old: (Some(left.clone()), Some(right.clone())),
                                new: (Some(left.clone()), None),
                            },
                        ))
                    } else {
                        Some((
                            left_timestamp.max(right_timestamp),
                            UpdatingData::Retract((Some(left.clone()), Some(right.clone()))),
                        ))
                    }
                }
                None => Some((
                    right_timestamp,
                    UpdatingData::Retract((None, Some(right.clone()))),
                )),
            },
        }
    }
}

pub struct InnerJoinProcessor<K: Key, T1: Data, T2: Data> {
    pub(crate) _t: PhantomData<(K, T1, T2)>,
}

impl<K: Key, T1: Data, T2: Data> InnerJoinProcessor<K, T1, T2> {
    pub fn new() -> Self {
        Self { _t: PhantomData }
    }
}

impl<K: Key, T1: Data, T2: Data> JoinProcessor<K, T1, T2, UpdatingData<(T1, T2)>>
    for InnerJoinProcessor<K, T1, T2>
{
    fn process_left(
        &self,
        left_timestamp: SystemTime,
        left_update: UpdatingData<T1>,
        right: Option<(SystemTime, &T2)>,
        _left_count: usize,
    ) -> Option<(SystemTime, UpdatingData<(T1, T2)>)> {
        right.map(|(right_timestamp, right)| match left_update {
            UpdatingData::Append(left) => (
                left_timestamp.max(right_timestamp),
                UpdatingData::Append((left.clone(), right.clone())),
            ),
            UpdatingData::Update { old, new } => (
                left_timestamp.max(right_timestamp),
                UpdatingData::Update {
                    old: (old.clone(), right.clone()),
                    new: (new.clone(), right.clone()),
                },
            ),
            UpdatingData::Retract(left) => (
                left_timestamp.max(right_timestamp),
                UpdatingData::Retract((left.clone(), right.clone())),
            ),
        })
    }

    fn process_right(
        &self,
        right_timestamp: SystemTime,
        right_update: UpdatingData<T2>,
        left: Option<(SystemTime, &T1)>,
        _right_count: usize,
    ) -> Option<(SystemTime, UpdatingData<(T1, T2)>)> {
        left.map(|(left_timestamp, left)| match right_update {
            UpdatingData::Append(right) => (
                left_timestamp.max(right_timestamp),
                UpdatingData::Append((left.clone(), right.clone())),
            ),
            UpdatingData::Update { old, new } => (
                left_timestamp.max(right_timestamp),
                UpdatingData::Update {
                    old: (left.clone(), old.clone()),
                    new: (left.clone(), new.clone()),
                },
            ),
            UpdatingData::Retract(right) => (
                left_timestamp.max(right_timestamp),
                UpdatingData::Retract((left.clone(), right.clone())),
            ),
        })
    }
}

#[co_process_fn(in_k1=K, in_t1=InT1, in_k2=K, in_t2=InT2, out_k=K, out_t=Output)]
impl<
        K: Key,
        InT1: Data,
        InT2: Data,
        P1: IncomingDataProcessor<InT1, T1>,
        P2: IncomingDataProcessor<InT2, T2>,
        T1: Data,
        T2: Data,
        Output: Data,
        P: JoinProcessor<K, T1, T2, Output>,
    > JoinWithExpiration<K, InT1, InT2, P1, P2, T1, T2, Output, P>
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

    async fn update_state<T: Data>(
        &mut self,
        ctx: &mut Context<K, Output>,
        key: K,
        timestamp: SystemTime,
        updating_data: UpdatingData<T>,
        table: char,
    ) {
        let mut state = ctx.state.get_key_time_multi_map(table).await;
        match updating_data {
            UpdatingData::Append(value) => {
                state.insert(timestamp, key, value).await;
            }
            UpdatingData::Update { old, new } => {
                let k = key.clone();
                state.delete_value(timestamp, k, old).await;
                state.insert(timestamp, key, new).await;
            }
            UpdatingData::Retract(value) => {
                state.delete_value(timestamp, key, value).await;
            }
        }
    }

    async fn process_left(&mut self, left_record: &Record<K, InT1>, ctx: &mut Context<K, Output>) {
        if let Some(watermark) = ctx.last_present_watermark() {
            if left_record.timestamp < watermark {
                return;
            }
        };
        let mut key = left_record.key.clone().unwrap();
        let left_update = P1::ensure_updating(left_record.value.clone());

        let mut left_state: KeyTimeMultiMap<K, T1, _> = ctx.state.get_key_time_multi_map('l').await;
        let left_count = left_state
            .get_all_values_with_timestamps(&mut key)
            .await
            .map_or(0, |values| values.count());

        let mut right_state: KeyTimeMultiMap<K, T2, _> =
            ctx.state.get_key_time_multi_map('r').await;

        let mut out_records = vec![];
        if let Some(right_rows) = right_state.get_all_values_with_timestamps(&mut key).await {
            out_records.extend(
                right_rows
                    .filter_map(|(timestamp, value)| {
                        self.processor.process_left(
                            left_record.timestamp.clone(),
                            left_update.clone(),
                            Some((timestamp.clone(), value)),
                            left_count,
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
                .process_left(left_record.timestamp, left_update.clone(), None, left_count)
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

        self.update_state(ctx, key, left_record.timestamp, left_update, 'l')
            .await;
    }

    async fn process_right(
        &mut self,
        right_record: &Record<K, InT2>,
        ctx: &mut Context<K, Output>,
    ) {
        if let Some(watermark) = ctx.last_present_watermark() {
            if right_record.timestamp < watermark {
                return;
            }
        };

        let mut key = right_record.key.clone().unwrap();
        let right_update = P2::ensure_updating(right_record.value.clone());

        let mut right_state: KeyTimeMultiMap<K, T2, _> =
            ctx.state.get_key_time_multi_map('r').await;
        let right_count = right_state
            .get_all_values_with_timestamps(&mut key)
            .await
            .map_or(0, |values| values.count());

        let mut left_state: KeyTimeMultiMap<K, T1, _> = ctx.state.get_key_time_multi_map('l').await;

        let mut out_records = vec![];
        if let Some(left_rows) = left_state.get_all_values_with_timestamps(&mut key).await {
            out_records.extend(
                left_rows
                    .filter_map(|(timestamp, value)| {
                        self.processor.process_right(
                            right_record.timestamp.clone(),
                            right_update.clone(),
                            Some((timestamp.clone(), value)),
                            right_count,
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
                .process_right(
                    right_record.timestamp,
                    right_update.clone(),
                    None,
                    right_count,
                )
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

        self.update_state(ctx, key, right_record.timestamp, right_update, 'r')
            .await;
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
