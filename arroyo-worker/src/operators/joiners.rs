use crate::operators::join_with_expiration::{
    Coercer, FullJoinProcessor, InnerJoinProcessor, JoinWithExpiration, LeftJoinProcessor,
    NoOpProcessor, RightJoinProcessor,
};
use arroyo_types::*;
use std::time::Duration;

pub fn left_join<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    T1,
    T2,
    Coercer<T1>,
    Coercer<T2>,
    T1,
    T2,
    UpdatingData<(T1, Option<T2>)>,
    LeftJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, LeftJoinProcessor::new())
}

pub fn left_join_left_updating<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    UpdatingData<T1>,
    T2,
    NoOpProcessor<T1>,
    Coercer<T2>,
    T1,
    T2,
    UpdatingData<(T1, Option<T2>)>,
    LeftJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, LeftJoinProcessor::new())
}

pub fn left_join_right_updating<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    T1,
    UpdatingData<T2>,
    Coercer<T1>,
    NoOpProcessor<T2>,
    T1,
    T2,
    UpdatingData<(T1, Option<T2>)>,
    LeftJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, LeftJoinProcessor::new())
}

pub fn left_join_both_updating<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    UpdatingData<T1>,
    UpdatingData<T2>,
    NoOpProcessor<T1>,
    NoOpProcessor<T2>,
    T1,
    T2,
    UpdatingData<(T1, Option<T2>)>,
    LeftJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, LeftJoinProcessor::new())
}

pub fn right_join<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    T1,
    T2,
    Coercer<T1>,
    Coercer<T2>,
    T1,
    T2,
    UpdatingData<(Option<T1>, T2)>,
    RightJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, RightJoinProcessor::new())
}

pub fn right_join_left_updating<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    UpdatingData<T1>,
    T2,
    NoOpProcessor<T1>,
    Coercer<T2>,
    T1,
    T2,
    UpdatingData<(Option<T1>, T2)>,
    RightJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, RightJoinProcessor::new())
}

pub fn right_join_right_updating<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    T1,
    UpdatingData<T2>,
    Coercer<T1>,
    NoOpProcessor<T2>,
    T1,
    T2,
    UpdatingData<(Option<T1>, T2)>,
    RightJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, RightJoinProcessor::new())
}

pub fn right_join_both_updating<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    UpdatingData<T1>,
    UpdatingData<T2>,
    NoOpProcessor<T1>,
    NoOpProcessor<T2>,
    T1,
    T2,
    UpdatingData<(Option<T1>, T2)>,
    RightJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, RightJoinProcessor::new())
}

pub fn inner_join<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    T1,
    T2,
    Coercer<T1>,
    Coercer<T2>,
    T1,
    T2,
    UpdatingData<(T1, T2)>,
    InnerJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, InnerJoinProcessor::new())
}

pub fn inner_join_left_updating<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    UpdatingData<T1>,
    T2,
    NoOpProcessor<T1>,
    Coercer<T2>,
    T1,
    T2,
    UpdatingData<(T1, T2)>,
    InnerJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, InnerJoinProcessor::new())
}

pub fn inner_join_right_updating<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    T1,
    UpdatingData<T2>,
    Coercer<T1>,
    NoOpProcessor<T2>,
    T1,
    T2,
    UpdatingData<(T1, T2)>,
    InnerJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, InnerJoinProcessor::new())
}

pub fn inner_join_both_updating<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    UpdatingData<T1>,
    UpdatingData<T2>,
    NoOpProcessor<T1>,
    NoOpProcessor<T2>,
    T1,
    T2,
    UpdatingData<(T1, T2)>,
    InnerJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, InnerJoinProcessor::new())
}

pub fn full_join<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    T1,
    T2,
    Coercer<T1>,
    Coercer<T2>,
    T1,
    T2,
    UpdatingData<(Option<T1>, Option<T2>)>,
    FullJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, FullJoinProcessor::new())
}

pub fn full_join_left_updating<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    UpdatingData<T1>,
    T2,
    NoOpProcessor<T1>,
    Coercer<T2>,
    T1,
    T2,
    UpdatingData<(Option<T1>, Option<T2>)>,
    FullJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, FullJoinProcessor::new())
}

pub fn full_join_right_updating<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    T1,
    UpdatingData<T2>,
    Coercer<T1>,
    NoOpProcessor<T2>,
    T1,
    T2,
    UpdatingData<(Option<T1>, Option<T2>)>,
    FullJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, FullJoinProcessor::new())
}

pub fn full_join_both_updating<K: Key, T1: Data, T2: Data>(
    left_expiration: Duration,
    right_expiration: Duration,
) -> JoinWithExpiration<
    K,
    UpdatingData<T1>,
    UpdatingData<T2>,
    NoOpProcessor<T1>,
    NoOpProcessor<T2>,
    T1,
    T2,
    UpdatingData<(Option<T1>, Option<T2>)>,
    FullJoinProcessor<K, T1, T2>,
> {
    JoinWithExpiration::new(left_expiration, right_expiration, FullJoinProcessor::new())
}
