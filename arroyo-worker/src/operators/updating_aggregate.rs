use std::marker::PhantomData;

use crate::engine::StreamNode;
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::{TableDeleteBehavior, TableDescriptor, TableType, TableWriteBehavior};
use arroyo_state::tables::keyed_map::KeyedState;
use arroyo_types::*;
use std::time::Duration;
use crate::old::Context;

#[derive(StreamNode)]
pub struct UpdatingAggregateOperator<K: Key, T: Data, BinA: Data, OutT: Data> {
    expiration: Duration,
    aggregator: fn(&K, &BinA) -> OutT,
    bin_merger: fn(&T, Option<&BinA>) -> Option<BinA>,
    _t: PhantomData<K>,
}

#[derive(Debug)]
enum StateOp<T: Data> {
    Set(T),
    Delete,
    #[allow(dead_code)]
    Update {
        new: T,
        old: T,
    },
}

#[process_fn(in_k = K, in_t = T, out_k = K, out_t = UpdatingData<OutT>)]
impl<K: Key, T: Data, BinA: Data, OutT: Data> UpdatingAggregateOperator<K, T, BinA, OutT> {
    fn name(&self) -> String {
        "UpdatingAggregate".to_string()
    }

    pub fn new(
        expiration: Duration,
        // TODO: this can consume the bin, as we drop it right after.
        aggregator: fn(&K, &BinA) -> OutT,
        bin_merger: fn(&T, Option<&BinA>) -> Option<BinA>,
    ) -> Self {
        UpdatingAggregateOperator {
            expiration,
            aggregator,
            bin_merger,
            _t: PhantomData,
        }
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![TableDescriptor {
            name: "a".to_string(),
            description: "window state".to_string(),
            table_type: TableType::TimeKeyMap as i32,
            delete_behavior: TableDeleteBehavior::NoReadsBeforeWatermark as i32,
            write_behavior: TableWriteBehavior::NoWritesBeforeWatermark as i32,
            retention_micros: self.expiration.as_micros() as u64,
        }]
    }

    async fn process_element(
        &mut self,
        record: &Record<K, T>,
        ctx: &mut Context<K, UpdatingData<OutT>>,
    ) {
        if let Some(watermark) = ctx.last_present_watermark() {
            if record.timestamp < watermark {
                return;
            }
        }
        let mut aggregating_map: KeyedState<K, BinA, _> = ctx.state.get_key_state('a').await;
        let mut mut_key = record.key.clone().unwrap();
        let key = mut_key.clone();
        let (new_value, state_op) = {
            let bin_aggregate = aggregating_map.get(&mut mut_key);
            match bin_aggregate {
                Some(bin_aggregate) => {
                    let old_aggregate = (self.aggregator)(&key, bin_aggregate);
                    let new_bin = (self.bin_merger)(&record.value, Some(bin_aggregate));
                    match new_bin {
                        Some(new_bin) => {
                            let new_aggregate = (self.aggregator)(&key, &new_bin);
                            if new_aggregate != old_aggregate {
                                (
                                    Some(UpdatingData::Update {
                                        old: old_aggregate,
                                        new: new_aggregate,
                                    }),
                                    Some(StateOp::Update {
                                        new: new_bin,
                                        old: bin_aggregate.clone(),
                                    }),
                                )
                            } else if new_bin != *bin_aggregate {
                                (
                                    None,
                                    Some(StateOp::Update {
                                        new: new_bin,
                                        old: bin_aggregate.clone(),
                                    }),
                                )
                            } else {
                                (None, None)
                            }
                        }
                        None => (
                            Some(UpdatingData::Retract(old_aggregate)),
                            Some(StateOp::Delete),
                        ),
                    }
                }
                None => {
                    let new_bin = (self.bin_merger)(&record.value, None);
                    match new_bin {
                        Some(new_bin) => {
                            let new_aggregate = (self.aggregator)(&key, &new_bin);
                            (
                                Some(UpdatingData::Append(new_aggregate)),
                                Some(StateOp::Set(new_bin)),
                            )
                        }
                        None => (None, None),
                    }
                }
            }
        };
        if let Some(state_op) = state_op {
            match state_op {
                StateOp::Set(new_bin) => {
                    aggregating_map
                        .insert(record.timestamp, mut_key, new_bin)
                        .await;
                }
                StateOp::Delete => {
                    aggregating_map.remove(&mut mut_key).await;
                }
                StateOp::Update { new, old: _ } => {
                    aggregating_map.insert(record.timestamp, mut_key, new).await;
                }
            }
        }
        if let Some(value) = new_value {
            ctx.collect(Record {
                timestamp: record.timestamp,
                key: Some(key),
                value,
            })
            .await;
        }
    }
}
