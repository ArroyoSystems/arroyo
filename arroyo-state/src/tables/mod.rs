use crate::DataOperation;
use arroyo_rpc::grpc::TableType;
use arroyo_types::from_micros;
use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;

pub mod global_keyed_map;
pub mod key_time_multi_map;
pub mod keyed_map;
pub mod time_key_map;

pub enum Compactor {
    TimeKeyMap,
    KeyTimeMultiMap,
}

pub struct DataTuple<K, V> {
    pub timestamp: SystemTime,
    pub key: K,
    pub value: Option<V>,
    pub operation: DataOperation,
}

/// BlindDataTuple's key and value are not decoded
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BlindDataTuple {
    pub key_hash: u64,
    pub timestamp: SystemTime,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub operation: DataOperation,
}

impl Compactor {
    pub(crate) fn for_table_type(table_type: TableType) -> Self {
        match table_type {
            TableType::Global | TableType::TimeKeyMap => Compactor::TimeKeyMap,
            TableType::KeyTimeMultiMap => Compactor::KeyTimeMultiMap,
        }
    }

    pub(crate) fn compact_tuples(&self, tuples: Vec<BlindDataTuple>) -> Vec<BlindDataTuple> {
        match self {
            Compactor::TimeKeyMap => {
                // keep only the latest entry for each key
                let mut reduced = BTreeMap::new();
                for tuple in tuples.into_iter() {
                    match tuple.operation {
                        DataOperation::Insert | DataOperation::DeleteKey => {}
                        DataOperation::DeleteValue | DataOperation::DeleteTimeRange(..) => {
                            panic!("Not supported")
                        }
                    }

                    let memory_key = (tuple.timestamp, tuple.key.clone());
                    reduced.insert(memory_key, tuple);
                }

                reduced.into_values().collect()
            }
            Compactor::KeyTimeMultiMap => {
                // Build a values map similar to KeyTimeMultiMap,
                // but with the values being the actual tuples.
                // Then flatten the map to get the compacted inserts.
                let mut values: HashMap<Vec<u8>, BTreeMap<SystemTime, Vec<BlindDataTuple>>> =
                    HashMap::new();
                let mut deletes = HashMap::new();
                for tuple in tuples.into_iter() {
                    let keep_deletes_key =
                        (tuple.timestamp, tuple.key.clone(), tuple.value.clone());
                    match tuple.operation {
                        DataOperation::Insert => {
                            values
                                .entry(tuple.key.clone())
                                .or_default()
                                .entry(tuple.timestamp)
                                .or_default()
                                .push(tuple);
                        }
                        DataOperation::DeleteKey => {
                            // Remove a key. Timestamp is not considered.
                            values.remove(tuple.key.as_slice());
                            deletes.insert(keep_deletes_key, tuple);
                        }
                        DataOperation::DeleteValue => {
                            // Remove a single value from a (key -> time -> values).
                            values.entry(tuple.key.clone()).and_modify(|map| {
                                map.entry(tuple.timestamp).and_modify(|values| {
                                    let position = values.iter().position(|stored_tuple| {
                                        stored_tuple.value == tuple.value.clone()
                                    });
                                    if let Some(position) = position {
                                        values.remove(position);
                                    }
                                });
                            });
                            deletes.insert(keep_deletes_key, tuple);
                        }
                        DataOperation::DeleteTimeRange(start, end) => {
                            // Remove range of times from a key.
                            // Timestamp of tuple is not considered (the range is in the DataOperation).
                            if let Some(key_map) = values.get_mut(tuple.key.as_slice()) {
                                key_map.retain(|time, _values| {
                                    !(from_micros(start)..from_micros(end)).contains(time)
                                })
                            }
                            deletes.insert(keep_deletes_key, tuple);
                        }
                    }
                }

                let mut reduced: Vec<BlindDataTuple> = vec![];

                // first add the deletes
                for (_, tuple) in deletes.into_iter() {
                    reduced.push(tuple);
                }

                // then flatten values to get the compacted inserts
                for (_, map) in values.into_iter() {
                    for (_, tuples) in map.into_iter() {
                        for tuple in tuples.into_iter() {
                            reduced.push(tuple);
                        }
                    }
                }

                reduced
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::tables::{BlindDataTuple, Compactor};
    use crate::DataOperation;
    use arroyo_types::to_micros;
    use std::time::{Duration, SystemTime};

    #[tokio::test]
    async fn test_time_key_map_compaction() {
        let t1 = SystemTime::now();
        let t2 = t1 + Duration::from_secs(1);

        let k1 = "k1".as_bytes().to_vec();
        let k2 = "k2".as_bytes().to_vec();
        let v1 = "v1".as_bytes().to_vec();

        let insert_1 = BlindDataTuple {
            key_hash: 123,
            timestamp: t1,
            key: k1.clone(),
            value: v1.clone(),
            operation: DataOperation::Insert,
        };

        let insert_2 = BlindDataTuple {
            key_hash: 123,
            timestamp: t2,
            key: k2.clone(),
            value: v1.clone(),
            operation: DataOperation::Insert,
        };

        let delete = BlindDataTuple {
            key_hash: 123,
            timestamp: t1,
            key: k1.clone(),
            value: v1.clone(),
            operation: DataOperation::DeleteKey,
        };

        let tuples_in = vec![insert_1.clone(), insert_2.clone(), delete.clone()];

        let tuples_out = Compactor::TimeKeyMap.compact_tuples(tuples_in);
        assert_eq!(vec![delete, insert_2], tuples_out);

        // test idempotence
        assert_eq!(
            tuples_out.clone(),
            Compactor::TimeKeyMap.compact_tuples(tuples_out),
        );
    }

    #[tokio::test]
    async fn test_key_time_multi_map_compaction() {
        let t1 = SystemTime::now();
        let t2 = t1 + Duration::from_secs(1);
        let t3 = t2 + Duration::from_secs(1);

        let k1 = "k1".as_bytes().to_vec();
        let v1 = "v1".as_bytes().to_vec();

        let insert_1 = BlindDataTuple {
            key_hash: 123,
            timestamp: t1,
            key: k1.clone(),
            value: v1.clone(),
            operation: DataOperation::Insert,
        };

        let insert_2 = BlindDataTuple {
            key_hash: 123,
            timestamp: t2,
            key: k1.clone(),
            value: v1.clone(),
            operation: DataOperation::Insert,
        };

        let delete_all = BlindDataTuple {
            key_hash: 123,
            timestamp: t2,
            key: k1.clone(),
            value: v1.clone(),
            operation: DataOperation::DeleteTimeRange(to_micros(t1), to_micros(t3)),
        };

        let insert_3 = BlindDataTuple {
            key_hash: 123,
            timestamp: t1,
            key: k1.clone(),
            value: v1.clone(),
            operation: DataOperation::Insert,
        };

        let tuples_in = vec![
            insert_1.clone(),
            insert_2.clone(),
            delete_all.clone(),
            insert_3.clone(),
        ];

        let tuples_out = Compactor::KeyTimeMultiMap.compact_tuples(tuples_in);
        assert_eq!(vec![delete_all, insert_3], tuples_out);

        // test idempotence
        assert_eq!(
            tuples_out.clone(),
            Compactor::KeyTimeMultiMap.compact_tuples(tuples_out),
        );
    }
}
