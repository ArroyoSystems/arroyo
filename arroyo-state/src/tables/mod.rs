use crate::DataOperation;
use arroyo_rpc::grpc::TableType;
use std::collections::HashMap;
use std::time::SystemTime;

pub mod global_keyed_map;
pub mod key_time_multi_map;
pub mod keyed_map;
pub mod time_key_map;

pub enum Compactor {
    TimeKeyMap,
    TimeKeyMultiMap,
}

pub struct DataTuple<K, V> {
    pub timestamp: SystemTime,
    pub key: K,
    pub value: Option<V>,
    pub operation: DataOperation,
}

/// BlindDataTuple's key and value are not decoded
#[derive(Debug)]
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
            TableType::KeyTimeMultiMap => Compactor::TimeKeyMultiMap,
        }
    }

    pub(crate) fn compact_tuples(&self, tuples: Vec<BlindDataTuple>) -> Vec<BlindDataTuple> {
        match self {
            Compactor::TimeKeyMap => {
                // keep only the latest entry for each key
                let mut reduced = HashMap::new();
                for tuple in tuples.into_iter() {
                    let memory_key = (tuple.timestamp, tuple.key.clone());
                    reduced.insert(memory_key, tuple);
                }

                reduced.into_values().collect()
            }
            Compactor::TimeKeyMultiMap => {
                // TODO: implement compaction
                tuples
            }
        }
    }
}
