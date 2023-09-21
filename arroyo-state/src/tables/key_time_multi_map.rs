use crate::metrics::TABLE_SIZE_GAUGE;
use crate::{BackingStore, DataOperation, StateBackend};
use arroyo_rpc::grpc::{CheckpointMetadata, TableDescriptor, TableType};
use arroyo_types::{from_micros, Data, Key, TaskInfo};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::SystemTime;

pub struct KeyTimeMultiMap<'a, K: Key, V: Data, S: BackingStore> {
    table: char,
    backing_store: &'a mut S,
    cache: &'a mut KeyTimeMultiMapCache<K, V>,
}

impl<'a, K: Key, V: Data, S: BackingStore> KeyTimeMultiMap<'a, K, V, S> {
    pub fn new(
        table: char,
        backing_store: &'a mut S,
        cache: &'a mut KeyTimeMultiMapCache<K, V>,
    ) -> Self {
        Self {
            table,
            backing_store,
            cache,
        }
    }
    pub async fn insert(&mut self, timestamp: SystemTime, mut key: K, mut value: V) {
        self.backing_store
            .write_data_tuple(
                self.table,
                TableType::KeyTimeMultiMap,
                timestamp,
                &mut key,
                &mut value,
            )
            .await;
        self.cache.insert(timestamp, key, value);

        TABLE_SIZE_GAUGE
            .with_label_values(&[
                &self.backing_store.task_info().operator_id,
                &self.backing_store.task_info().task_index.to_string(),
                &self.table.to_string(),
            ])
            .set(self.cache.values.len() as f64);
    }

    pub async fn get_time_range(
        &mut self,
        key: &mut K,
        start: SystemTime,
        end: SystemTime,
    ) -> Vec<&V> {
        let Some(key_map) = self.cache.values.get(key) else {
            return vec![];
        };
        key_map
            .range(start..end)
            .flat_map(|(_time, values)| values)
            .collect()
    }

    pub async fn clear_time_range(&mut self, key: &mut K, start: SystemTime, end: SystemTime) {
        if let Some(key_map) = self.cache.values.get_mut(key) {
            let times_to_remove = key_map.range(start..end);
            let times: Vec<_> = times_to_remove.map(|(time, _values)| *time).collect();
            for time in times {
                key_map.remove(&time);
            }
        };
    }

    pub fn expire_entries_before(&mut self, expiration_time: SystemTime) {
        self.cache.expire_entries_before(expiration_time);
    }

    pub async fn get_all_values_with_timestamps(
        &mut self,
        key: &mut K,
    ) -> Option<impl Iterator<Item = (SystemTime, &V)>> {
        self.cache.get_all_values_with_timestamps(key)
    }
}

pub struct KeyTimeMultiMapCache<K: Key, V: Data> {
    pub(crate) values: HashMap<K, BTreeMap<SystemTime, Vec<V>>>,
    pub(crate) expirations: BTreeMap<SystemTime, HashSet<K>>,
}

impl<K: Key, V: Data> KeyTimeMultiMapCache<K, V> {
    pub async fn from_checkpoint<S: BackingStore>(
        backing_store: &S,
        task_info: &TaskInfo,
        table: char,
        table_descriptor: &TableDescriptor,
        checkpoint_metadata: &CheckpointMetadata,
    ) -> Self {
        let mut values: HashMap<K, BTreeMap<SystemTime, Vec<V>>> = HashMap::new();
        // TODO: there may be a race here, as the initial checkpoint_metadata might get stale.
        // This is unlikely as this method is only called on start, but should probably be the domain of the backing store.
        let operator_metadata = StateBackend::load_operator_metadata(
            &task_info.job_id,
            &task_info.operator_id,
            checkpoint_metadata.epoch,
        )
        .await
        .expect("expect metadata for restoring from checkpoint");
        let min_valid_time = operator_metadata
            .min_watermark
            .map_or(SystemTime::UNIX_EPOCH, |min_watermark| {
                from_micros(min_watermark - table_descriptor.retention_micros)
            });

        for tuple in backing_store.get_data_tuples(table).await {
            if tuple.timestamp < min_valid_time {
                continue;
            }
            match tuple.operation {
                DataOperation::Insert => {
                    values
                        .entry(tuple.key)
                        .or_default()
                        .entry(tuple.timestamp)
                        .or_default()
                        .push(tuple.value.unwrap());
                }
                DataOperation::DeleteKey => {
                    values
                        .entry(tuple.key)
                        .or_default()
                        .remove(&tuple.timestamp);
                }
            }
        }
        let mut expirations: BTreeMap<SystemTime, HashSet<K>> = BTreeMap::new();
        for (time, key) in values.iter().map(|(key, map)| {
            let time = map.keys().next().unwrap();
            (*time, key.clone())
        }) {
            expirations.entry(time).or_default().insert(key);
        }
        Self {
            values,
            expirations,
        }
    }

    fn get_all_values_with_timestamps(
        &mut self,
        key: &mut K,
    ) -> Option<impl Iterator<Item = (SystemTime, &V)>> {
        if let Some(key_map) = self.values.get(key) {
            let result = key_map
                .iter()
                .flat_map(|(time, values)| values.iter().map(move |value| (*time, value)));
            Some(result)
        } else {
            None
        }
    }

    fn expire_entries_before(&mut self, time: SystemTime) {
        let times_to_remove = self.expirations.range(..time);
        let keys_to_remove: HashSet<_> = times_to_remove
            .flat_map(|(_time, keys)| keys.clone())
            .collect();
        for key in keys_to_remove {
            let key_data = self.values.get_mut(&key).unwrap();
            if *key_data.last_key_value().unwrap().0 <= time {
                self.values.remove(&key);
            } else {
                let retained_data = key_data.split_off(&time);
                let earliest_key = retained_data.first_key_value().unwrap().0;
                self.expirations
                    .entry(*earliest_key)
                    .or_default()
                    .insert(key);
                *key_data = retained_data;
            }
        }
    }

    // Insert a new value for a key at a given timestamp.
    // This potentially updates the earliest timestamp for the key.
    fn insert(&mut self, timestamp: SystemTime, key: K, value: V) {
        let current_entries = self.values.entry(key.clone()).or_default();
        // If there are no entries for this key, insert the new value.
        // the expiration is the timestamp of the new value.
        if current_entries.is_empty() {
            current_entries.insert(timestamp, vec![value]);
            self.expirations.entry(timestamp).or_default().insert(key);
        } else {
            // If there are entries for this key, check if the new value is earlier than the earliest value.
            let current_earliest = *current_entries.first_key_value().unwrap().0;
            if timestamp < current_earliest {
                // there definitely aren't any values at the new timestamp.
                current_entries.insert(timestamp, vec![value]);
                // remove the key from the previous earliest timestamp. If that map is empty also drop it.
                let current_earliest_keys = self.expirations.entry(current_earliest).or_default();
                current_earliest_keys.remove(&key);
                if current_earliest_keys.is_empty() {
                    self.expirations.remove(&current_earliest);
                }
                self.expirations.entry(timestamp).or_default().insert(key);
            } else {
                current_entries.entry(timestamp).or_default().push(value);
            }
        }
    }
}

impl<K: Key, V: Data> Default for KeyTimeMultiMapCache<K, V> {
    fn default() -> Self {
        Self {
            values: Default::default(),
            expirations: Default::default(),
        }
    }
}
