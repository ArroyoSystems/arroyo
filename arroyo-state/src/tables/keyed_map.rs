use crate::metrics::TABLE_SIZE_GAUGE;
use crate::BackingStore;
use arroyo_rpc::grpc::TableType;
use arroyo_types::{Data, Key};
use std::collections::HashMap;
use std::time::SystemTime;

pub struct KeyedState<'a, K: Key, V: Data, S: BackingStore> {
    table: char,
    backing_state: &'a mut S,
    cache: &'a mut KeyedStateCache<K, V>,
}

impl<'a, K: Key, V: Data, S: BackingStore> KeyedState<'a, K, V, S> {
    pub fn new(
        table: char,
        backing_store: &'a mut S,
        cache: &'a mut KeyedStateCache<K, V>,
    ) -> Self {
        Self {
            table,
            backing_state: backing_store,
            cache,
        }
    }

    pub async fn insert(&mut self, timestamp: SystemTime, mut key: K, mut value: V) {
        self.backing_state
            .write_data_tuple(
                self.table,
                TableType::TimeKeyMap,
                timestamp,
                &mut key,
                &mut value,
            )
            .await;
        self.cache.insert(key, value);

        TABLE_SIZE_GAUGE
            .with_label_values(&[
                &self.backing_state.task_info().operator_id,
                &self.backing_state.task_info().task_index.to_string(),
                &self.table.to_string(),
            ])
            .set(self.cache.values.len() as f64);
    }

    pub async fn remove(&mut self, key: &mut K) {
        self.cache.remove(&key);
        self.backing_state
            .delete_time_key(self.table, TableType::Global, SystemTime::UNIX_EPOCH, key)
            .await;
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.cache.values.get(key)
    }
}

pub struct KeyedStateCache<K: Key, V: Data> {
    values: HashMap<K, V>,
}

impl<K: Key, V: Data> KeyedStateCache<K, V> {
    pub async fn from_checkpoint<S: BackingStore>(backing_store: &S, table: char) -> Self {
        let mut values = HashMap::new();
        for (key, value) in backing_store.get_key_values(table).await {
            values.insert(key, value);
        }
        Self { values }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.values.insert(key, value);
    }
    pub fn remove(&mut self, key: &K) {
        self.values.remove(key);
    }
}

impl<K: Key, V: Data> Default for KeyedStateCache<K, V> {
    fn default() -> Self {
        Self {
            values: Default::default(),
        }
    }
}
