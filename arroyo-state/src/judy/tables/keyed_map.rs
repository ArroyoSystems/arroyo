use crate::BackingStore;
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

    pub async fn insert(&mut self, timestamp: SystemTime, mut key: K, value: V) {
        let mut wrapped = Some(value);
        /*self.backing_state
        .write_data_triple(
            self.table,
            TableType::TimeKeyMap,
            timestamp,
            &mut key,
            &mut wrapped,
        )
        .await;*/
        self.cache.insert(key, wrapped.unwrap());
    }

    pub async fn remove(&mut self, key: &mut K) {
        self.cache.remove(&key);
        self.backing_state
            .write_key_value::<K, Option<V>>(self.table, key, &mut None)
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
            match value {
                Some(value) => values.insert(key, value),
                None => values.remove(&key),
            };
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
