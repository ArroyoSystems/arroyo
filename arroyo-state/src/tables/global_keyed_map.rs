use crate::metrics::TABLE_SIZE_GAUGE;
use crate::BackingStore;
use arroyo_types::{Data, Key};
use std::collections::HashMap;

pub struct GlobalKeyedState<'a, K: Key, V: Data, S: BackingStore> {
    table: char,
    parquet: &'a mut S,
    cache: &'a mut GlobalKeyedStateCache<K, V>,
}

impl<'a, K: Key, V: Data, S: BackingStore> GlobalKeyedState<'a, K, V, S> {
    pub fn new(
        table: char,
        backing_store: &'a mut S,
        cache: &'a mut GlobalKeyedStateCache<K, V>,
    ) -> Self {
        Self {
            table,
            parquet: backing_store,
            cache,
        }
    }
    pub async fn insert(&mut self, mut key: K, mut value: V) {
        self.parquet
            .write_key_value(self.table, &mut key, &mut value)
            .await;
        self.cache.values.insert(key, value);

        TABLE_SIZE_GAUGE
            .with_label_values(&[
                &self.parquet.task_info().operator_id,
                &self.parquet.task_info().task_index.to_string(),
                &self.table.to_string(),
            ])
            .set(self.cache.values.len() as f64);
    }

    pub fn get_all(&mut self) -> Vec<&V> {
        self.cache.values.values().collect()
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.cache.values.get(key)
    }
}

pub struct GlobalKeyedStateCache<K: Key, V: Data> {
    values: HashMap<K, V>,
}

impl<K: Key, V: Data> GlobalKeyedStateCache<K, V> {
    pub async fn from_checkpoint<S: BackingStore>(backing_store: &S, table: char) -> Self {
        let mut values = HashMap::new();
        for (key, value) in backing_store.get_global_key_values(table).await {
            values.insert(key, value);
        }
        Self { values }
    }
}

impl<K: Key, V: Data> Default for GlobalKeyedStateCache<K, V> {
    fn default() -> Self {
        Self {
            values: Default::default(),
        }
    }
}
