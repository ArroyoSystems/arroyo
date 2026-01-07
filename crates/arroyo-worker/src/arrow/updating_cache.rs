use std::borrow::Borrow;
use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Copy, Clone)]
struct CacheNodePtr(Option<NonNull<CacheNode>>);

unsafe impl Send for CacheNodePtr {}
unsafe impl Sync for CacheNodePtr {}

struct CacheEntry<T: Send + Sync> {
    node: CacheNodePtr,
    generation: u64,
    data: T,
}

pub struct UpdatingCache<T: Send + Sync> {
    data: HashMap<Key, CacheEntry<T>>,
    eviction_list_head: CacheNodePtr,
    eviction_list_tail: CacheNodePtr,
    ttl: Duration,
}

struct CacheNode {
    updated: Instant,
    key: Key,
    prev: CacheNodePtr,
    next: CacheNodePtr,
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct Key(pub Arc<Vec<u8>>);

impl Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

struct TTLIter<'a, T: Send + Sync> {
    now: Instant,
    cache: &'a mut UpdatingCache<T>,
}

impl<T: Send + Sync> Iterator for TTLIter<'_, T> {
    type Item = (Arc<Vec<u8>>, T);

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let head = self.cache.eviction_list_head.0?;
            if self.now - (*head.as_ptr()).updated < self.cache.ttl {
                return None;
            }
            // safety -- we've just checked that the node exists, so this can't panic
            let k = self.cache.pop_front().unwrap();
            let v = self.cache.data.remove(&k).unwrap();
            Some((k.0, v.data))
        }
    }
}

impl<T: Send + Sync> UpdatingCache<T> {
    pub fn with_time_to_idle(ttl: Duration) -> Self {
        Self {
            data: Default::default(),
            eviction_list_tail: CacheNodePtr(None),
            eviction_list_head: CacheNodePtr(None),
            ttl,
        }
    }

    pub fn insert(&mut self, key: Arc<Vec<u8>>, now: Instant, generation: u64, value: T) {
        let key = Key(key.clone());

        if let Some(entry) = self.data.remove(&key) {
            // if this key already exists, we only replace it if the new generation is larger
            // than our existing generation
            if entry.generation < generation {
                self.remove_node(entry.node);
            } else {
                self.data.insert(key, entry);
                return;
            }
        }

        let node = self.push_back(now, key.clone());
        self.data.insert(
            key,
            CacheEntry {
                node,
                generation,
                data: value,
            },
        );

        self.eviction_list_tail = node;
    }

    pub fn time_out(&mut self, now: Instant) -> impl Iterator<Item = (Arc<Vec<u8>>, T)> + '_ {
        TTLIter { now, cache: self }
    }

    fn pop_front(&mut self) -> Option<Key> {
        unsafe {
            self.eviction_list_head.0.map(|head| {
                let boxed = Box::from_raw(head.as_ptr());
                let key = boxed.key;

                self.eviction_list_head = boxed.prev;
                if let Some(new) = self.eviction_list_head.0 {
                    (*new.as_ptr()).next = CacheNodePtr(None);
                } else {
                    self.eviction_list_tail = CacheNodePtr(None);
                }

                key
            })
        }
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&Key, &mut T)> {
        self.data.iter_mut().map(|(k, v)| (k, &mut v.data))
    }

    fn push_back(&mut self, updated: Instant, key: Key) -> CacheNodePtr {
        unsafe {
            let node = NonNull::new_unchecked(Box::into_raw(Box::new(CacheNode {
                updated,
                key,
                // push to the back
                prev: CacheNodePtr(None),
                next: CacheNodePtr(None),
            })));

            let wrapped_node = CacheNodePtr(Some(node));

            if let Some(tail) = self.eviction_list_tail.0 {
                (*tail.as_ptr()).prev = wrapped_node;
                (*node.as_ptr()).next = CacheNodePtr(Some(tail));
            } else {
                self.eviction_list_head = wrapped_node;
            }

            self.eviction_list_tail = wrapped_node;
            wrapped_node
        }
    }

    fn remove_node(&mut self, node: CacheNodePtr) {
        unsafe {
            if let Some(node) = node.0 {
                if self.eviction_list_head.0 == Some(node) {
                    self.eviction_list_head = (*node.as_ptr()).prev;
                }
                if self.eviction_list_tail.0 == Some(node) {
                    self.eviction_list_tail = (*node.as_ptr()).next;
                }

                if let Some(prev) = (*node.as_ptr()).prev.0 {
                    (*prev.as_ptr()).next = (*node.as_ptr()).next;
                }
                if let Some(next) = (*node.as_ptr()).next.0 {
                    (*next.as_ptr()).prev = (*node.as_ptr()).prev;
                }

                drop(Box::from_raw(node.as_ptr()));
            }
        }
    }

    fn update_node(&mut self, node: CacheNodePtr, timestamp: Instant) {
        unsafe {
            let Some(node_ptr) = node.0 else {
                return;
            };

            if self.eviction_list_tail.0 == Some(node_ptr) {
                // it's already at the back, just update the timestamp
                (*node_ptr.as_ptr()).updated = timestamp;
                return;
            }

            // Unlink node from current list.
            if self.eviction_list_head.0 == Some(node_ptr) {
                self.eviction_list_head = (*node_ptr.as_ptr()).prev;
                if let Some(new_head) = self.eviction_list_head.0 {
                    (*new_head.as_ptr()).next = CacheNodePtr(None);
                }
            } else {
                if let Some(prev) = (*node_ptr.as_ptr()).prev.0 {
                    (*prev.as_ptr()).next = (*node_ptr.as_ptr()).next;
                }
                if let Some(next) = (*node_ptr.as_ptr()).next.0 {
                    (*next.as_ptr()).prev = (*node_ptr.as_ptr()).prev;
                }
            }
            // Reinsert node at tail.

            (*node_ptr.as_ptr()).updated = timestamp;
            (*node_ptr.as_ptr()).prev = CacheNodePtr(None);
            (*node_ptr.as_ptr()).next = self.eviction_list_tail;

            if let Some(old_tail) = self.eviction_list_tail.0 {
                (*old_tail.as_ptr()).prev = CacheNodePtr(Some(node_ptr));
            }
            self.eviction_list_tail = CacheNodePtr(Some(node_ptr));
            if self.eviction_list_head.0.is_none() {
                self.eviction_list_head = CacheNodePtr(Some(node_ptr));
            }
        }
    }

    pub fn modify_and_update<E, F: Fn(&mut T) -> Result<(), E>>(
        &mut self,
        key: &[u8],
        now: Instant,
        f: F,
    ) -> Option<Result<(), E>> {
        let entry = self.data.get_mut(key)?;

        if let Err(e) = f(&mut entry.data) {
            return Some(Err(e));
        }

        entry.generation += 1;

        let node = entry.node;

        self.update_node(node, now);

        Some(Ok(()))
    }

    pub fn modify<E, F: Fn(&mut T) -> Result<(), E>>(
        &mut self,
        key: &[u8],
        f: F,
    ) -> Option<Result<(), E>> {
        let entry = self.data.get_mut(key)?;

        entry.generation += 1;

        if let Err(e) = f(&mut entry.data) {
            return Some(Err(e));
        }

        Some(Ok(()))
    }

    pub fn contains_key(&self, k: &[u8]) -> bool {
        self.data.contains_key(k)
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut T> {
        self.data.get_mut(key).map(|t| &mut t.data)
    }

    pub fn get_mut_generation(&mut self, key: &[u8]) -> Option<(&mut T, u64)> {
        self.data.get_mut(key).map(|t| (&mut t.data, t.generation))
    }

    pub fn get_mut_key_value(&mut self, key: &[u8]) -> Option<(Key, &mut T)> {
        let k = self.data.get_key_value(key)?.0.clone();
        Some((k, &mut self.data.get_mut(key)?.data))
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<T> {
        let node = self.data.remove(key)?;
        self.remove_node(node.node);
        Some(node.data)
    }
}

impl<T: Send + Sync> Drop for UpdatingCache<T> {
    fn drop(&mut self) {
        while self.pop_front().is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_modify() {
        let mut cache = UpdatingCache::with_time_to_idle(Duration::from_secs(60));

        let key = Arc::new(vec![1, 2, 3]);
        let now = Instant::now();
        cache.insert(key.clone(), now, 1, 42);

        assert!(
            cache
                .modify(&key, |x| {
                    *x = 43;
                    Ok::<(), ()>(())
                })
                .unwrap()
                .is_ok()
        );

        let v = cache.data.get(&Key(key)).unwrap();
        assert_eq!(v.data, 43);
    }

    #[test]
    fn test_timeout() {
        let mut cache = UpdatingCache::with_time_to_idle(Duration::from_millis(10));

        let key1 = Arc::new(vec![1]);
        let key2 = Arc::new(vec![2]);

        let start = Instant::now();
        cache.insert(key1.clone(), start, 1, "value1");
        cache.insert(key2.clone(), start + Duration::from_millis(5), 2, "value2");

        let check_time = start + Duration::from_millis(11);
        let timed_out: Vec<_> = cache.time_out(check_time).collect();
        assert_eq!(timed_out.len(), 1);
        assert_eq!(&*timed_out[0].0, &*key1);

        assert!(cache.data.contains_key(&Key(key2)));
        assert!(!cache.data.contains_key(&Key(key1)));
    }

    #[test]
    fn test_update_keeps_alive() {
        let mut cache = UpdatingCache::with_time_to_idle(Duration::from_millis(10));

        let key = Arc::new(vec![1]);
        let start = Instant::now();
        cache.insert(key.clone(), start, 1, "value");

        let update_time = start + Duration::from_millis(5);
        cache
            .modify_and_update(&key, update_time, |_| Ok::<(), ()>(()))
            .unwrap()
            .unwrap();

        let check_time = start + Duration::from_millis(11);
        let timed_out: Vec<_> = cache.time_out(check_time).collect();
        assert!(timed_out.is_empty());
        assert!(cache.data.contains_key(&Key(key)));
    }

    #[test]
    fn test_linked_list_ordering() {
        let mut cache = UpdatingCache::with_time_to_idle(Duration::from_secs(60));

        let key1 = Arc::new(vec![1]);
        let key2 = Arc::new(vec![2]);
        let key3 = Arc::new(vec![3]);

        let now = Instant::now();
        cache.insert(key1.clone(), now, 1, 1);
        cache.insert(key2.clone(), now + Duration::from_millis(1), 2, 2);
        cache.insert(key3.clone(), now + Duration::from_millis(2), 3, 3);

        let mut popped = Vec::new();
        while let Some(key) = cache.pop_front() {
            popped.push(key.0);
        }

        assert_eq!(popped, vec![key1, key2, key3]);
    }

    #[test]
    fn test_remove_node() {
        let mut cache = UpdatingCache::with_time_to_idle(Duration::from_secs(60));

        let key1 = Arc::new(vec![1]);
        let key2 = Arc::new(vec![2]);
        let key3 = Arc::new(vec![3]);

        let now = Instant::now();
        cache.insert(key1.clone(), now, 1, 1);
        cache.insert(key2.clone(), now, 2, 2);
        cache.insert(key3.clone(), now, 3, 3);

        let node_to_remove = cache.data.get(&Key(key2.clone())).unwrap().node;
        cache.remove_node(node_to_remove);

        let mut popped = Vec::new();
        while let Some(key) = cache.pop_front() {
            popped.push(key.0);
        }

        assert_eq!(popped, vec![key1, key3]);
    }

    #[test]
    fn reorder_with_update() {
        let mut cache = UpdatingCache::<i32>::with_time_to_idle(Duration::from_secs(10));
        let key1 = Arc::new(vec![1]);
        let key2 = Arc::new(vec![2]);
        let now = Instant::now();

        cache.insert(key1.clone(), now, 1, 100);
        cache.insert(key2.clone(), now, 2, 200);

        cache
            .modify_and_update(&[1], now + Duration::from_secs(1), |v| {
                *v += 1;
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();

        let _ = cache.modify_and_update(&[1], now + Duration::from_secs(2), |v| {
            *v += 1;
            Ok::<(), ()>(())
        });
    }

    #[test]
    fn test_ttl_eviction() {
        let ttl = Duration::from_millis(100);
        let mut cache = UpdatingCache::with_time_to_idle(ttl);
        let now = Instant::now();
        let key1 = Arc::new(vec![1]);
        let key2 = Arc::new(vec![2]);
        cache.insert(key1.clone(), now, 1, 10);
        cache.insert(key2.clone(), now, 2, 20);

        cache
            .modify_and_update(&[2], now + Duration::from_millis(50), |v| {
                *v += 1;
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();

        let now2 = now + Duration::from_millis(150);
        let evicted: Vec<_> = cache.time_out(now2).collect();
        assert_eq!(evicted.len(), 2);
        assert_eq!(evicted[0].0.as_ref(), &[1]);
        assert_eq!(evicted[1].0.as_ref(), &[2]);
    }

    #[test]
    fn test_remove_key() {
        let ttl = Duration::from_millis(100);
        let mut cache = UpdatingCache::with_time_to_idle(ttl);
        let now = Instant::now();
        let key = Arc::new(vec![1]);
        cache.insert(key.clone(), now, 1, 42);
        let value = cache.remove(&[1]).unwrap();
        assert_eq!(value, 42);
        assert!(!cache.contains_key(&[1]));
        let evicted: Vec<_> = cache.time_out(now + Duration::from_millis(200)).collect();
        assert!(evicted.is_empty());
    }

    #[test]
    fn test_update_order() {
        let ttl = Duration::from_secs(1);
        let mut cache = UpdatingCache::with_time_to_idle(ttl);
        let base = Instant::now();
        let key_a = Arc::new(vec![b'A']);
        let key_b = Arc::new(vec![b'B']);
        let key_c = Arc::new(vec![b'C']);
        cache.insert(key_a.clone(), base, 1, 1);
        cache.insert(key_b.clone(), base, 2, 2);
        cache.insert(key_c.clone(), base, 3, 3);

        let t_update = base + Duration::from_millis(500);
        cache
            .modify_and_update(b"B", t_update, |v| {
                *v += 10;
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();

        let t_eviction = base + Duration::from_secs(2);
        let evicted: Vec<_> = cache.time_out(t_eviction).collect();
        assert_eq!(evicted.len(), 3);
        assert_eq!(evicted[0].0.as_ref(), b"A");
        assert_eq!(evicted[1].0.as_ref(), b"C");
        assert_eq!(evicted[2].0.as_ref(), b"B");
    }

    #[test]
    fn test_get_mut_key_value() {
        let ttl = Duration::from_secs(1);
        let mut cache = UpdatingCache::with_time_to_idle(ttl);
        let base = Instant::now();
        let key = Arc::new(vec![1, 2, 3]);
        cache.insert(key.clone(), base, 1, 42);
        if let Some((k, v)) = cache.get_mut_key_value(&[1, 2, 3]) {
            *v += 1;
            assert_eq!(*v, 43);
            assert_eq!(k.0.as_ref(), &[1, 2, 3]);
        } else {
            panic!("Key not found");
        }
    }

    #[test]
    fn test_modify_error() {
        let ttl = Duration::from_secs(1);
        let mut cache = UpdatingCache::with_time_to_idle(ttl);
        let base = Instant::now();
        let key = Arc::new(vec![1]);
        cache.insert(key.clone(), base, 1, 42);
        let res = cache.modify(&[1], |_v| Err("error"));
        assert!(res.unwrap().is_err());
    }

    #[test]
    fn test_drop_cleanup() {
        let ttl = Duration::from_secs(1);
        {
            let mut cache = UpdatingCache::with_time_to_idle(ttl);
            let base = Instant::now();
            for i in 0..10 {
                cache.insert(Arc::new(vec![i as u8]), base, i as u64, i);
            }
        }
    }

    #[test]
    fn test_generational_replacement() {
        let ttl = Duration::from_secs(1);
        let mut cache = UpdatingCache::with_time_to_idle(ttl);
        let base = Instant::now();
        let key = Arc::new(vec![1]);

        cache.insert(key.clone(), base, 1, "first");
        assert_eq!(cache.get_mut(&[1]), Some(&mut "first"));

        cache.insert(key.clone(), base, 2, "second");
        assert_eq!(cache.get_mut(&[1]), Some(&mut "second"));

        cache.insert(key.clone(), base, 1, "third");
        assert_eq!(cache.get_mut(&[1]), Some(&mut "second"));
    }
}
