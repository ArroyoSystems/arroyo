use std::borrow::Borrow;
use std::collections::HashMap;
use std::ptr::{NonNull};
use std::sync::Arc;
use std::time::{Duration, Instant};
use arrow::row::RowConverter;
use arrow_array::ArrayRef;
use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::common::{exec_datafusion_err, Result as DFResult, ScalarValue};
use tracing::debug;

/// Abstract over aggregations that support retracts (sliding accumulators), which we can use
/// directly, and those that don't in which case we just need to store the raw values and aggregate
/// them on demand
enum IncrementalState {
    Sliding {
        accumulator: Box<dyn Accumulator>
    },
    Batch {
        expr: Arc<AggregateFunctionExpr>,
        data: HashMap<Vec<u8>, usize>,
        row_converter: Arc<RowConverter>
    },
}

impl IncrementalState {
    fn update_batch(&mut self, batch: &[ArrayRef]) -> DFResult<()> {
        match self {
            IncrementalState::Sliding { accumulator, .. } => {
                accumulator.update_batch(batch)?;
            },
            IncrementalState::Batch { data, row_converter, .. } => {
                for r in row_converter.convert_columns(batch)?.iter() {
                    if data.contains_key(r.as_ref()) {
                        *data.get_mut(r.as_ref()).unwrap() += 1;
                    } else {
                        data.insert(r.as_ref().to_vec(), 1);
                    }
                }
            }
        }

        Ok(())
    }

    fn retract_batch(&mut self, batch: &[ArrayRef]) -> DFResult<()> {
        match self {
            IncrementalState::Sliding { accumulator, .. } => accumulator.retract_batch(batch),
            IncrementalState::Batch { data, row_converter, .. } => {
                for r in row_converter.convert_columns(batch)?.iter() {
                    if data.contains_key(r.as_ref()) {
                        let v = data.get_mut(r.as_ref()).unwrap();
                        if *v == 1 {
                            data.remove(r.as_ref());
                        } else {
                            *v -= 1;
                        }
                    } else {
                        debug!("tried to retract value for missing key: {:?}; this implies an append \
                        was lost (possibly from source)", batch)
                    }
                }
                Ok(())
            }
        }
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        match self {
            IncrementalState::Sliding { accumulator, .. } => accumulator.evaluate(),
            IncrementalState::Batch { expr, data, row_converter, .. } => {
                if data.is_empty() {
                    Ok(ScalarValue::Null)
                } else {
                    let parser = row_converter.parser();
                    let input = row_converter.convert_rows(data.keys().map(|v| parser.parse(v)))?;
                    let mut acc = expr.create_accumulator()?;
                    acc.update_batch(&input)?;
                    acc.evaluate_mut()
                }
            }
        }
    }
}

#[derive(Copy, Clone)]
struct CacheNodePtr(Option<NonNull<CacheNode>>);

unsafe impl Send for CacheNodePtr {}
unsafe impl Sync for CacheNodePtr {}

struct CacheEntry {
    node: CacheNodePtr,
    data: Vec<IncrementalState>,
}

impl CacheEntry {
    fn update_batch(&mut self, batch: &[Vec<ArrayRef>], idx: Option<usize>) -> DFResult<()> {
        for (inputs, accs) in batch.iter().zip(self.data.iter_mut()) {
            let values = if let Some(idx) = idx {
                &inputs.iter().map(|c| c.slice(idx, 1)).collect()
            } else {
                inputs
            };

            accs.update_batch(values)?;
        }

        Ok(())
    }

    fn retract_batch(&mut self, batch: &[Vec<ArrayRef>], idx: Option<usize>) -> DFResult<()> {
        for (inputs, accs) in batch.iter().zip(self.data.iter_mut()) {
            let values = if let Some(idx) = idx {
                &inputs.iter().map(|c| c.slice(idx, 1)).collect()
            } else {
                inputs
            };

            accs.retract_batch(values)?;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<Vec<ScalarValue>> {
        self.data.iter_mut()
            .map(|s| s.evaluate())
            .collect::<DFResult<_>>()
    }
}

pub(crate) struct Cache {
    data: HashMap<Key, CacheEntry>,
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

#[derive(Hash, Eq, PartialEq)]
pub(crate) struct Key(Arc<Vec<u8>>);

impl Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl Cache {
    pub(crate) fn insert(&mut self, key: Arc<Vec<u8>>, value: Vec<IncrementalState>) {
        let node = self.push_back(Instant::now(), Key(key.clone()));

        self.data.insert(Key(key), CacheEntry {
            node,
            data: value,
        });

        self.eviction_list_tail = node;
    }
    
    fn time_out(&mut self, now: Instant) -> Vec<Arc<Vec<u8>>> {
        unsafe {
            let mut v = vec![];
            while let Some(head) = self.eviction_list_head.0 {
                if (*head.as_ptr()).updated - now > self.ttl {
                    // safety -- we've just checked that the node exists, so this can't panic
                    let k = self.pop_front().unwrap();
                    self.data.remove(&k);
                    v.push(k.0);
                } 
            }
            v
        }
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

    fn push_back(&mut self, updated: Instant, key: Key) -> CacheNodePtr {
        unsafe {
            let node =  NonNull::new_unchecked(Box::into_raw(Box::new(CacheNode {
                updated: Instant::now(),
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
    
    fn update_node(&mut self, node: CacheNodePtr, timestamp: Instant) {
        let node = node.0.unwrap();

        unsafe {
            if self.eviction_list_tail.0 == Some(node) {
                // nothing to do, it's already at the back
                (*node.as_ptr()).updated = timestamp;
                return;
            }

            let key = if self.eviction_list_head.0 == Some(node) {
                // if it's at the head, just use pop_front
                self.pop_front().unwrap()
            }  else {
                // otherwise, it's neither at the front or back --remove it from its current location
                if let Some(prev) = (*node.as_ptr()).prev.0 {
                    (*prev.as_ptr()).next = (*node.as_ptr()).next;
                }
                if let Some(next) = (*node.as_ptr()).next.0 {
                    (*next.as_ptr()).prev = (*node.as_ptr()).prev;
                }
                // free the memory
                Box::from_raw(node.as_ptr()).key
                // we don't need to update the head or tail, because they won't have changed
            };

            // re-insert it in the back
            self.push_back(timestamp, key);
        }
    }

    pub(crate) fn update_batch(&mut self, key: &[u8], batch: &[Vec<ArrayRef>], idx: Option<usize>) -> DFResult<()> {
        let entry = self.data.get_mut(key)
            .ok_or_else(|| exec_datafusion_err!("tried to update state for non-existent key"))?;

        entry.update_batch(batch, idx)?;
        
        let node = entry.node;

        self.update_node(node, Instant::now());
        
        Ok(())
    }
    
    pub(crate) fn retract_batch(&mut self, key: &[u8], batch: &[Vec<ArrayRef>], idx: Option<usize>) -> DFResult<()> {
        let entry = self.data.get_mut(key)
            .ok_or_else(|| exec_datafusion_err!("tried to update state for non-existent key"))?;

        entry.retract_batch(batch, idx)?;
        
        Ok(())
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        while let Some(_) = self.pop_front() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::DataType;
    use arrow_schema::{Field, Schema};
    use datafusion::common::DFSchema;
    use datafusion::execution::{SessionState, SessionStateBuilder, SessionStateDefaults};
    use datafusion::functions_aggregate::sum::{sum, sum_udaf};
    use datafusion::logical_expr::col;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

    #[test]
    fn test_cache_eviction() {
        let mut cache = Cache {
            data: HashMap::new(),
            eviction_list_head: CacheNodePtr(None),
            eviction_list_tail: CacheNodePtr(None), 
            ttl: Duration::from_secs(60),
        };
    
        let k1 = Arc::new(vec![1]);
        let k2 = Arc::new(vec![2]); 
        let k3 = Arc::new(vec![3]);
    
        cache.insert(k1.clone(), vec![]);
        cache.insert(k2.clone(), vec![]);
        cache.insert(k3.clone(), vec![]);
    
        let expired = cache.time_out(Instant::now() + Duration::from_secs(61));
        assert_eq!(expired.len(), 3);
        assert!(cache.data.is_empty());
    }
    
    #[test]
    fn test_cache_update() {
        let mut cache = Cache {
            data: HashMap::new(),
            eviction_list_head: CacheNodePtr(None),
            eviction_list_tail: CacheNodePtr(None),
            ttl: Duration::from_secs(60),
        };
    
        let k1 = Arc::new(vec![1]);
        let array = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let input = vec![vec![array]];

        let state = SessionStateBuilder::default()
            .with_default_features()
            .build();
        let schema = DFSchema::try_from(Schema::new(vec![Field::new("a", DataType::Int64, false)])).unwrap();
        
        let expr = DefaultPhysicalPlanner::default()
            .create_physical_expr(&sum(col("a")), &schema, &state).unwrap();
        
        cache.insert(k1.clone(), vec![IncrementalState::Batch {
            expr,
            data: HashMap::new(),
            row_converter: Arc::new(RowConverter::new(vec![DataType::Int64])),
        }]);
    
        cache.update_batch(&k1, &input, None).unwrap();
    
        let entry = cache.data.get_mut(&*k1).unwrap();
        let result = entry.evaluate().unwrap();
        assert_eq!(result[0], ScalarValue::Int64(Some(6)));
    }
    
}