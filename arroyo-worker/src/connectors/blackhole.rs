use std::marker::PhantomData;

use arroyo_macro::process_fn;
use arroyo_types::{Data, Key, Record};

use crate::engine::{Context, StreamNode};

#[derive(StreamNode)]
pub struct BlackholeSinkFunc<K: Key, T: Data> {
    _phantom: PhantomData<(K, T)>,
}

#[process_fn(in_k = K, in_t = T)]
impl<K: Key, T: Data> BlackholeSinkFunc<K, T> {
    pub fn new() -> BlackholeSinkFunc<K, T> {
        BlackholeSinkFunc {
            _phantom: PhantomData,
        }
    }

    pub fn from_config(_: &str) -> Self {
        Self::new()
    }

    fn name(&self) -> String {
        "BlackholeSink".to_string()
    }

    async fn process_element(&mut self, _record: &Record<K, T>, _ctx: &mut Context<(), ()>) {
        // no-op
    }
}
