use crate::engine::{Context, StreamNode};
use arroyo_macro::process_fn;
use arroyo_rpc::grpc::TableDescriptor;
use arroyo_types::Record;
use arroyo_types::{CheckpointBarrier, Data, Key};
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::StreamExt;
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use tokio::time::error::Elapsed;
use tracing::info;

pub enum FuturesEnum<T>
where
    T: Future + Send + 'static,
{
    Ordered(FuturesOrdered<T>),
    Unordered(FuturesUnordered<T>),
}

pub struct FuturesWrapper<T>
where
    T: Future + Send + 'static,
{
    futures: FuturesEnum<T>,
}

impl<T> FuturesWrapper<T>
where
    T: Future + Send + 'static,
{
    pub fn push_back(&mut self, f: T) {
        match &mut self.futures {
            FuturesEnum::Ordered(futures) => futures.push_back(f),
            FuturesEnum::Unordered(futures) => futures.push(f),
        }
    }

    pub async fn next(&mut self) -> Option<<T as Future>::Output> {
        match &mut self.futures {
            FuturesEnum::Ordered(futures) => futures.next().await,
            FuturesEnum::Unordered(futures) => futures.next().await,
        }
    }

    pub fn len(&self) -> usize {
        match &self.futures {
            FuturesEnum::Ordered(futures) => futures.len(),
            FuturesEnum::Unordered(futures) => futures.len(),
        }
    }
}

#[derive(StreamNode)]
pub struct AsyncMapOperator<
    InKey: Key,
    InT: Data,
    OutT: Data,
    FutureT: Future<Output = (usize, Result<OutT, Elapsed>)> + Send + 'static,
    FnT: Fn(usize, InT) -> FutureT + Send + 'static,
> {
    pub name: String,

    pub udf: FnT,
    pub futures: FuturesWrapper<FutureT>,
    max_concurrency: u64,

    next_id: usize, // i.e. inputs received so far, should start at 0
    inputs: VecDeque<Option<Record<InKey, InT>>>,

    _t: PhantomData<(InKey, InT, OutT)>,
}

#[process_fn(in_k = InKey, in_t = InT, out_k = InKey, out_t = OutT, futures = "futures")]
impl<
        InKey: Key,
        InT: Data,
        OutT: Data,
        FutureT: Future<Output = (usize, Result<OutT, Elapsed>)> + Send + 'static,
        FnT: Fn(usize, InT) -> FutureT + Send + 'static,
    > AsyncMapOperator<InKey, InT, OutT, FutureT, FnT>
{
    pub fn new(name: String, udf: FnT, ordered: bool, max_concurrency: u64) -> Self {
        let futures = if ordered {
            info!("Using ordered futures");
            FuturesWrapper {
                futures: FuturesEnum::Ordered(FuturesOrdered::new()),
            }
        } else {
            info!("Using unordered futures");
            FuturesWrapper {
                futures: FuturesEnum::Unordered(FuturesUnordered::new()),
            }
        };

        Self {
            name,
            udf,
            futures,
            max_concurrency,
            next_id: 0,
            inputs: VecDeque::new(),
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    async fn on_start(&mut self, ctx: &mut Context<InKey, OutT>) {
        let gs = ctx
            .state
            .get_global_keyed_state::<(usize, usize), Record<InKey, InT>>('a')
            .await;

        gs.get_key_values()
            .into_iter()
            .filter(|(k, _)| {
                let (task_index, _) = k;
                task_index % ctx.task_info.parallelism == ctx.task_info.task_index
            })
            .for_each(|(_, v)| {
                self.inputs.insert(self.next_id, Some(v.clone()));
                self.futures
                    .push_back((self.udf)(self.next_id, v.value.clone()));
                self.next_id += 1;
            });
    }

    async fn process_element(
        &mut self,
        record: &Record<InKey, InT>,
        _ctx: &mut Context<InKey, OutT>,
    ) {
        self.inputs.push_back(Some(record.clone()));

        self.futures
            .push_back((self.udf)(self.next_id, record.value.clone()));
        self.next_id += 1;
    }

    async fn on_collect(&mut self, id: usize) {
        // mark the input as collected by setting it to None,
        // then pop all the Nones from the front of the queue
        let index = self.inputs.len() - (self.next_id - id);
        self.inputs[index] = None;
        while let Some(None) = self.inputs.front() {
            self.inputs.pop_front();
        }
    }

    async fn handle_checkpoint(&mut self, _: &CheckpointBarrier, ctx: &mut Context<InKey, OutT>) {
        let mut gs = ctx
            .state
            .get_global_keyed_state::<(usize, usize), Record<InKey, InT>>('a')
            .await;

        for (i, record) in self.inputs.iter().filter_map(|x| x.clone()).enumerate() {
            let key = (ctx.task_info.task_index, i);
            gs.insert(key, record).await;
        }
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table("a", "AsyncMapOperator state")]
    }
}
