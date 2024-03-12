use crate::old::Context;
use arroyo_macro::{process_fn, StreamNode};
use arroyo_rpc::grpc::TableDescriptor;
use arroyo_types::{CheckpointBarrier, Data, Key, UdfContext};
use arroyo_types::{Message, Record};
use async_trait::async_trait;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::StreamExt;
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::time::error::Elapsed;
use tracing::{debug, info, warn};

// TODO: port to record batches.
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
    pub fn is_ordered(&self) -> bool {
        match &self.futures {
            FuturesEnum::Ordered(_) => true,
            FuturesEnum::Unordered(_) => false,
        }
    }
}

#[derive(StreamNode)]
pub struct AsyncMapOperator<
    InKey: Key,
    InT: Data,
    OutT: Data,
    FutureT: Future<Output = (usize, Result<OutT, Elapsed>)> + Send + 'static,
    FnT: Fn(usize, InT, Arc<ContextT>) -> FutureT + Send + 'static,
    ContextT: UdfContext + Send + 'static,
> {
    pub name: String,

    pub udf: FnT,
    pub futures: FuturesWrapper<FutureT>,
    udf_context: Arc<ContextT>,
    max_concurrency: u64,

    next_id: usize, // i.e. inputs received so far, should start at 0
    inputs: VecDeque<Option<Record<InKey, InT>>>,
    watermarks: VecDeque<(usize, arroyo_types::Watermark)>,
    _t: PhantomData<(InKey, InT, OutT, ContextT)>,
}

#[process_fn(in_k = InKey, in_t = InT, out_k = InKey, out_t = OutT, futures = "futures")]
impl<
        InKey: Key,
        InT: Data,
        OutT: Data,
        FutureT: Future<Output = (usize, Result<OutT, Elapsed>)> + Send + 'static,
        FnT: Fn(usize, InT, Arc<ContextT>) -> FutureT + Send + 'static,
        ContextT: UdfContext + Send + 'static,
    > AsyncMapOperator<InKey, InT, OutT, FutureT, FnT, ContextT>
{
    pub fn new(
        name: String,
        udf: FnT,
        context: ContextT,
        ordered: bool,
        max_concurrency: u64,
    ) -> Self {
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
            udf_context: Arc::new(context),
            max_concurrency,
            next_id: 0,
            inputs: VecDeque::new(),
            watermarks: VecDeque::new(),
            _t: PhantomData,
        }
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    async fn on_start(&mut self, ctx: &mut Context<InKey, OutT>) {
        self.udf_context.init().await;

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
                self.futures.push_back((self.udf)(
                    self.next_id,
                    v.value.clone(),
                    self.udf_context.clone(),
                ));
                self.next_id += 1;
            });
    }

    async fn process_element(
        &mut self,
        record: &Record<InKey, InT>,
        _ctx: &mut Context<InKey, OutT>,
    ) {
        self.inputs.push_back(Some(record.clone()));

        self.futures.push_back((self.udf)(
            self.next_id,
            record.value.clone(),
            self.udf_context.clone(),
        ));
        self.next_id += 1;
    }

    async fn on_collect(&mut self, id: usize, ctx: &mut Context<InKey, OutT>) {
        // mark the input as collected by setting it to None,
        // then pop all the Nones from the front of the queue
        let index = self.inputs.len() - (self.next_id - id);
        self.inputs[index] = None;
        while let Some(None) = self.inputs.front() {
            self.inputs.pop_front();
            if let Some((index, _watermark)) = self.watermarks.front() {
                // if index is 3, then that means that the watermark came in after 3 elements.
                // if we've read in 9 more, then next_id would be 12, and 12 - 3 = 9
                if *index + self.inputs.len() == self.next_id {
                    let (_index, watermark) = self.watermarks.pop_front().unwrap();
                    ctx.broadcast(arroyo_types::Message::Watermark(watermark))
                        .await;
                }
            }
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

    async fn handle_watermark(
        &mut self,
        watermark: arroyo_types::Watermark,
        _ctx: &mut Context<InKey, OutT>,
    ) {
        self.watermarks.push_back((self.next_id, watermark));
    }

    fn tables(&self) -> Vec<TableDescriptor> {
        vec![arroyo_state::global_table("a", "AsyncMapOperator state")]
    }

    async fn on_close(
        &mut self,
        ctx: &mut Context<InKey, OutT>,
        final_message: &Option<Message<InKey, OutT>>,
    ) {
        if let Some(Message::EndOfData) = final_message {
            debug!(
                "AsyncMapOperator end of data with {} futures",
                self.futures.len()
            );
            while let Some((id, result)) = self.futures.next().await {
                self.handle_future(id, result, ctx).await;
            }
        }
        self.udf_context.close().await;
    }

    async fn handle_future(
        &mut self,
        id: usize,
        result: Result<OutT, Elapsed>,
        ctx: &mut Context<InKey, OutT>,
    ) {
        match result {
            Ok(value) => {
                let index = self.inputs.len() - (self.next_id - id);
                let input = self.inputs[index].clone().unwrap();
                ctx.collector
                    .collect(Record {
                        timestamp: input.timestamp,
                        key: input.key.clone(),
                        value,
                    })
                    .await;
                self.on_collect(id, ctx).await;
            }
            Err(_e) => {
                if self.futures.is_ordered() {
                    unimplemented!(
                        "Ordered Async UDF timed out, currently panic to preserve ordering"
                    );
                }
                warn!("Unordered Async UDF timed out, retrying");
                self.futures.push_back((self.udf)(
                    id,
                    self.inputs[id].clone().unwrap().value.clone(),
                    self.udf_context.clone(),
                ));
            }
        }
    }
}

pub struct EmptyContext {}

#[async_trait]
impl UdfContext for EmptyContext {}
