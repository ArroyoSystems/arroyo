use arrow::array::{Array, ArrayBuilder, ArrayData, UInt64Builder};
use arroyo_udf_common::async_udf::{FfiAsyncUdfHandle, QueueData, ResultMutex};
use arroyo_udf_common::{ArrowDatum, FfiArrays};
use futures::stream::StreamExt;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::time::error::Elapsed;

pub use arroyo_udf_common::async_udf::{DrainResult, SendableFfiAsyncUdfHandle};
pub use async_ffi;
pub use tokio;

pub enum FuturesEnum<F: Future + Send + 'static> {
    Ordered(FuturesOrdered<F>),
    Unordered(FuturesUnordered<F>),
}

impl<T: Send, F: Future<Output = T> + Send + 'static> FuturesEnum<F> {
    pub fn push_back(&mut self, f: F) {
        match self {
            FuturesEnum::Ordered(futures) => futures.push_back(f),
            FuturesEnum::Unordered(futures) => futures.push(f),
        }
    }

    pub async fn next(&mut self) -> Option<T> {
        match self {
            FuturesEnum::Ordered(futures) => futures.next().await,
            FuturesEnum::Unordered(futures) => futures.next().await,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            FuturesEnum::Ordered(futures) => futures.len(),
            FuturesEnum::Unordered(futures) => futures.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            FuturesEnum::Ordered(futures) => futures.is_empty(),
            FuturesEnum::Unordered(futures) => futures.is_empty(),
        }
    }

    pub fn is_ordered(&self) -> bool {
        match self {
            FuturesEnum::Ordered(_) => true,
            FuturesEnum::Unordered(_) => false,
        }
    }
}

pub struct AsyncUdfHandle {
    pub tx: Sender<QueueData>,
    pub results: ResultMutex,
}

impl AsyncUdfHandle {
    pub fn into_ffi(self) -> *mut FfiAsyncUdfHandle {
        Box::leak(Box::new(self)) as *mut AsyncUdfHandle as *mut FfiAsyncUdfHandle
    }
}

pub async fn send(handle: SendableFfiAsyncUdfHandle, id: u64, arrays: FfiArrays) -> bool {
    let args = arrays.into_vec();

    unsafe {
        let handle = handle.ptr as *mut AsyncUdfHandle;
        (*handle).tx.send((id, args))
    }
    .await
    .is_ok()
}

pub fn drain_results(handle: SendableFfiAsyncUdfHandle) -> DrainResult {
    let handle = unsafe { &mut *(handle.ptr as *mut AsyncUdfHandle) };
    match handle.results.lock() {
        Ok(mut data) => {
            if data.0.is_empty() {
                return DrainResult::None;
            }

            let ids = data.0.finish();
            let results = data.1.finish();
            DrainResult::Data(FfiArrays::from_vec(vec![ids.to_data(), results.to_data()]))
        }
        Err(_) => DrainResult::Error,
    }
}

pub fn stop_runtime(handle: SendableFfiAsyncUdfHandle) {
    let handle = unsafe { Box::from_raw(&mut *(handle.ptr as *mut AsyncUdfHandle)) };
    // no-op, but explicit here to make clear the point of this function
    drop(handle);
}

pub type OutputT = (u64, Result<ArrowDatum, Elapsed>);

pub struct AsyncUdf<
    F: Future<Output = OutputT> + Send + 'static,
    FnT: Fn(u64, Duration, Vec<ArrayData>) -> F + Send,
> {
    futures: FuturesEnum<F>,
    rx: Receiver<QueueData>,
    results: ResultMutex,
    func: FnT,
    timeout: Duration,
    allowed_in_flight: usize,
}

impl<
    F: Future<Output = OutputT> + Send + 'static,
    FnT: Fn(u64, Duration, Vec<ArrayData>) -> F + Send + 'static,
> AsyncUdf<F, FnT>
{
    pub fn new(
        ordered: bool,
        timeout: Duration,
        allowed_in_flight: u32,
        builder: Box<dyn ArrayBuilder>,
        func: FnT,
    ) -> (Self, AsyncUdfHandle) {
        let (tx, rx) = channel(1);

        let results = Arc::new(Mutex::new((UInt64Builder::new(), builder)));

        let handle = AsyncUdfHandle {
            tx,
            results: results.clone(),
        };

        (
            Self {
                futures: if ordered {
                    FuturesEnum::Ordered(FuturesOrdered::new())
                } else {
                    FuturesEnum::Unordered(FuturesUnordered::new())
                },
                rx,
                results,
                func,
                timeout,
                allowed_in_flight: allowed_in_flight as usize,
            },
            handle,
        )
    }

    pub fn start(self) {
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                self.run().await;
            })
        });
    }

    async fn run(mut self) {
        loop {
            select! {
                item = self.rx.recv(), if self.futures.len() < self.allowed_in_flight => {
                    let Some((id, args)) = item else {
                        break;
                    };

                    self.futures.push_back((self.func)(id, self.timeout, args));
                }
                Some((id, result)) = self.futures.next() => {
                    self.handle_future(id, result).await;
                }
            }
        }
    }

    async fn handle_future(&mut self, id: u64, result: Result<ArrowDatum, Elapsed>) {
        let mut results = self.results.lock().unwrap();
        match result {
            Ok(value) => {
                results.0.append_value(id);
                value.append_to(&mut results.1);
            }
            Err(_) => {
                if self.futures.is_ordered() {
                    panic!("Ordered Async UDF timed out, currently panic to preserve ordering");
                }
                panic!("Async UDF timed out");
            }
        }
    }
}
