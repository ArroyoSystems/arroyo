use std::collections::HashMap;
use std::future::Future;
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use arrow::array::{Array, ArrayBuilder, ArrayData, BinaryBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder, UInt32Builder, UInt64Builder};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi};
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::stream::StreamExt;
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::error::Elapsed;

pub mod macros;

pub enum FuturesEnum<F: Future + Send + 'static> {
    Ordered(FuturesOrdered<F>),
    Unordered(FuturesUnordered<F>),
}

impl <T: Send, F: Future<Output = T> + Send + 'static> FuturesEnum<F> {
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
    pub fn is_ordered(&self) -> bool {
        match self {
            FuturesEnum::Ordered(_) => true,
            FuturesEnum::Unordered(_) => false,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct FfiArraySchema(FFI_ArrowArray, FFI_ArrowSchema);

#[repr(C)]
pub struct FfiArrays {
    ptr: *mut FfiArraySchema,
    len: usize,
    capacity: usize,
    error: bool,
}

impl FfiArrays {
    pub fn from_vec(value: Vec<ArrayData>) -> Self {
        let vec: Vec<_> = value.into_iter()
            .map(|a| to_ffi(&a).unwrap())
            .map(|(data, schema)| FfiArraySchema(data, schema))
            .collect();

        let len = vec.len();
        let capacity = vec.capacity();
        // the UDF dylib is responsible for freeing the memory of the args -- we leak it before
        // calling the udf so that if it panics, we don't try to double-free the args
        let ptr = vec.leak().as_mut_ptr();
        
        Self {
            ptr, len, capacity, error: false
        }
    }

    pub fn into_vec(self) -> Vec<ArrayData> {
        let vec = unsafe {
            Vec::from_raw_parts(self.ptr, self.len, self.capacity)
        };
        
        let args = vec.into_iter()
            .map(|FfiArraySchema(array, schema)| {
                unsafe { from_ffi(array, &schema).unwrap() }
            })
            .collect();

        args
    }
}

type OutputT = (usize, Result<ArrowDatum, Elapsed>);

unsafe impl Send for FfiArrays {}

enum ArrowDatum {
    U32(Option<u32>),
    U64(Option<u64>),
    I32(Option<i32>),
    I64(Option<i64>),
    F32(Option<f32>),
    F64(Option<f64>),
    String(Option<String>),
    Bytes(Option<Vec<u8>>),
    Timestamp(Option<SystemTime>),
}

fn to_nanos(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
}
impl ArrowDatum {
    fn append_to(self, builder: &mut dyn ArrayBuilder) {
        match self {
            ArrowDatum::U32(x) => builder.as_any_mut().downcast_mut::<UInt32Builder>().unwrap().append_option(x),
            ArrowDatum::U64(x) => builder.as_any_mut().downcast_mut::<UInt64Builder>().unwrap().append_option(x),
            ArrowDatum::I32(x) => builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap().append_option(x),
            ArrowDatum::I64(x) => builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap().append_option(x),
            ArrowDatum::F32(x) => builder.as_any_mut().downcast_mut::<Float32Builder>().unwrap().append_option(x),
            ArrowDatum::F64(x) => builder.as_any_mut().downcast_mut::<Float64Builder>().unwrap().append_option(x),
            ArrowDatum::String(x) => builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap().append_option(x),
            ArrowDatum::Bytes(x) => builder.as_any_mut().downcast_mut::<BinaryBuilder>().unwrap().append_option(x),
            ArrowDatum::Timestamp(x) => builder.as_any_mut().downcast_mut::<TimestampNanosecondBuilder>().unwrap()
                .append_option(x.map(to_nanos)),
        }
    }
}

type QueueData = (usize, Vec<ArrayData>);
type ResultMutex = Arc<Mutex<(Box<dyn ArrayBuilder>, UInt64Builder)>>;

pub struct AsyncUdf<F: Future<Output = OutputT> + Send + 'static, FnT: Fn(usize, Vec<ArrayData>) -> F + Send> {
    futures: FuturesEnum<F>,
    rx: Receiver<QueueData>,
    results: ResultMutex,
    inputs: HashMap<usize, Vec<ArrayData>>,
    func: FnT,
}

pub struct AsyncUdfHandle {
    tx: Sender<QueueData>,
    results: ResultMutex,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct SendableAsyncUdfHandle(*mut AsyncUdfHandle);
unsafe impl Send for SendableAsyncUdfHandle {}

#[repr(C)]
pub enum DrainResult {
    Data(FfiArrays),
    None,
    Error,
}

#[async_ffi::async_ffi]
pub async extern "C-unwind" fn send(handle: SendableAsyncUdfHandle, id: usize, arrays: FfiArrays) -> bool {
    let args = arrays.into_vec();
    
    return unsafe { handle.0.as_mut().unwrap() }.tx.send((id, args)).await.is_ok();
}

#[no_mangle]
pub extern "C-unwind" fn drain_results(handle: SendableAsyncUdfHandle) -> DrainResult {
    let handle = unsafe { handle.0.as_mut().unwrap() };
    match handle.results.lock() {
        Ok(mut data) => {
            if data.0.is_empty() {
                return DrainResult::None;
            }
            
            let results = data.0.finish();
            let ids = data.1.finish();
            DrainResult::Data(FfiArrays::from_vec(vec![results.to_data(), ids.to_data()]))
        }
        Err(_) => {
            DrainResult::Error
        }
    }
}


impl <F: Future<Output = OutputT> + Send + 'static, FnT: Fn(usize, Vec<ArrayData>) -> F + Send + 'static> AsyncUdf<F, FnT> {
    pub fn new(ordered: bool, builder: Box<dyn ArrayBuilder>, func: FnT) -> (Self, AsyncUdfHandle) {
        let (tx, rx) = channel(16);

        let results = Arc::new(Mutex::new((builder, UInt64Builder::new())));

        let handle = AsyncUdfHandle {
            tx,
            results: results.clone()
        };

        (Self {
            futures: if ordered {
                FuturesEnum::Ordered(FuturesOrdered::new())
            } else {
                FuturesEnum::Unordered(FuturesUnordered::new())
            },
            rx,
            results,
            inputs: HashMap::new(),
            func,
        }, handle)
    }

    pub fn start(self) {
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build().unwrap();
            runtime.block_on(async move {
                self.run().await;
            })
        });
    }

    async fn run(mut self) {
        loop {
            select! {
                item = self.rx.recv() => {
                    let Some((id, args)) = item else {
                        break;
                    };

                    self.inputs.insert(id, args.clone());
                    self.futures.push_back((self.func)(id, args));
                }
                Some((id, result)) = self.futures.next() => {
                    self.handle_future(id, result).await;
                }
            }
        }
    }

    async fn handle_future(
        &mut self,
        id: usize,
        result: Result<ArrowDatum, Elapsed>,
    ) {
        let mut results = self.results.lock().unwrap();
        match result {
            Ok(value) => {
                self.inputs.remove(&id);
                value.append_to(&mut results.0);
                results.1.append_value(id as u64);
            }
            Err(_) => {
                if self.futures.is_ordered() {
                    unimplemented!(
                        "Ordered Async UDF timed out, currently panic to preserve ordering"
                    );
                }
                eprintln!("Unordered Async UDF timed out, retrying");
                self.futures.push_back((self.func)(
                    id,
                    (*self.inputs.get(&id).as_ref().expect("missing input")).clone(),
                ));
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::{Duration, SystemTime};
    use arrow::array::{Array, ArrayData, ArrayDataBuilder, ArrayRef, PrimitiveArray, StringArray, StringBuilder, UInt32Builder};
    use arrow::datatypes::{UInt32Type, UInt64Type};
    use tokio::time::error::Elapsed;
    use tokio::time::Instant;
    use crate::{ArrowDatum, AsyncUdf, drain_results, DrainResult, FfiArrays, send, SendableAsyncUdfHandle};

    async fn udf_fn(a: usize, b: &str) -> u32 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        a as u32 + b.len() as u32
    }

    async fn wrapper(id: usize, args: Vec<ArrayData>) -> (usize, Result<ArrowDatum, Elapsed>) {
        let mut args = args.into_iter();
        let arg_1 = PrimitiveArray::<UInt64Type>::from(args.next().unwrap());
        let arg_2 = StringArray::from(args.next().unwrap());

        let result = udf_fn(arg_1.value(0) as usize, arg_2.value(0)).await;
        
        (id, Ok(ArrowDatum::U32(Some(result))))
    }

    extern "C-unwind" fn start(ordered: bool) -> SendableAsyncUdfHandle {
        let (x, handle) = AsyncUdf::new(ordered, Box::new(UInt32Builder::new()), wrapper);
        x.start();
        let handle = Box::new(handle);
        SendableAsyncUdfHandle(Box::leak(handle))
    }
    
    #[tokio::test]
    async fn test_async() {
        let handle = start(false);
        let arg1 = PrimitiveArray::<UInt64Type>::from(vec![2]);
        let arg2 = StringArray::from(vec!["hello"]);
        let args = FfiArrays::from_vec(vec![arg1.to_data(), arg2.to_data()]);
        assert_eq!(send(handle, 5, args).await, true);
        
        let start_time = Instant::now();
        loop {
            match drain_results(handle) {
                DrainResult::Data(data) => {
                    let data = data.into_vec();
                    assert_eq!(data.len(), 2);
                    let mut data = data.into_iter();
                    let values = PrimitiveArray::<UInt32Type>::from(data.next().unwrap());
                    let ids = PrimitiveArray::<UInt64Type>::from(data.next().unwrap());
                    assert_eq!(values.len(), 1);
                    assert_eq!(values.value(0), 7);
                    assert_eq!(ids.len(), 1);
                    assert_eq!(ids.value(0), 5);
                    return;
                }
                DrainResult::None => {
                    if start_time.elapsed() > Duration::from_secs(1) {
                        panic!("no results after 1 second");
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                DrainResult::Error => {
                    panic!("Async task failed")
                }
            }
            
        }
    }
}


