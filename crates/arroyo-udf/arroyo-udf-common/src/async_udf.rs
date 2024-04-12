use crate::{ArrowDatum, FfiArrays};
use arrow::array::{ArrayBuilder, ArrayData, UInt64Builder};
use std::sync::{Arc, Mutex};
use tokio::time::error::Elapsed;

pub type QueueData = (usize, Vec<ArrayData>);
pub type ResultMutex = Arc<Mutex<(UInt64Builder, Box<dyn ArrayBuilder>)>>;

#[repr(C)]
pub struct FfiAsyncUdfHandle {
    _data: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct SendableFfiAsyncUdfHandle {
    pub ptr: *mut FfiAsyncUdfHandle,
}

unsafe impl Send for SendableFfiAsyncUdfHandle {}

#[repr(C)]
pub enum DrainResult {
    Data(FfiArrays),
    None,
    Error,
}

pub type OutputT = (usize, Result<ArrowDatum, Elapsed>);
