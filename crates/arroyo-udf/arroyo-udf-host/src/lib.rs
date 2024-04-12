#[cfg(test)]
mod test;

use std::any::Any;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use anyhow::{anyhow, bail};
use arrow::array::{ArrayData, make_array, UInt64Array};
use arrow::datatypes::DataType;
use arrow::ffi::{from_ffi};
use async_ffi::FfiFuture;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature};
use dlopen2::wrapper::{Container, WrapperApi};
use arroyo_udf_common::{FfiArrays, FfiArraySchema, RunResult};
use arroyo_udf_common::async_udf::{DrainResult, SendableFfiAsyncUdfHandle};
use datafusion::error::Result as DFResult;

pub use arroyo_udf_common::parse;

#[derive(WrapperApi)]
pub struct UdfDylibInterface {
    run: unsafe extern "C-unwind" fn(args: FfiArrays) -> RunResult,
}

#[derive(WrapperApi)]
pub struct AsyncUdfDylibInterface {
    start: unsafe extern "C-unwind" fn(ordered: bool) -> SendableFfiAsyncUdfHandle,
    send: unsafe extern "C-unwind" fn(handle: SendableFfiAsyncUdfHandle, id: usize, arrays: FfiArrays) -> FfiFuture<bool>,
    drain_results: unsafe extern "C-unwind" fn(handle: SendableFfiAsyncUdfHandle) -> DrainResult,
    stop_runtime: unsafe extern "C-unwind" fn(handle: SendableFfiAsyncUdfHandle),
}

pub enum ContainerOrLocal<T: WrapperApi> {
    Container(Container<T>),
    Local(T)
}

impl <T: WrapperApi> ContainerOrLocal<T> {
    pub fn inner(&self) -> &T {
        match self {
            ContainerOrLocal::Container(t) => t.deref(),
            ContainerOrLocal::Local(t) => t,
        }
    } 
}

#[derive(Clone)]
pub enum UdfInterface {
    Sync(Arc<ContainerOrLocal<UdfDylibInterface>>),
    Async(Arc<ContainerOrLocal<AsyncUdfDylibInterface>>),
}

#[derive(Clone)]
pub struct UdfDylib {
    name: Arc<String>,
    signature: Arc<Signature>,
    return_type: Arc<DataType>,
    udf: UdfInterface,
}

impl UdfDylib {
    pub fn new(name: String, signature: Signature, return_type: DataType, udf: UdfInterface) -> Self {
        Self {
            name: Arc::new(name),
            signature: Arc::new(signature),
            return_type: Arc::new(return_type),
            udf,
        }
    }
}

pub struct SyncUdfDylib {
    name: Arc<String>,
    signature: Arc<Signature>,
    return_type: Arc<DataType>,
    udf: Arc<ContainerOrLocal<UdfDylibInterface>>,
}

impl SyncUdfDylib {
    pub fn new(name: String, signature: Signature, return_type: DataType, udf: UdfDylibInterface) -> Self {
        Self {
            name: Arc::new(name),
            signature: Arc::new(signature),
            return_type: Arc::new(return_type),
            udf: Arc::new(ContainerOrLocal::Local(udf))
        }
    }
}

impl Debug for SyncUdfDylib {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdfDylib").finish()
    }
}

impl TryFrom<&UdfDylib> for SyncUdfDylib {
    type Error = anyhow::Error;

    fn try_from(value: &UdfDylib) -> std::result::Result<Self, Self::Error> {
        let UdfInterface::Sync(udf) = &value.udf else {
            bail!("UDF is async but expected sync")
        };

        Ok(Self {
            name: value.name.clone(),
            signature: value.signature.clone(),
            return_type: value.return_type.clone(),
            udf: udf.clone(),
        })
    }
}

pub struct AsyncUdfDylib {
    name: Arc<String>,
    signature: Arc<Signature>,
    return_type: Arc<DataType>,
    handle: Option<SendableFfiAsyncUdfHandle>,
    udf: Arc<ContainerOrLocal<AsyncUdfDylibInterface>>,
}

impl TryFrom<&UdfDylib> for AsyncUdfDylib {
    type Error = anyhow::Error;

    fn try_from(value: &UdfDylib) -> std::result::Result<Self, Self::Error> {
        let UdfInterface::Async(udf) = &value.udf else {
            bail!("UDF is sync but expected async")
        };

        Ok(Self {
            name: value.name.clone(),
            signature: value.signature.clone(),
            handle: None,
            return_type: value.return_type.clone(),
            udf: udf.clone(),
        })
    }
}

impl AsyncUdfDylib {
    pub fn new(name: String, signature: Signature, return_type: DataType, udf: AsyncUdfDylibInterface) -> Self {
        Self {
            name: Arc::new(name),
            signature: Arc::new(signature),
            return_type: Arc::new(return_type),
            udf: Arc::new(ContainerOrLocal::Local(udf)),
            handle: None,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Starts the async UDF runtime; must be called before any data is sent into the UDF
    pub fn start(&mut self, ordered: bool) {
        if self.handle.is_none() {
            self.handle = Some(unsafe { self.udf.inner().start(ordered) });
        }
    }

    /// Sends a record into the UDF for processing. Each ArrayData must have a single item
    /// representing the value for the argument at its position in the vec. The given id will be
    /// returned back with the result.
    pub async fn send(&mut self, id: usize, data: Vec<ArrayData>) -> anyhow::Result<()> {
        assert!(data.iter().all(|d| d.len() == 1));
        let handle = self.handle.ok_or_else(|| anyhow!("async UDF {} has not been started", self.name))?;
        unsafe { self.udf.inner().send(handle, id, FfiArrays::from_vec(data)).await }
            .then(|| ())
            .ok_or_else(|| anyhow!("cannot send; Aync UDF {} has shut down", self.name))
    }

    /// Returns the ready results as a matching pair (ids, results) if any are available, or
    /// None otherwise.
    pub fn drain_results(&mut self) -> anyhow::Result<Option<(UInt64Array, ArrayData)>> {
        let handle = self.handle.ok_or_else(|| anyhow!("async UDF {} has not been started", self.name))?;
        match unsafe { self.udf.inner().drain_results(handle) } {
            DrainResult::Data(data) => {
                let mut v = data.into_vec()
                    .into_iter();
                Ok(Some((UInt64Array::from(v.next().unwrap()), v.next().unwrap())))
            }
            DrainResult::None => {
                Ok(None)
            }
            DrainResult::Error => {
                bail!("error fetching results from async UDF {}", self.name)
            }
        }
    }
}

impl Drop for AsyncUdfDylib {
    fn drop(&mut self) {
        eprintln!("dropping");
        if let Some(handle) = self.handle {
            unsafe { self.udf.inner().stop_runtime(handle) };
        }
    }
}

impl ScalarUDFImpl for SyncUdfDylib {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok((*self.return_type).clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let num_rows = args
            .iter()
            .map(|arg| {
                if let ColumnarValue::Array(array) = arg {
                    array.len()
                } else {
                    1
                }
            })
            .max()
            .unwrap();

        let args = args
            .iter()
            .map(|arg| {
                arg.clone().into_array(num_rows).unwrap().to_data()
            })
            .collect::<Vec<_>>();

        let args = FfiArrays::from_vec(args);

        let result = unsafe { (self.udf.inner().run)(args)};

        match result {
            RunResult::Ok(FfiArraySchema(array, schema)) => {
                let result_array = unsafe { from_ffi(array, &schema).unwrap() };
                Ok(ColumnarValue::Array(make_array(result_array)))
            }
            RunResult::Err => {
                panic!("panic in UDF {}", self.name);
            }
        }

    }
}