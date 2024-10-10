#[cfg(test)]
mod test;

use anyhow::{anyhow, bail};
use arrow::array::{make_array, Array, ArrayData, ArrayRef, UInt64Array};
use arrow::datatypes::DataType;
use arrow::ffi::from_ffi;
use arroyo_udf_common::async_udf::{DrainResult, SendableFfiAsyncUdfHandle};
use arroyo_udf_common::{FfiArraySchema, FfiArrays, RunResult};
use async_ffi::FfiFuture;
use datafusion::common::ScalarValue;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature};
use dlopen2::wrapper::{Container, WrapperApi};
use quote::{format_ident, ToTokens};
use std::any::Any;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use syn::{parse_file, Item};

pub use arroyo_udf_common::parse;
use arroyo_udf_common::parse::ParsedUdf;
use regex::Regex;
use toml::Table;

pub fn parse_dependencies(definition: &str) -> anyhow::Result<Table> {
    // get content of dependencies comment using regex
    let re = Regex::new(r"/\*\n(\[dependencies]\n[\s\S]*?)\*/").unwrap();
    if re.find_iter(definition).count() > 1 {
        bail!("Only one dependencies definition is allowed in a UDF");
    }

    let Some(deps) = re
        .captures(definition)
        .map(|captures| captures.get(1).unwrap().as_str())
    else {
        return Ok(Table::new());
    };

    let parsed: toml::Table =
        toml::from_str(deps).map_err(|e| anyhow!("invalid dependency definition: {:?}", e))?;

    let deps = parsed.get("dependencies").unwrap();

    Ok(deps
        .as_table()
        .ok_or_else(|| anyhow!("dependencies must be a TOML table, but found {}", deps))?
        .clone())
}

pub struct ParsedUdfFile {
    pub udf: ParsedUdf,
    pub definition: String,
    pub dependencies: Table,
}

impl ParsedUdfFile {
    pub fn try_parse(def: &str) -> anyhow::Result<Self> {
        let mut file = parse_file(def)?;

        let functions: Vec<_> = file
            .items
            .iter_mut()
            .filter_map(|item| match item {
                Item::Fn(function) => Some(function),
                _ => None,
            })
            .filter(|f| {
                f.attrs.iter().any(|a| {
                    a.path()
                        .segments
                        .last()
                        .is_some_and(|x| x.ident == format_ident!("udf"))
                })
            })
            .collect();

        match functions.len() {
            0 => bail!("UDF must contain a function with with the annotation #[udf]"),
            1 => {}
            _ => bail!("Only one function in a UDF may be annotated with #[udf]"),
        };

        let udf = ParsedUdf::try_parse(functions[0])?;

        Ok(ParsedUdfFile {
            udf,
            definition: file.into_token_stream().to_string(),
            dependencies: parse_dependencies(def)?,
        })
    }
}

#[derive(WrapperApi)]
pub struct UdfDylibInterface {
    __run: unsafe extern "C-unwind" fn(args: FfiArrays) -> RunResult,
}

impl UdfDylibInterface {
    pub fn new(run: unsafe extern "C-unwind" fn(FfiArrays) -> RunResult) -> Self {
        Self { __run: run }
    }
}

#[derive(WrapperApi)]
pub struct AsyncUdfDylibInterface {
    __start: unsafe extern "C-unwind" fn(
        ordered: bool,
        timeout_micros: u64,
        allowed_in_flight: u32,
    ) -> SendableFfiAsyncUdfHandle,
    __send: unsafe extern "C-unwind" fn(
        handle: SendableFfiAsyncUdfHandle,
        id: u64,
        arrays: FfiArrays,
    ) -> FfiFuture<bool>,
    __drain_results: unsafe extern "C-unwind" fn(handle: SendableFfiAsyncUdfHandle) -> DrainResult,
    __stop_runtime: unsafe extern "C-unwind" fn(handle: SendableFfiAsyncUdfHandle),
}

impl AsyncUdfDylibInterface {
    pub fn new(
        __start: extern "C-unwind" fn(
            ordered: bool,
            timeout_micros: u64,
            allowed_in_flight: u32,
        ) -> SendableFfiAsyncUdfHandle,
        __send: extern "C-unwind" fn(
            handle: SendableFfiAsyncUdfHandle,
            id: u64,
            arrays: FfiArrays,
        ) -> FfiFuture<bool>,
        __drain_results: extern "C-unwind" fn(handle: SendableFfiAsyncUdfHandle) -> DrainResult,
        __stop_runtime: extern "C-unwind" fn(handle: SendableFfiAsyncUdfHandle),
    ) -> Self {
        Self {
            __start,
            __send,
            __drain_results,
            __stop_runtime,
        }
    }
}

pub enum ContainerOrLocal<T: WrapperApi> {
    Container(Container<T>),
    Local(T),
}

impl<T: WrapperApi> ContainerOrLocal<T> {
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
    pub name: Arc<String>,
    pub signature: Arc<Signature>,
    pub return_type: Arc<DataType>,
    pub udf: UdfInterface,
}

impl UdfDylib {
    pub fn new(
        name: String,
        signature: Signature,
        return_type: DataType,
        udf: UdfInterface,
    ) -> Self {
        Self {
            name: Arc::new(name),
            signature: Arc::new(signature),
            return_type: Arc::new(return_type),
            udf,
        }
    }
}

#[derive(Clone)]
pub struct SyncUdfDylib {
    name: Arc<String>,
    signature: Arc<Signature>,
    return_type: Arc<DataType>,
    udf: Arc<ContainerOrLocal<UdfDylibInterface>>,
}

impl SyncUdfDylib {
    pub fn new(
        name: String,
        signature: Signature,
        return_type: DataType,
        udf: UdfDylibInterface,
    ) -> Self {
        Self {
            name: Arc::new(name),
            signature: Arc::new(signature),
            return_type: Arc::new(return_type),
            udf: Arc::new(ContainerOrLocal::Local(udf)),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
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
            handle: None,
            return_type: value.return_type.clone(),
            udf: udf.clone(),
        })
    }
}

impl AsyncUdfDylib {
    pub fn new(name: String, return_type: DataType, udf: AsyncUdfDylibInterface) -> Self {
        Self {
            name: Arc::new(name),
            return_type: Arc::new(return_type),
            udf: Arc::new(ContainerOrLocal::Local(udf)),
            handle: None,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn return_type(&self) -> &DataType {
        &self.return_type
    }

    /// Starts the async UDF runtime; must be called before any data is sent into the UDF
    pub fn start(&mut self, ordered: bool, timeout: Duration, allowed_in_flight: u32) {
        if self.handle.is_none() {
            self.handle = Some(unsafe {
                self.udf
                    .inner()
                    .__start(ordered, timeout.as_micros() as u64, allowed_in_flight)
            });
        }
    }

    /// Sends a record into the UDF for processing. Each ArrayData must have a single item
    /// representing the value for the argument at its position in the vec. The given id will be
    /// returned back with the result.
    pub async fn send(&mut self, id: u64, data: Vec<ArrayData>) -> anyhow::Result<()> {
        assert!(data.iter().all(|d| d.len() == 1));
        let handle = self
            .handle
            .ok_or_else(|| anyhow!("async UDF {} has not been started", self.name))?;
        unsafe {
            self.udf
                .inner()
                .__send(handle, id, FfiArrays::from_vec(data))
                .await
        }
        .then_some(())
        .ok_or_else(|| anyhow!("cannot send; Aync UDF {} has shut down", self.name))
    }

    /// Returns the ready results as a matching pair (ids, results) if any are available, or
    /// None otherwise.
    pub fn drain_results(&mut self) -> anyhow::Result<Option<(UInt64Array, ArrayData)>> {
        let handle = self
            .handle
            .ok_or_else(|| anyhow!("async UDF {} has not been started", self.name))?;
        match unsafe { self.udf.inner().__drain_results(handle) } {
            DrainResult::Data(data) => {
                let mut v = data.into_vec().into_iter();
                Ok(Some((
                    UInt64Array::from(v.next().unwrap()),
                    v.next().unwrap(),
                )))
            }
            DrainResult::None => Ok(None),
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
            unsafe { self.udf.inner().__stop_runtime(handle) };
        }
    }
}

impl SyncUdfDylib {
    pub fn invoke_udaf(&self, args: &[ArrayRef]) -> DFResult<ScalarValue> {
        let data: Vec<_> = args.iter().map(|a| a.to_data()).collect();

        let args = FfiArrays::from_vec(data);

        match unsafe { self.udf.inner().__run(args) } {
            RunResult::Ok(FfiArraySchema(array, schema)) => {
                let result_array = make_array(unsafe { from_ffi(array, &schema).unwrap() });
                assert_eq!(result_array.len(), 1);
                Ok(ScalarValue::try_from_array(result_array.as_ref(), 0).unwrap())
            }
            RunResult::Err => {
                panic!("panic in UDF {}", self.name);
            }
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

        let all_scalars = args.iter().all(|c| matches!(c, ColumnarValue::Scalar(_)));

        let args = args
            .iter()
            .map(|arg| arg.clone().into_array(num_rows).unwrap().to_data())
            .collect::<Vec<_>>();

        let args = FfiArrays::from_vec(args);

        let result = unsafe { (self.udf.inner().__run)(args) };

        match result {
            RunResult::Ok(FfiArraySchema(array, schema)) => {
                let result_array = unsafe { from_ffi(array, &schema).unwrap() };

                let array = make_array(result_array);
                if all_scalars {
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        &array, 0,
                    )?))
                } else {
                    Ok(ColumnarValue::Array(array))
                }
            }
            RunResult::Err => {
                panic!("panic in UDF {}", self.name);
            }
        }
    }
}

pub struct LocalUdf {
    pub def: &'static str,
    pub config: UdfDylib,
    pub is_aggregate: bool,
    pub is_async: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arroyo_udf_common::parse::{AsyncOptions, UdfType};

    #[test]
    fn test_parse_dependencies_valid() {
        let definition = r#"
/*
[dependencies]
serde = "1.0"
other_dep = { version = "1.0" }
*/

pub fn my_udf() -> i64 {
    1
}
        "#;

        assert_eq!(
            parse_dependencies(definition).unwrap(),
            vec![
                (
                    "serde".to_string(),
                    toml::value::Value::String("1.0".to_string())
                ),
                (
                    "other_dep".to_string(),
                    toml::value::Value::Table(toml::from_str("version = \"1.0\"").unwrap())
                )
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn test_parse_dependencies_none() {
        let definition = r#"
pub fn my_udf() -> i64 {
    1
}
        "#;

        assert_eq!(parse_dependencies(definition).unwrap(), Table::new(),);
    }

    #[test]
    fn test_parse_dependencies_multiple() {
        let definition = r#"
/*
[dependencies]
serde = "1.0"
*/

/*
[dependencies]
serde = "1.0"
*/

pub fn my_udf() -> i64 {
    1

        "#;
        assert!(parse_dependencies(definition).is_err());
    }

    #[test]
    fn test_attributes() {
        let s = r#"
            use arroyo_udf_plugin::udf;
            use std::time::Duration;
            
            #[udf(timeout="10ms", ordered, allowed_in_flight=100)]
            async fn hello(x: u64) -> i64 {
                tokio::time::sleep(Duration::from_millis(x * 100)).await;
                (x * 3) as i64
            }
        "#;

        let parsed = ParsedUdfFile::try_parse(s).unwrap();
        assert_eq!(
            parsed.udf.udf_type,
            UdfType::Async(AsyncOptions {
                ordered: true,
                timeout: Duration::from_millis(10),
                max_concurrency: 100,
            })
        );

        assert_eq!(parsed.udf.name.as_str(), "hello");
    }

    #[test]
    fn test_defaults() {
        let s = r#"
            use arroyo_udf_plugin::udf;
            use std::time::Duration;
            
            #[udf]
            async fn hello(x: u64) -> i64 {
                tokio::time::sleep(Duration::from_millis(x * 100)).await;
                (x * 3) as i64
            }
        "#;

        let parsed = ParsedUdfFile::try_parse(s).unwrap();
        assert_eq!(parsed.udf.udf_type, UdfType::Async(AsyncOptions::default()));

        assert_eq!(parsed.udf.name.as_str(), "hello");
    }
}
