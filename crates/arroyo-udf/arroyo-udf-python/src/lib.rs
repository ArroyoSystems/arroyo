mod interpreter;
mod pyarrow;
mod threaded;

use crate::threaded::ThreadedUdfInterpreter;
use anyhow::{anyhow, bail};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use arroyo_udf_common::parse::NullableType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{Bound, PyAny};
use std::any::Any;
use std::fmt::Debug;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, Mutex};

const UDF_PY_LIB: &str = include_str!("../python/arroyo_udf.py");

#[derive(Debug)]
pub struct PythonUDF {
    pub name: Arc<String>,
    pub task_tx: SyncSender<Vec<ArrayRef>>,
    pub result_rx: Arc<Mutex<Receiver<anyhow::Result<ArrayRef>>>>,
    pub definition: Arc<String>,
    pub signature: Arc<Signature>,
    pub arg_types: Arc<Vec<NullableType>>,
    pub return_type: Arc<NullableType>,
}

impl ScalarUDFImpl for PythonUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(self.return_type.data_type.clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
        let size = args
            .iter()
            .map(|e| match e {
                ColumnarValue::Array(a) => a.len(),
                ColumnarValue::Scalar(_) => 1,
            })
            .max()
            .unwrap_or(0);

        let args = args
            .iter()
            .map(|e| match e {
                ColumnarValue::Array(a) => a.clone(),
                ColumnarValue::Scalar(s) => Arc::new(s.to_array_of_size(size).unwrap()),
            })
            .collect();

        self.task_tx.send(args).map_err(|_| {
            DataFusionError::Execution("Python UDF interpreter shut down unexpectedly".to_string())
        })?;

        let result = self
            .result_rx
            .lock()
            .unwrap()
            .recv()
            .map_err(|_| {
                DataFusionError::Execution(
                    "Python UDF interpreter shut down unexpectedly".to_string(),
                )
            })?
            .map_err(|e| {
                DataFusionError::Execution(format!("Error in Python UDF {}: {}", self.name, e))
            })?;

        Ok(ColumnarValue::Array(result))
    }
}

fn extract_type_info(udf: &Bound<PyAny>) -> anyhow::Result<(Vec<NullableType>, NullableType)> {
    let attr = udf.getattr("__annotations__")?;
    let annotations: &Bound<PyDict> = attr.downcast().map_err(|e| {
        anyhow!(
            "__annotations__ object is not a dictionary: {}",
            e.to_string()
        )
    })?;

    // Iterate over annotations dictionary
    let (ok, err): (Vec<_>, Vec<_>) = annotations
        .iter()
        .map(|(k, v)| {
            python_type_to_arrow(
                k.downcast::<PyString>().unwrap().to_str().unwrap(),
                &v,
                false,
            )
        })
        .partition(|e| e.is_ok());

    if !err.is_empty() {
        bail!(
            "Could not register Python UDF: {}",
            err.into_iter()
                .map(|t| t.unwrap_err().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let mut result: Vec<_> = ok.into_iter().map(|t| t.unwrap()).collect();

    let ret = result
        .pop()
        .ok_or_else(|| anyhow!("No return type defined for function"))?;

    Ok((result, ret))
}

impl PythonUDF {
    pub async fn parse(body: impl Into<String>) -> anyhow::Result<Self> {
        ThreadedUdfInterpreter::new(Arc::new(body.into())).await
    }
}

fn python_type_to_arrow(
    var_name: &str,
    py_type: &Bound<PyAny>,
    nullable: bool,
) -> anyhow::Result<NullableType> {
    let name = py_type
        .getattr("__name__")
        .map_err(|e| anyhow!("Could not get name of type for argument {var_name}: {e}"))?
        .downcast::<PyString>()
        .map_err(|_| anyhow!("Argument type was not a string"))?
        .to_string();

    if name == "Optional" {
        return python_type_to_arrow(
            var_name,
            &py_type
                .getattr("__args__")
                .map_err(|_| anyhow!("Optional type does not have arguments"))?
                .downcast::<PyTuple>()
                .map_err(|e| anyhow!("__args__ is not a tuple: {e}"))?
                .get_item(0)?,
            true,
        );
    }

    let data_type = match name.as_str() {
        "int" => DataType::Int64,
        "float" => DataType::Float64,
        "str" => DataType::Utf8,
        "bool" => DataType::Boolean,
        "list" => bail!("lists are not yet supported"),
        other => bail!("Unsupported Python type: {}", other),
    };

    Ok(NullableType::new(data_type, nullable))
}

#[cfg(test)]
mod test {
    use crate::PythonUDF;
    use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, TypeSignature};
    use std::sync::Arc;

    #[tokio::test]
    async fn test() {
        let udf = r#"
from arroyo_udf import udf

@udf
def my_add(x: int, y: float) -> float:
    return float(x) + y
"#;

        let udf = PythonUDF::parse(udf).await.unwrap();
        assert_eq!(udf.name.as_str(), "my_add");
        if let datafusion::logical_expr::TypeSignature::OneOf(args) = &udf.signature.type_signature
        {
            let ts: Vec<_> = args
                .iter()
                .map(|e| {
                    if let TypeSignature::Exact(v) = e {
                        v
                    } else {
                        panic!(
                            "expected inner typesignature sto be exact, but found {:?}",
                            e
                        )
                    }
                })
                .collect();

            use arrow::datatypes::DataType::*;

            assert_eq!(
                ts,
                vec![
                    &vec![Int8, Float32],
                    &vec![Int8, Float64],
                    &vec![Int16, Float32],
                    &vec![Int16, Float64],
                    &vec![Int32, Float32],
                    &vec![Int32, Float64],
                    &vec![Int64, Float32],
                    &vec![Int64, Float64],
                    &vec![UInt8, Float32],
                    &vec![UInt8, Float64],
                    &vec![UInt16, Float32],
                    &vec![UInt16, Float64],
                    &vec![UInt32, Float32],
                    &vec![UInt32, Float64],
                    &vec![UInt64, Float32],
                    &vec![UInt64, Float64]
                ]
            );
        } else {
            panic!("Expected oneof type signature");
        }

        assert_eq!(
            udf.return_type.data_type,
            arrow::datatypes::DataType::Float64
        );
        assert!(!udf.return_type.nullable);

        let data = vec![
            ColumnarValue::Array(Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))),
            ColumnarValue::Array(Arc::new(arrow::array::Float64Array::from(vec![
                1.0, 2.0, 3.0,
            ]))),
        ];

        let result = udf.invoke(&data).unwrap();
        if let ColumnarValue::Array(a) = result {
            let a = a
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            assert_eq!(a.len(), 3);
            assert_eq!(a.value(0), 2.0);
            assert_eq!(a.value(1), 4.0);
            assert_eq!(a.value(2), 6.0);
        } else {
            panic!("Expected array result");
        }
    }
}
