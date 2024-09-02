mod pyarrow;

use crate::pyarrow::Converter;
use anyhow::{anyhow, bail};
use arrow::array::Array;
use arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFunction, PyList, PyString, PyTuple};
use pyo3::{Bound, Py, PyAny, Python};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use arroyo_udf_common::parse::NullableType;

pub const SUPPORT_CODE: &str = r#"
def udf(func):
    udf_registry.append(func)
    return func
"#;

#[derive(Debug)]
pub struct PythonUDF {
    pub name: Arc<String>,
    pub definition: Arc<String>,
    pub function: Py<PyFunction>,
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
        Python::with_gil(|py| {
            let size = args
                .get(0)
                .map(|c| match c {
                    ColumnarValue::Array(a) => a.len(),
                    ColumnarValue::Scalar(_) => 1,
                })
                .unwrap_or(1);

            let results: DFResult<Vec<_>> = (0..size)
                .map(|i| {
                    let args: datafusion::common::Result<Vec<_>> =
                        args.iter()
                            .map(|c| match c {
                                ColumnarValue::Array(a) => Converter::get_pyobject(py, &*a, i)
                                    .map_err(|e| {
                                        DataFusionError::Execution(format!(
                                            "Could not convert datatype to python: {}",
                                            e
                                        ))
                                    }),
                                ColumnarValue::Scalar(s) => {
                                    todo!("scalars")
                                }
                            })
                            .collect();

                    let args = PyTuple::new_bound(py, args?.drain(..));

                    self.function.call1(py, args).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "failed while calling Python UDF '{}': {}",
                            self.name, e
                        ))
                    })
                })
                .collect();

            let results =
                Converter::build_array(&self.return_type.data_type, py, &results?).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "could not convert results from Python UDF '{}' to arrow: {}",
                        self.name, e
                    ))
                })?;

            Ok(ColumnarValue::Array(results))
        })
    }
}

fn extract_type_info(
    udf: &Bound<PyAny>,
) -> anyhow::Result<(Vec<NullableType>, NullableType)> {
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
    pub fn parse(body: impl Into<String>) -> anyhow::Result<Self> {
        let body = format!("{}\n{}", SUPPORT_CODE, body.into());
        let (name, args, ret, function) = Python::with_gil(|py| {
            let globals = PyDict::new_bound(py);
            globals.set_item("udf_registry", PyList::empty_bound(py))?;
            py.run_bound(&body, Some(&globals), None)?;

            let udfs = globals.get_item("udf_registry")?.unwrap();
            match udfs.len()? {
                0 => bail!("The supplied code does not contain a UDF (UDF functions must be annotated with @udf)"),
                1 => {
                    let udf = udfs.get_item(0)?;
                    let name = udf.getattr("__name__")?.downcast::<PyString>().unwrap()
                        .to_string();
                    let (args, ret) = extract_type_info(&udfs.get_item(0).unwrap())?;
                    Ok((name, args, ret, udf.downcast().unwrap().clone().unbind()))
                }
                _ => {
                    bail!("More than one function was annotated with @udf, which is not supported");
                }
            }
        })?;

        let arg_dts = args.iter().map(|t| t.data_type.clone()).collect();

        Ok(Self {
            name: Arc::new(name),
            function,
            definition: Arc::new(body),
            signature: Arc::new(Signature {
                type_signature: TypeSignature::Exact(arg_dts),
                volatility: Volatility::Volatile,
            }),
            arg_types: Arc::new(args),
            return_type: Arc::new(ret),
        })
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
    use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl};
    use std::sync::Arc;

    #[test]
    fn test() {
        pyo3::prepare_freethreaded_python();
        let udf = r#"
@udf
def my_add(x: int, y: float) -> float:
    return float(x) + y
"#;

        let udf = PythonUDF::parse(udf).unwrap();
        assert_eq!(udf.name.as_str(), "my_add");
        if let datafusion::logical_expr::TypeSignature::Exact(args) = &udf.signature.type_signature
        {
            assert_eq!(
                args,
                &vec![
                    arrow::datatypes::DataType::Int64,
                    arrow::datatypes::DataType::Float64
                ]
            );
        } else {
            panic!("Expected exact type signature");
        }

        assert_eq!(udf.return_type.data_type, arrow::datatypes::DataType::Float64);
        assert_eq!(udf.return_type.nullable, false);

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
