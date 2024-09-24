use anyhow::{anyhow, bail};
use arrow::datatypes::DataType;
use arroyo_udf_common::parse::NullableType;
use pyo3::prelude::{PyAnyMethods, PyDictMethods, PyStringMethods, PyTupleMethods};
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{Bound, PyAny};

pub fn extract_type_info(udf: &Bound<PyAny>) -> anyhow::Result<(Vec<NullableType>, NullableType)> {
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
