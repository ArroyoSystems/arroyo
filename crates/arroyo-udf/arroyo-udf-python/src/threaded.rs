use crate::interpreter::SubInterpreter;
use crate::pyarrow::Converter;
use crate::types::extract_type_info;
use crate::{PythonUDF, UDF_PY_LIB};
use anyhow::anyhow;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use arroyo_udf_common::parse::NullableType;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use itertools::Itertools;
use pyo3::prelude::*;
use pyo3::types::{PyFunction, PyList, PyString, PyTuple};
use std::sync::{Arc, Mutex};
use std::thread;

pub struct ThreadedUdfInterpreter {}

impl ThreadedUdfInterpreter {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(body: Arc<String>) -> anyhow::Result<PythonUDF> {
        let (task_tx, task_rx) = std::sync::mpsc::sync_channel(0);
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(0);
        let (parse_tx, parse_rx) = std::sync::mpsc::sync_channel(0);

        thread::spawn({
            let body = body.clone();
            move || {
                let interpreter = SubInterpreter::new().unwrap();
                let (name, arg_types, ret) = match Self::parse(&interpreter, &body) {
                    Ok(p) => p,
                    Err(e) => {
                        parse_tx.send(Err(anyhow!("{}", e.to_string()))).unwrap();
                        return;
                    }
                };

                parse_tx
                    .send(Ok((name.clone(), arg_types.clone(), ret.clone())))
                    .unwrap();

                while let Ok(args) = task_rx.recv() {
                    result_tx
                        .send(Self::execute(
                            &interpreter,
                            &name,
                            &arg_types,
                            args,
                            &ret.data_type,
                        ))
                        .expect("python result queue closed");
                }
            }
        });

        let (name, arg_types, return_type) = parse_rx.recv()??;

        let type_signature = Self::get_typesignature(&arg_types);

        Ok(PythonUDF {
            name,
            task_tx,
            result_rx: Arc::new(Mutex::new(result_rx)),
            definition: body,
            signature: Arc::new(Signature {
                type_signature,
                volatility: Volatility::Volatile,
            }),
            arg_types,
            return_type,
        })
    }

    fn get_typesignature(args: &[NullableType]) -> TypeSignature {
        let input = args
            .iter()
            .map(|arg| Self::get_alternatives(&arg.data_type))
            .multi_cartesian_product()
            .map(TypeSignature::Exact)
            .collect();

        TypeSignature::OneOf(input)
    }

    fn get_alternatives(dt: &DataType) -> Vec<DataType> {
        match dt {
            DataType::Int64 => vec![
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::UInt8,
                DataType::UInt16,
                DataType::UInt32,
                DataType::UInt64,
            ],
            DataType::Float64 => vec![DataType::Float32, DataType::Float64],
            _ => vec![dt.clone()],
        }
    }

    fn execute(
        interpreter: &SubInterpreter,
        name: &str,
        arg_types: &[NullableType],
        args: Vec<ArrayRef>,
        ret_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        interpreter
            .with_gil(|py| {
                let function = py.eval_bound(name, None, None).unwrap();
                let function = function.downcast::<PyFunction>().unwrap();

                let size = args.first().map(|e| e.len()).unwrap_or(0);

                let results: anyhow::Result<Vec<_>> = (0..size)
                    .map(|i| {
                        let args: anyhow::Result<Vec<_>> = args
                            .iter()
                            .map(|a| {
                                Converter::get_pyobject(py, a, i).map_err(|e| {
                                    anyhow!("Could not convert datatype to python: {}", e)
                                })
                            })
                            .collect();

                        let mut args = args?;

                        // we don't call the UDF on null arguments unless it's declared with an
                        // optional type
                        if args
                            .iter()
                            .zip(arg_types)
                            .any(|(arg, t)| arg.is_none(py) && !t.nullable)
                        {
                            Ok(py.None())
                        } else {
                            let args = PyTuple::new_bound(py, args.drain(..));

                            function
                                .call1(args)
                                .map_err(|e| {
                                    anyhow!("failed while calling Python UDF '{}': {}", &name, e)
                                })
                                .map(|r| r.into())
                        }
                    })
                    .collect();

                Converter::build_array(ret_type, py, &results?).map_err(|e| {
                    anyhow!(
                        "could not convert results from Python UDF '{}' to arrow: {}",
                        name,
                        e
                    )
                    .into()
                })
            })
            .map_err(|e| e.into())
    }

    #[allow(clippy::type_complexity)]
    fn parse(
        interpreter: &SubInterpreter,
        body: &str,
    ) -> anyhow::Result<(Arc<String>, Arc<Vec<NullableType>>, Arc<NullableType>)> {
        interpreter.with_gil(|py| {
            let lib = PyModule::from_code_bound(py, UDF_PY_LIB, "arroyo_udf", "arroyo_udf")?;

            py.run_bound(body, None, None)?;

            let udfs = lib.call_method0( "get_udfs")?;
            let udfs: &Bound<PyList> = udfs.downcast().unwrap();

            match udfs.len() {
                0 => Err(anyhow!("The supplied code does not contain a UDF (UDF functions must be annotated with @udf)").into()),
                1 => {
                    let udf = udfs.get_item(0)?;
                    let name = udf.getattr("__name__")?.downcast::<PyString>().unwrap()
                        .to_string();
                    let (args, ret) = extract_type_info(&udfs.get_item(0).unwrap())?;
                    Ok((Arc::new(name), Arc::new(args), Arc::new(ret)))
                }
                _ => Err(anyhow!("More than one function was annotated with @udf, which is not supported").into()),
            }
        }).map_err(|e| e.into())
    }
}
