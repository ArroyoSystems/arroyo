use std::sync::{Arc, Mutex};
use std::thread;
use anyhow::{anyhow, bail};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use pyo3::prelude::*;
use pyo3::types::{PyFunction, PyList, PyString, PyTuple};
use arroyo_udf_common::parse::NullableType;
use crate::{extract_type_info, PythonUDF, UDF_PY_LIB};
use crate::interpreter::SubInterpreter;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use crate::pyarrow::Converter;

pub struct ThreadedUdfInterpreter {
}

impl ThreadedUdfInterpreter {
    pub async fn new(body: Arc<String>) -> anyhow::Result<PythonUDF> {
        let (task_tx, mut task_rx) = std::sync::mpsc::sync_channel(1);
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
        let (parse_tx, parse_rx) = std::sync::mpsc::sync_channel(1);

        thread::spawn({let body = body.clone(); move || {
            let interpreter = SubInterpreter::new().unwrap();
            let (name, args, ret) = match Self::parse(&interpreter, &*body) {
                Ok(p) => p,
                Err(e) => {
                    parse_tx.send(Err(anyhow!("{}", e.to_string()))).unwrap();
                    return;
                }
            };

            parse_tx.send(Ok((name.clone(), args.clone(), ret.clone()))).unwrap();

            loop {
                match task_rx.recv() {
                    Ok(args) => {
                        result_tx.send(Self::execute(&interpreter, &name, args, &ret.data_type))
                            .expect("python result queue closed");
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }});

        let (name, arg_types, return_type) = parse_rx.recv()??;
        let arg_dts = arg_types.iter().map(|t| t.data_type.clone()).collect();        

        Ok(PythonUDF {
            name,
            task_tx,
            result_rx: Arc::new(Mutex::new(result_rx)),
            definition: body,
            signature: Arc::new(Signature {
                type_signature: TypeSignature::Exact(arg_dts),
                volatility: Volatility::Volatile,
            }),
            arg_types,
            return_type,
        })
    }
    
    fn execute(interpreter: &SubInterpreter, name: &str, args: Vec<ArrayRef>, ret_type: &DataType) -> anyhow::Result<ArrayRef> {
        interpreter.with_gil(|py| {
            let function = py.eval_bound(name, None, None)
                .unwrap();
            let function = function.downcast::<PyFunction>().unwrap();

            let size = args.get(0).map(|e| e.len()).unwrap_or(0);

            let results: anyhow::Result<Vec<_>> = (0..size)
                .map(|i| {
                    let args: anyhow::Result<Vec<_>> =
                        args.iter()
                            .map(|a| Converter::get_pyobject(py, &*a, i)
                                .map_err(|e| {
                                    anyhow!(
                                        "Could not convert datatype to python: {}",
                                        e
                                    )
                            }))
                            .collect();

                    let args = PyTuple::new_bound(py, args?.drain(..));

                    function.call1(args).map_err(|e| {
                        anyhow!(
                            "failed while calling Python UDF '{}': {}",
                            &name, e
                        )
                    }).map(|r| r.into())
                })
                .collect();
            
                Converter::build_array(&ret_type, py, &results?).map_err(|e| {
                    anyhow!(
                        "could not convert results from Python UDF '{}' to arrow: {}",
                        name, e
                    )
                })           
        })
    }

    fn parse(interpreter: &SubInterpreter, body: &str) -> anyhow::Result<(Arc<String>, Arc<Vec<NullableType>>, Arc<NullableType>)> {
        interpreter.with_gil(|py| {
            let lib = PyModule::from_code_bound(py, UDF_PY_LIB, "arroyo_udf", "arroyo_udf")?;

            py.run_bound(&body, None, None)?;

            let udfs = lib.call_method0( "get_udfs")?;
            let udfs: &Bound<PyList> = udfs.downcast().unwrap();

            match udfs.len() {
                0 => bail!("The supplied code does not contain a UDF (UDF functions must be annotated with @udf)"),
                1 => {
                    let udf = udfs.get_item(0)?;
                    let name = udf.getattr("__name__")?.downcast::<PyString>().unwrap()
                        .to_string();
                    let (args, ret) = extract_type_info(&udfs.get_item(0).unwrap())?;
                    Ok((Arc::new(name), Arc::new(args), Arc::new(ret)))
                }
                _ => {
                    bail!("More than one function was annotated with @udf, which is not supported");
                }
            }
        })
    }
}
