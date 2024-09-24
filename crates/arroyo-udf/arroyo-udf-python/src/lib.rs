#[cfg(feature = "python-enabled")]
mod interpreter;
#[cfg(feature = "python-enabled")]
mod pyarrow;
#[cfg(feature = "python-enabled")]
mod threaded;
#[cfg(feature = "python-enabled")]
mod types;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use arroyo_udf_common::parse::NullableType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature};
use std::any::Any;
use std::fmt::Debug;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, Mutex};

#[cfg(not(feature = "python-enabled"))]
const NOT_ENABLED_ERROR: &str =
    "Python is not enabled in this build of Arroyo. See https://doc.arroyo.dev/udfs/python/udfs \
            for more information on how to obtain a Python-enabled build.";

#[cfg(feature = "python-enabled")]
const UDF_PY_LIB: &str = include_str!("../python/arroyo_udf.py");

#[derive(Debug)]
pub struct PythonUDF {
    pub name: Arc<String>,
    pub(crate) task_tx: SyncSender<Vec<ArrayRef>>,
    pub(crate) result_rx: Arc<Mutex<Receiver<anyhow::Result<ArrayRef>>>>,
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

impl PythonUDF {
    #[allow(unused)]
    pub async fn parse(body: impl Into<String>) -> anyhow::Result<Self> {
        #[cfg(feature = "python-enabled")]
        {
            crate::threaded::ThreadedUdfInterpreter::new(Arc::new(body.into())).await
        }

        #[cfg(not(feature = "python-enabled"))]
        {
            anyhow::bail!(NOT_ENABLED_ERROR)
        }
    }
}
