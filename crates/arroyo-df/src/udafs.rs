use crate::physical::UdfDylib;
use arrow::array::ArrayData;
use arrow::buffer::OffsetBuffer;
use arrow_array::cast::as_list_array;
use arrow_array::{make_array, Array, ListArray};
use arrow_schema::{DataType, FieldRef};
use datafusion::arrow::array::ArrayRef;
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use datafusion_common::DataFusionError;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl};
use std::fmt::Debug;
use std::sync::Arc;

/// An Arroyo UDAF is a scalar function that takes vector arguments. This Accumulator infra
/// exists to wrap an Arroyo UDAF in a DF UDAF that first accumulates the array of data, then
/// passes it to the vector-taking UDF.
#[derive(Debug)]
pub struct ArroyoUdaf {
    values: Vec<ArrayRef>,
    data_type: DataType,
    inner: FieldRef,
    udf: UdfDylib,
}

impl ArroyoUdaf {
    pub fn new(data_type: DataType, udf: UdfDylib) -> Self {
        println!("creating udaf with datatype {:?}", data_type);
        let inner = match &data_type {
            DataType::List(inner) => Arc::clone(inner),
            _ => panic!("udaf {} type {:?} is not a list", udf.name(), data_type),
        };

        ArroyoUdaf {
            data_type,
            values: vec![],
            udf,
            inner,
        }
    }
}

impl ArroyoUdaf {
    fn list_from_arr(&self, arr: ArrayRef) -> ListArray {
        let offsets = OffsetBuffer::from_lengths([arr.len()]);

        ListArray::new(self.inner.clone(), offsets, arr, None)
    }
    fn concatenate_array(&self) -> Result<ListArray> {
        let element_arrays: Vec<&dyn Array> = self.values.iter().map(|a| a.as_ref()).collect();

        let arr = arrow::compute::concat(&element_arrays)?;

        Ok(self.list_from_arr(arr))
    }
}

impl Accumulator for ArroyoUdaf {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        println!("update batch");
        if values.is_empty() {
            return Ok(());
        }

        self.values.push(values[0].clone());

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            let data = Arc::new(make_array(ArrayData::new_empty(&self.data_type)));
            return Ok(ScalarValue::List(Arc::new(self.list_from_arr(data))));
        }

        println!("in eval");

        let ColumnarValue::Scalar(scalar) = self
            .udf
            .invoke(&[ColumnarValue::Scalar(ScalarValue::List(Arc::new(
                self.concatenate_array()?,
            )))])
            .unwrap()
        else {
            return Err(DataFusionError::Execution(format!(
                "UDAF {} returned an array result",
                self.udf.name()
            )));
        };
        println!("EVALUATED: {:?}", scalar);

        Ok(scalar)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::List(Arc::new(self.concatenate_array()?))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let list_arr = as_list_array(&states[0]);
        for arr in list_arr.iter().flatten() {
            self.values.push(arr);
        }

        Ok(())
    }
}

// Fake UDAF used just for plan-time
#[derive(Debug)]
pub struct EmptyUdaf {}
impl Accumulator for EmptyUdaf {
    fn update_batch(&mut self, _: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        unreachable!()
    }

    fn size(&self) -> usize {
        unreachable!()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        unreachable!()
    }

    fn merge_batch(&mut self, _: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }
}
