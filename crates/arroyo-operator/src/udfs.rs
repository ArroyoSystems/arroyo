use arrow::array::cast::as_list_array;
use arrow::array::{new_empty_array, Array, ArrayRef, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, FieldRef, IntervalUnit, TimeUnit};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arroyo_udf_host::SyncUdfDylib;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::Accumulator;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct UdafArg {
    values: Vec<ArrayRef>,
    inner: FieldRef,
}

impl UdafArg {
    pub fn new(inner: FieldRef) -> Self {
        UdafArg {
            values: vec![],
            inner,
        }
    }

    fn concatenate_array(&self) -> Result<ListArray> {
        let element_arrays: Vec<&dyn Array> = self.values.iter().map(|a| a.as_ref()).collect();

        let arr = arrow::compute::concat(&element_arrays)?;

        Ok(list_from_arr(&self.inner, arr))
    }
}

/// An Arroyo UDAF is a scalar function that takes vector arguments. This Accumulator infra
/// exists to wrap an Arroyo UDAF in a DF UDAF that first accumulates the array of data, then
/// passes it to the vector-taking UDF.
#[derive(Debug)]
pub struct ArroyoUdaf {
    args: Vec<UdafArg>,
    output_type: Arc<DataType>,
    udf: Arc<SyncUdfDylib>,
}

impl ArroyoUdaf {
    pub fn new(args: Vec<UdafArg>, output_type: Arc<DataType>, udf: Arc<SyncUdfDylib>) -> Self {
        assert!(
            !args.is_empty(),
            "UDAF {} has no arguments, but UDAFs must have at least one",
            udf.name()
        );
        ArroyoUdaf {
            args,
            output_type,
            udf,
        }
    }
}

impl Accumulator for ArroyoUdaf {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        for (arg, v) in self.args.iter_mut().zip(values) {
            arg.values.push(v.clone());
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.args[0].values.is_empty() {
            return Ok(scalar_none(&self.output_type));
        }

        let args: Result<Vec<_>> = self
            .args
            .iter()
            .map(|arg| {
                let element_arrays: Vec<&dyn Array> =
                    arg.values.iter().map(|a| a.as_ref()).collect();
                Ok(arrow::compute::concat(&element_arrays)?)
            })
            .collect();

        self.udf.invoke_udaf(&args?[..])
    }

    fn size(&self) -> usize {
        let values = self
            .args
            .iter()
            .map(|a| {
                std::mem::size_of::<ArrayRef>() * a.values.capacity()
                    + a.values
                        .iter()
                        .map(|v| v.get_array_memory_size())
                        .sum::<usize>()
            })
            .sum::<usize>();

        std::mem::size_of_val(self) + values
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let states: Result<Vec<_>> = self
            .args
            .iter()
            .map(|arg| Ok(ScalarValue::List(Arc::new(arg.concatenate_array()?))))
            .collect();
        states
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        for (arg, arr) in self.args.iter_mut().zip(states) {
            for l in as_list_array(&arr).iter().flatten() {
                arg.values.push(l);
            }
        }

        Ok(())
    }
}

fn list_from_arr(field_ref: &FieldRef, arr: ArrayRef) -> ListArray {
    let offsets = OffsetBuffer::from_lengths([arr.len()]);

    ListArray::new(field_ref.clone(), offsets, arr, None)
}

fn scalar_none(datatype: &DataType) -> ScalarValue {
    match datatype {
        DataType::Boolean => ScalarValue::Boolean(None),
        DataType::Int8 => ScalarValue::Int8(None),
        DataType::Int16 => ScalarValue::Int16(None),
        DataType::Int32 => ScalarValue::Int32(None),
        DataType::Int64 => ScalarValue::Int64(None),
        DataType::UInt8 => ScalarValue::UInt8(None),
        DataType::UInt16 => ScalarValue::UInt16(None),
        DataType::UInt32 => ScalarValue::UInt32(None),
        DataType::UInt64 => ScalarValue::UInt64(None),
        DataType::Float32 => ScalarValue::Float32(None),
        DataType::Float64 => ScalarValue::Float64(None),
        DataType::Timestamp(TimeUnit::Second, tz) => ScalarValue::TimestampSecond(None, tz.clone()),
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            ScalarValue::TimestampMillisecond(None, tz.clone())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            ScalarValue::TimestampMicrosecond(None, tz.clone())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            ScalarValue::TimestampNanosecond(None, tz.clone())
        }
        DataType::Interval(IntervalUnit::YearMonth) => ScalarValue::IntervalYearMonth(None),
        DataType::Interval(IntervalUnit::DayTime) => ScalarValue::IntervalDayTime(None),
        DataType::Interval(IntervalUnit::MonthDayNano) => ScalarValue::IntervalMonthDayNano(None),
        DataType::Duration(TimeUnit::Second) => ScalarValue::DurationSecond(None),
        DataType::Duration(TimeUnit::Millisecond) => ScalarValue::DurationMillisecond(None),
        DataType::Duration(TimeUnit::Microsecond) => ScalarValue::DurationMicrosecond(None),
        DataType::Duration(TimeUnit::Nanosecond) => ScalarValue::DurationNanosecond(None),
        DataType::Null => ScalarValue::Null,
        DataType::Date32 => ScalarValue::Date32(None),
        DataType::Date64 => ScalarValue::Date64(None),
        DataType::Time32(TimeUnit::Second) => ScalarValue::Time32Second(None),
        DataType::Time32(TimeUnit::Millisecond) => ScalarValue::Time32Millisecond(None),
        DataType::Time64(TimeUnit::Microsecond) => ScalarValue::Time64Microsecond(None),
        DataType::Time64(TimeUnit::Nanosecond) => ScalarValue::Time64Nanosecond(None),
        DataType::Binary => ScalarValue::Binary(None),
        DataType::FixedSizeBinary(size) => ScalarValue::FixedSizeBinary(*size, None),
        DataType::LargeBinary => ScalarValue::LargeBinary(None),
        DataType::Utf8 => ScalarValue::Utf8(None),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
        DataType::List(item) => ScalarValue::List(Arc::new(list_from_arr(
            item,
            new_empty_array(item.data_type()),
        ))),
        DataType::FixedSizeList(_, _) => todo!(),
        DataType::LargeList(_) => todo!(),
        DataType::Struct(_) => todo!(),
        DataType::Union(_, _) => todo!(),
        DataType::Dictionary(_, _) => todo!(),
        DataType::Decimal128(_, _) => todo!(),
        DataType::Decimal256(_, _) => todo!(),
        DataType::Map(_, _) => todo!(),
        DataType::RunEndEncoded(_, _) => todo!(),
        DataType::Float16 => unimplemented!("cannot represent float16 as scalar"),
        DataType::Time32(TimeUnit::Microsecond) => {
            unimplemented!("cannot represent time32 microseconds as scalar")
        }
        DataType::Time32(TimeUnit::Nanosecond) => {
            unimplemented!("cannot represent time32 nanos as scalar")
        }
        DataType::Time64(TimeUnit::Second) => {
            unimplemented!("cannot represent time64 seconds as scalar")
        }
        DataType::Time64(TimeUnit::Millisecond) => {
            unimplemented!("cannot represent time64 millis as scalar")
        }
        DataType::BinaryView
        | DataType::Utf8View
        | DataType::ListView(_)
        | DataType::LargeListView(_) => unimplemented!("views are not supported"),
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct FfiArraySchemaPair(FFI_ArrowArray, FFI_ArrowSchema);

#[repr(C)]
pub struct FfiArrayResult(FFI_ArrowArray, FFI_ArrowSchema, bool);
