use datafusion::arrow::array::ArrayRef;
use datafusion::scalar::ScalarValue;
use datafusion::{error::Result, physical_plan::Accumulator};
use std::fmt::Debug;

// Fake UDAF used just for plan-time
#[derive(Debug)]
pub struct EmptyUdaf {}
impl Accumulator for EmptyUdaf {
    fn update_batch(&mut self, _: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        unreachable!()
    }

    fn size(&self) -> usize {
        unreachable!()
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        unreachable!()
    }

    fn merge_batch(&mut self, _: &[ArrayRef]) -> Result<()> {
        unreachable!()
    }
}
