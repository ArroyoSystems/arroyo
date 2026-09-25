use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow_array::cast::AsArray;
use arrow_array::{Array, StringArray, StringViewArray};

#[derive(Clone, Copy)]
pub enum StringArrayRef<'a> {
    Utf8(&'a StringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> StringArrayRef<'a> {
    pub fn new(array: &'a dyn Array) -> Result<Self, ArrowError> {
        match array.data_type() {
            DataType::Utf8 => Ok(Self::Utf8(array.as_string::<i32>())),
            DataType::Utf8View => Ok(Self::Utf8View(array.as_string_view())),
            data_type => Err(ArrowError::InvalidArgumentError(format!(
                "expected Utf8 or Utf8View, got {data_type}"
            ))),
        }
    }

    pub fn value(&self, index: usize) -> &'a str {
        match self {
            Self::Utf8(array) => array.value(index),
            Self::Utf8View(array) => array.value(index),
        }
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = Option<&'a str>> + '_ {
        let array: &dyn Array = match self {
            Self::Utf8(array) => *array,
            Self::Utf8View(array) => *array,
        };
        (0..array.len()).map(move |index| (!array.is_null(index)).then(|| self.value(index)))
    }
}
