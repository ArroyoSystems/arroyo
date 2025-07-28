use arrow_array::cast::AsArray;
use arrow_array::{
    Array, Date32Array, Date64Array, Decimal128Array, GenericStringArray, OffsetSizeTrait,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_json::writer::NullableEncoder;
use arrow_json::{Encoder, EncoderFactory, EncoderOptions};
use arrow_schema::{ArrowError, DataType, FieldRef, TimeUnit};
use arroyo_rpc::formats::{DecimalEncoding, TimestampFormat};
use arroyo_types::ArroyoExtensionType;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;

/// Custom JSON encoder factory that adds some Arroyo-specific features:
///   * Customizable time format handling, controlled by the supplied `TimestampFormat`
///   * Support for serializing RawJson
///   * Customizable decimal encoding, to handle writing to Avro and Kafka Connect
#[derive(Debug)]
pub struct ArroyoEncoderFactory {
    pub timestamp_format: TimestampFormat,
    pub decimal_encoding: DecimalEncoding,
}

impl EncoderFactory for ArroyoEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        let encoder: Box<dyn Encoder> = match (
            self.decimal_encoding,
            self.timestamp_format,
            array.data_type(),
        ) {
            (_, TimestampFormat::UnixMillis, DataType::Timestamp(TimeUnit::Second, _)) => {
                Box::new(UnixMillisTimeEncoder::TimestampSeconds(
                    array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .clone(),
                ))
            }
            (_, TimestampFormat::UnixMillis, DataType::Timestamp(TimeUnit::Millisecond, _)) => {
                Box::new(UnixMillisTimeEncoder::TimestampMillis(
                    array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .clone(),
                ))
            }
            (_, TimestampFormat::UnixMillis, DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                Box::new(UnixMillisTimeEncoder::TimestampMicros(
                    array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .clone(),
                ))
            }
            (_, TimestampFormat::UnixMillis, DataType::Timestamp(TimeUnit::Nanosecond, _)) => {
                Box::new(UnixMillisTimeEncoder::TimestampNanos(
                    array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .clone(),
                ))
            }
            (_, TimestampFormat::UnixMillis, DataType::Date32) => {
                Box::new(UnixMillisTimeEncoder::Date32(
                    array
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                        .clone(),
                ))
            }
            (_, TimestampFormat::UnixMillis, DataType::Date64) => {
                Box::new(UnixMillisTimeEncoder::Date64(
                    array
                        .as_any()
                        .downcast_ref::<Date64Array>()
                        .unwrap()
                        .clone(),
                ))
            }
            (_, _, DataType::Utf8) => {
                if matches!(
                    ArroyoExtensionType::from_map(field.metadata()),
                    Some(ArroyoExtensionType::JSON)
                ) {
                    Box::new(RawJsonEncoder(array.as_string::<i32>().clone()))
                } else {
                    return Ok(None);
                }
            }
            (DecimalEncoding::Bytes, _, DataType::Decimal128(_, _)) => {
                Box::new(DecimalEncoder::BytesEncoder(
                    array
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .clone(),
                ))
            }
            (DecimalEncoding::String, _, DataType::Decimal128(_, _)) => {
                Box::new(DecimalEncoder::StringEncoder(
                    array
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .clone(),
                ))
            }
            _ => {
                return Ok(None);
            }
        };

        Ok(Some(NullableEncoder::new(encoder, array.nulls().cloned())))
    }
}

enum DecimalEncoder {
    StringEncoder(Decimal128Array),
    BytesEncoder(Decimal128Array),
}

impl Encoder for DecimalEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        match self {
            DecimalEncoder::StringEncoder(array) => {
                out.push(b'"');
                out.extend_from_slice(array.value_as_string(idx).as_bytes());
                out.push(b'"');
            }
            DecimalEncoder::BytesEncoder(array) => {
                let v = array.value(idx);
                out.push(b'"');
                out.extend_from_slice(BASE64_STANDARD.encode(v.to_be_bytes()).as_bytes());
                out.push(b'"');
            }
        }
    }
}

enum UnixMillisTimeEncoder {
    Date32(Date32Array),
    Date64(Date64Array),
    TimestampNanos(TimestampNanosecondArray),
    TimestampMicros(TimestampMicrosecondArray),
    TimestampMillis(TimestampMillisecondArray),
    TimestampSeconds(TimestampSecondArray),
}

impl Encoder for UnixMillisTimeEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let millis = match self {
            UnixMillisTimeEncoder::Date32(a) => (a.value(idx) as f64) * 86400.0 * 1000.0,
            UnixMillisTimeEncoder::Date64(a) => a.value(idx) as f64,
            UnixMillisTimeEncoder::TimestampNanos(a) => a.value(idx) as f64 / 1_000_000.0,
            UnixMillisTimeEncoder::TimestampMicros(a) => a.value(idx) as f64 / 1_000.0,
            UnixMillisTimeEncoder::TimestampMillis(a) => a.value(idx) as f64,
            UnixMillisTimeEncoder::TimestampSeconds(a) => a.value(idx) as f64 * 1000.0,
        } as i64;

        out.extend_from_slice(millis.to_string().as_bytes());
    }
}

struct RawJsonEncoder<O: OffsetSizeTrait>(GenericStringArray<O>);

impl<O: OffsetSizeTrait> Encoder for RawJsonEncoder<O> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(self.0.value(idx)) {
            serde_json::to_writer(out, &v)
                // cannot fail when writing to a Vec buffer
                .unwrap()
        }
    }
}
