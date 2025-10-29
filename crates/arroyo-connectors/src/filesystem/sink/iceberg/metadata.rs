use anyhow::{anyhow, bail};
use std::collections::HashMap;

use crate::filesystem::config::IcebergPartitioning;
use bincode::{Decode, Encode};
use futures::{StreamExt, TryStreamExt};
use iceberg::spec::{
    visit_schema, DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, ListType,
    Literal, MapType, NestedFieldRef, PrimitiveType, Schema as IceSchema, Schema as IcebergSchema,
    SchemaVisitor, StructType, Type,
};
use iceberg::transform::create_transform_function;
use itertools::Itertools;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::statistics::Statistics;
use parquet::format::FileMetaData;
use parquet::thrift::TSerializable;
use thrift::protocol::{TCompactOutputProtocol, TOutputProtocol};
use tracing::warn;
use uuid::Uuid;

const DEFAULT_MAP_FIELD_NAME: &str = "key_value";

// bounds encoded as iceberg Datums
#[derive(Debug, Decode, Encode, Clone, PartialEq, Eq, Default)]
pub struct BoundsEncoded {
    pub min: Option<Vec<u8>>,
    pub max: Option<Vec<u8>>,
}

#[derive(Debug, Decode, Encode, Clone, PartialEq, Eq, Default)]
pub struct ColumnAccum {
    pub compressed_size: u64,
    pub num_values: u64,
    pub null_values: u64,
    pub bounds: BoundsEncoded,
}

#[derive(Debug, Decode, Encode, Clone, PartialEq, Eq)]
pub struct IcebergFileMetadata {
    pub row_count: u64,
    pub split_offsets: Vec<i64>,
    pub columns: HashMap<i32, ColumnAccum>,
}

impl IcebergFileMetadata {
    pub fn from_parquet(meta: FileMetaData, iceberg_schema: &IcebergSchema) -> IcebergFileMetadata {
        let mut buffer = Vec::new();
        {
            let mut protocol = TCompactOutputProtocol::new(&mut buffer);
            meta.write_to_out_protocol(&mut protocol)
                .expect("unable to write out parquet metadata");
            protocol
                .flush()
                .expect("unable to flush parquet metadata to buffer");
        }

        let meta = ParquetMetaDataReader::decode_metadata(&buffer)
            .expect("failed to decode parquet metadata");

        let index_by_parquet_path = {
            let mut visitor = IndexByParquetPathName::new();
            visit_schema(iceberg_schema, &mut visitor).expect("unable to traverse Iceberg schema");
            visitor
        };

        let file_md = meta.file_metadata();
        let schema = file_md.schema_descr();
        let col_descs = schema.columns();

        let mut columns: HashMap<i32, ColumnAccum> = HashMap::with_capacity(col_descs.len());

        let mut min_max_agg = MinMaxColAggregator::new(iceberg_schema);

        let mut split_offsets = Vec::with_capacity(meta.num_row_groups());
        for rg in meta.row_groups() {
            if let Some(off) = rg.file_offset() {
                split_offsets.push(off);
            }
            for col in rg.columns().iter() {
                let Some(&field_id) =
                    index_by_parquet_path.get(&col.column_descr().path().string())
                else {
                    continue;
                };

                let acc = columns.entry(field_id).or_default();

                acc.compressed_size += col.compressed_size() as u64;
                acc.num_values += col.num_values() as u64;

                if let Some(statistics) = col.statistics() {
                    acc.null_values += statistics.null_count_opt().unwrap_or(0);

                    if let Err(e) = min_max_agg.update(field_id, statistics) {
                        warn!("unable to compute iceberg statistics for column: {:?}", e);
                    }
                }
            }
        }

        let (mins, maxes) = min_max_agg.produce();
        for (k, v) in columns.iter_mut() {
            if let Some(min) = mins.get(k) {
                v.bounds.min = Some(min.to_bytes().unwrap().into_vec());
            }

            if let Some(max) = maxes.get(k) {
                v.bounds.max = Some(max.to_bytes().unwrap().into_vec());
            }
        }

        IcebergFileMetadata {
            row_count: file_md.num_rows() as u64,
            split_offsets,
            columns,
        }
    }
}

pub fn build_datafile_from_meta(
    ice_schema: &IceSchema,
    meta: &IcebergFileMetadata,
    partitioning: &IcebergPartitioning,
    file_path: String,
    file_size_bytes: u64,
    partition_spec_id: i32,
) -> anyhow::Result<DataFile> {
    let mut column_sizes: HashMap<i32, u64> = HashMap::new();
    let mut value_counts: HashMap<i32, u64> = HashMap::new();
    let mut null_counts: HashMap<i32, u64> = HashMap::new();
    let mut lower_bounds: HashMap<i32, Datum> = HashMap::new();
    let mut upper_bounds: HashMap<i32, Datum> = HashMap::new();

    for (fid, acc) in &meta.columns {
        *column_sizes.entry(*fid).or_insert(0) += acc.compressed_size;

        *value_counts.entry(*fid).or_insert(0) += acc.num_values;
        *null_counts.entry(*fid).or_insert(0) += acc.null_values;

        let Some(field) = ice_schema.field_by_id(*fid) else {
            continue;
        };

        let Some(pt) = field.field_type.as_primitive_type() else {
            continue;
        };

        if let Some(min) = &acc.bounds.min {
            lower_bounds.insert(*fid, Datum::try_from_bytes(min, pt.clone())?);
        }

        if let Some(max) = &acc.bounds.max {
            upper_bounds.insert(*fid, Datum::try_from_bytes(max, pt.clone())?);
        }
    }

    let partition: anyhow::Result<Vec<_>> = partitioning
        .fields
        .iter()
        .map(|f| {
            let f_id = ice_schema
                .field_id_by_name(&f.field)
                .ok_or_else(|| anyhow!("partition field not found in schema '{}'!", f.field))?;
            let v = lower_bounds
                .get(&f_id)
                .ok_or_else(|| anyhow!("no metadata for partition field '{}'", f.field))?;
            let fun = create_transform_function(&f.transform.into())?;
            let result = fun
                .transform_literal_result(v)
                .map_err(|e| anyhow!("failed to compute partition function {}", f))?;

            Ok(Some(Literal::Primitive(result.literal().clone())))
        })
        .collect();

    let partition = iceberg::spec::Struct::from_iter(partition?.into_iter());

    let df = DataFileBuilder::default()
        .file_path(file_path)
        .file_format(DataFileFormat::Parquet)
        .record_count(meta.row_count)
        .file_size_in_bytes(file_size_bytes)
        .content(DataContentType::Data)
        .partition(partition)
        .partition_spec_id(partition_spec_id)
        .split_offsets(meta.split_offsets.clone())
        .column_sizes(column_sizes)
        .value_counts(value_counts)
        .null_value_counts(null_counts)
        .lower_bounds(lower_bounds)
        .upper_bounds(upper_bounds)
        .build()?;

    Ok(df)
}

// The following helper code is vendored from https://github.com/apache/iceberg-rust with some
// minimal modifications; used under the terms of the Apache 2 License

struct MinMaxColAggregator<'a> {
    lower_bounds: HashMap<i32, Datum>,
    upper_bounds: HashMap<i32, Datum>,
    schema: &'a IcebergSchema,
}

impl<'a> MinMaxColAggregator<'a> {
    /// Creates new and empty `MinMaxColAggregator`
    fn new(schema: &'a IcebergSchema) -> Self {
        Self {
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            schema,
        }
    }

    fn update_state_min(&mut self, field_id: i32, datum: Datum) {
        self.lower_bounds
            .entry(field_id)
            .and_modify(|e| {
                if *e > datum {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    fn update_state_max(&mut self, field_id: i32, datum: Datum) {
        self.upper_bounds
            .entry(field_id)
            .and_modify(|e| {
                if *e < datum {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    /// Update statistics
    fn update(&mut self, field_id: i32, value: &Statistics) -> anyhow::Result<()> {
        let Some(ty) = self
            .schema
            .field_by_id(field_id)
            .map(|f| f.field_type.as_ref())
        else {
            // Following java implementation: https://github.com/apache/iceberg/blob/29a2c456353a6120b8c882ed2ab544975b168d7b/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L163
            // Ignore the field if it is not in schema.
            return Ok(());
        };
        let Type::Primitive(ty) = ty.clone() else {
            bail!(
                "Composed type {} is not supported for min max aggregation.",
                ty
            );
        };

        if value.min_is_exact() {
            let Some(min_datum) = get_parquet_stat_min_as_datum(&ty, value)? else {
                bail!("Statistics {} is not match with field type {}.", value, ty);
            };

            self.update_state_min(field_id, min_datum);
        }

        if value.max_is_exact() {
            let Some(max_datum) = get_parquet_stat_max_as_datum(&ty, value)? else {
                bail!("Statistics {} is not match with field type {}.", value, ty);
            };

            self.update_state_max(field_id, max_datum);
        }

        Ok(())
    }

    /// Returns lower and upper bounds
    fn produce(self) -> (HashMap<i32, Datum>, HashMap<i32, Datum>) {
        (self.lower_bounds, self.upper_bounds)
    }
}

pub(crate) fn get_parquet_stat_min_as_datum(
    primitive_type: &PrimitiveType,
    stats: &Statistics,
) -> anyhow::Result<Option<Datum>> {
    Ok(match (primitive_type, stats) {
        (PrimitiveType::Boolean, Statistics::Boolean(stats)) => {
            stats.min_opt().map(|val| Datum::bool(*val))
        }
        (PrimitiveType::Int, Statistics::Int32(stats)) => {
            stats.min_opt().map(|val| Datum::int(*val))
        }
        (PrimitiveType::Date, Statistics::Int32(stats)) => {
            stats.min_opt().map(|val| Datum::date(*val))
        }
        (PrimitiveType::Long, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::long(*val))
        }
        (PrimitiveType::Time, Statistics::Int64(stats)) => {
            let Some(val) = stats.min_opt() else {
                return Ok(None);
            };

            Some(Datum::time_micros(*val)?)
        }
        (PrimitiveType::Timestamp, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::timestamp_micros(*val))
        }
        (PrimitiveType::Timestamptz, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::timestamptz_micros(*val))
        }
        (PrimitiveType::TimestampNs, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::timestamp_nanos(*val))
        }
        (PrimitiveType::TimestamptzNs, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::timestamptz_nanos(*val))
        }
        (PrimitiveType::Float, Statistics::Float(stats)) => {
            stats.min_opt().map(|val| Datum::float(*val))
        }
        (PrimitiveType::Double, Statistics::Double(stats)) => {
            stats.min_opt().map(|val| Datum::double(*val))
        }
        (PrimitiveType::String, Statistics::ByteArray(stats)) => {
            let Some(val) = stats.min_opt() else {
                return Ok(None);
            };

            Some(Datum::string(val.as_utf8()?))
        }
        (PrimitiveType::Uuid, Statistics::FixedLenByteArray(stats)) => {
            let Some(bytes) = stats.min_bytes_opt() else {
                return Ok(None);
            };
            if bytes.len() != 16 {
                bail!("Invalid length of uuid bytes.");
            }
            Some(Datum::uuid(Uuid::from_bytes(
                bytes[..16].try_into().unwrap(),
            )))
        }
        (PrimitiveType::Fixed(len), Statistics::FixedLenByteArray(stat)) => {
            let Some(bytes) = stat.min_bytes_opt() else {
                return Ok(None);
            };
            if bytes.len() != *len as usize {
                bail!("Invalid length of fixed bytes.");
            }
            Some(Datum::fixed(bytes.to_vec()))
        }
        (PrimitiveType::Binary, Statistics::ByteArray(stat)) => {
            return Ok(stat
                .min_bytes_opt()
                .map(|bytes| Datum::binary(bytes.to_vec())));
        }
        _ => {
            return Ok(None);
        }
    })
}

pub fn get_parquet_stat_max_as_datum(
    primitive_type: &PrimitiveType,
    stats: &Statistics,
) -> anyhow::Result<Option<Datum>> {
    Ok(match (primitive_type, stats) {
        (PrimitiveType::Boolean, Statistics::Boolean(stats)) => {
            stats.max_opt().map(|val| Datum::bool(*val))
        }
        (PrimitiveType::Int, Statistics::Int32(stats)) => {
            stats.max_opt().map(|val| Datum::int(*val))
        }
        (PrimitiveType::Date, Statistics::Int32(stats)) => {
            stats.max_opt().map(|val| Datum::date(*val))
        }
        (PrimitiveType::Long, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::long(*val))
        }
        (PrimitiveType::Time, Statistics::Int64(stats)) => {
            let Some(val) = stats.max_opt() else {
                return Ok(None);
            };

            Some(Datum::time_micros(*val)?)
        }
        (PrimitiveType::Timestamp, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::timestamp_micros(*val))
        }
        (PrimitiveType::Timestamptz, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::timestamptz_micros(*val))
        }
        (PrimitiveType::TimestampNs, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::timestamp_nanos(*val))
        }
        (PrimitiveType::TimestamptzNs, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::timestamptz_nanos(*val))
        }
        (PrimitiveType::Float, Statistics::Float(stats)) => {
            stats.max_opt().map(|val| Datum::float(*val))
        }
        (PrimitiveType::Double, Statistics::Double(stats)) => {
            stats.max_opt().map(|val| Datum::double(*val))
        }
        (PrimitiveType::String, Statistics::ByteArray(stats)) => {
            let Some(val) = stats.max_opt() else {
                return Ok(None);
            };

            Some(Datum::string(val.as_utf8()?))
        }

        (PrimitiveType::Uuid, Statistics::FixedLenByteArray(stats)) => {
            let Some(bytes) = stats.max_bytes_opt() else {
                return Ok(None);
            };
            if bytes.len() != 16 {
                bail!("Invalid length of uuid bytes.");
            }
            Some(Datum::uuid(Uuid::from_bytes(
                bytes[..16].try_into().unwrap(),
            )))
        }
        (PrimitiveType::Fixed(len), Statistics::FixedLenByteArray(stat)) => {
            let Some(bytes) = stat.max_bytes_opt() else {
                return Ok(None);
            };
            if bytes.len() != *len as usize {
                bail!("Invalid length of fixed bytes.");
            }
            Some(Datum::fixed(bytes.to_vec()))
        }
        (PrimitiveType::Binary, Statistics::ByteArray(stat)) => {
            return Ok(stat
                .max_bytes_opt()
                .map(|bytes| Datum::binary(bytes.to_vec())));
        }
        _ => {
            return Ok(None);
        }
    })
}

/// A mapping from Parquet column path names to internal field id
struct IndexByParquetPathName {
    name_to_id: HashMap<String, i32>,

    field_names: Vec<String>,

    field_id: i32,
}

impl IndexByParquetPathName {
    /// Creates a new, empty `IndexByParquetPathName`
    pub fn new() -> Self {
        Self {
            name_to_id: HashMap::new(),
            field_names: Vec::new(),
            field_id: 0,
        }
    }

    /// Retrieves the internal field ID
    pub fn get(&self, name: &str) -> Option<&i32> {
        self.name_to_id.get(name)
    }
}

impl Default for IndexByParquetPathName {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaVisitor for IndexByParquetPathName {
    type T = ();

    fn before_struct_field(&mut self, field: &NestedFieldRef) -> iceberg::Result<()> {
        self.field_names.push(field.name.to_string());
        self.field_id = field.id;
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &NestedFieldRef) -> iceberg::Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &NestedFieldRef) -> iceberg::Result<()> {
        self.field_names.push(format!("list.{}", field.name));
        self.field_id = field.id;
        Ok(())
    }

    fn after_list_element(&mut self, _field: &NestedFieldRef) -> iceberg::Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_key(&mut self, field: &NestedFieldRef) -> iceberg::Result<()> {
        self.field_names
            .push(format!("{DEFAULT_MAP_FIELD_NAME}.key"));
        self.field_id = field.id;
        Ok(())
    }

    fn after_map_key(&mut self, _field: &NestedFieldRef) -> iceberg::Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_value(&mut self, field: &NestedFieldRef) -> iceberg::Result<()> {
        self.field_names
            .push(format!("{DEFAULT_MAP_FIELD_NAME}.value"));
        self.field_id = field.id;
        Ok(())
    }

    fn after_map_value(&mut self, _field: &NestedFieldRef) -> iceberg::Result<()> {
        self.field_names.pop();
        Ok(())
    }

    fn schema(&mut self, _schema: &IcebergSchema, _value: Self::T) -> iceberg::Result<Self::T> {
        Ok(())
    }

    fn field(&mut self, _field: &NestedFieldRef, _value: Self::T) -> iceberg::Result<Self::T> {
        Ok(())
    }

    fn r#struct(
        &mut self,
        _struct: &StructType,
        _results: Vec<Self::T>,
    ) -> iceberg::Result<Self::T> {
        Ok(())
    }

    fn list(&mut self, _list: &ListType, _value: Self::T) -> iceberg::Result<Self::T> {
        Ok(())
    }

    fn map(
        &mut self,
        _map: &MapType,
        _key_value: Self::T,
        _value: Self::T,
    ) -> iceberg::Result<Self::T> {
        Ok(())
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> iceberg::Result<Self::T> {
        let full_name = self
            .field_names
            .iter()
            .map(String::as_str)
            .collect_vec()
            .join(".");
        let field_id = self.field_id;
        if let Some(existing_field_id) = self.name_to_id.get(full_name.as_str()) {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!(
                    "Invalid schema: multiple fields for name {full_name}: {field_id} and {existing_field_id}"
                ),
            ));
        } else {
            self.name_to_id.insert(full_name, field_id);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use std::io::Cursor;
    use std::sync::Arc;

    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    use crate::filesystem::config::{IcebergPartitioningField, Transform};
    use iceberg::spec::{NestedField, PrimitiveLiteral, PrimitiveType as PT, Schema, Type as IT};

    fn iceberg_schema() -> Schema {
        let id = NestedField::required(1, "id", IT::Primitive(PT::Long));
        let price = NestedField::optional(2, "price", IT::Primitive(PT::Int));
        Schema::builder()
            .with_fields(vec![id.into(), price.into()])
            .build()
            .unwrap()
    }

    fn write_parquet() -> FileMetaData {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("price", DataType::Int32, true),
        ]);

        let id_rg1 = Int64Array::from(vec![5, 100, 42, 42, 42, 99]);
        let price_rg1 = Int32Array::from(vec![
            Some(-123),
            Some(5678),
            None,
            Some(0),
            Some(100),
            Some(-500),
        ]);

        let rb1 = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![Arc::new(id_rg1), Arc::new(price_rg1)],
        )
        .unwrap();

        let id_rg2 = Int64Array::from(vec![1, 200, 150, 199]);
        let price_rg2 = Int32Array::from(vec![Some(-2000), Some(8000), Some(7), Some(-3)]);

        let rb2 = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![Arc::new(id_rg2), Arc::new(price_rg2)],
        )
        .unwrap();

        // Write two batches => two row groups
        let mut buf = Cursor::new(Vec::new());
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build();

        let mut writer =
            ArrowWriter::try_new(&mut buf, Arc::new(arrow_schema), Some(props)).unwrap();
        writer.write(&rb1).unwrap();
        writer.flush().unwrap();
        writer.write(&rb2).unwrap();

        writer.close().unwrap()
    }

    #[test]
    fn test_parquet_to_data_file() {
        let metadata = write_parquet();

        let ice_schema = iceberg_schema();

        let ifm = IcebergFileMetadata::from_parquet(metadata, &ice_schema);

        assert_eq!(ifm.row_count, 10);
        assert_eq!(ifm.split_offsets.len(), 2);
        assert!(ifm.split_offsets[0] <= ifm.split_offsets[1]);

        assert!(ifm.columns.contains_key(&1));
        assert!(ifm.columns.contains_key(&2));

        let df = build_datafile_from_meta(
            &ice_schema,
            &ifm,
            &IcebergPartitioning {
                fields: vec![IcebergPartitioningField {
                    name: None,
                    field: "id".to_string(),
                    transform: Transform::Bucket(4),
                }],
                ..Default::default()
            },
            "s3://bucket/data/file.parquet".to_string(),
            1234,
            0,
        )
        .unwrap();

        let cs = df.column_sizes();
        assert!(cs.get(&1).is_some());
        assert!(cs.get(&2).is_some());

        let vc = df.value_counts();
        let nc = df.null_value_counts();
        assert_eq!(vc.get(&1).cloned(), Some(10));
        assert_eq!(nc.get(&1).cloned(), Some(0));
        assert_eq!(vc.get(&2).cloned(), Some(10));
        assert_eq!(nc.get(&2).cloned(), Some(1));

        let lb = df.lower_bounds();
        let ub = df.upper_bounds();

        match lb.get(&1).expect("id lb").literal() {
            PrimitiveLiteral::Long(v) => assert_eq!(*v, 1),
            other => panic!("unexpected id lower bound: {other:?}"),
        }
        match ub.get(&1).expect("id ub").literal() {
            PrimitiveLiteral::Long(v) => assert_eq!(*v, 200),
            other => panic!("unexpected id upper bound: {other:?}"),
        }

        match lb.get(&2).expect("price lb").literal() {
            PrimitiveLiteral::Int(d) => assert_eq!(*d, -2000),
            other => panic!("unexpected price lower bound: {other:?}"),
        }
        match ub.get(&2).expect("price ub").literal() {
            PrimitiveLiteral::Int(d) => assert_eq!(*d, 8000),
            other => panic!("unexpected price upper bound: {other:?}"),
        }
    }
}
