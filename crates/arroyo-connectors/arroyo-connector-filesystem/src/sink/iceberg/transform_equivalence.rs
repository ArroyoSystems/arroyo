//! Checks that the two derivations of an Iceberg partition value agree:
//!
//! - the key the sink groups rows into files by: the `ice_*` expressions compiled by the
//!   [`Partitioner`] and evaluated over the batch
//! - the partition tuple written to the manifest at commit: iceberg-rust's
//!   `transform_literal_result` applied to the column lower bound from the parquet statistics,
//!   in [`build_datafile_from_meta`]
//!
//! Both are driven through the real code paths. The manifest side writes each sample as a
//! one-row parquet file with [`ParquetBatchBufferingWriter`]. Every sample array includes a
//! null, whose key and manifest value must both be null.

use super::metadata::build_datafile_from_meta;
use super::schema::add_parquet_field_ids;
use crate::config::{FileSystemSink, IcebergPartitioning, IcebergPartitioningField, Transform};
use crate::sink::parquet::ParquetBatchBufferingWriter;
use crate::sink::partitioning::{Partitioner, PartitionerMode};
use crate::sink::{BatchBufferingWriter, FsEventLogger};
use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampNanosecondArray, UInt32Array,
};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Field, Float64Type, Int16Type, Int32Type, Int64Type,
    TimeUnit, TimestampMicrosecondType, UInt32Type,
};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::errors::DataflowError;
use arroyo_rpc::formats::{Format, ParquetFormat};
use arroyo_types::get_test_task_info;
use iceberg::spec::{Literal, Schema as IcebergSchema};
use std::collections::HashMap;
use std::sync::Arc;

fn samples() -> Vec<(&'static str, ArrayRef)> {
    const US_PER_HOUR: i64 = 3_600_000_000;
    const US_PER_DAY: i64 = 24 * US_PER_HOUR;
    // instants around the epoch and month, day and hour boundaries on both sides of it
    let micros = vec![
        Some(0),
        Some(1),
        Some(-1),
        Some(1_682_937_000_000_000), // 2023-05-01T10:30:00
        Some(1_704_067_199_999_999), // 2023-12-31T23:59:59.999999
        Some(-US_PER_HOUR),          // 1969-12-31T23:00:00
        Some(-US_PER_HOUR - 1),      // 1969-12-31T22:59:59.999999
        Some(-US_PER_DAY),           // 1969-12-31T00:00:00
        Some(-US_PER_DAY - 1),       // 1969-12-30T23:59:59.999999
        Some(-31 * US_PER_DAY),      // 1969-12-01T00:00:00
        Some(-31 * US_PER_DAY - 1),  // 1969-11-30T23:59:59.999999
        Some(-365 * US_PER_DAY - 1), // 1968-12-31T23:59:59.999999
        None,
    ];

    vec![
        (
            "SMALLINT",
            Arc::new(Int16Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(17),
                Some(-15),
                Some(i16::MAX),
                None,
            ])),
        ),
        (
            "INT",
            Arc::new(Int32Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(34),
                Some(17_486),
                Some(-2000),
                Some(127),
                Some(i32::MAX),
                None,
            ])),
        ),
        (
            "BIGINT",
            Arc::new(Int64Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(34),
                Some(2000),
                Some(-1567),
                Some(81_068_000_000),
                Some(i64::MAX),
                None,
            ])),
        ),
        (
            "INT UNSIGNED",
            Arc::new(UInt32Array::from(vec![
                Some(0),
                Some(1),
                Some(34),
                Some(4_000_000_000),
                None,
            ])),
        ),
        (
            "TEXT",
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("a"),
                Some("iceberg"),
                Some("hello world"),
                Some("héllo wörld"),
                Some("日本語のテキスト"),
                Some("🦀🦀🦀🦀"),
                None,
            ])),
        ),
        (
            "BYTEA",
            Arc::new(BinaryArray::from(vec![
                Some(&b""[..]),
                Some(&b"ab"[..]),
                Some(&b"iceberg"[..]),
                Some(&[0xff, 0x00, 0x01, 0x02][..]),
                None,
            ])),
        ),
        (
            "DATE",
            Arc::new(Date32Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(17_486),
                Some(19_478),
                Some(19_723),
                Some(-31),
                Some(-32),
                Some(-366),
                None,
            ])),
        ),
        (
            "TIMESTAMP(6)",
            Arc::new(TimestampMicrosecondArray::from(micros)),
        ),
        (
            "DECIMAL(10, 2)",
            Arc::new(
                Decimal128Array::from(vec![
                    Some(0),
                    Some(1),
                    Some(-1),
                    Some(12345),
                    Some(-12345),
                    None,
                ])
                .with_precision_and_scale(10, 2)
                .unwrap(),
            ),
        ),
        (
            "BOOLEAN",
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        ),
        (
            "DOUBLE",
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(-1.5),
                Some(3.25),
                None,
            ])),
        ),
    ]
}

const TRANSFORMS: &[Transform] = &[
    Transform::Identity,
    Transform::Bucket { arg0: 16 },
    Transform::Bucket { arg0: 1000 },
    Transform::Truncate { arg0: 3 },
    Transform::Truncate { arg0: 10 },
    Transform::Year,
    Transform::Month,
    Transform::Day,
    Transform::Hour,
    Transform::Void,
];

enum Outcome {
    /// Rejected at plan time, by the Iceberg partition spec or by the sink's partition expressions
    Rejected(String),
    Agrees,
    /// `kind` is the first failure across the samples:
    ///
    /// - "mismatch": both succeed with different values
    /// - "key error": the sink fails to compile or evaluate the key, so the pipeline fails at
    ///   runtime
    /// - "manifest error": the commit fails to compute the partition tuple, and retries forever
    Diverges {
        kind: &'static str,
        detail: String,
    },
}

fn to_literal(array: &dyn Array, i: usize) -> Result<Option<Literal>, String> {
    if array.data_type() == &DataType::Null || array.is_null(i) {
        return Ok(None);
    }

    Ok(Some(match array.data_type() {
        DataType::Boolean => Literal::bool(array.as_boolean().value(i)),
        DataType::Int16 => Literal::int(array.as_primitive::<Int16Type>().value(i)),
        DataType::Int32 => Literal::int(array.as_primitive::<Int32Type>().value(i)),
        DataType::Date32 => Literal::date(array.as_primitive::<Date32Type>().value(i)),
        DataType::Int64 => Literal::long(array.as_primitive::<Int64Type>().value(i)),
        DataType::UInt32 => Literal::long(array.as_primitive::<UInt32Type>().value(i)),
        DataType::Float64 => Literal::double(array.as_primitive::<Float64Type>().value(i)),
        DataType::Utf8 => Literal::string(array.as_string::<i32>().value(i)),
        DataType::Binary => Literal::binary(array.as_binary::<i32>().value(i).to_vec()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Literal::long(array.as_primitive::<TimestampMicrosecondType>().value(i))
        }
        DataType::Decimal128(_, _) => {
            Literal::decimal(array.as_primitive::<Decimal128Type>().value(i))
        }
        other => return Err(format!("no literal conversion for key type {other}")),
    }))
}

fn describe(e: DataflowError) -> String {
    match e {
        DataflowError::ConnectorError {
            error,
            source: Some(source),
            ..
        } => format!("{error}: {source}"),
        e => e.to_string(),
    }
}

fn event_logger() -> FsEventLogger {
    FsEventLogger {
        task_info: Some(Arc::new(get_test_task_info())),
        connection_id: Arc::new(String::new()),
        output_format: "parquet",
        table_format: "iceberg",
    }
}

/// The partition value the commit writes to the manifest for a file holding just `row`
fn manifest_partition(
    schema: &Arc<ArroyoSchema>,
    iceberg_schema: &IcebergSchema,
    partitioning: &IcebergPartitioning,
    row: &RecordBatch,
) -> Result<Option<Literal>, String> {
    let config = FileSystemSink {
        path: String::new(),
        storage_options: HashMap::new(),
        rolling_policy: Default::default(),
        file_naming: Default::default(),
        partitioning: Default::default(),
        multipart: Default::default(),
        version: None,
    };

    let mut writer = ParquetBatchBufferingWriter::new(
        &config,
        Format::Parquet(ParquetFormat::default()),
        schema.clone(),
        Some(Arc::new(iceberg_schema.clone())),
        event_logger(),
    )
    .map_err(describe)?;

    writer.add_batch_data(row);
    let (bytes, metadata) = writer.close();

    let df = build_datafile_from_meta(
        iceberg_schema,
        &metadata.expect("iceberg writer produces metadata"),
        partitioning,
        "s3://bucket/data/file.parquet".to_string(),
        bytes.len() as u64,
        0,
    )
    .map_err(describe)?;

    Ok(df.partition()[0].clone())
}

fn check(values: &ArrayRef, iceberg_schema: &IcebergSchema, transform: Transform) -> Outcome {
    let schema = Arc::new(ArroyoSchema::from_fields(vec![Field::new(
        "v",
        values.data_type().clone(),
        true,
    )]));

    let partitioning = IcebergPartitioning {
        fields: vec![IcebergPartitioningField {
            name: None,
            field: "v".to_string(),
            transform_fn: transform,
        }],
        ..Default::default()
    };

    // the planner runs both checks (`IcebergConnector::from_config`)
    if let Err(e) = partitioning.as_partition_spec(Arc::new(iceberg_schema.clone())) {
        return Outcome::Rejected(format!("spec: {e}"));
    }
    if let Err(e) = partitioning.partition_expr(&schema.schema) {
        return Outcome::Rejected(format!("expr: {e}"));
    }

    let diverges = |kind, detail: String| Outcome::Diverges { kind, detail };

    let partitioner = match Partitioner::new(
        PartitionerMode::Iceberg(partitioning.clone()),
        &schema.schema,
    ) {
        Ok(p) => p,
        Err(e) => return diverges("key error", format!("compiling: {e}")),
    };

    let batch = RecordBatch::try_new(
        schema.schema.clone(),
        vec![
            values.clone(),
            Arc::new(TimestampNanosecondArray::from(vec![0; values.len()])),
        ],
    )
    .unwrap();

    let keys = match partitioner.partition_keys(&batch) {
        Ok(k) => k,
        Err(e) => return diverges("key error", format!("evaluating: {e}")),
    };

    for i in 0..values.len() {
        let value = to_literal(values, i).unwrap();
        let key = match to_literal(&keys[0], i) {
            Ok(k) => k,
            Err(e) => return diverges("key error", e),
        };

        if value.is_none() && key.is_some() {
            return diverges("mismatch", format!("null: key {key:?}, expected null"));
        }

        let manifest =
            match manifest_partition(&schema, iceberg_schema, &partitioning, &batch.slice(i, 1)) {
                Ok(m) => m,
                Err(e) => return diverges("manifest error", format!("{value:?}: {e}")),
            };

        if key != manifest {
            return diverges(
                "mismatch",
                format!("{value:?}: key {key:?}, manifest {manifest:?}"),
            );
        }
    }

    Outcome::Agrees
}

#[test]
fn partition_key_matches_manifest_partition() {
    let mut report = vec![];
    let mut divergences = vec![];

    for (type_name, values) in samples() {
        let schema =
            ArroyoSchema::from_fields(vec![Field::new("v", values.data_type().clone(), true)]);
        let iceberg_schema =
            iceberg::arrow::arrow_schema_to_schema(&add_parquet_field_ids(&schema.schema))
                .unwrap_or_else(|e| panic!("{type_name} is not representable in Iceberg: {e}"));

        for transform in TRANSFORMS {
            let label = IcebergPartitioningField {
                name: None,
                field: "v".to_string(),
                transform_fn: *transform,
            }
            .to_string();

            let line = match check(&values, &iceberg_schema, *transform) {
                Outcome::Rejected(reason) => format!("rejected  {reason}"),
                Outcome::Agrees => "agrees".to_string(),
                Outcome::Diverges { kind, detail } => {
                    divergences.push((type_name, label.clone(), kind));
                    format!("DIVERGES  {kind}: {detail}")
                }
            };
            report.push(format!("{type_name:<15} {label:<18} {line}"));
        }
    }

    println!("{}", report.join("\n"));

    assert!(
        divergences.is_empty(),
        "partition key and manifest partition diverge for {divergences:?}; see the report above"
    );
}
