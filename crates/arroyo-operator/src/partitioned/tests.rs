use super::*;
use arrow::array::{
    Array, AsArray, BooleanBuilder, Int32Array, Int32Builder, Int64Array, ListBuilder, StringArray,
    StringBuilder, StructArray, UInt32Array,
};
use arrow::compute::take_record_batch;
use arrow::datatypes::{Field, Int32Type, Int64Type};
use datafusion::physical_expr::expressions::Column;
use std::time::Duration;

fn limits(max_bytes: usize) -> CoalescingConfig {
    CoalescingConfig {
        enabled: true,
        max_rows: 1000,
        max_bytes,
        max_delay: Duration::from_secs(1).into(),
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::Int64, false),
        Field::new("k", DataType::Int32, true),
        Field::new("s", DataType::Utf8, true),
    ]))
}

/// A piece whose `seq` column numbers its rows from `start`, with keys `k`
fn piece(start: i64, k: &[Option<i32>]) -> RecordBatch {
    let n = k.len() as i64;
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(start..start + n)),
            Arc::new(Int32Array::from(k.to_vec())),
            Arc::new(StringArray::from_iter_values(
                (start..start + n).map(|i| format!("row {i}")),
            )),
        ],
    )
    .unwrap()
}

/// (key, seqs) for each partition, in partition order
fn layout(batch: &PartitionedBatch) -> Vec<(Option<i32>, Vec<i64>)> {
    batch
        .iter()
        .map(|p| {
            let k = p.batch.column(1).as_primitive::<Int32Type>();
            let seqs = p.batch.column(0).as_primitive::<Int64Type>().values();
            (k.is_valid(0).then(|| k.value(0)), seqs.to_vec())
        })
        .collect()
}

#[test]
fn flush_empties_the_buffer() {
    let mut input = PartitionedInput::new(
        &schema(),
        vec![Arc::new(Column::new("k", 1))],
        limits(1 << 30),
    )
    .unwrap();
    assert!(input.push(piece(0, &[Some(1), Some(2)])).unwrap().is_none());
    assert!(input.push(piece(2, &[Some(1)])).unwrap().is_none());
    assert_eq!(
        layout(&input.flush().unwrap().unwrap()),
        vec![(Some(1), vec![0, 2]), (Some(2), vec![1])]
    );

    // the next batch starts empty
    assert!(input.push(piece(3, &[Some(2), Some(3)])).unwrap().is_none());
    assert_eq!(
        layout(&input.flush().unwrap().unwrap()),
        vec![(Some(2), vec![3]), (Some(3), vec![4])]
    );
    assert!(input.flush().unwrap().is_none());
}

#[test]
fn sliced_batch_memory_size_scales_with_slice_not_parent() {
    let n = 4096;
    let ids = Arc::new(Int32Array::from_iter_values(0..n));
    let strings = Arc::new(StringArray::from_iter_values(
        (0..n).map(|i| format!("payload-{i:020}")),
    ));
    let mut lists = ListBuilder::new(StringBuilder::new());
    for i in 0..n {
        lists.values().append_value(format!("tag-{i:010}"));
        lists.values().append_value("common");
        lists.append(true);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("s", DataType::Utf8, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));
    let full =
        RecordBatch::try_new(schema.clone(), vec![ids, strings, Arc::new(lists.finish())]).unwrap();

    let full_size = record_batch_slice_memory_size(&full);
    let slice = full.slice(100, 16);
    let slice_size = record_batch_slice_memory_size(&slice);

    // Full is 4096 rows (~250KB); a 16-row slice should be ~1/256th of that, not the full parent
    assert!(
        slice_size * 100 < full_size,
        "expected slice_size ({slice_size}) << full_size ({full_size})"
    );

    // Pushing 16-row slices into PartitionedInput with a 16KB byte cap should coalesce
    // multiple slices rather than flushing immediately on the first slice.
    let mut input = PartitionedInput::new(
        &schema,
        vec![Arc::new(Column::new("id", 0))],
        limits(16 * 1024),
    )
    .unwrap();
    assert!(input.push(full.slice(0, 16)).unwrap().is_none());
    assert!(input.push(full.slice(16, 16)).unwrap().is_none());
    assert_eq!(input.flush().unwrap().unwrap().num_rows(), 32);
}

/// `n` rows alternating between keys 1 and 2, with dense boolean lists (bit-packed children,
/// some null lists), a struct holding a list, and a list of lists
fn list_batch(n: usize) -> RecordBatch {
    let k = Int32Array::from_iter_values((0..n).map(|i| (i % 2) as i32 + 1));

    let mut flags = ListBuilder::new(BooleanBuilder::new());
    for i in 0..n {
        if i % 7 == 3 {
            flags.append(false);
        } else {
            for j in 0..(i % 5) * 64 {
                flags.values().append_value((i + j) % 3 == 0);
            }
            flags.append(true);
        }
    }
    let flags = flags.finish();

    let mut tags = ListBuilder::new(StringBuilder::new());
    for i in 0..n {
        for j in 0..i % 3 {
            tags.values().append_value(format!("tag-{i}-{j}"));
        }
        tags.append(true);
    }
    let tags: ArrayRef = Arc::new(tags.finish());
    let meta = StructArray::from(vec![(
        Arc::new(Field::new("tags", tags.data_type().clone(), true)),
        tags,
    )]);

    let mut nested = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
    for i in 0..n {
        for j in 0..i % 4 {
            for v in 0..j {
                nested.values().values().append_value((i * 10 + v) as i32);
            }
            nested.values().append(true);
        }
        nested.append(true);
    }
    let nested = nested.finish();

    let schema = Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("flags", flags.data_type().clone(), true),
        Field::new("meta", meta.data_type().clone(), false),
        Field::new("nested", nested.data_type().clone(), true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(k),
            Arc::new(flags),
            Arc::new(meta),
            Arc::new(nested),
        ],
    )
    .unwrap()
}

#[test]
fn multi_key_flush_groups_list_columns() {
    let n = 100;
    let full = list_batch(n);
    let mut input = PartitionedInput::new(
        &full.schema(),
        vec![Arc::new(Column::new("k", 0))],
        limits(1 << 30),
    )
    .unwrap();
    // uneven zero-copy slices, so pieces start at non-zero offsets into shared list buffers
    for (start, len) in [(0, 13), (13, 40), (53, 1), (54, 46)] {
        assert!(input.push(full.slice(start, len)).unwrap().is_none());
    }
    let grouped = input.flush().unwrap().unwrap();
    assert_eq!(grouped.num_partitions(), 2);

    // key 1 (even rows) then key 2 (odd rows), each in arrival order
    let order =
        UInt32Array::from_iter_values((0..n as u32).step_by(2).chain((1..n as u32).step_by(2)));
    assert_eq!(grouped.data, take_record_batch(&full, &order).unwrap());
    assert_eq!(grouped.offsets, vec![0, n / 2, n]);
}

#[test]
fn row_runs_merge_consecutive_rows_of_a_piece() {
    assert_eq!(
        row_runs(&[(0, 0), (0, 1), (1, 0), (0, 2), (0, 4), (0, 5), (1, 1)]),
        vec![(0, 0, 2), (1, 0, 1), (0, 2, 3), (0, 4, 6), (1, 1, 2)]
    );
}
