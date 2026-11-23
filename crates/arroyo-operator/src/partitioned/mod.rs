#[cfg(test)]
mod tests;

use arrow::array::{
    Array, ArrayData, ArrayRef, ByteView, MutableArrayData, RecordBatch, RecordBatchOptions,
    make_array,
};
use arrow::compute::{concat_batches, interleave};
use arrow::datatypes::{DataType, Schema};
use arrow::error::ArrowError;
use arrow::row::{Row, RowConverter, Rows, SortField};
use arroyo_rpc::config::CoalescingConfig;
use arroyo_rpc::errors::{DataflowError, DataflowResult};
use datafusion::common::{HashMap, Result as DFResult};
use datafusion::physical_expr::PhysicalExpr;
use std::sync::Arc;
use tokio::time::Instant;

/// An operator's input grouped by partition key.
///
/// Each key appears at most once. The rows of a partition keep their arrival order, and
/// partitions are ordered by the first arrival of their key.
pub struct PartitionedBatch {
    /// All rows, ordered by partition
    data: RecordBatch,
    /// Partition `i` is `data[offsets[i]..offsets[i + 1]]`
    offsets: Vec<usize>,
    /// Row-encoded keys, one per partition
    key_rows: Rows,
}

/// One partition of a [`PartitionedBatch`]
pub struct Partition<'a> {
    /// The row-encoded key. Byte-equal keys are equal partitions.
    pub key: Row<'a>,
    /// This partition's rows (a zero-copy slice of the grouped data)
    pub batch: RecordBatch,
}

impl PartitionedBatch {
    pub fn num_partitions(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn num_rows(&self) -> usize {
        self.data.num_rows()
    }

    pub fn iter(&self) -> impl Iterator<Item = Partition<'_>> {
        self.offsets.windows(2).enumerate().map(|(i, w)| Partition {
            key: self.key_rows.row(i),
            batch: self.data.slice(w[0], w[1] - w[0]),
        })
    }
}

/// Buffers a sink's input and groups it by partition key when a limit is reached or it is
/// flushed
pub(crate) struct PartitionedInput {
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    converter: RowConverter,
    limits: CoalescingConfig,
    /// Buffered pieces, in arrival order
    pieces: Vec<RecordBatch>,
    /// Row-encoded keys of every buffered row, in arrival order
    rows: Rows,
    bytes: usize,
    /// When the buffer must be flushed; set exactly when it is non-empty
    deadline: Option<Instant>,
}

impl PartitionedInput {
    pub(crate) fn new(
        schema: &Schema,
        expressions: Vec<Arc<dyn PhysicalExpr>>,
        limits: CoalescingConfig,
    ) -> DataflowResult<Self> {
        if limits.max_rows == 0 || limits.max_bytes == 0 || limits.max_delay.is_zero() {
            return Err(DataflowError::ArgumentError(
                "partitioned-input limits must be positive".into(),
            ));
        }
        let fields = expressions
            .iter()
            .map(|expr| Ok(SortField::new(expr.data_type(schema)?)))
            .collect::<DFResult<_>>()?;
        let converter = RowConverter::new(fields)?;
        Ok(Self {
            expressions,
            rows: converter.empty_rows(0, 0),
            converter,
            limits,
            pieces: vec![],
            bytes: 0,
            deadline: None,
        })
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Buffers `batch`, and returns the grouped input if that reaches a limit
    pub(crate) fn push(&mut self, batch: RecordBatch) -> DataflowResult<Option<PartitionedBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let keys: Vec<ArrayRef> = self
            .expressions
            .iter()
            .map(|expr| expr.evaluate(&batch)?.into_array(batch.num_rows()))
            .collect::<DFResult<_>>()?;
        self.converter.append(&mut self.rows, &keys)?;
        self.bytes += record_batch_slice_memory_size(&batch);
        self.pieces.push(batch);
        self.deadline
            .get_or_insert_with(|| Instant::now() + *self.limits.max_delay);
        if self.rows.num_rows() >= self.limits.max_rows || self.bytes >= self.limits.max_bytes {
            self.flush()
        } else {
            Ok(None)
        }
    }

    /// Groups everything buffered by key and empties the buffer
    pub(crate) fn flush(&mut self) -> DataflowResult<Option<PartitionedBatch>> {
        if self.deadline.take().is_none() {
            return Ok(None);
        }
        self.bytes = 0;
        let pieces = std::mem::take(&mut self.pieces);
        let grouped = self.group(&pieces);
        self.rows.clear();
        Ok(Some(grouped?))
    }

    /// The only copy of the data is one pass over all pieces (or one `concat` when there is a
    /// single key); see [`interleave_pieces`].
    fn group(&self, pieces: &[RecordBatch]) -> Result<PartitionedBatch, ArrowError> {
        let total_rows = self.rows.num_rows();

        // Assign each row a group id, in order of first appearance. The table borrows the row
        // bytes, so there is no allocation per row.
        let mut groups: HashMap<&[u8], u32> = HashMap::new();
        let mut first_rows: Vec<usize> = vec![];
        let mut counts: Vec<usize> = vec![];
        let mut group_ids: Vec<u32> = Vec::with_capacity(total_rows);
        for (i, row) in self.rows.iter().enumerate() {
            let next = first_rows.len() as u32;
            let g = *groups.entry(row.data()).or_insert_with(|| {
                first_rows.push(i);
                counts.push(0);
                next
            });
            counts[g as usize] += 1;
            group_ids.push(g);
        }

        let mut offsets = Vec::with_capacity(counts.len() + 1);
        offsets.push(0);
        for c in &counts {
            offsets.push(offsets.last().unwrap() + c);
        }

        let data = if counts.len() == 1 {
            // a single key needs no reordering
            if pieces.len() == 1 {
                pieces[0].clone()
            } else {
                concat_batches(&pieces[0].schema(), pieces)?
            }
        } else {
            // a stable counting sort by group: (piece, row) indices ordered by group, and by
            // arrival within a group
            let mut cursor = offsets[..counts.len()].to_vec();
            let mut indices = vec![(0usize, 0usize); total_rows];
            let mut group_iter = group_ids.iter();
            for (p, piece) in pieces.iter().enumerate() {
                for r in 0..piece.num_rows() {
                    let g = *group_iter.next().unwrap() as usize;
                    indices[cursor[g]] = (p, r);
                    cursor[g] += 1;
                }
            }

            interleave_pieces(pieces, &indices)?
        };

        let mut key_rows = self.converter.empty_rows(
            first_rows.len(),
            first_rows
                .iter()
                .map(|i| self.rows.row(*i).data().len())
                .sum(),
        );
        for i in &first_rows {
            key_rows.push(self.rows.row(*i));
        }

        Ok(PartitionedBatch {
            data,
            offsets,
            key_rows,
        })
    }
}

/// Gathers `indices` (`(piece, row)` pairs) from `pieces` into one batch, like
/// `interleave_record_batch`, but copies columns containing lists by runs of consecutive rows.
///
/// Arrow's `interleave` builds a 16-byte index pair for every list element before copying, so
/// a list of bit-packed booleans needs 128 times its own size in temporary memory, far beyond
/// the coalescing limits. A `MutableArrayData` copy extends child ranges instead, as `take` and
/// `concat` do. Other columns keep `interleave`, which is faster for scattered rows.
fn interleave_pieces(
    pieces: &[RecordBatch],
    indices: &[(usize, usize)],
) -> Result<RecordBatch, ArrowError> {
    let schema = pieces[0].schema();
    let mut runs = None;
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let values: Vec<&dyn Array> = pieces.iter().map(|p| p.column(i).as_ref()).collect();
            if contains_list(field.data_type()) {
                let runs = runs.get_or_insert_with(|| row_runs(indices));
                Ok(copy_runs(&values, runs, indices.len()))
            } else {
                interleave(&values, indices)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(indices.len())),
    )
}

/// Whether `interleave` reaches its per-element list path for a column of this type
fn contains_list(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(_) | DataType::LargeList(_) => true,
        DataType::Struct(fields) => fields.iter().any(|f| contains_list(f.data_type())),
        _ => false,
    }
}

/// Collapses `(piece, row)` indices into `(piece, start, end)` runs of consecutive rows
fn row_runs(indices: &[(usize, usize)]) -> Vec<(usize, usize, usize)> {
    let mut runs: Vec<(usize, usize, usize)> = vec![];
    for &(piece, row) in indices {
        match runs.last_mut() {
            Some((p, _, end)) if *p == piece && *end == row => *end += 1,
            _ => runs.push((piece, row, row + 1)),
        }
    }
    runs
}

fn copy_runs(values: &[&dyn Array], runs: &[(usize, usize, usize)], len: usize) -> ArrayRef {
    let data: Vec<ArrayData> = values.iter().map(|v| v.to_data()).collect();
    let mut out = MutableArrayData::new(data.iter().collect(), false, len);
    for &(piece, start, end) in runs {
        out.extend(piece, start, end);
    }
    make_array(out.freeze())
}

/// Estimates the memory size of the rows actually present in `batch`, rather than the full
/// capacity of backing buffers pinned by zero-copy slices (such as local shuffle outputs).
fn record_batch_slice_memory_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|col| array_slice_memory_size(&col.to_data()))
        .sum()
}

fn array_slice_memory_size(data: &ArrayData) -> usize {
    let len = data.len();
    let null_bytes = if data.nulls().is_some() {
        len.div_ceil(8)
    } else {
        0
    };

    match data.data_type() {
        DataType::List(_) | DataType::Map(_, _) => {
            let offsets_bytes = (len + 1) * size_of::<i32>();
            let offsets = data.buffer::<i32>(0);
            let start = offsets[0] as usize;
            let child_len = (offsets[len] - offsets[0]) as usize;
            let child_bytes =
                array_slice_memory_size(&data.child_data()[0].slice(start, child_len));
            null_bytes + offsets_bytes + child_bytes
        }
        DataType::LargeList(_) => {
            let offsets_bytes = (len + 1) * size_of::<i64>();
            let offsets = data.buffer::<i64>(0);
            let start = offsets[0] as usize;
            let child_len = (offsets[len] - offsets[0]) as usize;
            let child_bytes =
                array_slice_memory_size(&data.child_data()[0].slice(start, child_len));
            null_bytes + offsets_bytes + child_bytes
        }
        DataType::FixedSizeList(_, n) => {
            let n = *n as usize;
            let child = data.child_data()[0].slice(data.offset() * n, len * n);
            null_bytes + array_slice_memory_size(&child)
        }
        DataType::Struct(_) => {
            let children_bytes: usize = data.child_data().iter().map(array_slice_memory_size).sum();
            null_bytes + children_bytes
        }
        DataType::Utf8View | DataType::BinaryView => {
            let views = data.buffer::<u128>(0);
            let slice_views = &views[..len];
            let inline_bytes = len * size_of::<u128>();
            let out_of_line: usize = slice_views
                .iter()
                .map(|v| {
                    let byte_len = ByteView::from(*v).length as usize;
                    if byte_len > 12 { byte_len } else { 0 }
                })
                .sum();
            null_bytes + inline_bytes + out_of_line
        }
        _ => data
            .get_slice_memory_size()
            .unwrap_or_else(|_| data.get_array_memory_size()),
    }
}
