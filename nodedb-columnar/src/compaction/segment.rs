//! Single-segment compaction: drop deleted rows from one segment, write a new one.

use nodedb_types::columnar::ColumnarSchema;

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::memtable::ColumnarMemtable;
use crate::reader::SegmentReader;
use crate::writer::SegmentWriter;

use super::extract::extract_row_value;

/// Default compaction threshold: compact when >20% of rows are deleted.
pub const DEFAULT_DELETE_RATIO_THRESHOLD: f64 = 0.2;

/// Result of a compaction operation.
pub struct CompactionResult {
    /// The new compacted segment bytes. Empty if all rows were deleted.
    pub segment: Option<Vec<u8>>,
    /// Number of live rows in the new segment.
    pub live_rows: usize,
    /// Number of rows removed (deleted).
    pub removed_rows: usize,
}

/// Compact a single segment by removing deleted rows.
///
/// Reads the segment, skips rows marked in the delete bitmap, and writes
/// a new segment with only live rows. Returns `None` segment if all rows
/// were deleted.
pub fn compact_segment(
    segment_data: &[u8],
    deletes: &DeleteBitmap,
    schema: &ColumnarSchema,
    profile_tag: u8,
) -> Result<CompactionResult, ColumnarError> {
    let reader = SegmentReader::open(segment_data)?;
    let total_rows = reader.row_count() as usize;
    let deleted = deletes.deleted_count() as usize;
    let live = total_rows.saturating_sub(deleted);

    if live == 0 {
        return Ok(CompactionResult {
            segment: None,
            live_rows: 0,
            removed_rows: total_rows,
        });
    }

    // Read all columns without delete masking — we'll filter manually.
    let col_count = reader.column_count();
    let mut decoded_cols = Vec::with_capacity(col_count);
    for i in 0..col_count {
        decoded_cols.push(reader.read_column(i)?);
    }

    // Build a new memtable with only live rows.
    let mut memtable = ColumnarMemtable::new(schema);
    let mut row_values = Vec::with_capacity(schema.columns.len());

    for row_idx in 0..total_rows {
        if deletes.is_deleted(row_idx as u32) {
            continue;
        }

        row_values.clear();
        for (col_idx, decoded) in decoded_cols.iter().enumerate() {
            let value = extract_row_value(decoded, row_idx, &schema.columns[col_idx].column_type);
            row_values.push(value);
        }

        memtable.append_row(&row_values)?;
    }

    let (schema, columns, row_count) = memtable.drain();
    let writer = SegmentWriter::new(profile_tag);
    let new_segment = writer.write_segment(&schema, &columns, row_count)?;

    Ok(CompactionResult {
        segment: Some(new_segment),
        live_rows: row_count,
        removed_rows: deleted,
    })
}
