//! Multi-segment compaction: merge multiple sources into a single new segment.

use nodedb_types::columnar::ColumnarSchema;

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::memtable::ColumnarMemtable;
use crate::reader::SegmentReader;
use crate::writer::SegmentWriter;

use super::extract::extract_row_value;
use super::segment::CompactionResult;

/// Compact multiple segments into a single merged segment.
///
/// Reads all source segments, skips deleted rows from each, and writes
/// a single merged output segment. This reduces segment count and reclaims
/// space from deleted rows across all sources.
pub fn compact_segments(
    segments: &[(&[u8], &DeleteBitmap)],
    schema: &ColumnarSchema,
    profile_tag: u8,
) -> Result<CompactionResult, ColumnarError> {
    let mut memtable = ColumnarMemtable::new(schema);
    let mut total_removed = 0usize;
    let mut row_values = Vec::with_capacity(schema.columns.len());

    for &(segment_data, deletes) in segments {
        let reader = SegmentReader::open(segment_data)?;
        let total_rows = reader.row_count() as usize;

        let mut decoded_cols = Vec::with_capacity(reader.column_count());
        for i in 0..reader.column_count() {
            decoded_cols.push(reader.read_column(i)?);
        }

        for row_idx in 0..total_rows {
            if deletes.is_deleted(row_idx as u32) {
                total_removed += 1;
                continue;
            }

            row_values.clear();
            for (col_idx, decoded) in decoded_cols.iter().enumerate() {
                let value =
                    extract_row_value(decoded, row_idx, &schema.columns[col_idx].column_type);
                row_values.push(value);
            }

            memtable.append_row(&row_values)?;
        }
    }

    let live_rows = memtable.row_count();
    if live_rows == 0 {
        return Ok(CompactionResult {
            segment: None,
            live_rows: 0,
            removed_rows: total_removed,
        });
    }

    let (schema, columns, row_count) = memtable.drain();
    let writer = SegmentWriter::new(profile_tag);
    let new_segment = writer.write_segment(&schema, &columns, row_count)?;

    Ok(CompactionResult {
        segment: Some(new_segment),
        live_rows: row_count,
        removed_rows: total_removed,
    })
}
