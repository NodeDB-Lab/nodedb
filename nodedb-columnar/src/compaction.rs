//! Segment compaction: merge segments, drop deleted rows, re-encode.
//!
//! Compaction reads one or more source segments along with their delete
//! bitmaps, filters out deleted rows, and writes a new compacted segment.
//! The caller is responsible for the atomic metadata swap (WAL commit marker
//! → swap segment references → delete old files).
//!
//! Triggered when a segment's delete ratio exceeds the threshold (default 20%)
//! or when the segment count exceeds a limit.

use nodedb_types::columnar::ColumnarSchema;

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::memtable::ColumnarMemtable;
use crate::reader::{DecodedColumn, SegmentReader};
use crate::writer::SegmentWriter;

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

/// Extract a single row value from a DecodedColumn.
fn extract_row_value(
    col: &DecodedColumn,
    row_idx: usize,
    col_type: &nodedb_types::columnar::ColumnType,
) -> nodedb_types::value::Value {
    use nodedb_types::value::Value;

    match col {
        DecodedColumn::Int64 { values, valid } => {
            if !valid[row_idx] {
                Value::Null
            } else {
                Value::Integer(values[row_idx])
            }
        }
        DecodedColumn::Float64 { values, valid } => {
            if !valid[row_idx] {
                Value::Null
            } else {
                Value::Float(values[row_idx])
            }
        }
        DecodedColumn::Timestamp { values, valid } => {
            if !valid[row_idx] {
                Value::Null
            } else {
                Value::Integer(values[row_idx]) // Timestamp stored as micros.
            }
        }
        DecodedColumn::Bool { values, valid } => {
            if !valid[row_idx] {
                Value::Null
            } else {
                Value::Bool(values[row_idx])
            }
        }
        DecodedColumn::Binary {
            data,
            offsets,
            valid,
        } => {
            if !valid[row_idx] {
                return Value::Null;
            }
            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;
            let bytes = &data[start..end];

            match col_type {
                nodedb_types::columnar::ColumnType::String => {
                    Value::String(String::from_utf8_lossy(bytes).into_owned())
                }
                nodedb_types::columnar::ColumnType::Bytes
                | nodedb_types::columnar::ColumnType::Geometry => Value::Bytes(bytes.to_vec()),
                _ => Value::Bytes(bytes.to_vec()),
            }
        }
        DecodedColumn::DictEncoded {
            ids,
            dictionary,
            valid,
        } => {
            if !valid[row_idx] {
                return Value::Null;
            }
            let id = ids[row_idx] as usize;
            if let Some(s) = dictionary.get(id) {
                Value::String(s.clone())
            } else {
                Value::Null
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::*;
    use crate::memtable::ColumnarMemtable;
    use crate::writer::SegmentWriter;

    fn test_schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("score", ColumnType::Float64),
        ])
        .expect("valid")
    }

    fn write_segment(rows: usize) -> Vec<u8> {
        let schema = test_schema();
        let mut mt = ColumnarMemtable::new(&schema);
        for i in 0..rows {
            mt.append_row(&[
                Value::Integer(i as i64),
                Value::String(format!("user_{i}")),
                if i % 3 == 0 {
                    Value::Null
                } else {
                    Value::Float(i as f64 * 0.5)
                },
            ])
            .expect("append");
        }
        let (schema, columns, row_count) = mt.drain();
        SegmentWriter::plain()
            .write_segment(&schema, &columns, row_count)
            .expect("write")
    }

    #[test]
    fn compact_removes_deleted_rows() {
        let segment = write_segment(100);
        let mut deletes = DeleteBitmap::new();

        // Delete rows 0, 10, 20, ..., 90 (10 rows).
        for i in (0..100).step_by(10) {
            deletes.mark_deleted(i);
        }

        let result = compact_segment(&segment, &deletes, &test_schema(), 0).expect("compact");

        assert_eq!(result.live_rows, 90);
        assert_eq!(result.removed_rows, 10);
        assert!(result.segment.is_some());

        // Verify the compacted segment has correct row count.
        let new_seg = result.segment.as_ref().expect("segment");
        let reader = SegmentReader::open(new_seg).expect("open");
        assert_eq!(reader.row_count(), 90);

        // Verify that deleted rows are gone: row 0 (id=0) was deleted,
        // so the first row should be id=1.
        let col = reader.read_column(0).expect("read id");
        match col {
            DecodedColumn::Int64 { values, valid } => {
                assert_eq!(values[0], 1); // First live row.
                assert!(valid[0]);
                // Row at index 8 should be id=9 (rows 0,10 deleted, so 1..9 = 9 rows, idx 8 = id 9).
                assert_eq!(values[8], 9);
            }
            _ => panic!("expected Int64"),
        }
    }

    #[test]
    fn compact_all_deleted() {
        let segment = write_segment(10);
        let mut deletes = DeleteBitmap::new();
        for i in 0..10 {
            deletes.mark_deleted(i);
        }

        let result = compact_segment(&segment, &deletes, &test_schema(), 0).expect("compact");

        assert_eq!(result.live_rows, 0);
        assert_eq!(result.removed_rows, 10);
        assert!(result.segment.is_none());
    }

    #[test]
    fn compact_no_deletes() {
        let segment = write_segment(50);
        let deletes = DeleteBitmap::new();

        let result = compact_segment(&segment, &deletes, &test_schema(), 0).expect("compact");

        assert_eq!(result.live_rows, 50);
        assert_eq!(result.removed_rows, 0);
        assert!(result.segment.is_some());
    }

    #[test]
    fn merge_multiple_segments() {
        let seg1 = write_segment(50);
        let seg2 = write_segment(30);

        let mut del1 = DeleteBitmap::new();
        del1.mark_deleted_batch(&[0, 1, 2]); // Delete 3 from seg1.

        let del2 = DeleteBitmap::new(); // No deletes from seg2.

        let result =
            compact_segments(&[(&seg1, &del1), (&seg2, &del2)], &test_schema(), 0).expect("merge");

        assert_eq!(result.live_rows, 77); // 50-3 + 30 = 77.
        assert_eq!(result.removed_rows, 3);
        assert!(result.segment.is_some());

        let new_seg = result.segment.as_ref().expect("segment");
        let reader = SegmentReader::open(new_seg).expect("open");
        assert_eq!(reader.row_count(), 77);
    }

    #[test]
    fn compact_preserves_string_data() {
        let segment = write_segment(20);
        let mut deletes = DeleteBitmap::new();
        deletes.mark_deleted(0); // Delete first row.

        let result = compact_segment(&segment, &deletes, &test_schema(), 0).expect("compact");
        let new_seg = result.segment.as_ref().expect("segment");
        let reader = SegmentReader::open(new_seg).expect("open");

        // Read the name column (string).
        let col = reader.read_column(1).expect("read name");
        match col {
            DecodedColumn::Binary {
                data,
                offsets,
                valid,
            } => {
                // First row should be "user_1" (user_0 was deleted).
                let start = offsets[0] as usize;
                let end = offsets[1] as usize;
                let first_name = std::str::from_utf8(&data[start..end]).expect("utf8");
                assert_eq!(first_name, "user_1");
                assert!(valid[0]);
            }
            _ => panic!("expected Binary"),
        }
    }
}
