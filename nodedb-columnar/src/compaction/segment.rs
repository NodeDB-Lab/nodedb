// SPDX-License-Identifier: Apache-2.0

//! Single-segment compaction: drop purged rows from one segment, write a new one.

use nodedb_mem::ScopedMemory;
use nodedb_types::columnar::{ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::memtable::ColumnarMemtable;
use crate::reader::{DecodedColumn, SegmentReader, decoded_cell_value};
use crate::versions::SegmentVersions;
use crate::writer::SegmentWriter;

/// Default compaction threshold: compact when >20% of rows are deleted.
pub const DEFAULT_DELETE_RATIO_THRESHOLD: f64 = 0.2;

/// Result of a compaction operation.
pub struct CompactionResult {
    /// The new compacted segment bytes. `None` if every row was purged.
    pub segment: Option<Vec<u8>>,
    /// Number of rows the new segment holds: open rows and history.
    pub live_rows: usize,
    /// Number of purged rows dropped.
    pub removed_rows: usize,
    /// Old row index to new row index. `None` for a dropped row.
    pub row_map: Vec<Option<u32>>,
    /// The stamps of the new segment's rows: `sys_from` of every row and
    /// `sys_to` of every ended row kept as history.
    pub versions: SegmentVersions,
    /// The delete bitmap of the new segment: every kept row a write ended.
    pub ended: DeleteBitmap,
}

/// Compact a single segment by removing purged rows.
///
/// A row the delete bitmap marks and `versions` gives no `sys_to` is purged
/// and dropped. A marked row with a `sys_to` is history: it stays, keeps
/// its `sys_from` and `sys_to`, and stays marked. Returns `None` segment if
/// every row was purged. Apply the result with
/// `MutationEngine::install_compacted_segment`.
///
/// When `kek` is `Some`, the output segment is wrapped in an AES-256-GCM
/// SEGC envelope. The input segment must be plaintext (the caller is
/// responsible for decrypting before passing to this function).
///
/// `memory` tracks working-buffer allocations against the bound database,
/// tenant, and engine budget.
pub fn compact_segment(
    segment_data: &[u8],
    deletes: &DeleteBitmap,
    versions: &SegmentVersions,
    schema: &ColumnarSchema,
    profile_tag: u8,
    memory: &ScopedMemory,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> Result<CompactionResult, ColumnarError> {
    let reader = SegmentReader::open(segment_data)?;
    let total_rows = reader.row_count() as usize;
    let purged = |row: u32| deletes.is_deleted(row) && versions.sys_to(row).is_none();
    let removed = (0..total_rows)
        .filter(|&row| u32::try_from(row).is_ok_and(purged))
        .count();

    if removed == total_rows {
        return Ok(CompactionResult {
            segment: None,
            live_rows: 0,
            removed_rows: total_rows,
            row_map: vec![None; total_rows],
            versions: SegmentVersions::new(),
            ended: DeleteBitmap::new(),
        });
    }

    // Read all columns without delete masking — we'll filter manually.
    let col_count = reader.column_count();
    // Reserve budget for the decoded-column pointer vec (each entry is a fat pointer).
    let _cols_guard = memory.reserve(col_count * std::mem::size_of::<usize>() * 3)?;
    let mut decoded_cols = Vec::with_capacity(col_count);
    for i in 0..col_count {
        decoded_cols.push(reader.read_column(i)?);
    }

    // Build a new memtable with every row that is not purged.
    let mut memtable = ColumnarMemtable::new(schema);
    let col_len = schema.columns.len();
    let _row_guard = memory.reserve(col_len * std::mem::size_of::<usize>() * 3)?;
    let mut row_values = Vec::with_capacity(col_len);
    let mut row_map = Vec::with_capacity(total_rows);
    let mut new_versions = SegmentVersions::new();
    let mut ended = DeleteBitmap::new();
    let mut next_row: u32 = 0;

    for row_idx in 0..total_rows {
        let row = u32::try_from(row_idx).map_err(|_| {
            ColumnarError::Serialization(format!(
                "segment row {row_idx} is past the u32 row index range"
            ))
        })?;
        if purged(row) {
            row_map.push(None);
            continue;
        }

        row_values.clear();
        for (col_idx, decoded) in decoded_cols.iter().enumerate() {
            let col = &schema.columns[col_idx];
            row_values.push(carried_cell(decoded, row_idx, &col.column_type)?);
        }

        memtable.append_row(&row_values)?;
        new_versions.push(versions.sys_from(row).unwrap_or(i64::MIN));
        if deletes.is_deleted(row)
            && let Some(sys_to) = versions.sys_to(row)
        {
            new_versions.end(next_row, sys_to);
            ended.mark_deleted(next_row);
        }
        row_map.push(Some(next_row));
        next_row += 1;
    }

    let (schema, columns, row_count) = memtable.drain();
    let writer = SegmentWriter::new(profile_tag, memory.clone());
    let new_segment = writer.write_segment(&schema, &columns, row_count, kek)?;

    Ok(CompactionResult {
        segment: Some(new_segment),
        live_rows: row_count,
        removed_rows: removed,
        row_map,
        versions: new_versions,
        ended,
    })
}

/// One cell as the value that re-appends it to a memtable unchanged.
///
/// A JSON cell carries its stored MessagePack bytes, which a JSON column
/// appends as is. Its decoded value does not always re-append: a JSON string
/// scalar reads as `Value::String`, which a JSON column parses as JSON text.
/// Every other cell carries its read value.
fn carried_cell(
    decoded: &DecodedColumn,
    row_idx: usize,
    declared: &ColumnType,
) -> Result<Value, ColumnarError> {
    let value = decoded_cell_value(decoded, row_idx, declared)?;
    if !matches!(declared, ColumnType::Json) || value == Value::Null {
        return Ok(value);
    }
    let DecodedColumn::Binary { data, offsets, .. } = decoded else {
        return Ok(value);
    };
    // `decoded_cell_value` read this cell, so its byte range is in bounds.
    let start = offsets[row_idx] as usize;
    let end = offsets[row_idx + 1] as usize;
    Ok(Value::Bytes(data[start..end].to_vec()))
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use crate::delete_bitmap::DeleteBitmap;
    use crate::memtable::ColumnarMemtable;
    use crate::reader::{DecodedColumn, SegmentReader};
    use crate::test_support::test_memory;
    use crate::writer::SegmentWriter;

    use super::compact_segment;
    use crate::versions::SegmentVersions;

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
        SegmentWriter::new(crate::writer::PROFILE_PLAIN, test_memory())
            .write_segment(&schema, &columns, row_count, None)
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

        let result = compact_segment(
            &segment,
            &deletes,
            &SegmentVersions::new(),
            &test_schema(),
            0,
            &test_memory(),
            None,
        )
        .expect("compact");

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

        let result = compact_segment(
            &segment,
            &deletes,
            &SegmentVersions::new(),
            &test_schema(),
            0,
            &test_memory(),
            None,
        )
        .expect("compact");

        assert_eq!(result.live_rows, 0);
        assert_eq!(result.removed_rows, 10);
        assert!(result.segment.is_none());
    }

    #[test]
    fn compact_no_deletes() {
        let segment = write_segment(50);
        let deletes = DeleteBitmap::new();

        let result = compact_segment(
            &segment,
            &deletes,
            &SegmentVersions::new(),
            &test_schema(),
            0,
            &test_memory(),
            None,
        )
        .expect("compact");

        assert_eq!(result.live_rows, 50);
        assert_eq!(result.removed_rows, 0);
        assert!(result.segment.is_some());
    }

    #[test]
    fn compact_preserves_string_data() {
        let segment = write_segment(20);
        let mut deletes = DeleteBitmap::new();
        deletes.mark_deleted(0); // Delete first row.

        let result = compact_segment(
            &segment,
            &deletes,
            &SegmentVersions::new(),
            &test_schema(),
            0,
            &test_memory(),
            None,
        )
        .expect("compact");
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

    /// Compaction carries every surviving cell unchanged: identifiers,
    /// decimals, booleans, geometry text and JSON, a JSON string scalar
    /// included.
    #[test]
    fn compact_preserves_typed_cells() {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("u", ColumnType::Uuid),
            ColumnDef::nullable("ul", ColumnType::Ulid),
            ColumnDef::nullable("dec", ColumnType::Decimal(None)),
            ColumnDef::nullable("b", ColumnType::Bool),
            ColumnDef::nullable("geo", ColumnType::Geometry),
            ColumnDef::nullable("j", ColumnType::Json),
        ])
        .expect("valid");
        let mut mt = ColumnarMemtable::new(&schema);
        for i in 0..6u128 {
            let json = if i % 2 == 0 {
                Value::String("\"scalar\"".into())
            } else {
                Value::Array(vec![Value::Integer(i as i64)])
            };
            mt.append_row(&[
                Value::Integer(i as i64),
                Value::Uuid(uuid::Uuid::from_u128(i + 1).to_string()),
                Value::Ulid(ulid::Ulid(i + 1).to_string()),
                Value::Decimal(rust_decimal::Decimal::new(i as i64 * 5, 1)),
                Value::Bool(i % 3 == 0),
                Value::String(format!("POINT({i} 2)")),
                json,
            ])
            .expect("append");
        }
        let expected: Vec<Vec<Value>> = (1..6)
            .map(|i| mt.get_row(i).expect("read").expect("row"))
            .collect();
        let (schema, columns, row_count) = mt.drain();
        let segment = SegmentWriter::new(0, test_memory())
            .write_segment(&schema, &columns, row_count, None)
            .expect("write");

        let mut deletes = DeleteBitmap::new();
        deletes.mark_deleted(0);
        let result = compact_segment(
            &segment,
            &deletes,
            &SegmentVersions::new(),
            &schema,
            0,
            &test_memory(),
            None,
        )
        .expect("compact");
        let new_seg = result.segment.as_ref().expect("segment");
        let reader = SegmentReader::open(new_seg).expect("open");
        let indices: Vec<usize> = (0..schema.columns.len()).collect();
        let decoded = reader.read_columns(&indices, &[]).expect("read");

        for (row, want_row) in expected.iter().enumerate() {
            for ((col, def), want) in decoded.iter().zip(&schema.columns).zip(want_row) {
                let got = crate::reader::decoded_cell_value(col, row, &def.column_type)
                    .expect("decodable cell");
                assert_eq!(&got, want, "row {row} column '{}'", def.name);
            }
        }
    }

    /// An ended row with a `sys_to` is history: compaction keeps it, its
    /// stamps, and its mark. A marked row without one is purged and goes.
    #[test]
    fn compact_keeps_history_and_drops_purged_rows() {
        let segment = write_segment(4);
        let mut deletes = DeleteBitmap::new();
        deletes.mark_deleted(0);
        deletes.mark_deleted(2);
        let versions = SegmentVersions::from_parts(vec![10, 11, 12, 13], [(2, 20)]);

        let result = compact_segment(
            &segment,
            &deletes,
            &versions,
            &test_schema(),
            0,
            &test_memory(),
            None,
        )
        .expect("compact");

        assert_eq!(result.removed_rows, 1);
        assert_eq!(result.live_rows, 3);
        assert_eq!(result.row_map, vec![None, Some(0), Some(1), Some(2)]);
        assert_eq!(result.versions.sys_from(1), Some(12));
        assert_eq!(result.versions.sys_to(1), Some(20));
        assert!(result.ended.is_deleted(1));
        assert!(!result.ended.is_deleted(0));
    }
}
