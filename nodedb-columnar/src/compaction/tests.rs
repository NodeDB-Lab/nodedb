use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::delete_bitmap::DeleteBitmap;
use crate::memtable::ColumnarMemtable;
use crate::reader::{DecodedColumn, SegmentReader};
use crate::writer::SegmentWriter;

use super::segment::compact_segment;
use super::segments::compact_segments;

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
