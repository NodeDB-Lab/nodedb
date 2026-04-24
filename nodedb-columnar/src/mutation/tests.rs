use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::error::ColumnarError;
use crate::pk_index::encode_pk;
use crate::wal_record::ColumnarWalRecord;

use super::engine::MutationEngine;

fn test_schema() -> ColumnarSchema {
    ColumnarSchema::new(vec![
        ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ColumnDef::required("name", ColumnType::String),
        ColumnDef::nullable("score", ColumnType::Float64),
    ])
    .expect("valid")
}

#[test]
fn insert_and_pk_check() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    let result = engine
        .insert(&[
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Float(0.75),
        ])
        .expect("insert");

    assert_eq!(result.wal_records.len(), 1);
    assert!(matches!(
        &result.wal_records[0],
        ColumnarWalRecord::InsertRow { .. }
    ));

    assert_eq!(engine.pk_index().len(), 1);
    assert_eq!(engine.memtable().row_count(), 1);
}

#[test]
fn delete_by_pk() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    engine
        .insert(&[
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Null,
        ])
        .expect("insert");

    let result = engine.delete(&Value::Integer(1)).expect("delete");
    assert_eq!(result.wal_records.len(), 1);
    assert!(matches!(
        &result.wal_records[0],
        ColumnarWalRecord::DeleteRows { .. }
    ));

    // PK should be removed from index.
    assert!(engine.pk_index().is_empty());
}

#[test]
fn delete_nonexistent_pk() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    let err = engine.delete(&Value::Integer(999));
    assert!(matches!(err, Err(ColumnarError::PrimaryKeyNotFound)));
}

#[test]
fn update_row() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    engine
        .insert(&[
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Float(0.5),
        ])
        .expect("insert");

    // Update: change name and score, keep same PK.
    let result = engine
        .update(
            &Value::Integer(1),
            &[
                Value::Integer(1),
                Value::String("Alice Updated".into()),
                Value::Float(0.75),
            ],
        )
        .expect("update");

    // Should produce 2 WAL records: delete + insert.
    assert_eq!(result.wal_records.len(), 2);
    assert!(matches!(
        &result.wal_records[0],
        ColumnarWalRecord::DeleteRows { .. }
    ));
    assert!(matches!(
        &result.wal_records[1],
        ColumnarWalRecord::InsertRow { .. }
    ));

    // PK index should still have 1 entry.
    assert_eq!(engine.pk_index().len(), 1);
    // Memtable should have 2 rows (original + updated).
    assert_eq!(engine.memtable().row_count(), 2);
}

#[test]
fn memtable_flush_remaps_pk() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    for i in 0..5 {
        engine
            .insert(&[
                Value::Integer(i),
                Value::String(format!("u{i}")),
                Value::Null,
            ])
            .expect("insert");
    }

    // Simulate flush: memtable becomes segment 1.
    let result = engine.on_memtable_flushed(1);
    assert_eq!(result.wal_records.len(), 1);
    assert!(matches!(
        &result.wal_records[0],
        ColumnarWalRecord::MemtableFlushed {
            segment_id: 1,
            row_count: 5,
            ..
        }
    ));

    // PK index entries should now point to segment 1.
    let pk = encode_pk(&Value::Integer(3));
    let loc = engine.pk_index().get(&pk).expect("pk exists");
    assert_eq!(loc.segment_id, 1);
    assert_eq!(loc.row_index, 3);
}

#[test]
fn multiple_inserts_and_deletes() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    for i in 0..10 {
        engine
            .insert(&[
                Value::Integer(i),
                Value::String(format!("u{i}")),
                Value::Null,
            ])
            .expect("insert");
    }

    // Delete odd-numbered rows.
    for i in (1..10).step_by(2) {
        engine.delete(&Value::Integer(i)).expect("delete");
    }

    assert_eq!(engine.pk_index().len(), 5); // 0, 2, 4, 6, 8.
}

#[test]
fn should_compact_threshold() {
    let mut engine = MutationEngine::new("test".into(), test_schema());

    // Insert and flush to create a real segment.
    for i in 0..10 {
        engine
            .insert(&[
                Value::Integer(i),
                Value::String(format!("u{i}")),
                Value::Null,
            ])
            .expect("insert");
    }
    engine.on_memtable_flushed(1);

    // Delete 3 out of 10 rows = 30% > 20% threshold.
    for i in 0..3 {
        engine.delete(&Value::Integer(i)).expect("delete");
    }

    assert!(engine.should_compact(1, 10));
}
