// SPDX-License-Identifier: Apache-2.0

//! Primary-key requirements for indexed mutation entrypoints.

use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use super::{BatchConflict, BatchRow, MutationEngine};
use crate::error::ColumnarError;
use crate::memtable::ColumnarMemtable;

fn keyless_schema() -> ColumnarSchema {
    ColumnarSchema::new(vec![ColumnDef::required("value", ColumnType::Int64)]).expect("schema")
}

fn snapshot(engine: &MutationEngine) -> Vec<u8> {
    let snapshot = engine.export_snapshot(&[], &[]).expect("snapshot");
    zerompk::to_msgpack_vec(&snapshot).expect("encode")
}

#[test]
fn keyless_single_row_mutations_leave_the_engine_untouched() {
    let mut engine = MutationEngine::new("rows".into(), keyless_schema());
    let values = [Value::Integer(7)];
    let before = snapshot(&engine);
    assert!(matches!(
        engine.insert(&values, 10),
        Err(ColumnarError::MissingPrimaryKey)
    ));
    assert!(matches!(
        engine.insert_with_surrogate(&values, Surrogate(1), 10),
        Err(ColumnarError::MissingPrimaryKey)
    ));
    assert!(matches!(
        engine.insert_if_absent(&values, 10),
        Err(ColumnarError::MissingPrimaryKey)
    ));
    assert!(matches!(
        engine.install_version(&values, None, 10, None),
        Err(ColumnarError::MissingPrimaryKey)
    ));
    assert!(matches!(
        engine.encode_pk_from_row(&values),
        Err(ColumnarError::MissingPrimaryKey)
    ));
    assert_eq!(snapshot(&engine), before);
    assert_eq!(engine.memtable().row_count(), 0);
    assert!(engine.pk_index().is_empty());
    assert!(engine.memtable_surrogates().is_empty());
    assert!(engine.version_chains().is_empty());
    assert_eq!(engine.newest_ordinal(), None);
}

#[test]
fn keyless_batch_mutations_leave_the_engine_untouched() {
    let mut engine = MutationEngine::new("rows".into(), keyless_schema());
    let values = [Value::Integer(7)];
    let before = snapshot(&engine);
    for conflict in [BatchConflict::Upsert, BatchConflict::Skip] {
        let rows = (0..2).map(|_| BatchRow {
            values: &values,
            surrogate: Some(Surrogate(1)),
            stamp: 10,
        });
        assert!(matches!(
            engine.insert_batch(rows, conflict),
            Err(ColumnarError::MissingPrimaryKey)
        ));
        assert_eq!(snapshot(&engine), before);
    }
    assert!(matches!(
        engine.insert_batch([], BatchConflict::Upsert),
        Err(ColumnarError::MissingPrimaryKey)
    ));
    assert_eq!(snapshot(&engine), before);
}

#[test]
fn keyless_primary_key_lookups_and_mutations_return_the_schema_error() {
    let mut engine = MutationEngine::new("rows".into(), keyless_schema());
    let values = [Value::Integer(7)];
    let before = snapshot(&engine);
    assert!(matches!(
        engine.lookup_memtable_row_by_pk(&[]),
        Err(ColumnarError::MissingPrimaryKey)
    ));
    assert!(matches!(
        engine.delete(&Value::Integer(7), 10),
        Err(ColumnarError::MissingPrimaryKey)
    ));
    assert!(matches!(
        engine.close(&values, None, 10),
        Err(ColumnarError::MissingPrimaryKey)
    ));
    assert!(matches!(
        engine.update(&Value::Integer(7), &values, None, 10),
        Err(ColumnarError::MissingPrimaryKey)
    ));
    assert!(matches!(
        engine.update_closing(&Value::Integer(7), &values, &values, None, 10),
        Err(ColumnarError::MissingPrimaryKey)
    ));
    assert_eq!(snapshot(&engine), before);
}

#[test]
fn raw_keyless_memtables_preserve_duplicate_rows() {
    let mut memtable = ColumnarMemtable::new(&keyless_schema());
    let values = [Value::Integer(7)];
    memtable.append_row(&values).expect("append");
    memtable.append_row(&values).expect("append");
    assert_eq!(memtable.row_count(), 2);
    assert_eq!(memtable.get_row(0).expect("read"), Some(values.to_vec()));
    assert_eq!(memtable.get_row(1).expect("read"), Some(values.to_vec()));
}
