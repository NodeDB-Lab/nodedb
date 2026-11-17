// SPDX-License-Identifier: BUSL-1.1

//! Accepted tuple bytes retain current-schema read and index semantics.

use super::{InstalledDocumentRow, installed_schema::canonical_stored};
use crate::{
    bridge::envelope::RowVersion,
    data::executor::{
        core_loop::{CoreLoop, tests::make_core_with_dir},
        doc_format,
    },
    engine::document::store::{CollectionConfig, StorageKey},
    types::{DatabaseId, InstalledDocument, SharedInstalledDocument, TenantId},
};
use nodedb_physical::physical_plan::StorageMode;
use nodedb_types::{
    Surrogate, Value,
    columnar::{ColumnDef, ColumnType, DroppedColumn, StrictSchema},
};
use std::sync::Arc;

const TID: u64 = 1;
const COLL: &str = "projections";
fn key() -> (DatabaseId, TenantId, String) {
    (DatabaseId::DEFAULT, TenantId::new(TID), COLL.into())
}
fn row() -> InstalledDocumentRow<'static> {
    InstalledDocumentRow {
        database_id: 0,
        tid: TID,
        collection: COLL,
        storage_key: StorageKey::for_surrogate(Surrogate::new(1)),
        surrogate: Surrogate::new(1),
        wal_lsn: None,
    }
}
fn original() -> StrictSchema {
    StrictSchema::new(vec![
        ColumnDef::required("n", ColumnType::Int64),
        ColumnDef::nullable("obsolete", ColumnType::String),
    ])
    .unwrap()
}
fn added_status() -> StrictSchema {
    let mut schema = original();
    schema.version = 2;
    let mut status = ColumnDef::nullable("status", ColumnType::String);
    status.added_at_version = 2;
    status.default = Some("'ready'".into());
    schema.columns.push(status);
    schema
}
fn image(schema: StrictSchema, values: &[Value], sys_from: i64) -> SharedInstalledDocument {
    let stored = nodedb_strict::TupleEncoder::new(&schema)
        .encode(values)
        .unwrap();
    let schema = Arc::new(StorageMode::Strict { schema });
    let version = RowVersion::open(sys_from);
    let canonical = canonical_stored(&stored, &schema, Some(version)).unwrap();
    InstalledDocument {
        canonical,
        stored,
        version,
        schema,
        chain_head: None,
    }
}
fn older() -> SharedInstalledDocument {
    image(
        original(),
        &[Value::Integer(7), Value::String("before".into())],
        10,
    )
}
fn register(core: &mut CoreLoop, schema: StrictSchema) {
    core.doc_configs.insert(
        key(),
        CollectionConfig::new(COLL)
            .with_storage_mode(StorageMode::Strict { schema })
            .with_index("status")
            .with_index("obsolete"),
    );
}
#[test]
fn old_strict_image_after_add_keeps_raw_bytes_and_default_equality_index() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    register(&mut core, added_status());
    let accepted = older();
    for _ in 0..2 {
        let txn = core.sparse.begin_write().unwrap();
        let (outcome, _) = core
            .apply_installed_document(&txn, row(), &accepted)
            .unwrap();
        assert_eq!(outcome.installed, accepted);
        txn.commit().unwrap();
        assert_eq!(
            core.sparse.get(0, TID, COLL, &row().storage_key).unwrap(),
            Some(accepted.stored.clone())
        );
        assert_eq!(
            core.sparse
                .versioned_index_lookup_as_of(0, TID, COLL, "status", "ready", None)
                .unwrap(),
            vec![row().storage_key]
        );
    }
    let Value::Object(captured) = doc_format::decode_document_value(&accepted.canonical).unwrap()
    else {
        panic!("Object");
    };
    assert!(!captured.contains_key("status"));
    let stats = core
        .stats_store
        .get(0, TID, COLL, "status")
        .unwrap()
        .unwrap();
    assert_eq!(stats.min_value.as_deref(), Some("ready"));
    assert!(stats.non_null_count > 0);
}
#[test]
fn dropped_columns_leave_read_projections_and_indexes_but_remain_in_accepted_image() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let mut current = added_status();
    current.version = 3;
    let dropped = current.columns.remove(1);
    current.dropped_columns.push(DroppedColumn {
        def: dropped,
        position: 1,
        dropped_at_version: 3,
    });
    register(&mut core, current);
    let accepted = older();
    let projection = core.installed_read_projection(&key(), &accepted).unwrap();
    let Value::Object(native) = doc_format::decode_document_value(&projection.native).unwrap()
    else {
        panic!("Object");
    };
    assert!(!native.contains_key("obsolete"));
    assert_eq!(native.get("status"), Some(&Value::String("ready".into())));
    assert!(projection.document.get("obsolete").is_none());
    assert_eq!(
        projection
            .document
            .get("status")
            .and_then(serde_json::Value::as_str),
        Some("ready")
    );
    let txn = core.sparse.begin_write().unwrap();
    let (outcome, _) = core
        .apply_installed_document(&txn, row(), &accepted)
        .unwrap();
    assert_eq!(outcome.installed, accepted);
    txn.commit().unwrap();
    assert_eq!(
        core.sparse.get(0, TID, COLL, &row().storage_key).unwrap(),
        Some(accepted.stored)
    );
    assert!(
        core.sparse
            .versioned_index_lookup_as_of(0, TID, COLL, "obsolete", "before", None)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        core.sparse
            .versioned_index_lookup_as_of(0, TID, COLL, "status", "ready", None)
            .unwrap(),
        vec![row().storage_key]
    );
}
#[test]
fn historical_strict_image_uses_current_defaults_without_replacing_newer_current_state() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    register(&mut core, added_status());
    let newer = image(
        added_status(),
        &[
            Value::Integer(8),
            Value::String("after".into()),
            Value::String("newer".into()),
        ],
        20,
    );
    let txn = core.sparse.begin_write().unwrap();
    core.apply_installed_document(&txn, row(), &newer).unwrap();
    txn.commit().unwrap();
    let accepted = older();
    let txn = core.sparse.begin_write().unwrap();
    let (outcome, _) = core
        .apply_installed_document(&txn, row(), &accepted)
        .unwrap();
    assert!(outcome.history.is_some());
    assert_eq!(outcome.installed, accepted);
    txn.commit().unwrap();
    assert_eq!(
        core.sparse
            .versioned_index_lookup_as_of(0, TID, COLL, "status", "ready", Some(10))
            .unwrap(),
        vec![row().storage_key]
    );
    assert!(
        core.sparse
            .versioned_index_lookup_as_of(0, TID, COLL, "status", "ready", None)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        core.sparse
            .versioned_index_lookup_as_of(0, TID, COLL, "status", "newer", None)
            .unwrap(),
        vec![row().storage_key]
    );
    assert_eq!(
        core.sparse.get(0, TID, COLL, &row().storage_key).unwrap(),
        Some(newer.stored.clone())
    );
    assert_eq!(
        core.doc_cache.get(0, TID, COLL, &row().storage_key),
        Some(newer.stored.as_slice())
    );
}
#[test]
fn equal_raw_old_tuple_reapplies_current_native_vector_and_sparse_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let original = StrictSchema::new(vec![ColumnDef::required("n", ColumnType::Int64)]).unwrap();
    let accepted = image(original.clone(), &[Value::Integer(7)], 10);
    let mut current = original;
    current.version = 2;
    let mut embedding = ColumnDef::nullable("embedding", ColumnType::Vector(3));
    embedding.added_at_version = 2;
    embedding.default = Some("'[1,0,0]'".into());
    let mut terms = ColumnDef::nullable("terms", ColumnType::SparseVector);
    terms.added_at_version = 2;
    terms.default = Some("'{3: 1.0}'".into());
    current.columns.extend([embedding, terms]);
    core.doc_configs.insert(
        key(),
        CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict { schema: current }),
    );
    for pass in 0..2 {
        let txn = core.sparse.begin_write().unwrap();
        let (outcome, _) = core
            .apply_installed_document(&txn, row(), &accepted)
            .unwrap();
        assert_eq!(outcome.installed, accepted);
        txn.commit().unwrap();
        let dense = CoreLoop::vector_index_key(0, TID, COLL, "embedding");
        assert_eq!(core.vector_collections.get(&dense).unwrap().live_count(), 1);
        let sparse = CoreLoop::sparse_index_key(0, TID, COLL, "terms");
        assert!(
            core.sparse_vector_indexes
                .get(&sparse)
                .and_then(|index| index.doc_image(&row().storage_key.to_string()))
                .is_some()
        );
        assert_eq!(
            core.sparse.get(0, TID, COLL, &row().storage_key).unwrap(),
            Some(accepted.stored.clone())
        );
        if pass == 0 {
            core.vector_collections.clear();
            core.vector_doc_map.clear();
            core.sparse_vector_indexes.clear();
        }
    }
}
