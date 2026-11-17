// SPDX-License-Identifier: BUSL-1.1

//! Exact source images feed replay and prepared UPDATE side effects.

use super::{InstalledDocumentRow, PointPutParams};
use crate::{
    bridge::envelope::RowVersion,
    data::executor::{
        core_loop::{CoreLoop, tests::make_core_with_dir},
        doc_format,
        enforcement::{
            typeguard::{CompiledTypeGuard, GuardScope},
            unique::UniqueJudge,
        },
    },
    engine::document::store::{CollectionConfig, StorageKey},
    types::{DatabaseId, SharedInstalledDocument, TenantId},
};
use nodedb_physical::physical_plan::{GeneratedColumnSpec, StorageMode};
use nodedb_types::{
    Surrogate, TypeGuardFieldDef, Value,
    columnar::{ColumnDef, ColumnType, StrictSchema},
};
use std::{collections::HashMap, sync::Arc};

const TID: u64 = 1;
const COLL: &str = "images";
fn key() -> (DatabaseId, TenantId, String) {
    (DatabaseId::DEFAULT, TenantId::new(TID), COLL.into())
}
fn row(surrogate: u32) -> InstalledDocumentRow<'static> {
    InstalledDocumentRow {
        database_id: 0,
        tid: TID,
        collection: COLL,
        surrogate: Surrogate::new(surrogate),
        storage_key: StorageKey::for_surrogate(Surrogate::new(surrogate)),
        wal_lsn: None,
    }
}
fn apply(
    core: &mut CoreLoop,
    txn: &redb::WriteTransaction,
    surrogate: u32,
    bytes: &[u8],
) -> super::PointPutOutcome {
    core.apply_point_put(
        txn,
        PointPutParams {
            database_id: 0,
            tid: TID,
            collection: COLL,
            storage_key: StorageKey::for_surrogate(Surrogate::new(surrogate)),
            surrogate: Surrogate::new(surrogate),
            value: bytes,
            index_text: true,
            user_roles: &[],
            enforce: false,
            unique: UniqueJudge::Row,
            wal_lsn: None,
            resolved_targets: &[],
            guards: GuardScope::Document,
        },
    )
    .unwrap()
}
fn generated(multiplier: i64) -> GeneratedColumnSpec {
    let (expr, depends_on) =
        nodedb_query::expr_parse::parse_generated_expr(&format!("amount * {multiplier}")).unwrap();
    GeneratedColumnSpec {
        name: "doubled".into(),
        expr,
        depends_on,
    }
}
fn status(default: &str) -> CompiledTypeGuard {
    CompiledTypeGuard::compile(
        COLL,
        &TypeGuardFieldDef {
            field: "status".into(),
            type_expr: "STRING".into(),
            required: false,
            check_expr: None,
            default_expr: Some(format!("'{default}'")),
            value_expr: None,
        },
    )
}
#[test]
fn strict_source_captures_coercion_default_and_generated_values_once_with_shared_schema() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let schema = StrictSchema::new(vec![
        ColumnDef::required("_rowid", ColumnType::Int64),
        ColumnDef::nullable("amount", ColumnType::Int64),
        ColumnDef::nullable("coerced", ColumnType::Int64),
        ColumnDef::nullable("status", ColumnType::String),
        ColumnDef::nullable("doubled", ColumnType::Int64),
    ])
    .unwrap();
    let mut config = CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict { schema });
    config.type_guards = vec![status("source")];
    config.enforcement.generated_columns = vec![generated(2)];
    core.doc_configs.insert(key(), config);
    let bytes =
        nodedb_types::json_to_msgpack(&serde_json::json!({"amount": 21, "coerced": "7"})).unwrap();
    let txn = core.sparse.begin_write().unwrap();
    let first = apply(&mut core, &txn, 1, &bytes);
    let second = apply(&mut core, &txn, 2, &bytes);
    assert!(Arc::ptr_eq(
        &first.installed.schema,
        &second.installed.schema
    ));
    let Value::Object(fields) =
        doc_format::decode_document_value(&first.installed.canonical).unwrap()
    else {
        panic!("Object");
    };
    assert_eq!(fields.get("_rowid"), Some(&Value::Integer(1)));
    assert_eq!(fields.get("coerced"), Some(&Value::Integer(7)));
    assert_eq!(fields.get("status"), Some(&Value::String("source".into())));
    assert_eq!(fields.get("doubled"), Some(&Value::Integer(42)));
    let image = first.installed;
    txn.commit().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let (mut replay, _tx, _rx) = make_core_with_dir(destination.path());
    let mut config = CollectionConfig::new(COLL).with_storage_mode((*image.schema).clone());
    config.type_guards = vec![status("destination")];
    config.enforcement.generated_columns = vec![generated(100)];
    replay.doc_configs.insert(key(), config);
    let txn = replay.sparse.begin_write().unwrap();
    let (outcome, head) = replay
        .apply_installed_document(&txn, row(1), &image)
        .unwrap();
    assert!(head.is_none());
    assert_eq!(outcome.installed, image);
    txn.commit().unwrap();
    assert_eq!(
        replay
            .sparse
            .get(0, TID, COLL, &row(1).storage_key)
            .unwrap(),
        Some(image.stored)
    );
}
#[test]
fn tagged_strict_images_keep_dense_and_sparse_indexing_active() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let schema = StrictSchema::new(vec![
        ColumnDef::required("_rowid", ColumnType::Int64),
        ColumnDef::nullable("embedding", ColumnType::Vector(3)),
        ColumnDef::nullable("terms", ColumnType::SparseVector),
    ])
    .unwrap();
    core.doc_configs.insert(
        key(),
        CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict { schema }),
    );
    let body = nodedb_types::value_to_msgpack(&Value::Object(HashMap::from([
        (
            "embedding".into(),
            Value::Array(vec![
                Value::Float(1.0),
                Value::Float(0.0),
                Value::Float(0.0),
            ]),
        ),
        ("terms".into(), Value::String("{3: 1.0}".into())),
    ])))
    .unwrap();
    let txn = core.sparse.begin_write().unwrap();
    let outcome = apply(&mut core, &txn, 1, &body);
    assert!(matches!(
        zerompk::from_msgpack::<Value>(&outcome.installed.canonical).unwrap(),
        Value::Object(_)
    ));
    let dense = CoreLoop::vector_index_key(0, TID, COLL, "embedding");
    assert_eq!(core.vector_collections.get(&dense).unwrap().live_count(), 1);
    let sparse = CoreLoop::sparse_index_key(0, TID, COLL, "terms");
    assert!(
        core.sparse_vector_indexes
            .get(&sparse)
            .and_then(|index| index.doc_image(&row(1).storage_key.to_string()))
            .is_some()
    );
    txn.commit().unwrap();
}
fn schemaless(n: i64, sys_from: i64) -> SharedInstalledDocument {
    let bytes = nodedb_types::json_to_msgpack(&serde_json::json!({"n": n})).unwrap();
    crate::types::InstalledDocument {
        canonical: bytes.clone(),
        stored: bytes,
        version: RowVersion::open(sys_from),
        schema: Arc::new(StorageMode::Schemaless),
        chain_head: None,
    }
}
#[test]
fn older_installed_image_only_adds_history_and_preserves_current_row_and_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    core.doc_configs
        .insert(key(), CollectionConfig::new(COLL).with_index("n"));
    let newer = schemaless(2, 20);
    let txn = core.sparse.begin_write().unwrap();
    core.apply_installed_document(&txn, row(1), &newer).unwrap();
    txn.commit().unwrap();
    let older = schemaless(1, 10);
    let txn = core.sparse.begin_write().unwrap();
    let (outcome, _) = core.apply_installed_document(&txn, row(1), &older).unwrap();
    assert!(outcome.history.is_some());
    assert!(outcome.memory_undo.is_empty());
    txn.commit().unwrap();
    assert_eq!(
        core.sparse.get(0, TID, COLL, &row(1).storage_key).unwrap(),
        Some(newer.stored)
    );
    assert_eq!(outcome.installed.version, older.version);
    assert_eq!(
        core.doc_cache.get(0, TID, COLL, &row(1).storage_key),
        Some(newer.canonical.as_slice())
    );
    assert_eq!(
        core.sparse
            .versioned_index_lookup_as_of(0, TID, COLL, "n", "2", None)
            .unwrap(),
        vec![row(1).storage_key]
    );
    assert!(
        core.sparse
            .versioned_index_lookup_as_of(0, TID, COLL, "n", "1", None)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        core.sparse
            .versioned_index_lookup_as_of(0, TID, COLL, "n", "1", Some(10))
            .unwrap(),
        vec![row(1).storage_key]
    );
}

#[test]
fn transactional_stats_storage_error_rejects_the_document_before_commit() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let table: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("column_stats");
    let txn = core.sparse.begin_write().unwrap();
    let held_stats = txn.open_table(table).unwrap();
    let image = schemaless(2, 20);
    let Err(error) = core.apply_installed_document(&txn, row(1), &image) else {
        panic!("expected stats table error");
    };
    assert!(matches!(error, crate::Error::Storage { ref engine, .. } if engine == "stats"));
    assert!(
        core.doc_cache
            .get(0, TID, COLL, &row(1).storage_key)
            .is_none()
    );
    drop(held_stats);
    drop(txn);
    assert!(
        core.sparse
            .get(0, TID, COLL, &row(1).storage_key)
            .unwrap()
            .is_none()
    );
}

#[test]
fn captured_native_strict_image_installs_without_inventing_catalog_declarations() {
    let source_dir = tempfile::tempdir().unwrap();
    let (mut source, _tx, _rx) = make_core_with_dir(source_dir.path());
    let schema = StrictSchema::new(vec![ColumnDef::required("span", ColumnType::Range)]).unwrap();
    let span = Value::Range {
        start: Some(Box::new(Value::Integer(1))),
        end: Some(Box::new(Value::Integer(9))),
        inclusive: true,
    };
    let stored = nodedb_strict::TupleEncoder::new(&schema)
        .encode(std::slice::from_ref(&span))
        .unwrap();
    source.doc_configs.insert(
        key(),
        CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict { schema }),
    );
    let image = source
        .capture_stored_document(&key(), &stored, RowVersion::open(17))
        .unwrap();
    let destination_dir = tempfile::tempdir().unwrap();
    let (mut destination, _tx, _rx) = make_core_with_dir(destination_dir.path());
    assert!(!destination.doc_configs.contains_key(&key()));
    let txn = destination.sparse.begin_write().unwrap();
    let (outcome, _) = destination
        .apply_installed_document(&txn, row(1), &image)
        .unwrap();
    let Value::Object(fields) =
        doc_format::decode_document_value(&outcome.installed.canonical).unwrap()
    else {
        panic!("Object");
    };
    assert_eq!(fields.get("span"), Some(&span));
    txn.commit().unwrap();
    assert_eq!(
        destination
            .sparse
            .get(0, TID, COLL, &row(1).storage_key)
            .unwrap(),
        Some(stored)
    );
    assert!(!destination.doc_configs.contains_key(&key()));
}
