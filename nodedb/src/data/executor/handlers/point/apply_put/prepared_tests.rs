// SPDX-License-Identifier: BUSL-1.1

//! Actual prepared images are inspected once and applied without derivation.

use super::PointPutParams;
use crate::{
    data::executor::{
        core_loop::tests::make_core_with_dir,
        doc_format,
        enforcement::{
            typeguard::{CompiledTypeGuard, GuardScope},
            unique::UniqueJudge,
        },
    },
    engine::document::store::{CollectionConfig, StorageKey},
    types::{DatabaseId, TenantId},
};
use nodedb_physical::physical_plan::{GeneratedColumnSpec, StorageMode};
use nodedb_types::{
    Surrogate, TypeGuardFieldDef, Value,
    columnar::{ColumnDef, ColumnType, StrictSchema},
};

const TID: u64 = 1;
const COLL: &str = "prepared";
fn key() -> (DatabaseId, TenantId, String) {
    (DatabaseId::DEFAULT, TenantId::new(TID), COLL.into())
}
fn params(body: &[u8], enforce: bool) -> PointPutParams<'_> {
    PointPutParams {
        database_id: 0,
        tid: TID,
        collection: COLL,
        storage_key: StorageKey::for_surrogate(Surrogate::new(1)),
        surrogate: Surrogate::new(1),
        value: body,
        index_text: true,
        user_roles: &[],
        enforce,
        unique: UniqueJudge::Row,
        wal_lsn: None,
        resolved_targets: &[],
        guards: GuardScope::Document,
    }
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
fn prepared_point_put_exposes_actual_row_without_writing_and_applies_the_same_image_once() {
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
    config.type_guards = vec![status("first")];
    config.enforcement.generated_columns = vec![generated(2)];
    core.doc_configs.insert(key(), config);
    let body =
        nodedb_types::json_to_msgpack(&serde_json::json!({"amount": 21, "coerced": "7"})).unwrap();
    let txn = core.sparse.begin_write().unwrap();
    let prepared = core.prepare_point_put(&txn, params(&body, false)).unwrap();
    let selected = prepared.installed().clone();
    let Value::Object(fields) = doc_format::decode_document_value(&selected.canonical).unwrap()
    else {
        panic!("Object");
    };
    assert_eq!(fields.get("_rowid"), Some(&Value::Integer(1)));
    assert_eq!(fields.get("coerced"), Some(&Value::Integer(7)));
    assert_eq!(fields.get("status"), Some(&Value::String("first".into())));
    assert_eq!(fields.get("doubled"), Some(&Value::Integer(42)));
    let row_key = params(&body, false).storage_key;
    assert!(
        core.sparse
            .get_in_txn(&txn, 0, TID, COLL, &row_key)
            .unwrap()
            .is_none()
    );
    assert!(core.doc_cache.get(0, TID, COLL, &row_key).is_none());
    let config = core.doc_configs.get_mut(&key()).unwrap();
    config.type_guards = vec![status("second")];
    config.enforcement.generated_columns = vec![generated(100)];
    let outcome = core.apply_prepared_point_put(&txn, prepared).unwrap();
    assert_eq!(outcome.installed, selected);
    txn.commit().unwrap();
    assert_eq!(
        core.sparse.get(0, TID, COLL, &row_key).unwrap(),
        Some(selected.stored)
    );
}
#[test]
fn prepared_point_put_preserves_original_enforcement_policy() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let first = nodedb_types::json_to_msgpack(&serde_json::json!({"n": 1})).unwrap();
    let second = nodedb_types::json_to_msgpack(&serde_json::json!({"n": 2})).unwrap();
    let txn = core.sparse.begin_write().unwrap();
    core.apply_point_put(&txn, params(&first, false)).unwrap();
    txn.commit().unwrap();
    let first_stored = core
        .sparse
        .get(0, TID, COLL, &params(&first, false).storage_key)
        .unwrap()
        .expect("first row stored");
    let Value::Object(first_fields) = nodedb_types::object_msgpack::decode_object(&first_stored)
        .expect("stored body decodes as a native object")
    else {
        panic!("Object");
    };
    assert_eq!(first_fields.get("n"), Some(&Value::Integer(1)));
    let mut config = CollectionConfig::new(COLL);
    config.enforcement.append_only = true;
    core.doc_configs.insert(key(), config);
    let txn = core.sparse.begin_write().unwrap();
    let prepared = core.prepare_point_put(&txn, params(&second, true)).unwrap();
    assert!(matches!(
        core.apply_prepared_point_put(&txn, prepared),
        Err(crate::Error::AppendOnlyViolation { .. })
    ));
    drop(txn);
    assert_eq!(
        core.sparse
            .get(0, TID, COLL, &params(&first, false).storage_key)
            .unwrap(),
        Some(first_stored)
    );
    let txn = core.sparse.begin_write().unwrap();
    let prepared = core
        .prepare_point_put(&txn, params(&second, false))
        .unwrap();
    core.apply_prepared_point_put(&txn, prepared).unwrap();
    txn.commit().unwrap();
    let second_stored = core
        .sparse
        .get(0, TID, COLL, &params(&second, false).storage_key)
        .unwrap()
        .expect("second row stored");
    let Value::Object(second_fields) = nodedb_types::object_msgpack::decode_object(&second_stored)
        .expect("stored body decodes as a native object")
    else {
        panic!("Object");
    };
    assert_eq!(second_fields.get("n"), Some(&Value::Integer(2)));
}
