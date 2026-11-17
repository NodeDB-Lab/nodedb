// SPDX-License-Identifier: BUSL-1.1

//! Frozen materialization retains native fields and checks runtime config drift.

use super::{FreezeDocumentBodyInput, FrozenDocumentBody, PointPutParams};
use crate::{
    data::executor::{
        core_loop::{CoreLoop, tests::make_core_with_dir},
        doc_format,
        enforcement::{
            typeguard::{CompiledTypeGuard, GuardScope},
            unique::UniqueJudge,
        },
    },
    engine::document::store::{CollectionConfig, StorageKey},
    types::{DatabaseId, TenantId},
};
use nodedb_physical::physical_plan::GeneratedColumnSpec;
use nodedb_types::{Surrogate, TypeGuardFieldDef, Value};
use std::collections::HashMap;

const COLL: &str = "frozen";
fn key() -> (DatabaseId, TenantId, String) {
    (DatabaseId::DEFAULT, TenantId::new(1), COLL.into())
}
fn params(value: &[u8]) -> PointPutParams<'_> {
    PointPutParams {
        database_id: 0,
        tid: 1,
        collection: COLL,
        storage_key: StorageKey::for_surrogate(Surrogate::new(1)),
        surrogate: Surrogate::new(1),
        value,
        index_text: false,
        user_roles: &[],
        enforce: false,
        unique: UniqueJudge::Unit,
        wal_lsn: None,
        resolved_targets: &[],
        guards: GuardScope::Document,
    }
}
fn freeze(core: &CoreLoop, value: &[u8]) -> FrozenDocumentBody {
    core.freeze_document_body(FreezeDocumentBodyInput {
        config_key: &key(),
        surrogate: Surrogate::new(1),
        value,
        guards: GuardScope::Document,
    })
    .unwrap()
}
fn generated(name: &str, expression: &str) -> GeneratedColumnSpec {
    let (expr, depends_on) = nodedb_query::expr_parse::parse_generated_expr(expression).unwrap();
    GeneratedColumnSpec {
        name: name.into(),
        expr,
        depends_on,
    }
}

#[test]
fn frozen_body_retains_native_fields_during_generated_and_default_materialization() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let mut config = CollectionConfig::new(COLL);
    config.enforcement.generated_columns = vec![generated("copy", "large")];
    config.type_guards = vec![CompiledTypeGuard::compile(
        COLL,
        &TypeGuardFieldDef {
            field: "status".into(),
            type_expr: "STRING".into(),
            required: false,
            check_expr: None,
            default_expr: Some("'ready'".into()),
            value_expr: None,
        },
    )];
    core.doc_configs.insert(key(), config);
    let original = HashMap::from([
        ("large".into(), Value::Integer(i64::MAX)),
        (
            "decimal".into(),
            Value::Decimal("123456789.123456789".parse().unwrap()),
        ),
        ("bytes".into(), Value::Bytes(vec![0, 255, 127])),
        (
            "uuid".into(),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
        ),
        (
            "nested".into(),
            Value::Array(vec![
                Value::Bytes(vec![9]),
                Value::Decimal("0.01".parse().unwrap()),
            ]),
        ),
    ]);
    let raw = zerompk::to_msgpack_vec(&Value::Object(original.clone())).unwrap();
    let frozen = freeze(&core, &raw);
    let Value::Object(fields) = doc_format::decode_document_value(&frozen.value).unwrap() else {
        panic!("object");
    };
    for (name, value) in original {
        assert_eq!(fields.get(&name), Some(&value));
    }
    assert_eq!(fields.get("copy"), Some(&Value::Integer(i64::MAX)));
    assert_eq!(fields.get("status"), Some(&Value::String("ready".into())));
}

/// A volatile DEFAULT evaluates once, when the body freezes. Every later
/// completion reuses the bound value, and a logged apply never evaluates it.
#[test]
fn frozen_completion_selects_new_versions_without_evaluating_the_body_again() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    use nodedb_physical::physical_plan::StorageMode;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    let schema = StrictSchema::new(vec![
        ColumnDef::required("_rowid", ColumnType::Int64),
        ColumnDef::nullable("default_token", ColumnType::String).with_default("UUID_V4()"),
        ColumnDef::nullable("token", ColumnType::String),
    ])
    .unwrap();
    let mut config = CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict { schema });
    config.enforcement.generated_columns = vec![generated("token", "default_token")];
    core.doc_configs.insert(key(), config);
    let raw = b"{}";
    let frozen = freeze(&core, raw);
    let txn = core.sparse.begin_write().unwrap();
    core.apply_scope.replicated_ordinal = Some(100);
    let first = core
        .prepare_point_put_body(&txn, params(raw), Some(&frozen), true)
        .unwrap();
    let selected = first.installed().clone();
    core.apply_prepared_point_put(&txn, first).unwrap();
    let second = core
        .prepare_point_put_body(&txn, params(b"invalid ignored input"), Some(&frozen), true)
        .unwrap();
    assert_eq!(second.installed().canonical, selected.canonical);
    assert_eq!(second.installed().version.sys_from, 101);
    // Evaluation happened once, at freezing. A logged apply never evaluates
    // the volatile DEFAULT: the unbound body is refused, and the frozen body,
    // which carries the bound value, installs the selected row again.
    core.apply_scope.journaled = true;
    let unbound = core.prepare_point_put(&txn, params(raw));
    assert!(
        matches!(&unbound, Err(crate::Error::Internal { detail }) if detail.contains("default_token")),
        "a logged apply refuses an unbound volatile DEFAULT"
    );
    let rebound = core.prepare_point_put(&txn, params(&frozen.value)).unwrap();
    assert_eq!(rebound.installed().canonical, selected.canonical);
    core.apply_scope.journaled = false;
}

#[test]
fn frozen_completion_refuses_registration_incarnation_and_enforcement_drift_before_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let raw = b"{}";
    let unregistered = freeze(&core, raw);
    core.doc_configs.insert(key(), CollectionConfig::new(COLL));
    let txn = core.sparse.begin_write().unwrap();
    assert!(matches!(
        core.prepare_point_put_body(&txn, params(raw), Some(&unregistered), true),
        Err(crate::Error::DataPlane(
            crate::bridge::envelope::ErrorCode::ConflictRetry
        ))
    ));
    let registered = freeze(&core, raw);
    core.doc_configs.get_mut(&key()).unwrap().incarnation = nodedb_types::Hlc::new(1, 0);
    assert!(
        core.prepare_point_put_body(&txn, params(raw), Some(&registered), true)
            .is_err()
    );
    let reincarnated = freeze(&core, raw);
    core.doc_configs
        .get_mut(&key())
        .unwrap()
        .enforcement
        .append_only = true;
    assert!(
        core.prepare_point_put_body(&txn, params(raw), Some(&reincarnated), true)
            .is_err()
    );
    core.doc_configs.remove(&key());
    assert!(
        core.prepare_point_put_body(&txn, params(raw), Some(&registered), true)
            .is_err()
    );
    assert!(
        core.sparse
            .get_in_txn(&txn, 0, 1, COLL, &params(raw).storage_key)
            .unwrap()
            .is_none()
    );
}

#[test]
fn frozen_body_rejects_unreadable_documents_and_expression_errors() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    assert!(
        core.freeze_document_body(FreezeDocumentBodyInput {
            config_key: &key(),
            surrogate: Surrogate::new(1),
            value: b"bad",
            guards: GuardScope::Document
        })
        .is_err()
    );
    let mut config = CollectionConfig::new(COLL);
    config.enforcement.generated_columns = vec![generated("value", "1 / 0")];
    core.doc_configs.insert(key(), config);
    assert!(
        core.freeze_document_body(FreezeDocumentBodyInput {
            config_key: &key(),
            surrogate: Surrogate::new(1),
            value: b"{}",
            guards: GuardScope::Document
        })
        .is_err()
    );
}

#[test]
fn frozen_strict_completion_encodes_selected_temporal_metadata_without_body_changes() {
    use nodedb_physical::physical_plan::StorageMode;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let schema =
        StrictSchema::new_bitemporal(vec![ColumnDef::nullable("amount", ColumnType::Int64)])
            .unwrap();
    core.doc_configs.insert(
        key(),
        CollectionConfig::new(COLL)
            .with_storage_mode(StorageMode::Strict {
                schema: schema.clone(),
            })
            .with_bitemporal(true),
    );
    let raw = nodedb_types::json_to_msgpack(&serde_json::json!({"amount": "7"})).unwrap();
    let frozen = freeze(&core, &raw);
    let first = core
        .complete_frozen_body(&frozen, true, 100, 20, 30)
        .unwrap();
    let second = core
        .complete_frozen_body(&frozen, true, 101, 40, 50)
        .unwrap();
    assert_eq!(
        doc_format::decode_document_value(&first.value).unwrap(),
        doc_format::decode_document_value(&second.value).unwrap()
    );
    assert_ne!(first.stored, second.stored);
    assert_eq!(
        first.stored,
        crate::data::executor::strict_format::bytes_to_binary_tuple_bitemporal(
            &frozen.value,
            &schema,
            100,
            20,
            30,
            COLL
        )
        .unwrap()
    );
    let composed = core
        .build_stored_body(super::stored_body::StoredBodyInput {
            config_key: &key(),
            surrogate: Surrogate::new(1),
            value: &raw,
            bitemporal: true,
            sys_from: 100,
            valid_from_ms: 20,
            valid_until_ms: 30,
            guards: GuardScope::Document,
        })
        .unwrap();
    assert_eq!(composed.stored, first.stored);
}

#[test]
fn frozen_chain_completion_retains_native_user_fields() {
    use crate::data::executor::enforcement::chain_guard::ChainIntent;
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let mut config = CollectionConfig::new(COLL);
    config.enforcement.hash_chain = true;
    core.doc_configs.insert(key(), config);
    let original = HashMap::from([
        ("large".into(), Value::Integer(i64::MAX)),
        (
            "decimal".into(),
            Value::Decimal("1.23456789123456789".parse().unwrap()),
        ),
        ("bytes".into(), Value::Bytes(vec![0, 255])),
        (
            "uuid".into(),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
        ),
    ]);
    let raw = zerompk::to_msgpack_vec(&Value::Object(original.clone())).unwrap();
    let frozen = freeze(&core, &raw);
    core.chain.intents.insert(
        (key(), 1),
        ChainIntent::Link {
            head: crate::types::hash_chain::ChainHead::genesis(),
        },
    );
    let completed = core
        .complete_frozen_body(&frozen, false, 100, i64::MIN, i64::MAX)
        .unwrap();
    let Value::Object(fields) = doc_format::decode_document_value(&completed.value).unwrap() else {
        panic!("object");
    };
    for (name, value) in original {
        assert_eq!(fields.get(&name), Some(&value));
    }
    assert_eq!(fields.get("id"), Some(&Value::String("1".into())));
    assert_eq!(fields.get("_chain_seq"), Some(&Value::Integer(1)));
}

#[test]
fn frozen_preparation_uses_fresh_version_floor_despite_carried_historical_stamps() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let raw = b"{}";
    let frozen = freeze(&core, raw);
    core.apply_scope.replicated_ordinal = Some(100);
    core.apply_scope.bitemporal_stamps.insert(
        1,
        crate::data::executor::handlers::transaction::overlay::BitemporalStamp {
            sys_from: 5,
            valid_from_ms: 6,
            valid_until_ms: 7,
        },
    );
    let txn = core.sparse.begin_write().unwrap();
    core.sparse
        .versioned_tombstone_in_txn(&txn, 0, 1, COLL, &params(raw).storage_key, 200)
        .unwrap();
    let prepared = core
        .prepare_point_put_body(&txn, params(raw), Some(&frozen), true)
        .unwrap();
    assert_eq!(prepared.installed().version.sys_from, 201);
    assert_eq!(prepared.installed().version.valid_from_ms, i64::MIN);
    assert_eq!(prepared.installed().version.valid_until_ms, i64::MAX);
}

#[test]
fn frozen_chain_preparation_checks_pending_transaction_predecessor() {
    use crate::data::executor::enforcement::chain_guard::ChainIntent;
    use crate::types::hash_chain::ChainHead;
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let mut config = CollectionConfig::new(COLL);
    config.enforcement.hash_chain = true;
    core.doc_configs.insert(key(), config);
    let raw = b"{}";
    let frozen = freeze(&core, raw);
    let genesis = ChainHead::genesis();
    core.chain.heads.insert(key(), genesis.clone());
    core.chain
        .intents
        .insert((key(), 1), ChainIntent::Link { head: genesis });
    let txn = core.sparse.begin_write().unwrap();
    let pending = ChainHead {
        seq: 1,
        hash: "pending".into(),
    };
    core.sparse
        .put_chain_head_in_txn(&txn, 0, 1, COLL, &pending)
        .unwrap();
    assert!(matches!(
        core.prepare_point_put_body(&txn, params(raw), Some(&frozen), true),
        Err(crate::Error::DataPlane(
            crate::bridge::envelope::ErrorCode::ConflictRetry
        ))
    ));
    assert!(
        core.sparse
            .get_in_txn(&txn, 0, 1, COLL, &params(raw).storage_key)
            .unwrap()
            .is_none()
    );
    core.chain.heads.insert(key(), pending.clone());
    core.chain
        .intents
        .insert((key(), 1), ChainIntent::Link { head: pending });
    assert!(
        core.prepare_point_put_body(&txn, params(raw), Some(&frozen), true)
            .is_ok()
    );
}

#[test]
fn strict_defaults_precede_generated_fields_and_preserve_explicit_null() {
    use nodedb_physical::physical_plan::StorageMode;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let schema = StrictSchema::new(vec![
        ColumnDef::required("_rowid", ColumnType::Int64),
        ColumnDef::nullable("amount", ColumnType::Int64).with_default("7"),
        ColumnDef::nullable("derived", ColumnType::Int64),
    ])
    .unwrap();
    let mut config = CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict {
        schema: schema.clone(),
    });
    config.enforcement.generated_columns = vec![generated("derived", "amount")];
    core.doc_configs.insert(key(), config);
    let frozen = freeze(&core, b"{}");
    let completed = core
        .complete_frozen_body(&frozen, false, 10, i64::MIN, i64::MAX)
        .unwrap();
    let decoder = nodedb_strict::TupleDecoder::new(&schema);
    assert_eq!(
        decoder
            .extract_by_name(&completed.stored, "amount")
            .unwrap(),
        Value::Integer(7)
    );
    assert_eq!(
        decoder
            .extract_by_name(&completed.stored, "derived")
            .unwrap(),
        Value::Integer(7)
    );
    let explicit_null = freeze(&core, br#"{"amount":null}"#);
    let Value::Object(fields) = doc_format::decode_document_value(&explicit_null.value).unwrap()
    else {
        panic!("object");
    };
    assert_eq!(fields.get("amount"), Some(&Value::Null));
    assert_eq!(fields.get("derived"), Some(&Value::Null));
}

#[test]
fn point_updates_preserve_native_fields_for_literal_and_expression_assignments() {
    use crate::data::executor::handlers::point::update::post_image::{
        PointUpdateImage, point_update_body_to_msgpack,
    };
    use nodedb_physical::physical_plan::UpdateValue;
    let dir = tempfile::tempdir().unwrap();
    let (core, _tx, _rx) = make_core_with_dir(dir.path());
    let fields = HashMap::from([
        (
            "decimal".into(),
            Value::Decimal("1.23456789".parse().unwrap()),
        ),
        (
            "uuid".into(),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
        ),
        ("bytes".into(), Value::Bytes(vec![0, 255])),
        ("n".into(), Value::Integer(1)),
    ]);
    let current = zerompk::to_msgpack_vec(&Value::Object(fields.clone())).unwrap();
    for assignment in [
        UpdateValue::Literal(nodedb_types::value_to_msgpack(&Value::Integer(2)).unwrap()),
        UpdateValue::Expr(
            nodedb_query::expr_parse::parse_generated_expr("n + 1")
                .unwrap()
                .0,
        ),
    ] {
        let updates = vec![("n".into(), assignment)];
        let body = core
            .compute_point_update_body(PointUpdateImage {
                config_key: &key(),
                current_bytes: &current,
                updates: &updates,
                is_strict: false,
                bitemporal: false,
                sys_from: 0,
                declared_primary_key: None,
            })
            .unwrap();
        let Value::Object(actual) =
            doc_format::decode_document_value(&point_update_body_to_msgpack(&body)).unwrap()
        else {
            panic!("object");
        };
        for name in ["decimal", "uuid", "bytes"] {
            assert_eq!(actual.get(name), fields.get(name));
        }
        assert_eq!(actual.get("n").and_then(Value::as_f64), Some(2.0));
    }
}

#[test]
fn strict_defaults_reject_invalid_expressions_and_row_context() {
    use nodedb_physical::physical_plan::StorageMode;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    for expression in [
        "7 trailing",
        "missing_column",
        "1 / 0",
        "nextval('counter')",
    ] {
        let schema = StrictSchema::new(vec![
            ColumnDef::nullable("amount", ColumnType::Int64).with_default(expression),
        ])
        .unwrap();
        core.doc_configs.insert(
            key(),
            CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict { schema }),
        );
        assert!(
            core.freeze_document_body(FreezeDocumentBodyInput {
                config_key: &key(),
                surrogate: Surrogate::new(1),
                value: b"{}",
                guards: GuardScope::Document
            })
            .is_err(),
            "{expression}"
        );
    }
}
