// SPDX-License-Identifier: BUSL-1.1

//! Actual writer authority separates fresh row versions from exact restored versions.

use crate::{
    data::executor::{
        core_loop::{CoreLoop, tests::make_core_with_dir},
        enforcement::{typeguard::GuardScope, unique::UniqueJudge},
        handlers::{
            point::{apply_delete::PointDeleteParams, apply_put::PointPutParams},
            transaction::overlay::BitemporalStamp,
        },
    },
    engine::{document::store::StorageKey, sparse::btree_versioned::VersionedPut},
};
use nodedb_types::Surrogate;

const TID: u64 = 1;
const COLL: &str = "ordinals";
fn key() -> StorageKey {
    StorageKey::for_surrogate(Surrogate::new(1))
}
fn body(n: i64) -> Vec<u8> {
    nodedb_types::json_to_msgpack(&serde_json::json!({"n": n})).unwrap()
}
/// The native tagged object a point put stores for `body(n)`.
fn stored(n: i64) -> Vec<u8> {
    let fields =
        std::collections::HashMap::from([("n".to_string(), nodedb_types::Value::Integer(n))]);
    zerompk::to_msgpack_vec(&nodedb_types::Value::Object(fields)).unwrap()
}
fn params(bytes: &[u8]) -> PointPutParams<'_> {
    PointPutParams {
        database_id: 0,
        tid: TID,
        collection: COLL,
        storage_key: key(),
        surrogate: Surrogate::new(1),
        value: bytes,
        index_text: false,
        user_roles: &[],
        enforce: false,
        unique: UniqueJudge::Unit,
        wal_lsn: None,
        resolved_targets: &[],
        guards: GuardScope::Document,
    }
}
fn delete() -> PointDeleteParams<'static> {
    PointDeleteParams {
        database_id: 0,
        tid: TID,
        collection: COLL,
        document_id: "one",
        surrogate: Surrogate::new(1),
        user_roles: &[],
        enforce: false,
        resolved_targets: &[],
    }
}
fn raw(core: &CoreLoop, txn: &redb::WriteTransaction, sys_from: i64, bytes: &[u8]) {
    core.sparse
        .versioned_put_in_txn(
            txn,
            VersionedPut {
                database_id: 0,
                tenant: TID,
                coll: COLL,
                doc_id: &key(),
                sys_from,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                body: bytes,
            },
        )
        .unwrap();
}
#[test]
fn repeated_key_normal_unit_raises_above_committed_puts_pending_puts_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let txn = core.sparse.begin_write().unwrap();
    raw(&core, &txn, 99, &body(0));
    txn.commit().unwrap();
    core.apply_scope.replicated_ordinal = Some(77);
    let txn = core.sparse.begin_write().unwrap();
    for (value, expected) in [(1, 100), (2, 101)] {
        let bytes = body(value);
        let prepared = core.prepare_point_put(&txn, params(&bytes)).unwrap();
        assert_eq!(prepared.installed().version.sys_from, expected);
        let outcome = core.apply_prepared_point_put(&txn, prepared).unwrap();
        assert_eq!(outcome.installed.version.sys_from, expected);
    }
    let deleted = core.apply_point_delete(&txn, delete()).unwrap();
    assert_eq!(deleted.version_sys_from, Some(102));
    assert!(
        core.sparse
            .get_in_txn(&txn, 0, TID, COLL, &key())
            .unwrap()
            .is_none()
    );
    let bytes = body(3);
    let prepared = core.prepare_point_put(&txn, params(&bytes)).unwrap();
    assert_eq!(prepared.installed().version.sys_from, 103);
    core.apply_prepared_point_put(&txn, prepared).unwrap();
    assert_eq!(
        core.sparse
            .versioned_latest_sys_from_in_txn(&txn, 0, TID, COLL, &key())
            .unwrap(),
        Some(103)
    );
    txn.commit().unwrap();
    for (ordinal, expected) in [
        (99, body(0)),
        (100, stored(1)),
        (101, stored(2)),
        (103, stored(3)),
    ] {
        assert_eq!(
            core.sparse
                .versioned_get_as_of(0, TID, COLL, &key(), Some(ordinal), None)
                .unwrap(),
            Some(expected)
        );
    }
    assert!(
        core.sparse
            .versioned_get_as_of(0, TID, COLL, &key(), Some(102), None)
            .unwrap()
            .is_none()
    );
}
#[test]
fn fresh_put_and_delete_refuse_pending_maximum_ordinal_without_reusing_it() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    core.apply_scope.replicated_ordinal = Some(77);
    let txn = core.sparse.begin_write().unwrap();
    raw(&core, &txn, i64::MAX, &body(1));
    assert!(matches!(
        core.prepare_point_put(&txn, params(&body(2))),
        Err(crate::Error::NumericValueOutOfRange { .. })
    ));
    assert!(matches!(
        core.apply_point_delete(&txn, delete()),
        Err(crate::Error::NumericValueOutOfRange { .. })
    ));
    assert_eq!(
        core.sparse.get_in_txn(&txn, 0, TID, COLL, &key()).unwrap(),
        Some(body(1))
    );
    core.sparse
        .versioned_tombstone_in_txn(&txn, 0, TID, COLL, &key(), i64::MAX)
        .unwrap();
    assert!(matches!(
        core.apply_point_delete(&txn, delete()),
        Err(crate::Error::NumericValueOutOfRange { .. })
    ));
    assert_eq!(
        core.sparse
            .versioned_latest_sys_from_in_txn(&txn, 0, TID, COLL, &key())
            .unwrap(),
        Some(i64::MAX)
    );
    drop(txn);
    assert!(
        core.sparse
            .versioned_latest_sys_from(0, TID, COLL, &key())
            .unwrap()
            .is_none()
    );
}
#[test]
fn forced_restore_put_and_delete_keep_exact_history_stamps_below_pending_newest_version() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    core.apply_scope.replicated_ordinal = Some(500);
    let txn = core.sparse.begin_write().unwrap();
    raw(&core, &txn, 200, &body(2));
    core.apply_scope.bitemporal_stamps.insert(
        1,
        BitemporalStamp {
            sys_from: 100,
            valid_from_ms: 20,
            valid_until_ms: 30,
        },
    );
    let bytes = body(1);
    let prepared = core.prepare_point_put(&txn, params(&bytes)).unwrap();
    assert_eq!(prepared.installed().version.sys_from, 100);
    assert_eq!(prepared.installed().version.valid_from_ms, 20);
    assert_eq!(prepared.installed().version.valid_until_ms, 30);
    let outcome = core.apply_prepared_point_put(&txn, prepared).unwrap();
    assert!(outcome.history.is_some());
    core.apply_scope.bitemporal_stamps.insert(
        1,
        BitemporalStamp {
            sys_from: 150,
            valid_from_ms: i64::MIN,
            valid_until_ms: i64::MAX,
        },
    );
    let outcome = core.apply_point_delete(&txn, delete()).unwrap();
    assert_eq!(outcome.version_sys_from, Some(150));
    assert!(outcome.history.is_some());
    assert_eq!(
        core.sparse
            .versioned_latest_sys_from_in_txn(&txn, 0, TID, COLL, &key())
            .unwrap(),
        Some(200)
    );
    txn.commit().unwrap();
    assert_eq!(
        core.sparse
            .versioned_get_as_of(0, TID, COLL, &key(), Some(100), None)
            .unwrap(),
        Some(stored(1))
    );
    assert!(
        core.sparse
            .versioned_get_as_of(0, TID, COLL, &key(), Some(150), None)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        core.sparse.get(0, TID, COLL, &key()).unwrap(),
        Some(body(2))
    );
}
