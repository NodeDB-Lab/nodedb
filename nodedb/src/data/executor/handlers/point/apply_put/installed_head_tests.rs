// SPDX-License-Identifier: BUSL-1.1

//! Captured heads use the caller transaction and publish only after commit.

use super::{InstalledDocumentRow, PointPutParams};
use crate::{
    bridge::envelope::RowVersion,
    data::executor::{core_loop::tests::make_core_with_dir, enforcement::chain_guard::ChainGuard},
    engine::document::store::{CollectionConfig, StorageKey},
    types::{DatabaseId, InstalledDocument, TenantId, hash_chain::ChainHead},
};
use nodedb_physical::physical_plan::StorageMode;
use nodedb_types::Surrogate;
use std::sync::Arc;

const TID: u64 = 1;
const COLL: &str = "heads";
fn key() -> (DatabaseId, TenantId, String) {
    (DatabaseId::DEFAULT, TenantId::new(TID), COLL.into())
}
fn row(surrogate: u32) -> InstalledDocumentRow<'static> {
    InstalledDocumentRow {
        database_id: 0,
        tid: TID,
        collection: COLL,
        storage_key: StorageKey::for_surrogate(Surrogate::new(surrogate)),
        surrogate: Surrogate::new(surrogate),
        wal_lsn: None,
    }
}
fn image(seq: u64, hash: &str) -> crate::types::SharedInstalledDocument {
    let bytes = nodedb_types::json_to_msgpack(&serde_json::json!({"n": seq})).unwrap();
    InstalledDocument {
        canonical: bytes.clone(),
        stored: bytes,
        version: RowVersion::open(10),
        schema: Arc::new(StorageMode::Schemaless),
        chain_head: Some(ChainHead {
            seq,
            hash: hash.into(),
        }),
    }
}
#[test]
fn installed_heads_use_pending_transaction_state_and_never_move_backwards() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let txn = core.sparse.begin_write().unwrap();
    let (_, first) = core
        .apply_installed_document(&txn, row(1), &image(5, "head-5"))
        .unwrap();
    let first = first.unwrap();
    assert!(first.changed);
    assert!(first.prior.is_none());
    assert!(!core.chain.heads.contains_key(&key()));
    assert!(core.sparse.get_chain_head(0, TID, COLL).unwrap().is_none());
    assert_eq!(
        core.sparse
            .get_chain_head_in_txn(&txn, 0, TID, COLL)
            .unwrap(),
        Some(first.installed.clone())
    );
    let (_, older) = core
        .apply_installed_document(&txn, row(2), &image(3, "head-3"))
        .unwrap();
    let older = older.unwrap();
    assert!(!older.changed);
    assert_eq!(older.installed, first.installed);
    let (_, same) = core
        .apply_installed_document(&txn, row(3), &image(5, "head-5"))
        .unwrap();
    let same = same.unwrap();
    assert!(!same.changed);
    assert_eq!(same.installed, first.installed);
    txn.commit().unwrap();
    core.publish_installed_head(0, TID, &first);
    core.publish_installed_head(0, TID, &older);
    core.publish_installed_head(0, TID, &same);
    assert_eq!(core.chain.heads.get(&key()), Some(&first.installed));
    assert_eq!(
        core.sparse.get_chain_head(0, TID, COLL).unwrap(),
        Some(first.installed)
    );
}
#[test]
fn equal_head_positions_with_different_hashes_reject_before_row_changes() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let head = ChainHead {
        seq: 5,
        hash: "accepted".into(),
    };
    core.sparse.put_chain_head(0, TID, COLL, &head).unwrap();
    core.chain.heads.insert(key(), head.clone());
    let txn = core.sparse.begin_write().unwrap();
    assert!(
        core.apply_installed_document(&txn, row(1), &image(5, "different"))
            .is_err()
    );
    assert_eq!(
        core.sparse
            .get_chain_head_in_txn(&txn, 0, TID, COLL)
            .unwrap(),
        Some(head.clone())
    );
    assert!(
        core.sparse
            .get_in_txn(&txn, 0, TID, COLL, &row(1).storage_key)
            .unwrap()
            .is_none()
    );
    drop(txn);
    assert_eq!(core.chain.heads.get(&key()), Some(&head));
}
#[test]
fn inconsistent_committed_head_cache_rejects_before_installation() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    core.sparse
        .put_chain_head(
            0,
            TID,
            COLL,
            &ChainHead {
                seq: 2,
                hash: "durable".into(),
            },
        )
        .unwrap();
    let txn = core.sparse.begin_write().unwrap();
    assert!(
        core.apply_installed_document(&txn, row(1), &image(5, "new"))
            .is_err()
    );
    assert!(
        core.sparse
            .get_in_txn(&txn, 0, TID, COLL, &row(1).storage_key)
            .unwrap()
            .is_none()
    );
}
#[test]
fn mutated_chain_guard_requires_actual_settled_head_for_capture() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let mut config = CollectionConfig::new(COLL);
    config.enforcement.hash_chain = true;
    config.enforcement.append_only = true;
    core.doc_configs.insert(key(), config);
    let mut guard = ChainGuard::begin(&core, 0, TID, COLL);
    let bytes = nodedb_types::json_to_msgpack(&serde_json::json!({"n": 1})).unwrap();
    guard
        .chain_insert(&mut core, Surrogate::new(1), &bytes)
        .unwrap();
    assert!(guard.capture_installed_head(&core).is_err());
    let txn = core.sparse.begin_write().unwrap();
    let mut outcome = core
        .apply_point_put(
            &txn,
            PointPutParams {
                database_id: 0,
                tid: TID,
                collection: COLL,
                storage_key: row(1).storage_key,
                surrogate: Surrogate::new(1),
                value: &bytes,
                index_text: false,
                user_roles: &[],
                enforce: true,
                unique: crate::data::executor::enforcement::unique::UniqueJudge::Row,
                wal_lsn: None,
                resolved_targets: &[],
                guards: crate::data::executor::enforcement::typeguard::GuardScope::Document,
            },
        )
        .unwrap();
    guard
        .settle_installed(&mut core, Surrogate::new(1), &mut outcome.installed)
        .unwrap();
    assert_eq!(
        outcome.installed.chain_head.as_ref(),
        core.chain.heads.get(&key())
    );
    core.chain.heads.remove(&key());
    assert!(guard.capture_installed_head(&core).is_err());
}
