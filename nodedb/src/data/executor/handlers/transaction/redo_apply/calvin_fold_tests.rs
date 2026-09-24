// SPDX-License-Identifier: BUSL-1.1

//! A Calvin redo record folds its materialized sums from its stamp, the same
//! way live and in restart replay.

use nodedb_physical::physical_plan::{MaterializedSumBinding, RedoSumTargets, ResolvedSumTarget};
use nodedb_types::{DatabaseId, Surrogate, TenantId};
use nodedb_wal::record::{RecordType, WalRecordArgs};
use nodedb_wal::{TombstoneSet, WalRecord};

use super::entry::CommittedRedo;
use super::test_commit::doc_put_sub_record;
use crate::bridge::envelope::Status;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::doc_format;
use crate::engine::document::store::CollectionConfig;
use crate::types::Lsn;
use crate::wal::{CalvinStamp, RedoRecord};

const DB: u64 = 0;
const TID: u64 = 1;
/// Source and target share a vShard, so the inline fold writes the target
/// on this core.
const SOURCE: &str = "local_charges";
const TARGET: &str = "local_balances";
const ACCOUNT: &str = "a1";
const TARGET_SURROGATE: Surrogate = Surrogate(4242);
const LSN: u64 = 70;

/// A core whose source collection folds `amount` into the target's
/// `balance`, with the target row seeded at 100.
fn core_with_sum(dir: &std::path::Path) -> (CoreLoop, Box<dyn std::any::Any>) {
    let (mut core, req, resp) = make_core_with_dir(dir);
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(TID), TARGET.to_string()),
        CollectionConfig::new(TARGET),
    );
    let mut source = CollectionConfig::new(SOURCE);
    source.enforcement.materialized_sum_sources = vec![MaterializedSumBinding {
        target_collection: TARGET.to_string(),
        target_column: "balance".to_string(),
        join_column: "account_id".to_string(),
        value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        declared_primary_key: None,
    }];
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(TID), SOURCE.to_string()),
        source,
    );
    let seed = serde_json::json!({"id": ACCOUNT, "balance": "100"});
    core.sparse
        .put(
            DB,
            TID,
            TARGET,
            &nodedb_types::StorageKey::for_surrogate(TARGET_SURROGATE),
            &doc_format::encode_to_msgpack(&seed),
        )
        .expect("seed target row");
    (core, Box::new((req, resp)))
}

fn sum_targets() -> Vec<RedoSumTargets> {
    vec![RedoSumTargets {
        collection: SOURCE.to_string(),
        resolved: vec![ResolvedSumTarget::new(TARGET, ACCOUNT, TARGET_SURROGATE)],
        deferred: Vec::new(),
    }]
}

/// A Calvin redo record writing one source row of `amount` 25.
fn calvin_redo() -> Vec<u8> {
    let body = doc_format::encode_to_msgpack(&serde_json::json!({
        "account_id": ACCOUNT,
        "amount": 25,
    }));
    RedoRecord {
        version: 1,
        ops: vec![doc_put_sub_record(SOURCE, "c1", &body, 11)],
        calvin_stamp: Some(CalvinStamp {
            epoch: 3,
            position: 0,
            vshard_id: 0,
            collections: vec![SOURCE.to_string()],
            sum_targets: sum_targets(),
        }),
    }
    .to_bytes()
    .expect("encode redo")
}

fn balance(core: &CoreLoop) -> Option<String> {
    let stored = core
        .sparse
        .get(
            DB,
            TID,
            TARGET,
            &nodedb_types::StorageKey::for_surrogate(TARGET_SURROGATE),
        )
        .expect("read target")?;
    doc_format::decode_document(&stored)
        .ok()?
        .get("balance")
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

#[test]
fn restart_replay_folds_a_calvin_record_to_the_live_total() {
    let redo = calvin_redo();

    let live_dir = tempfile::tempdir().expect("tempdir");
    let (mut live, _live_ends) = core_with_sum(live_dir.path());
    let mut task = make_default_task();
    task.wal_lsn = Some(Lsn::new(LSN));
    let response = live.install_committed_redo(
        &task,
        TID,
        CommittedRedo {
            redo: &redo,
            collections: &[SOURCE.to_string()],
            sum_targets: &sum_targets(),
        },
    );
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert_eq!(balance(&live).as_deref(), Some("125"));

    let replay_dir = tempfile::tempdir().expect("tempdir");
    let (mut replayed, _replay_ends) = core_with_sum(replay_dir.path());
    let record = WalRecord::new(WalRecordArgs {
        record_type: RecordType::TransactionRedo as u32,
        lsn: LSN,
        tenant_id: TID,
        vshard_id: 0,
        database_id: DB,
        payload: redo,
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record");
    replayed
        .replay_transaction_redo_wal(std::slice::from_ref(&record), 1, &TombstoneSet::new())
        .expect("replay");
    assert_eq!(
        balance(&replayed),
        balance(&live),
        "restart replay folds to the total the live install produced"
    );

    replayed
        .replay_transaction_redo_wal(std::slice::from_ref(&record), 1, &TombstoneSet::new())
        .expect("replay again");
    assert_eq!(
        balance(&replayed).as_deref(),
        Some("125"),
        "a second replay over the installed row folds nothing"
    );
}
