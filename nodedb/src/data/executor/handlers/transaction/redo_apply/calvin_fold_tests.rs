// SPDX-License-Identifier: BUSL-1.1

//! A Calvin redo record folds its materialized sums at its live install. The
//! fold rows travel in the install's write set, so the funnel journals them
//! as parts of the record's group, and restart replay applies those parts.

use nodedb_physical::physical_plan::{MaterializedSumBinding, RedoSumTargets, ResolvedSumTarget};
use nodedb_types::{DatabaseId, Surrogate, TenantId};
use nodedb_wal::record::{RecordType, WalRecordArgs};
use nodedb_wal::{TombstoneSet, WalRecord};

use super::entry::CommittedRedo;
use super::test_commit::doc_put_sub_record;
use crate::bridge::envelope::Status;
use crate::control::server::wal_dispatch::{GroupOrigin, WriteSetTarget, plan_group_parts};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::doc_format;
use crate::engine::document::store::CollectionConfig;
use crate::types::{Lsn, VShardId};
use crate::wal::{CalvinStamp, RedoRecord, WriteGroup, WriteGroupRecord};

const DB: u64 = 0;
pub(in crate::data::executor) const TID: u64 = 1;
/// Source and target share a vShard, so the inline fold writes the target
/// on this core.
pub(in crate::data::executor) const SOURCE: &str = "local_charges";
pub(in crate::data::executor) const TARGET: &str = "local_balances";
const ACCOUNT: &str = "a1";
pub(super) const TARGET_SURROGATE: Surrogate = Surrogate(4242);
const LSN: u64 = 70;

/// A core whose source collection folds `amount` into the target's
/// `balance`, with the target row seeded at 100.
pub(in crate::data::executor) fn core_with_sum(
    dir: &std::path::Path,
) -> (CoreLoop, Box<dyn std::any::Any>) {
    let (mut core, req, resp) = make_core_with_dir(dir);
    configure_sum(&mut core);
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

/// Declare on `core` that the source collection folds `amount` into the
/// target's `balance`. The target row is left as the stores hold it.
pub(in crate::data::executor) fn configure_sum(core: &mut CoreLoop) {
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
}

pub(in crate::data::executor) fn sum_targets() -> Vec<RedoSumTargets> {
    vec![RedoSumTargets {
        collection: SOURCE.to_string(),
        resolved: vec![ResolvedSumTarget::new(TARGET, ACCOUNT, TARGET_SURROGATE)],
    }]
}

/// A Calvin redo record writing one source row of `amount` 25.
pub(in crate::data::executor) fn calvin_redo() -> Vec<u8> {
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
        }),
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
    }
    .to_bytes()
    .expect("encode redo")
}

pub(in crate::data::executor) fn balance(core: &CoreLoop) -> Option<String> {
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

/// The WAL record at `lsn` of `record_type` carrying `payload`.
fn wal_record(record_type: RecordType, lsn: u64, payload: Vec<u8>) -> WalRecord {
    WalRecord::new(WalRecordArgs {
        record_type: record_type as u32,
        lsn,
        tenant_id: TID,
        vshard_id: 0,
        database_id: DB,
        payload,
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record")
}

/// The live install folds the target row and reports it in its write set.
/// Restart replay of the record and the parts journalled from that write
/// set reaches the live total. A replay of the record alone folds nothing:
/// the stamp carries no fold.
#[test]
fn restart_replay_applies_the_journalled_fold_rows_not_the_stamp() {
    let redo = calvin_redo();

    let live_dir = tempfile::tempdir().expect("tempdir");
    let (mut live, _live_ends) = core_with_sum(live_dir.path());
    let mut task = make_default_task();
    task.wal_lsn = Some(Lsn::new(LSN));
    let response = live.execute_apply_transaction_redo(
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
    assert!(
        !response.write_set.is_empty(),
        "the fold target row travels in the write set"
    );

    let parts = plan_group_parts(
        &WriteSetTarget {
            tenant_id: TenantId::new(TID),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            collection: SOURCE,
            origin: GroupOrigin { lsn: Lsn::new(LSN) },
        },
        &response.write_set,
        1 << 20,
    )
    .expect("plan parts");
    let count = parts.count().expect("part count");
    let mut records = vec![wal_record(RecordType::TransactionRedo, LSN, redo.clone())];
    for (part, (_, ops)) in (1..=count).zip(parts.runs) {
        let group = WriteGroupRecord {
            group: WriteGroup::part_of(LSN, part, count),
            ops,
            redo: None,
        };
        records.push(wal_record(
            RecordType::WriteGroup,
            LSN + u64::from(part),
            group.to_bytes().expect("encode part"),
        ));
    }

    let replay_dir = tempfile::tempdir().expect("tempdir");
    let (mut replayed, _replay_ends) = core_with_sum(replay_dir.path());
    replayed
        .replay_transaction_redo_wal(&records, 1, &TombstoneSet::new())
        .expect("replay");
    assert_eq!(
        balance(&replayed),
        balance(&live),
        "restart replay of the record and its parts reaches the live total"
    );

    let bare_dir = tempfile::tempdir().expect("tempdir");
    let (mut bare, _bare_ends) = core_with_sum(bare_dir.path());
    bare.replay_transaction_redo_wal(
        &[wal_record(RecordType::TransactionRedo, LSN, redo)],
        1,
        &TombstoneSet::new(),
    )
    .expect("replay record alone");
    assert_eq!(
        balance(&bare).as_deref(),
        Some("100"),
        "the record alone folds nothing"
    );
}
