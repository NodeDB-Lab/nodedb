// SPDX-License-Identifier: BUSL-1.1

//! A committed record whose install never became durable folds once at
//! restart replay, and a second restart keeps the single-fold total.
//!
//! The live core installs the record with its store commits deferred, and
//! the crash comes before its write set is stored. The WAL holds the
//! record. Each test cuts the core's stores at that instant, the way a
//! power loss does, and boots from the cut.

use std::path::Path;
use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::{MetaOp, RedoOrigin};
use nodedb_wal::{TombstoneSet, WalRecord};

use super::super::calvin_fold_tests::{
    ACCOUNT, SOURCE, TID, balance, configure_sum, core_with_sum, sum_targets,
};
use super::super::calvin_fold_tests::{TARGET, TARGET_SURROGATE};
use super::super::fold_ledger::encode_target_row;
use super::super::test_commit::doc_put_sub_record;
use crate::bootstrap::write_group_settle::StoredWriteSets;
use crate::bootstrap::write_group_settle::settle::{
    SettleOutcome, close_replayed_groups, settle_against,
};
use crate::bridge::envelope::{Admission, PhysicalPlan, Priority, Request, Status};
use crate::bridge::envelope::{RowEffect, WriteSetEntry};
use crate::control::server::wal_dispatch::{GroupOrigin, WalAppendRequest, append_group_origin};
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::doc_format;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, WalManager};
use crate::wal::{RedoRecord, WriteSetCapture};

/// A single-shard commit writing one source row of `amount` 25.
fn commit_plan() -> PhysicalPlan {
    commit_plan_of(&[("c1", 25, 11)])
}

/// A single-shard commit writing one source row per `(id, amount,
/// surrogate)`, in order.
fn commit_plan_of(rows: &[(&str, i64, u32)]) -> PhysicalPlan {
    let ops = rows
        .iter()
        .map(|(id, amount, surrogate)| {
            let body = doc_format::encode_to_msgpack(&serde_json::json!({
                "account_id": ACCOUNT,
                "amount": amount,
            }));
            doc_put_sub_record(SOURCE, id, &body, *surrogate)
        })
        .collect();
    let redo = RedoRecord {
        version: 1,
        ops,
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
        sum_targets: Vec::new(),
    }
    .to_bytes()
    .expect("encode redo");
    PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo,
        collections: vec![SOURCE.to_string()],
        sum_targets: sum_targets(),
        origin: RedoOrigin::Commit,
        calvin: None,
        reads: Vec::new(),
        snapshot_ordinal: None,
    })
}

fn request(plan: PhysicalPlan, origin: GroupOrigin) -> Request {
    Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(TID),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
        plan,
        deadline: Instant::now() + Duration::from_secs(5),
        priority: Priority::Normal,
        trace_id: TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: Some(origin.lsn),
        resolved_now_ms: None,
        commit_hlc: None,
        system_ordinal: None,
        admission: Admission::Admitted,
    }
}

/// The live node at the crash: the record's WAL copy is durable, its
/// install ran with deferred commits, and its write set is not stored.
struct Crashed {
    wal: WalManager,
    _wal_dir: tempfile::TempDir,
    stores: tempfile::TempDir,
    origin: u64,
    /// The target row writes the live install answered, one per write.
    live_write_set: Vec<WriteSetEntry>,
}

fn crash_after_install_pass() -> Crashed {
    crash_after_installing(commit_plan(), "125")
}

/// The live node at the crash after installing `plan`, whose live fold
/// leaves the balance at `live_balance`.
fn crash_after_installing(plan: PhysicalPlan, live_balance: &str) -> Crashed {
    let wal_dir = tempfile::tempdir().expect("tempdir");
    let wal = WalManager::open_for_testing(&wal_dir.path().join("wal")).expect("open wal");
    let store_dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _ends) = core_with_sum(store_dir.path());
    let origin = append_group_origin(WalAppendRequest {
        wal: wal.appender(NO_APPLY_KEY),
        event_source: crate::event::EventSource::User,
        tenant_id: TenantId::new(TID),
        vshard_id: VShardId::new(0),
        database_id: DatabaseId::DEFAULT,
        plan: &plan,
        credentials: None,
        now_override: None,
    })
    .expect("origin")
    .origin;
    // A concurrent WAL sync makes the record durable.
    wal.sync().expect("sync");
    core.begin_journalled();
    let response = core.execute(&ExecutionTask::new(request(plan, origin)));
    assert_eq!(response.status, Status::Ok, "{response:?}");
    assert_eq!(
        balance(&core).as_deref(),
        Some(live_balance),
        "the live fold"
    );
    let stores = crash_image(store_dir.path());
    Crashed {
        wal,
        _wal_dir: wal_dir,
        stores,
        origin: origin.lsn.as_u64(),
        live_write_set: response.write_set,
    }
}

/// The stores under `dir` as a power loss at this instant leaves them, with
/// the core's WAL directory: a booted core reads stream chunks from it, as
/// a node reads its own WAL.
pub(in crate::data::executor::handlers::transaction::redo_apply) fn crash_image(
    dir: &Path,
) -> tempfile::TempDir {
    let image = tempfile::tempdir().expect("tempdir");
    for store in ["sparse/core-0.redb", "graph/core-0.redb"] {
        let to = image.path().join(store);
        std::fs::create_dir_all(to.parent().expect("parent")).expect("mkdir");
        std::fs::copy(dir.join(store), &to).expect("copy store");
    }
    let wal = dir.join("wal");
    if wal.is_dir() {
        let to = image.path().join("wal");
        std::fs::create_dir_all(&to).expect("mkdir wal");
        for entry in std::fs::read_dir(&wal).expect("read wal dir") {
            let path = entry.expect("wal entry").path();
            if path.is_file() {
                let name = path.file_name().expect("file name");
                std::fs::copy(&path, to.join(name)).expect("copy wal file");
            }
        }
    }
    image
}

/// Settle `wal` against the write sets the stores at `stores` hold, as
/// boot does. Returns the outcome and the stream replay then reads.
pub(in crate::data::executor::handlers::transaction::redo_apply) fn settle(
    wal: &WalManager,
    stores: &Path,
) -> (SettleOutcome, Vec<WalRecord>) {
    let records = wal.replay().expect("replay");
    let outcome = {
        let stored = StoredWriteSets::read(stores).expect("read write sets");
        let mut outcome = settle_against(wal, &records, &stored.captures).expect("settle");
        stored
            .settle_verdicts(&mut outcome.never_durable)
            .expect("settle verdicts");
        stored.clear().expect("clear");
        outcome
    };
    wal.sync().expect("sync");
    (outcome, wal.replay().expect("replay"))
}

/// Boot a core over `stores` and replay `records` with the settle's
/// verdicts. Returns the target balance and the write sets replay stored.
pub(in crate::data::executor::handlers::transaction::redo_apply) fn boot(
    stores: &Path,
    outcome: &SettleOutcome,
    records: &[WalRecord],
) -> (Option<String>, Vec<WriteSetCapture>) {
    let (mut core, _req, _resp) = make_core_with_dir(stores);
    configure_sum(&mut core);
    core.set_never_durable_redos(&outcome.never_durable)
        .expect("fold ledger");
    core.replay_all_wal(records, 1, &TombstoneSet::new())
        .expect("replay");
    let captures = core.take_replay_captures();
    (balance(&core), captures)
}

/// The crash reverts the fold. Boot judges the record never durable,
/// leaves its group open, and replay folds it once. Boot journals the fold
/// rows, and the next boot replays the record plainly to the same total.
#[test]
fn a_never_durable_record_folds_once_and_its_group_closes() {
    let crashed = crash_after_install_pass();

    let (outcome, records) = settle(&crashed.wal, crashed.stores.path());
    assert!(
        outcome.never_durable.contains(crashed.origin),
        "the record whose write set the crash cut is never durable"
    );
    let (balance_once, captures) = boot(crashed.stores.path(), &outcome, &records);
    assert_eq!(
        balance_once.as_deref(),
        Some("125"),
        "replay folds the reverted delta"
    );
    assert_eq!(captures.len(), 1, "replay stores the record's fold rows");
    assert_eq!(captures[0].origin, crashed.origin);
    assert!(
        !captures[0].entries.is_empty(),
        "the write set holds the target row"
    );
    assert!(close_replayed_groups(&crashed.wal, &captures).expect("close groups"));

    let (outcome, records) = settle(&crashed.wal, crashed.stores.path());
    assert!(
        outcome.never_durable.is_empty(),
        "the parts make the record complete"
    );
    assert!(!outcome.appended, "the closed group needs nothing");
    let (balance_twice, captures) = boot(crashed.stores.path(), &outcome, &records);
    assert_eq!(
        balance_twice.as_deref(),
        Some("125"),
        "a second restart folds nothing"
    );
    assert!(captures.is_empty());
}

/// A crash after replay stored the fold rows and before boot journalled
/// them: the next boot judges the record complete from the stored write
/// set, journals its parts, and replays it without a second fold.
#[test]
fn a_crash_after_the_replay_write_set_is_stored_folds_nothing_again() {
    let crashed = crash_after_install_pass();

    let (outcome, records) = settle(&crashed.wal, crashed.stores.path());
    let (balance_once, captures) = boot(crashed.stores.path(), &outcome, &records);
    assert_eq!(balance_once.as_deref(), Some("125"));
    assert_eq!(captures.len(), 1);

    let (outcome, records) = settle(&crashed.wal, crashed.stores.path());
    assert!(
        outcome.never_durable.is_empty(),
        "the stored write set makes the record complete"
    );
    assert!(
        outcome.appended,
        "boot journals the parts from the write set"
    );
    let (balance_twice, captures) = boot(crashed.stores.path(), &outcome, &records);
    assert_eq!(balance_twice.as_deref(), Some("125"), "no second fold");
    assert!(captures.is_empty());
}

/// A boot that crashes after replay folded the record and before it stored
/// the record's write set persists none of the record's writes. The next
/// boot judges the record never durable again and folds it exactly once.
#[cfg(feature = "failpoints")]
#[test]
fn a_boot_cut_before_the_replay_write_set_is_stored_folds_once_at_the_next_boot() {
    use nodedb_types::fail_point::{FailAction, FailGuard};

    let crashed = crash_after_install_pass();
    let (outcome, records) = settle(&crashed.wal, crashed.stores.path());
    assert!(outcome.never_durable.contains(crashed.origin));
    {
        let _armed = FailGuard::install("replay_fold::before_write_set", FailAction::Panic);
        let cut = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            boot(crashed.stores.path(), &outcome, &records)
        }));
        assert!(
            cut.is_err(),
            "the boot stops before the write set is stored"
        );
    }

    let (outcome, records) = settle(&crashed.wal, crashed.stores.path());
    assert!(
        outcome.never_durable.contains(crashed.origin),
        "nothing of the cut boot is durable, so the record stays never durable"
    );
    let (balance_once, captures) = boot(crashed.stores.path(), &outcome, &records);
    assert_eq!(balance_once.as_deref(), Some("125"), "one fold, not two");
    assert_eq!(captures.len(), 1);
}

/// A replay with no never-durable verdict folds nothing: the record with
/// no stored write set and no verdict replays plainly.
#[test]
fn a_core_without_the_verdict_folds_nothing() {
    let crashed = crash_after_install_pass();
    let (_outcome, records) = settle(&crashed.wal, crashed.stores.path());
    let (balance_plain, captures) =
        boot(crashed.stores.path(), &SettleOutcome::default(), &records);
    assert_eq!(balance_plain.as_deref(), Some("100"));
    assert!(captures.is_empty());
}

/// The core keeps the targets of the never-durable record from the record's
/// WAL copy.
#[test]
fn the_wal_copy_carries_the_install_targets() {
    let crashed = crash_after_install_pass();
    let records = crashed.wal.replay().expect("replay");
    let stored = records
        .iter()
        .find(|record| record.header.lsn == crashed.origin)
        .expect("the record");
    let redo = RedoRecord::from_bytes(&stored.payload).expect("decode");
    assert_eq!(redo.sum_targets, sum_targets());
}

/// The `balance` each target row write in `entries` installed, in order.
fn balances(entries: &[WriteSetEntry]) -> Vec<String> {
    entries
        .iter()
        .map(|entry| match &entry.effect {
            RowEffect::InstalledPut { image, .. } => doc_format::decode_document(&image.stored)
                .ok()
                .and_then(|doc| {
                    doc.get("balance")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                })
                .unwrap_or_default(),
            other => panic!("a target row write is an installed put, not {other:?}"),
        })
        .collect()
}

/// Leave in the stores of `crashed` what a batched install of its record
/// made durable through its first `ledger` ops: the target row as op
/// `ledger - 1` wrote it, the fold ledger, and each target row write of
/// those ops as the live install made it.
fn seed_durable_ops(crashed: &Crashed, ledger: usize) {
    let (core, _req, _resp) = make_core_with_dir(crashed.stores.path());
    let written = &crashed.live_write_set[..ledger];
    let Some(last) = written.last() else {
        return;
    };
    let RowEffect::InstalledPut { image, .. } = &last.effect else {
        panic!("a target row write is an installed put");
    };
    core.sparse
        .put(
            DatabaseId::DEFAULT.as_u64(),
            TID,
            TARGET,
            &nodedb_types::StorageKey::for_surrogate(TARGET_SURROGATE),
            &image.stored,
            image.version.sys_from,
        )
        .expect("the durable target row");
    let txn = core.sparse.begin_durable_write().expect("txn");
    core.sparse
        .put_fold_ledger_in_txn(&txn, crashed.origin, ledger as u64)
        .expect("ledger");
    for (op, entry) in written.iter().enumerate() {
        core.sparse
            .put_install_target_in_txn(
                &txn,
                crashed.origin,
                op as u64,
                0,
                &encode_target_row(entry).expect("encode"),
            )
            .expect("target row");
    }
    txn.commit().expect("commit");
}

/// The batched install rows the stores at `stores` hold for `origin`: the
/// fold ledgers and the target row writes.
pub(in crate::data::executor::handlers::transaction::redo_apply) fn install_rows(
    stores: &Path,
    origin: u64,
) -> (Vec<(u64, u64)>, usize) {
    let sparse =
        crate::engine::sparse::btree::SparseEngine::open(&stores.join("sparse/core-0.redb"))
            .expect("open sparse");
    let mut targets = 0;
    sparse
        .scan_install_targets(origin, |_| {
            targets += 1;
            Ok(())
        })
        .expect("scan");
    (sparse.load_fold_ledgers().expect("ledgers"), targets)
}

/// `records` with the record at `origin` split as the WAL splits a record
/// over its limit: the origin keeps its first `chunks[0]` ops, and each
/// later chunk becomes a continuation past the stream's last LSN.
fn split_record(records: &[WalRecord], origin: u64, chunks: &[usize]) -> Vec<WalRecord> {
    use nodedb_wal::record::{RecordStamp, RecordType, WalRecordArgs};

    let position = records
        .iter()
        .position(|record| record.header.lsn == origin)
        .expect("the record");
    let header = records[position].header;
    let mut redo = RedoRecord::from_bytes(&records[position].payload).expect("decode");
    let stamp = RecordStamp {
        apply_key: header.apply_key,
        event_source: header.event_source,
        commit_hlc: header.commit_hlc,
        system_ordinal: header.system_ordinal,
    };
    let record = |record_type: RecordType, lsn: u64, payload: Vec<u8>| {
        WalRecord::new_stamped(
            WalRecordArgs {
                record_type: record_type as u32,
                lsn,
                tenant_id: header.tenant_id,
                vshard_id: header.vshard_id,
                database_id: header.database_id,
                payload,
                encryption_key: None,
                preamble_bytes: None,
            },
            stamp,
        )
        .expect("wal record")
    };
    let mut rest = std::mem::take(&mut redo.ops).into_iter();
    let mut next_chunk = |count: usize| rest.by_ref().take(count).collect::<Vec<_>>();
    redo.ops = next_chunk(chunks[0]);
    let metadata = crate::wal::ContinuedRedo {
        row_sources: redo.row_sources.clone(),
        row_changes: redo.row_changes.clone(),
    };
    let mut split = records.to_vec();
    split[position] = record(
        RecordType::TransactionRedo,
        origin,
        redo.to_bytes().expect("encode origin"),
    );
    let last = records
        .iter()
        .map(|record| record.header.lsn)
        .max()
        .unwrap_or(origin);
    for (index, count) in (1u32..).zip(&chunks[1..]) {
        let group = crate::wal::WriteGroupRecord {
            schemas: Vec::new(),
            group: crate::wal::WriteGroup::continuing(origin, index),
            ops: next_chunk(*count),
            redo: Some(metadata.clone()),
        };
        split.push(record(
            RecordType::WriteGroup,
            last + u64::from(index),
            group.to_bytes().expect("encode continuation"),
        ));
    }
    split
}

/// A batched install made its first two ops durable before the crash: their
/// rows, their folds, the fold ledger at 2 and their target row writes.
/// Replay folds only the third op. The commit that stores the write set
/// removes the ledger and the stored target rows.
#[test]
fn a_ledger_skips_the_folds_already_durable() {
    let crashed = crash_after_installing(
        commit_plan_of(&[("c1", 25, 11), ("c2", 30, 12), ("c3", 45, 13)]),
        "200",
    );
    seed_durable_ops(&crashed, 2);

    let (outcome, records) = settle(&crashed.wal, crashed.stores.path());
    assert!(outcome.never_durable.contains(crashed.origin));
    let (balance_once, captures) = boot(crashed.stores.path(), &outcome, &records);
    assert_eq!(
        balance_once.as_deref(),
        Some("200"),
        "the sum holds one fold of every op"
    );
    assert_eq!(captures.len(), 1);
    assert_eq!(
        install_rows(crashed.stores.path(), crashed.origin),
        (Vec::new(), 0),
        "the write set's commit removes the ledger and the target rows"
    );
}

/// The write set of a record replay folded above a ledger holds the stored
/// target row writes, then the replayed ones: the entry sequence of the
/// record's whole live install, one entry per target write.
#[test]
fn the_fold_capture_merges_stored_targets() {
    let crashed = crash_after_installing(
        commit_plan_of(&[("c1", 25, 11), ("c2", 30, 12), ("c3", 45, 13)]),
        "200",
    );
    seed_durable_ops(&crashed, 1);

    let (outcome, records) = settle(&crashed.wal, crashed.stores.path());
    let (balance_once, captures) = boot(crashed.stores.path(), &outcome, &records);
    assert_eq!(balance_once.as_deref(), Some("200"));
    assert_eq!(captures.len(), 1);
    let replayed = captures[0].write_set().expect("write set");
    assert_eq!(replayed.len(), crashed.live_write_set.len());
    assert_eq!(balances(&replayed), ["125", "155", "200"]);
    assert_eq!(balances(&replayed), balances(&crashed.live_write_set));
    assert_eq!(
        replayed[0].effect, crashed.live_write_set[0].effect,
        "the stored write enters as the live install made it"
    );
}

/// A never-durable record split over the WAL record limit folds every op
/// once, its continuations' ops included, in the live install's order.
#[test]
fn a_split_record_folds_every_continuation_op_once() {
    let crashed = crash_after_installing(
        commit_plan_of(&[
            ("c1", 10, 11),
            ("c2", 20, 12),
            ("c3", 30, 13),
            ("c4", 40, 14),
        ]),
        "200",
    );
    let (outcome, records) = settle(&crashed.wal, crashed.stores.path());
    assert!(outcome.never_durable.contains(crashed.origin));
    let records = split_record(&records, crashed.origin, &[1, 2, 1]);
    let (balance_once, captures) = boot(crashed.stores.path(), &outcome, &records);
    assert_eq!(balance_once.as_deref(), Some("200"), "one fold of every op");
    assert_eq!(captures.len(), 1);
    let replayed = captures[0].write_set().expect("write set");
    assert_eq!(balances(&replayed), ["110", "130", "160", "200"]);
    assert_eq!(balances(&replayed), balances(&crashed.live_write_set));
}

/// A ledger inside a continuation skips exactly the ops below it: the
/// origin's op and the continuation's first op are durable, and replay
/// folds the rest.
#[test]
fn a_ledger_inside_a_continuation_skips_the_ops_below_it() {
    let crashed = crash_after_installing(
        commit_plan_of(&[
            ("c1", 10, 11),
            ("c2", 20, 12),
            ("c3", 30, 13),
            ("c4", 40, 14),
        ]),
        "200",
    );
    seed_durable_ops(&crashed, 2);
    let (outcome, records) = settle(&crashed.wal, crashed.stores.path());
    let records = split_record(&records, crashed.origin, &[1, 2, 1]);
    let (balance_once, captures) = boot(crashed.stores.path(), &outcome, &records);
    assert_eq!(
        balance_once.as_deref(),
        Some("200"),
        "the ops below the ledger fold nothing again"
    );
    assert_eq!(captures.len(), 1);
    let replayed = captures[0].write_set().expect("write set");
    assert_eq!(balances(&replayed), balances(&crashed.live_write_set));
    assert_eq!(
        install_rows(crashed.stores.path(), crashed.origin),
        (Vec::new(), 0)
    );
}

/// A batch's source rows written under a fold ledger record, in their own
/// transactions, the ledger past their last op and each target row write
/// they made, in write order.
#[test]
fn a_batch_under_a_fold_ledger_records_its_ops_and_targets() {
    use super::super::fold_ledger::FoldLedger;
    use super::super::state::{RedoApplyPass, RedoApplyScope};
    use crate::wal::redo::op_index::RedoOpIndex;

    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _ends) = core_with_sum(dir.path());
    let lsn = 90;
    let records: Vec<WalRecord> = [("c1", 25, 11u32), ("c2", 30, 12)]
        .iter()
        .map(|(id, amount, surrogate)| {
            let body = doc_format::encode_to_msgpack(&serde_json::json!({
                "account_id": ACCOUNT,
                "amount": amount,
            }));
            let sub = doc_put_sub_record(SOURCE, id, &body, *surrogate);
            WalRecord::new(nodedb_wal::record::WalRecordArgs {
                record_type: sub.record_type,
                lsn,
                tenant_id: TID,
                vshard_id: 0,
                database_id: DatabaseId::DEFAULT.as_u64(),
                payload: sub.payload,
                encryption_key: None,
                preamble_bytes: None,
            })
            .expect("wal record")
        })
        .collect();
    let mut scope = RedoApplyScope::new(RedoApplyPass::Install, sum_targets());
    scope.ledger = Some(FoldLedger { lsn, op_base: 5 });
    core.redo_apply.scope = Some(scope);
    core.replay_document_redo(
        &records,
        &RedoOpIndex::new(vec![Some(0), Some(1)]),
        1,
        &TombstoneSet::new(),
    );
    let scope = core.redo_apply.scope.take().expect("scope");
    assert!(scope.error.is_none(), "{:?}", scope.error);
    assert_eq!(balance(&core).as_deref(), Some("155"));

    assert_eq!(
        core.sparse.load_fold_ledgers().expect("ledgers"),
        [(lsn, 7)],
        "the ledger counts the record's ops through the batch's last"
    );
    let stored = core.stored_install_targets(lsn).expect("stored rows");
    assert_eq!(balances(&stored), ["125", "155"], "every write, in order");
    assert!(stored.iter().all(|entry| {
        entry.surrogate == TARGET_SURROGATE.as_u32() && entry.collection.as_deref() == Some(TARGET)
    }));
}
