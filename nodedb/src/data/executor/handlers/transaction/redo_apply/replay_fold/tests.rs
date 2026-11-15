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
use super::super::test_commit::doc_put_sub_record;
use crate::bootstrap::write_group_settle::StoredWriteSets;
use crate::bootstrap::write_group_settle::settle::{
    SettleOutcome, close_replayed_groups, settle_against,
};
use crate::bridge::envelope::{Admission, PhysicalPlan, Priority, Request, Status};
use crate::control::server::wal_dispatch::{GroupOrigin, WalAppendRequest, append_group_origin};
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::doc_format;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, WalManager};
use crate::wal::{RedoRecord, WriteSetCapture};

/// A single-shard commit writing one source row of `amount` 25.
fn commit_plan() -> PhysicalPlan {
    let body = doc_format::encode_to_msgpack(&serde_json::json!({
        "account_id": ACCOUNT,
        "amount": 25,
    }));
    let redo = RedoRecord {
        version: 1,
        ops: vec![doc_put_sub_record(SOURCE, "c1", &body, 11)],
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
}

fn crash_after_install_pass() -> Crashed {
    let wal_dir = tempfile::tempdir().expect("tempdir");
    let wal = WalManager::open_for_testing(&wal_dir.path().join("wal")).expect("open wal");
    let store_dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _ends) = core_with_sum(store_dir.path());
    let plan = commit_plan();
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
    assert_eq!(balance(&core).as_deref(), Some("125"), "the live fold");
    let stores = crash_image(store_dir.path());
    Crashed {
        wal,
        _wal_dir: wal_dir,
        stores,
        origin: origin.lsn.as_u64(),
    }
}

/// The stores under `dir` as a power loss at this instant leaves them.
fn crash_image(dir: &Path) -> tempfile::TempDir {
    let image = tempfile::tempdir().expect("tempdir");
    for store in ["sparse/core-0.redb", "graph/core-0.redb"] {
        let to = image.path().join(store);
        std::fs::create_dir_all(to.parent().expect("parent")).expect("mkdir");
        std::fs::copy(dir.join(store), &to).expect("copy store");
    }
    image
}

/// Settle `wal` against the write sets the stores at `stores` hold, as
/// boot does. Returns the outcome and the stream replay then reads.
fn settle(wal: &WalManager, stores: &Path) -> (SettleOutcome, Vec<WalRecord>) {
    let records = wal.replay().expect("replay");
    let outcome = {
        let stored = StoredWriteSets::read(stores).expect("read write sets");
        let outcome = settle_against(wal, &records, &stored.captures).expect("settle");
        stored.clear().expect("clear");
        outcome
    };
    wal.sync().expect("sync");
    (outcome, wal.replay().expect("replay"))
}

/// Boot a core over `stores` and replay `records` with the settle's
/// verdicts. Returns the target balance and the write sets replay stored.
fn boot(
    stores: &Path,
    outcome: &SettleOutcome,
    records: &[WalRecord],
) -> (Option<String>, Vec<WriteSetCapture>) {
    let (mut core, _req, _resp) = make_core_with_dir(stores);
    configure_sum(&mut core);
    core.set_never_durable_redos(&outcome.never_durable);
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
