// SPDX-License-Identifier: BUSL-1.1

//! Crash windows of a journalled write.
//!
//! A grouped write runs with its store commits deferred, then stores its
//! write set beside its effects. Each test cuts the core's stores at one
//! instant, the way a power loss does, settles the WAL as boot does, and
//! checks the three views agree: restart replay onto the cut stores, replay
//! onto an empty base (a point-in-time restore), and the settled WAL the
//! event stream is rebuilt from.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::{
    CalvinInstall, CalvinReplySpec, DocumentOp, MetaOp, RedoOrigin, UpdateValue,
};
use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate, Value};
use nodedb_wal::{TombstoneSet, WalRecord};

use crate::bootstrap::write_group_settle::StoredWriteSets;
use crate::bootstrap::write_group_settle::settle::settle_against;
use crate::bridge::dispatch::JournalGroup;
use crate::bridge::envelope::{
    Admission, PhysicalPlan, Priority, Request, Response, Status, WriteSetEntry,
};
use crate::control::cluster::calvin::scheduler::recover_all_applied;
use crate::control::security::catalog::SystemCatalog;
use crate::control::server::wal_dispatch::{
    GroupOrigin, WalAppendRequest, WriteSetTarget, append_group_origin, append_group_parts,
};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::handlers::transaction::redo_apply::calvin_fold_tests::{
    SOURCE, balance, calvin_redo, configure_sum, core_with_sum, sum_targets,
};
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
use crate::wal::GroupMembership;
use crate::wal::manager::{NO_APPLY_KEY, WalManager};
use nodedb_types::RowIdentity;

const TID: u64 = 1;
const ORDERS: &str = "orders";
/// The collection a sum fold writes into.
const TOTALS: &str = "totals";

/// The live node: its WAL and one core.
struct Live {
    wal: WalManager,
    wal_dir: tempfile::TempDir,
    store_dir: tempfile::TempDir,
    core: CoreLoop,
}

impl Live {
    fn open() -> Self {
        Self::open_with(|dir| make_core_with_dir(dir).0)
    }

    /// A live node whose core `make` builds over its store directory.
    fn open_with(make: impl FnOnce(&Path) -> CoreLoop) -> Self {
        let wal_dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&wal_dir.path().join("wal")).expect("open wal");
        let store_dir = tempfile::tempdir().expect("tempdir");
        let core = make(store_dir.path());
        Self {
            wal,
            wal_dir,
            store_dir,
            core,
        }
    }

    /// Append the origin of `plan`'s group, as the funnel does before
    /// dispatch.
    fn append_origin(&self, plan: &PhysicalPlan) -> GroupOrigin {
        append_group_origin(WalAppendRequest {
            wal: self.wal.appender(NO_APPLY_KEY),
            event_source: crate::event::EventSource::User,
            tenant_id: TenantId::new(TID),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan,
            credentials: None,
            now_override: None,
        })
        .expect("origin")
        .origin
    }

    /// Run `plan` through every step of the funnel with no crash.
    fn committed(&mut self, plan: PhysicalPlan) {
        let origin = self.append_origin(&plan);
        let response = self
            .core
            .execute(&ExecutionTask::new(request(plan, origin)));
        assert_eq!(response.status, Status::Ok, "{response:?}");
        append_group_parts(
            self.wal
                .appender(NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::User),
            WriteSetTarget {
                tenant_id: TenantId::new(TID),
                vshard_id: VShardId::new(0),
                database_id: DatabaseId::DEFAULT,
                collection: ORDERS,
                origin,
            },
            &response.write_set,
        )
        .expect("parts");
        self.wal.sync().expect("sync");
    }

    /// Append `plan`'s origin and run it journalled on the core. With
    /// `stored`, the core stores its write set. The parts are never
    /// appended: the crash comes first.
    fn journalled_until_crash(&mut self, plan: PhysicalPlan, stored: bool) -> GroupOrigin {
        self.journalled_in(ORDERS, plan, stored)
    }

    /// [`Self::journalled_until_crash`] for a write whose group falls back
    /// to `collection`.
    fn journalled_in(&mut self, collection: &str, plan: PhysicalPlan, stored: bool) -> GroupOrigin {
        let origin = self.append_origin(&plan);
        let task = ExecutionTask::new(request(plan, origin));
        self.core.begin_journalled();
        let response: Response = self.core.execute(&task);
        assert_eq!(response.status, Status::Ok, "{response:?}");
        if stored {
            let group = JournalGroup {
                origin: origin.lsn,
                collection: collection.to_string(),
                apply_key: NO_APPLY_KEY,
                commit_hlc: None,
                change_position: None,
            };
            self.core.finish_journalled_task(&task, group, &response);
        }
        self.wal.sync().expect("sync");
        origin
    }

    /// The core's stores as a power loss at this instant leaves them.
    fn crash_image(&self) -> tempfile::TempDir {
        let image = tempfile::tempdir().expect("tempdir");
        for store in ["sparse/core-0.redb", "graph/core-0.redb"] {
            let from = self.store_dir.path().join(store);
            let to = image.path().join(store);
            std::fs::create_dir_all(to.parent().expect("parent")).expect("mkdir");
            std::fs::copy(&from, &to).expect("copy store");
        }
        image
    }

    /// The WAL as a power loss at this instant leaves it.
    fn wal_image(&self) -> tempfile::TempDir {
        self.wal.sync().expect("sync");
        let image = tempfile::tempdir().expect("tempdir");
        copy_dir(&self.wal_dir.path().join("wal"), &image.path().join("wal"));
        image
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("mkdir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy file");
        }
    }
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
        admission: Admission::Admitted,
    }
}

fn note_doc(note: &str) -> Vec<u8> {
    let mut obj = HashMap::new();
    obj.insert("note".to_string(), Value::String(note.into()));
    nodedb_types::value_to_msgpack(&Value::Object(obj)).expect("encode")
}

fn put(note: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointPut {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, ORDERS),
        document_id: "o1".to_string(),
        value: note_doc(note),
        surrogate: Surrogate::new(1),
        pk_bytes: b"o1".to_vec(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    })
}

fn update(note: &str) -> PhysicalPlan {
    let literal = nodedb_types::value_to_msgpack(&Value::String(note.into())).expect("encode");
    PhysicalPlan::Document(DocumentOp::PointUpdate {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, ORDERS),
        document_id: "o1".to_string(),
        surrogate: Some(Surrogate::new(1)),
        pk_bytes: b"o1".to_vec(),
        updates: vec![("note".to_string(), UpdateValue::Literal(literal))],
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
        declared_primary_key: None,
    })
}

/// Settle `wal` against the write sets the stores at `stores` hold, as boot
/// does, and return the stream replay then reads.
fn settle(wal: &WalManager, stores: &Path) -> Vec<WalRecord> {
    let records = wal.replay().expect("replay");
    {
        let stored = StoredWriteSets::read(stores).expect("read write sets");
        settle_against(wal, &records, &stored.captures).expect("settle");
        stored.clear().expect("clear");
    }
    wal.sync().expect("sync");
    wal.replay().expect("replay")
}

/// The `note` of every stored row, after replaying `records` onto the stores
/// at `dir`.
fn notes_after_replay(dir: &Path, records: &[WalRecord]) -> Vec<String> {
    let (mut core, _req, _resp) = make_core_with_dir(dir);
    core.replay_all_wal(records, 1, &TombstoneSet::new())
        .expect("replay");
    core.sparse
        .scan_all_for_tenant(DatabaseId::DEFAULT.as_u64(), TID)
        .expect("rows")
        .into_iter()
        .filter_map(|(_, bytes)| {
            let (start, _) = nodedb_query::msgpack_scan::extract_field(&bytes, 0, "note")?;
            nodedb_query::msgpack_scan::read_str(&bytes, start).map(str::to_string)
        })
        .collect()
}

/// Restart replay, a restore onto an empty base, and the settled WAL agree.
fn assert_views_agree(stores: &Path, records: &[WalRecord], note: &str) {
    let base = tempfile::tempdir().expect("tempdir");
    assert_eq!(
        notes_after_replay(stores, records),
        vec![note.to_string()],
        "restart state"
    );
    assert_eq!(
        notes_after_replay(base.path(), records),
        vec![note.to_string()],
        "point-in-time restore"
    );
    let present: std::collections::HashSet<u64> =
        records.iter().map(|record| record.header.lsn).collect();
    assert!(
        membership(records)
            .broken_through(u64::MAX)
            .iter()
            .all(|lsn| !present.contains(lsn)),
        "the event stream holds no origin or part of a broken group"
    );
}

/// The record groups of `records`.
fn membership(records: &[WalRecord]) -> GroupMembership {
    let mut groups = GroupMembership::default();
    for record in records {
        if nodedb_wal::record::RecordType::from_raw(record.logical_record_type())
            == Some(nodedb_wal::record::RecordType::WriteGroup)
        {
            let group = crate::wal::WriteGroupRecord::from_bytes(&record.payload)
                .expect("group")
                .group;
            groups.observe(record.header.lsn, group);
        }
    }
    groups
}

/// A crash after apply and before the write set is stored rolls the
/// update's effects back. Boot cancels its group, so every view keeps the
/// row as it stood before.
#[test]
fn a_crash_before_the_write_set_is_stored_drops_the_write_everywhere() {
    let mut live = Live::open();
    live.committed(put("old"));
    let origin = live.journalled_until_crash(update("new"), false);
    let stores = live.crash_image();

    let records = settle(&live.wal, stores.path());
    assert!(
        !records.iter().any(|r| r.header.lsn == origin.lsn.as_u64()),
        "the update's origin is cancelled"
    );
    assert_views_agree(stores.path(), &records, "old");
}

/// A crash after the write set is stored and before its parts are journalled
/// keeps the update's effects. Boot journals the parts from the stored write
/// set, so every view holds the update.
#[test]
fn a_crash_after_the_write_set_is_stored_keeps_the_write_everywhere() {
    let mut live = Live::open();
    live.committed(put("old"));
    let origin = live.journalled_until_crash(update("new"), true);
    let stores = live.crash_image();

    let records = settle(&live.wal, stores.path());
    assert!(
        !membership(&records)
            .broken_through(u64::MAX)
            .contains(&origin.lsn.as_u64()),
        "the update's group is whole"
    );
    assert_views_agree(stores.path(), &records, "new");
    assert!(
        StoredWriteSets::read(stores.path())
            .expect("read")
            .captures
            .is_empty(),
        "boot drops the settled write sets"
    );
}

/// A crash that cuts the update's origin from the WAL after its write set
/// is stored keeps the update's durable effects. Boot journals the origin
/// again from the stored write set, then its parts.
#[test]
fn a_crash_that_cuts_the_origin_journals_the_write_again() {
    let mut live = Live::open();
    live.committed(put("old"));
    let cut = live.wal_image();
    live.journalled_until_crash(update("new"), true);
    let stores = live.crash_image();

    let cut_wal = WalManager::open_for_testing(&cut.path().join("wal")).expect("open cut wal");
    let before = cut_wal.next_lsn();
    let records = settle(&cut_wal, stores.path());
    assert!(
        records.iter().any(|r| r.header.lsn >= before.as_u64()),
        "boot journals the cut write"
    );
    assert_views_agree(stores.path(), &records, "new");
}

/// A put of row `id` with `note`.
fn put_row(id: &str, surrogate: u32, note: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointPut {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, ORDERS),
        document_id: id.to_string(),
        value: note_doc(note),
        surrogate: Surrogate::new(surrogate),
        pk_bytes: id.as_bytes().to_vec(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    })
}

/// The target row `t` of another collection, as a sum fold stores it.
fn target_row(note: &str) -> WriteSetEntry {
    WriteSetEntry::put(100, RowIdentity::from_user_key("t"), note_doc(note))
        .in_collection(TOTALS.to_string())
}

/// Write A folds the target row, then crashes before its parts. A later
/// write B of another row folds the same target row and journals it. Boot
/// journals A's parts at the WAL tail, after B's. Replay orders every part
/// by its write's origin, so the target row keeps B's later image.
#[test]
fn a_part_boot_journals_never_regresses_a_later_write_of_its_row() {
    let live = Live::open();
    let plan_a = put_row("a", 1, "a");
    let a = live.append_origin(&plan_a);
    let b = live.append_origin(&put_row("b", 2, "b"));
    append_group_parts(
        live.wal
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User),
        WriteSetTarget {
            tenant_id: TenantId::new(TID),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            collection: ORDERS,
            origin: b,
        },
        &[target_row("fresh")],
    )
    .expect("parts of b");
    live.wal.sync().expect("sync");

    // A's write set as the core stores it: the fold of the target row.
    let task_a = ExecutionTask::new(request(plan_a, a));
    let mut response_a = live.core.response_ok(&task_a);
    response_a.write_set = vec![target_row("stale")];
    let group_a = JournalGroup {
        origin: a.lsn,
        collection: ORDERS.to_string(),
        apply_key: NO_APPLY_KEY,
        commit_hlc: None,
        change_position: None,
    };
    let stored_a = live
        .core
        .write_set_capture(&task_a, group_a, &response_a)
        .expect("capture a");
    let records = live.wal.replay().expect("replay");
    assert!(settle_against(&live.wal, &records, &[stored_a]).expect("settle"));
    live.wal.sync().expect("sync");
    let records = live.wal.replay().expect("replay");
    let tail = records.iter().map(|r| r.header.lsn).max().unwrap_or(0);
    assert!(tail > b.lsn.as_u64(), "a's parts sit after b's");

    let base = tempfile::tempdir().expect("tempdir");
    let notes = notes_after_replay(base.path(), &records);
    assert!(notes.contains(&"fresh".to_string()), "{notes:?}");
    assert!(!notes.contains(&"stale".to_string()), "{notes:?}");
}

/// A write set stored by a clean run settles as whole: boot appends nothing
/// for a group whose parts the WAL holds.
#[test]
fn a_settled_group_needs_nothing_at_boot() {
    let mut live = Live::open();
    live.committed(put("old"));
    let records = live.wal.replay().expect("replay");
    let stores = live.crash_image();
    let stored = StoredWriteSets::read(stores.path()).expect("read");
    assert!(
        !settle_against(&live.wal, &records, &stored.captures).expect("settle"),
        "a whole WAL needs no record"
    );
}

/// The stamped install of the Calvin slice at `(3, 0)` on vShard 0. Its
/// record writes one source row, and its sum target folds 25 into the
/// target's balance.
fn calvin_install() -> PhysicalPlan {
    PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo: calvin_redo(),
        collections: vec![SOURCE.to_string()],
        sum_targets: sum_targets(),
        origin: RedoOrigin::Commit,
        calvin: Some(CalvinInstall {
            epoch: 3,
            position: 0,
            epoch_system_ms: 0,
            reply: CalvinReplySpec::Count(Vec::new()),
            user_write: true,
        }),
    })
}

/// The target's balance after replaying `records` onto the stores at `dir`,
/// on a core that declares the sum.
fn balance_after_replay(dir: &Path, records: &[WalRecord]) -> Option<String> {
    let (mut core, _req, _resp) = make_core_with_dir(dir);
    configure_sum(&mut core);
    core.replay_all_wal(records, 1, &TombstoneSet::new())
        .expect("replay");
    balance(&core)
}

/// The Calvin positions boot recovery finds applied on vShard 0 in `wal`.
fn recovered_positions(wal: &WalManager) -> BTreeSet<(u64, u32)> {
    let catalog = SystemCatalog::open_in_memory().expect("in-memory catalog");
    recover_all_applied(wal, &catalog, &|_| None)
        .expect("recover applied positions")
        .remove(&0)
        .map(|recovered| recovered.applied_tail)
        .unwrap_or_default()
}

/// A crash cuts the origin of a stamped Calvin install from the WAL after
/// the install folded its sum and stored its write set. Boot journals the
/// stamped record again, then the fold rows as its parts. Recovery finds
/// the position applied, and replay keeps the single-apply total.
#[test]
fn a_crash_that_cuts_a_calvin_redo_origin_journals_it_again() {
    let mut live = Live::open_with(|dir| core_with_sum(dir).0);
    let cut = live.wal_image();
    live.journalled_in(SOURCE, calvin_install(), true);
    assert_eq!(balance(&live.core).as_deref(), Some("125"), "the live fold");
    let stores = live.crash_image();

    let cut_wal = WalManager::open_for_testing(&cut.path().join("wal")).expect("open cut wal");
    assert!(
        recovered_positions(&cut_wal).is_empty(),
        "the crash cuts the stamped record"
    );
    let records = settle(&cut_wal, stores.path());
    assert!(
        recovered_positions(&cut_wal).contains(&(3, 0)),
        "recovery finds the stamp of the record boot journals"
    );
    assert!(
        membership(&records).broken_through(u64::MAX).is_empty(),
        "the journalled record's group is whole"
    );
    assert_eq!(
        balance_after_replay(stores.path(), &records).as_deref(),
        Some("125"),
        "restart replay onto the cut stores keeps the single-apply total"
    );
    let base = tempfile::tempdir().expect("tempdir");
    assert_eq!(
        balance_after_replay(base.path(), &records).as_deref(),
        Some("125"),
        "a point-in-time restore reaches the single-apply total"
    );
}
