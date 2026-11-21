// SPDX-License-Identifier: BUSL-1.1

//! Restart replay judges a committed record whose install has no outcome,
//! as its live install would have.
//!
//! Each test appends committed records as the write funnel does, and no
//! core installs them: the WAL a crash leaves between a record's append and
//! its install's verdict. A stream marker's validate writes nothing
//! durable, so a crash while it validates, parked between its batches or
//! not, leaves the same WAL and stores. Each test then settles and boots as
//! boot does.

use nodedb_physical::physical_plan::{
    CalvinInstall, CalvinReplySpec, MetaOp, PhysicalPlan, RedoOrigin, RedoPins,
};
use nodedb_types::{StorageKey, Surrogate};
use nodedb_wal::record::RecordType;
use nodedb_wal::{TombstoneSet, WalRecord};

use crate::bootstrap::write_group_settle::close_judged_redos;
use crate::bootstrap::write_group_settle::settle::SettleOutcome;
use crate::control::distributed_applier::ProposalLedger;
use crate::control::server::wal_dispatch::{WalAppendRequest, append_group_origin, wal_append};
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::document::store::DocumentEngine;
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::NO_APPLY_KEY;
use crate::wal::{BootVerdict, BootVerdictOutcome, RedoSubRecord};

use super::super::replay_fold::tests::settle;
use super::super::stream_validate::tests::{StreamWal, TID, framed_pinned, record};
use super::super::test_commit::test_stamp;
use super::super::unique_handover_tests::{COLL, put, unique_core};

/// The apply key of the data-group entry each record applies.
const KEY: u64 = 0xB007;

/// A node's stores and WAL, with no core open.
pub(super) struct Node {
    dir: tempfile::TempDir,
    wal: StreamWal,
}

pub(super) fn node() -> Node {
    let dir = tempfile::tempdir().expect("tempdir");
    let (core, req, resp) = unique_core(dir.path());
    let wal = StreamWal::open(&core);
    drop((core, req, resp));
    Node { dir, wal }
}

/// A whole committed record of `ops`, whose transaction read at
/// `snapshot`.
fn whole(ops: Vec<RedoSubRecord>, snapshot: Option<i64>) -> PhysicalPlan {
    whole_pinned(ops, snapshot, &RedoPins::default())
}

/// [`whole`] whose plan carries `pins`, which the WAL copy then holds.
pub(super) fn whole_pinned(
    ops: Vec<RedoSubRecord>,
    snapshot: Option<i64>,
    pins: &RedoPins,
) -> PhysicalPlan {
    PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo: record(ops).to_bytes().expect("encode redo"),
        collections: vec![COLL.to_string()],
        sum_targets: Vec::new(),
        origin: RedoOrigin::Commit,
        calvin: None,
        reads: Vec::new(),
        snapshot_ordinal: snapshot,
        pins: pins.clone(),
    })
}

/// A stream marker of `ops`, its content in the node's WAL.
fn marker(node: &mut Node, ops: Vec<RedoSubRecord>) -> PhysicalPlan {
    marker_pinned(node, ops, &RedoPins::default())
}

/// [`marker`] whose content header carries `pins`.
pub(super) fn marker_pinned(
    node: &mut Node,
    ops: Vec<RedoSubRecord>,
    pins: &RedoPins,
) -> PhysicalPlan {
    let reference = node.wal.write(&framed_pinned(&record(ops), &[], &[], pins));
    let marker = reference.marker(None, None, Vec::new()).expect("marker");
    PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo: marker.to_bytes().expect("encode marker"),
        collections: vec![COLL.to_string()],
        sum_targets: Vec::new(),
        origin: RedoOrigin::Commit,
        calvin: None,
        reads: Vec::new(),
        snapshot_ordinal: None,
        pins: Default::default(),
    })
}

/// Append `plan`'s records as the funnel does before it dispatches, and
/// make them durable. Returns the record's LSN.
pub(super) fn append(node: &Node, plan: &PhysicalPlan) -> u64 {
    let origin = append_group_origin(WalAppendRequest {
        wal: node.wal.manager().appender(KEY),
        event_source: crate::event::EventSource::User,
        tenant_id: TenantId::new(TID),
        vshard_id: VShardId::new(0),
        database_id: DatabaseId::DEFAULT,
        plan,
        credentials: None,
        now_override: None,
    })
    .expect("append")
    .origin;
    node.wal.manager().sync().expect("sync");
    origin.lsn.as_u64()
}

/// Append the index step `plan` as its own record, as a live index step
/// journals it, and make it durable. Returns the record's LSN.
pub(super) fn append_index_step(node: &Node, plan: &PhysicalPlan) -> u64 {
    let lsn = wal_append(WalAppendRequest {
        wal: node
            .wal
            .manager()
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User),
        event_source: crate::event::EventSource::User,
        tenant_id: TenantId::new(TID),
        vshard_id: VShardId::new(0),
        database_id: DatabaseId::DEFAULT,
        plan,
        credentials: None,
        now_override: None,
    })
    .expect("index step append")
    .lsn
    .expect("an index step journals its own record");
    node.wal.manager().sync().expect("sync");
    lsn.as_u64()
}

/// A booted core and the verdicts its replay reached.
pub(super) struct Booted {
    pub(super) core: CoreLoop,
    _ends: Box<dyn std::any::Any>,
    pub(super) verdicts: Vec<BootVerdict>,
}

/// Builds the core a boot opens over a directory of stores.
pub(super) type MakeCore =
    fn(&std::path::Path) -> (CoreLoop, Box<dyn std::any::Any>, Box<dyn std::any::Any>);

/// Boot a core over the node's stores, replaying `records` with the
/// settle's verdicts.
fn boot(node: &Node, outcome: &SettleOutcome, records: &[WalRecord]) -> Booted {
    boot_with(node, outcome, records, unique_core)
}

/// [`boot`] of the core `make_core` builds.
fn boot_with(
    node: &Node,
    outcome: &SettleOutcome,
    records: &[WalRecord],
    make_core: MakeCore,
) -> Booted {
    let (mut core, req, resp) = make_core(node.dir.path());
    core.set_never_durable_redos(&outcome.never_durable)
        .expect("fold ledger");
    core.replay_all_wal(records, 1, &TombstoneSet::new())
        .expect("replay");
    let verdicts = core.take_boot_verdicts();
    Booted {
        core,
        _ends: Box::new((req, resp)),
        verdicts,
    }
}

/// Settle and boot, then write the verdicts to the WAL as boot does.
fn boot_and_close(node: &Node) -> Booted {
    boot_and_close_with(node, unique_core)
}

/// [`boot_and_close`] of the core `make_core` builds.
pub(super) fn boot_and_close_with(node: &Node, make_core: MakeCore) -> Booted {
    let (outcome, records) = settle(node.wal.manager(), node.dir.path());
    let booted = boot_with(node, &outcome, &records, make_core);
    close_judged_redos(node.wal.manager(), &booted.verdicts, &[]).expect("close judged");
    booted
}

pub(super) fn outcome_of(verdicts: &[BootVerdict], lsn: u64) -> Option<BootVerdictOutcome> {
    verdicts
        .iter()
        .find(|verdict| verdict.judged.lsn == lsn)
        .map(|verdict| verdict.outcome)
}

/// The surrogates the unique index names for `code`.
fn owners(core: &CoreLoop, code: &str) -> Vec<u32> {
    let mut owners: Vec<u32> = DocumentEngine::new(&core.sparse, 0, TID)
        .index_lookup(COLL, "code", code, None)
        .expect("index lookup")
        .iter()
        .map(|key| key.surrogate().as_u32())
        .collect();
    owners.sort_unstable();
    owners
}

/// Whether the row of `surrogate` is stored.
pub(super) fn stored(core: &CoreLoop, surrogate: u32) -> bool {
    core.current_row_body(
        0,
        TID,
        COLL,
        &StorageKey::for_surrogate(Surrogate::new(surrogate)),
    )
    .expect("read row")
    .is_some()
}

fn present(node: &Node, lsn: u64) -> bool {
    node.wal
        .manager()
        .replay()
        .expect("replay")
        .iter()
        .any(|record| record.header.lsn == lsn)
}

/// A record whose snapshot an earlier record of the WAL moved is refused
/// at its place: the earlier record replays first, and the record's write
/// holds a version above its snapshot. Nothing of it applies, its records
/// are cancelled, and a second boot keeps it cancelled.
#[test]
fn a_record_whose_snapshot_moved_is_refused_at_boot() {
    let node = node();
    let snapshot = test_stamp();
    let first = append(&node, &whole(vec![put(1, "A")], None));
    let moved = append(&node, &whole(vec![put(1, "B")], Some(snapshot)));

    let booted = boot_and_close(&node);
    assert_eq!(
        outcome_of(&booted.verdicts, first),
        Some(BootVerdictOutcome::Passed)
    );
    assert_eq!(
        outcome_of(&booted.verdicts, moved),
        Some(BootVerdictOutcome::Refused {
            final_refusal: false
        })
    );
    assert_eq!(owners(&booted.core, "A"), vec![1], "the earlier row stands");
    assert!(
        owners(&booted.core, "B").is_empty(),
        "nothing of it applied"
    );
    drop(booted);
    assert!(!present(&node, moved), "its record is cancelled");

    let again = boot_and_close(&node);
    assert!(again.verdicts.is_empty(), "the WAL settled both records");
    assert_eq!(owners(&again.core, "A"), vec![1]);
    assert!(owners(&again.core, "B").is_empty());
}

/// The same record with a snapshot above the earlier write passes and
/// applies once.
#[test]
fn a_record_whose_snapshot_holds_applies_once() {
    let node = node();
    let first = append(&node, &whole(vec![put(1, "A")], None));
    let snapshot = test_stamp();
    let later = append(&node, &whole(vec![put(1, "B")], Some(snapshot)));

    let booted = boot_and_close(&node);
    assert_eq!(
        outcome_of(&booted.verdicts, first),
        Some(BootVerdictOutcome::Passed)
    );
    assert_eq!(
        outcome_of(&booted.verdicts, later),
        Some(BootVerdictOutcome::Passed)
    );
    assert_eq!(owners(&booted.core, "B"), vec![1]);
    drop(booted);

    let again = boot_and_close(&node);
    assert!(again.verdicts.is_empty(), "each passed group closed");
    assert_eq!(owners(&again.core, "B"), vec![1]);
    assert!(owners(&again.core, "A").is_empty());
}

/// A record that claims a unique value an earlier record of the same WAL
/// holds is refused for good. Its markers carry the entry's key, so the
/// proposal ledger counts the refusal as the entry's outcome.
#[test]
fn a_unique_conflict_with_an_earlier_record_is_refused() {
    let node = node();
    append(&node, &whole(vec![put(1, "A")], None));
    let clash = append(&node, &whole(vec![put(2, "A")], None));

    let booted = boot_and_close(&node);
    assert_eq!(
        outcome_of(&booted.verdicts, clash),
        Some(BootVerdictOutcome::Refused {
            final_refusal: true
        })
    );
    assert_eq!(owners(&booted.core, "A"), vec![1]);
    assert!(!stored(&booted.core, 2), "nothing of it applied");
    drop(booted);

    let records = node.wal.manager().replay().expect("replay");
    assert!(!records.iter().any(|record| record.header.lsn == clash));
    let ledger = ProposalLedger::from_records(&records, 8);
    assert!(ledger.prior(KEY).is_some());
}

/// A crash after boot judged a record and before its verdict reached the
/// WAL leaves the record unsettled. The next boot reads the stored verdict
/// and applies nothing of the refused record, though the earlier record's
/// row, which replay wrote, stands.
#[test]
fn a_stored_verdict_decides_the_next_boot() {
    let node = node();
    append(&node, &whole(vec![put(1, "A")], None));
    let clash = append(&node, &whole(vec![put(2, "A")], None));

    let (outcome, records) = settle(node.wal.manager(), node.dir.path());
    let crashed = boot(&node, &outcome, &records);
    assert_eq!(
        outcome_of(&crashed.verdicts, clash),
        Some(BootVerdictOutcome::Refused {
            final_refusal: true
        })
    );
    drop(crashed);
    assert!(present(&node, clash), "the crash came before the cancel");

    let booted = boot_and_close(&node);
    assert_eq!(
        outcome_of(&booted.verdicts, clash),
        Some(BootVerdictOutcome::Refused {
            final_refusal: true
        })
    );
    assert!(!stored(&booted.core, 2));
    drop(booted);
    assert!(!present(&node, clash));
}

/// A stream marker whose content claims a unique value an earlier record
/// holds is refused at boot, and a marker whose content passes applies
/// exactly once, across two boots.
#[test]
fn a_stream_marker_is_judged_at_boot() {
    let mut node = node();
    append(&node, &whole(vec![put(1, "A")], None));
    let clash_plan = marker(&mut node, vec![put(2, "A")]);
    let clash = append(&node, &clash_plan);
    let fresh_plan = marker(&mut node, vec![put(3, "C")]);
    let fresh = append(&node, &fresh_plan);

    let booted = boot_and_close(&node);
    assert_eq!(
        outcome_of(&booted.verdicts, clash),
        Some(BootVerdictOutcome::Refused {
            final_refusal: true
        })
    );
    assert_eq!(
        outcome_of(&booted.verdicts, fresh),
        Some(BootVerdictOutcome::Passed)
    );
    assert!(!stored(&booted.core, 2));
    assert_eq!(owners(&booted.core, "C"), vec![3]);
    drop(booted);

    let again = boot_and_close(&node);
    assert!(again.verdicts.is_empty());
    assert_eq!(owners(&again.core, "C"), vec![3], "applied once");
    assert!(!stored(&again.core, 2));
}

/// A Calvin slice's stage vote validated it, and its transaction committed
/// on every shard it writes. Its WAL copy carries no boundary, boot judges
/// it not, and replay applies it as before.
#[test]
fn a_calvin_slice_is_not_judged() {
    let node = node();
    // A Calvin slice's record carries the slice's stamp, as every Calvin
    // payload does.
    let mut slice = record(vec![put(1, "A")]);
    slice.calvin_stamp = Some(crate::wal::CalvinStamp {
        epoch: 1,
        position: 0,
        vshard_id: 0,
        mirror_database: None,
    });
    let calvin = PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo: slice.to_bytes().expect("encode redo"),
        collections: vec![COLL.to_string()],
        sum_targets: Vec::new(),
        origin: RedoOrigin::Commit,
        calvin: Some(CalvinInstall {
            epoch: 1,
            position: 0,
            epoch_system_ms: 1,
            reply: CalvinReplySpec::Frozen,
            user_write: true,
            mirror: false,
        }),
        reads: Vec::new(),
        snapshot_ordinal: None,
        pins: Default::default(),
    });
    let lsn = append(&node, &calvin);
    let records = node.wal.manager().replay().expect("replay");
    assert!(
        !records
            .iter()
            .any(|record| RecordType::from_raw(record.logical_record_type())
                == Some(RecordType::RedoBoundary)),
        "a Calvin slice carries no boundary"
    );
    let (outcome, records) = settle(node.wal.manager(), node.dir.path());
    assert!(outcome.never_durable.judged(lsn).is_none());
    let booted = boot(&node, &outcome, &records);
    assert!(booted.verdicts.is_empty());
    assert_eq!(owners(&booted.core, "A"), vec![1]);
}

/// A verdict decides its record whichever core stored it: after a change
/// of the core count, the record's new core finds the verdict an old core
/// stored and validates nothing.
#[test]
fn a_verdict_another_core_stored_decides_the_record() {
    let node = node();
    let lsn = append(&node, &whole(vec![put(1, "A")], None));
    {
        let path = crate::data::executor::snapshot::layout::sparse_store_path(node.dir.path(), 1);
        let store = crate::engine::sparse::btree::SparseEngine::open(&path).expect("open store");
        let refused = crate::wal::redo::StoredVerdict {
            lsn,
            outcome: BootVerdictOutcome::Refused {
                final_refusal: true,
            },
        };
        store
            .put_boot_verdicts(&[(lsn, refused.to_bytes().expect("encode"))])
            .expect("store verdict");
    }

    let booted = boot_and_close(&node);
    assert_eq!(
        outcome_of(&booted.verdicts, lsn),
        Some(BootVerdictOutcome::Refused {
            final_refusal: true
        }),
        "the stored verdict decides, though the record would pass"
    );
    assert!(owners(&booted.core, "A").is_empty());
}

/// Verdicts with no write between them share one durable commit. Two
/// refused records after a passed one take two commits: one before replay
/// writes the passed record, one for both refusals.
#[test]
fn verdicts_with_no_write_between_them_share_a_commit() {
    let node = node();
    let base = append(&node, &whole(vec![put(1, "A")], None));
    let first = append(&node, &whole(vec![put(2, "A")], None));
    let second = append(&node, &whole(vec![put(3, "A")], None));

    let booted = boot_and_close(&node);
    assert_eq!(
        outcome_of(&booted.verdicts, base),
        Some(BootVerdictOutcome::Passed)
    );
    for refused in [first, second] {
        assert_eq!(
            outcome_of(&booted.verdicts, refused),
            Some(BootVerdictOutcome::Refused {
                final_refusal: true
            })
        );
    }
    assert_eq!(booted.core.redo_apply.boot_verdict_commits, 2);
}

/// A stream marker a batched install left a cursor for passed its validate
/// live. Boot stores its verdict before replay completes it and drops the
/// cursor, so a crash before its group closes leaves the next boot the
/// verdict, and no second validate.
#[test]
fn a_record_with_an_install_cursor_stores_its_verdict() {
    use crate::wal::redo::install_cursor::{CursorState, InstallCursor};
    let mut node = node();
    let plan = marker(&mut node, vec![put(3, "C")]);
    let PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { redo, .. }) = &plan else {
        panic!("a redo plan");
    };
    let reference = crate::wal::RedoStreamRef::of_marker(
        &crate::wal::RedoRecord::from_bytes(redo).expect("decode marker"),
    )
    .expect("reference")
    .expect("a marker");
    let lsn = append(&node, &plan);
    {
        let path = crate::data::executor::snapshot::layout::sparse_store_path(node.dir.path(), 0);
        let store = crate::engine::sparse::btree::SparseEngine::open(&path).expect("open store");
        let cursor = InstallCursor {
            lsn,
            stream: reference.stream,
            state: CursorState::Installing {
                arm: crate::wal::redo::replay_arm::RedoArm::Document,
                batch: 0,
                ops_through: 1,
            },
        };
        let txn = store.begin_durable_write().expect("begin");
        store
            .put_install_cursor_in_txn(&txn, lsn, &cursor.to_bytes().expect("encode cursor"))
            .expect("store cursor");
        txn.commit().expect("commit");
    }

    let (outcome, records) = settle(node.wal.manager(), node.dir.path());
    let booted = boot(&node, &outcome, &records);
    assert_eq!(
        outcome_of(&booted.verdicts, lsn),
        Some(BootVerdictOutcome::Passed)
    );
    let stored = booted
        .core
        .sparse
        .load_boot_verdicts()
        .expect("load verdicts");
    assert!(
        stored.iter().any(|(at, _)| *at == lsn),
        "the verdict is stored though the cursor dropped"
    );
    assert_eq!(owners(&booted.core, "C"), vec![3]);
}
