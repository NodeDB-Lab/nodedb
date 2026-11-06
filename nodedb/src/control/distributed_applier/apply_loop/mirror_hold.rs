// SPDX-License-Identifier: BUSL-1.1

//! Hold a mirror's entry until the metadata log decides its cut.
//!
//! A mirror's applier commits a cut's data entries first, then proposes the
//! `MirrorCut` that installs the cut's schema. So a replica reaches a data entry
//! before its metadata group raised the cut. The hold runs before the entry is
//! prepared: a Calvin claim, a key check and a stream take happen once the
//! verdict is known, and a skipped entry touches none of them.
//!
//! A waiting entry stays at the front of its group's backlog, so the group's
//! later entries wait behind it in log order. Other groups never wait, and
//! its group's running entries still finish. The pipeline pumps the group again
//! once the cut watch moves.
//!
//! The hold stops a shared data group for every database in it. The stop lasts
//! from the cut's first data entry to the `MirrorCut` apply. A new applier's
//! fence ends it when the former applier died inside a cut.

use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_types::{DatabaseId, MirrorEntryStamp};

use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::distributed_applier::propose_tracker::AppliedWrite;
use crate::control::mirror::MirrorVerdict;
use crate::control::state::SharedState;
use crate::control::wal_replication::ReplicatedWrite;
use crate::control::wal_replication::types::MirrorCalvinFloor;

use super::context::ApplyContext;
use super::lane::QueuedEntry;
use super::proposal_gate::EntryOutcome;

/// What the front entry of a group does now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MirrorTurn {
    /// The entry starts as any entry does.
    Proceed,
    /// The entry stays at the front of its group's backlog.
    Wait,
    /// The entry concludes with no effect, on every replica.
    Skip,
}

/// Decide the front entry of a group.
///
/// The entry's metadata floor comes first. It names a catalog that holds the
/// mirror, so the verdict tells a dropped database from one this node has not
/// created yet.
pub(super) fn mirror_turn(state: &SharedState, queued: &QueuedEntry) -> MirrorTurn {
    let Some(entry) = queued.decoded.as_ref() else {
        return MirrorTurn::Proceed;
    };
    let Some(stamp) = entry.mirror else {
        return MirrorTurn::Proceed;
    };
    if state.applied_index_watcher(METADATA_GROUP_ID).current() < entry.metadata_floor {
        return MirrorTurn::Wait;
    }
    if matches!(entry.write, ReplicatedWrite::MirrorSeal { last: true, .. }) {
        return MirrorTurn::Proceed;
    }
    let database = DatabaseId::new(entry.database_id);
    match state.mirror.cuts.verdict(database, stamp.cut, stamp.epoch) {
        MirrorVerdict::Apply => MirrorTurn::Proceed,
        MirrorVerdict::Wait => MirrorTurn::Wait,
        MirrorVerdict::Skip => MirrorTurn::Skip,
    }
}

/// Apply a mirror's seal: every earlier entry of the group concluded, so the
/// group applied the stamp's cut. The seal also folds the source's fully
/// applied Calvin epochs, which bounds the mirror's ledgers.
///
/// A seal with no stamp names no cut, so it records nothing on every replica.
pub(super) fn apply_mirror_seal(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    database_id: u64,
    stamp: Option<MirrorEntryStamp>,
    calvin_floors: &[MirrorCalvinFloor],
) -> EntryOutcome {
    if let Some(stamp) = stamp {
        for floor in calvin_floors {
            ctx.state
                .calvin
                .applied
                .get_or_create_mirror(database_id, floor.vshard)
                .fold(floor.epoch);
        }
        ctx.state
            .mirror
            .cuts
            .note_seal(DatabaseId::new(database_id), pos.group_id, stamp.cut);
    } else {
        tracing::warn!(
            group_id = pos.group_id,
            log_index = pos.log_index,
            database_id,
            "a mirror seal carries no stamp and records nothing"
        );
    }
    ctx.tracker.complete(
        pos.group_id,
        pos.log_index,
        pos.applied_key,
        Ok(AppliedWrite::new(Vec::new())),
    );
    EntryOutcome::Applied {
        durable: true,
        result: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_raft::message::LogEntry;
    use nodedb_types::{MirrorCutAdvance, MirrorCutState, MirrorStatus};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::distributed_applier::applier::ApplyBatch;
    use crate::control::distributed_applier::proposal_ledger::{
        PROPOSAL_LEDGER_CAPACITY, ProposalLedger,
    };
    use crate::control::distributed_applier::propose_tracker::ProposeTracker;
    use crate::control::mirror::test_fixture::mirror_descriptor;
    use crate::control::wal_replication::ReplicatedEntry;
    use crate::wal::WalManager;

    use super::super::pipeline::Pipeline;

    const DB: DatabaseId = DatabaseId::new(1300);
    const GROUP: u64 = 4;

    fn state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("state");
        state.mirror.cuts.apply_descriptor(&mirror_descriptor(
            DB.as_u64(),
            "mirror_db",
            MirrorStatus::Following,
        ));
        (state, dir)
    }

    fn raise(state: &SharedState, cut: u64, epoch: u64) {
        let next = MirrorCutState::default()
            .advanced(&MirrorCutAdvance {
                cut,
                epoch,
                ..MirrorCutAdvance::default()
            })
            .expect("raises");
        state.mirror.cuts.apply_state(DB, &next);
    }

    fn seal(index: u64, cut: u64, epoch: u64, last: bool) -> LogEntry {
        let mut entry = ReplicatedEntry::new(
            1,
            DB.as_u64(),
            2,
            ReplicatedWrite::MirrorSeal {
                last,
                calvin_floors: vec![MirrorCalvinFloor {
                    vshard: 2,
                    epoch: 7,
                }],
            },
        );
        entry.mirror = Some(MirrorEntryStamp {
            source_group: 0,
            source_index: 0,
            cut,
            epoch,
        });
        LogEntry {
            term: 1,
            index,
            data: entry.encode().expect("encode"),
        }
    }

    fn queued(entry: LogEntry) -> QueuedEntry {
        QueuedEntry::new(entry)
    }

    #[test]
    fn an_entry_with_no_stamp_never_holds() {
        let (state, _dir) = state();
        let entry = ReplicatedEntry::new(
            1,
            DB.as_u64(),
            2,
            ReplicatedWrite::MirrorSeal {
                last: false,
                calvin_floors: Vec::new(),
            },
        );
        let plain = queued(LogEntry {
            term: 1,
            index: 3,
            data: entry.encode().expect("encode"),
        });
        assert_eq!(mirror_turn(&state, &plain), MirrorTurn::Proceed);
        let noop = queued(LogEntry {
            term: 1,
            index: 3,
            data: Vec::new(),
        });
        assert_eq!(mirror_turn(&state, &noop), MirrorTurn::Proceed);
    }

    #[test]
    fn an_entry_waits_for_its_cut_then_follows_its_epoch() {
        let (state, _dir) = state();
        let entry = queued(seal(3, 1, 5, false));
        assert_eq!(mirror_turn(&state, &entry), MirrorTurn::Wait);
        raise(&state, 1, 5);
        assert_eq!(mirror_turn(&state, &entry), MirrorTurn::Proceed);
        let stale = queued(seal(4, 1, 4, false));
        assert_eq!(mirror_turn(&state, &stale), MirrorTurn::Skip);
    }

    /// A new applier that resumes PROMOTE stamps its own epoch on the last
    /// seal, so the last seal applies whatever the row says of the epoch.
    #[test]
    fn the_last_seal_applies_for_any_epoch() {
        let (state, _dir) = state();
        raise(&state, 1, 5);
        let last = queued(seal(3, 1, 9, true));
        assert_eq!(mirror_turn(&state, &last), MirrorTurn::Proceed);
    }

    /// An entry whose floor this node has not reached waits. Its mirror can be
    /// one this node has not created yet.
    #[test]
    fn an_entry_waits_for_its_metadata_floor_before_any_verdict() {
        let (state, _dir) = state();
        let watcher = state.applied_index_watcher(METADATA_GROUP_ID);
        let floor = watcher.current() + 2;
        let mut entry = ReplicatedEntry::from_bytes(&seal(3, 1, 5, false).data).expect("decode");
        entry.metadata_floor = floor;
        entry.database_id = DB.as_u64() + 1;
        let held = queued(LogEntry {
            term: 1,
            index: 3,
            data: entry.encode().expect("encode"),
        });
        assert_eq!(mirror_turn(&state, &held), MirrorTurn::Wait);
        watcher.bump(floor);
        assert_eq!(
            mirror_turn(&state, &held),
            MirrorTurn::Skip,
            "past its floor, an unknown database is a dropped one"
        );
    }

    /// The whole lane: a held entry stops its group with no effect. The cut
    /// releases it in order. A former applier's copy leaves no trace, and its
    /// key stays free for the new epoch's copy.
    #[tokio::test]
    async fn a_held_entry_stops_its_group_until_its_cut_is_ready() {
        let (state, _dir) = state();
        let tracker = Arc::new(ProposeTracker::new());
        let ctx = ApplyContext {
            state: &state,
            tracker: &tracker,
        };
        let mut pipeline = Pipeline::new(ctx, ProposalLedger::new(PROPOSAL_LEDGER_CAPACITY));
        let stale = seal(1, 1, 4, false);
        let stale_key = queued(stale.clone()).proposal_key();
        pipeline.accept(ApplyBatch {
            group_id: GROUP,
            entries: vec![stale, seal(2, 1, 5, false)],
        });

        pipeline.pump();
        pipeline.settle();
        assert!(pipeline.awaits_mirror());
        assert_eq!(tracker.started_through(GROUP), 0, "nothing starts");
        assert_eq!(state.mirror.cuts.sealed_cut(DB, GROUP), None);

        raise(&state, 1, 5);
        pipeline.pump();
        pipeline.settle();
        assert!(!pipeline.awaits_mirror());
        assert_eq!(tracker.started_through(GROUP), 2, "both entries start");
        assert_eq!(state.mirror.cuts.sealed_cut(DB, GROUP), Some(1));
        assert!(
            state
                .calvin
                .applied
                .get_or_create_mirror(DB.as_u64(), 2)
                .is_applied(7, 0),
            "the seal folds the source's fully applied epoch"
        );

        let carried = tracker.committed_keys_through(GROUP, 2);
        assert!(
            !carried.keys.iter().any(|(_, key)| *key == stale_key),
            "a snapshot carries no key of a skipped copy"
        );
    }
}
