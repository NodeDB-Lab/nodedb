// SPDX-License-Identifier: BUSL-1.1

//! Derivation of the sequencer leader's starting epoch, and the service's
//! response to a halted state machine.
//!
//! The reasoning that guards the seed stays next to the one function that
//! implements it.

use std::sync::Mutex;
use std::sync::atomic::Ordering;

use tracing::{debug, info, warn};

use crate::calvin::sequencer::config::SEQUENCER_GROUP_ID;
use crate::calvin::sequencer::state_machine::SequencerStateMachine;
use crate::multi_raft::MultiRaft;

use super::core::SequencerService;

/// The next epoch this leader mints, and the sequencer term it was seeded
/// in.
///
/// The cursor is valid only within its term. Another leader can mint epochs
/// in a later term, so a node that leads again re-derives its seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct EpochCursor {
    pub term: u64,
    pub next: u64,
}

impl SequencerService {
    /// Derive the epoch seed once per sequencer term, then reuse it within
    /// that term.
    ///
    /// Delegates the safety gate to [`derive_epoch_seed`]. `None` means it is
    /// not yet safe to mint an epoch on this node and the caller must skip
    /// minting for this tick.
    ///
    /// Publishes the outcome to `metrics.epoch_seeded` so the readiness probe
    /// can tell whether a Calvin submit landing here can be sequenced.
    pub(super) fn ensure_epoch_seeded(&mut self) -> Option<u64> {
        let seed = self.derive_or_cached_epoch();
        self.metrics
            .epoch_seeded
            .store(seed.is_some(), Ordering::Relaxed);
        seed
    }

    /// The seed itself, without the readiness publication.
    fn derive_or_cached_epoch(&mut self) -> Option<u64> {
        // Checked ahead of the cached seed, not just before deriving one: a halt
        // can land long after the seed was taken. A halted state machine refuses
        // every epoch batch, so a minted epoch would only manufacture identities
        // that nothing on this node will ever apply.
        if self.state_machine_halted() {
            return None;
        }
        // A cursor from an earlier term can trail epochs a later leader
        // minted. Minting from it would send the epoch backwards.
        let term = self.sequencer_current_term();
        if let Some(cursor) = self.current_epoch
            && cursor.term == term
        {
            return Some(cursor.next);
        }
        self.current_epoch = None;
        let cursor = derive_epoch_seed(self.node_id, &self.multi_raft, &self.state_machine)?;
        self.current_epoch = Some(cursor);
        Some(cursor.next)
    }

    /// Record that `next` is the next epoch to mint in the cursor's term.
    /// No-op without a seeded cursor.
    pub(super) fn advance_epoch(&mut self, next: u64) {
        if let Some(cursor) = &mut self.current_epoch {
            cursor.next = next;
        }
    }

    /// This node's current term in the sequencer group, `0` when the group
    /// is not mounted here.
    fn sequencer_current_term(&self) -> u64 {
        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.group_leader_at_term(SEQUENCER_GROUP_ID).1
    }

    /// Whether this node's sequencer state machine has stopped applying epoch
    /// batches after an unrecoverable epoch regression.
    pub(super) fn state_machine_halted(&self) -> bool {
        self.state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_halted()
    }

    /// Fail every queued submission fast while the state machine is halted.
    ///
    /// A halt scopes the fault to sequencing: reads, non-Calvin writes, metadata
    /// and every other engine on this node are unaffected, so the node keeps
    /// serving. What it must not do is keep accepting Calvin work — nothing will
    /// ever sequence it. Dropping each submission's assignment makes the
    /// awaiting Control-Plane caller observe a closed channel immediately and
    /// surface an error, instead of every writer hanging to its deadline behind
    /// a queue that will never drain. Reservation requests degrade to plain OCC
    /// the same way they do on a follower.
    pub(super) fn shed_submissions_after_halt(&mut self) {
        let discarded = self.discard_inbox();
        let reservations_discarded = self.reservation_receiver.drain_all_discard();
        if !self.halt_reported {
            self.halt_reported = true;
            tracing::error!(
                node_id = self.node_id,
                "sequencer state machine halted on an epoch regression; this node has stopped \
                 sequencing and is failing Calvin submissions fast. Every other query path \
                 keeps serving — operator intervention is required to resume sequencing."
            );
        }
        if discarded > 0 || reservations_discarded > 0 {
            debug!(
                node_id = self.node_id,
                discarded, reservations_discarded, "sequencer halted; shed queued submissions"
            );
        }
    }
}

/// Derive the first epoch this node may propose in its current sequencer
/// term, returning `None` while it is not yet safe to derive one.
///
/// INVARIANT: **the first epoch a restarted leader proposes must be
/// strictly greater than any epoch already committed to the sequencer
/// log.** An epoch number is half of every transaction's `(epoch,
/// position)` identity and is also the state machine's ordering check, so
/// re-minting a committed epoch is not a numbering blemish: on replay each
/// replica meets the historical epoch first, then the duplicate, and
/// refuses the duplicate's batch — every transaction in it is lost and its
/// waiters hang to their deadlines.
///
/// The seed can only come from the state machine's `next_epoch()`, and that
/// counter is in-memory: it is rebuilt solely by replaying the sequencer
/// group's committed log. Reading it while the service is being constructed
/// therefore always answers 0, however much history the log holds — the
/// Raft loop that drives the replay is not spawned until later in startup.
/// So the read happens here, lazily, on the first leader tick, gated on the
/// group having applied everything its local log holds.
///
/// The gate compares against the LOG TIP, not `commit_index`: a node that
/// has just won an election can still observe `commit_index` behind its own
/// log (its term's no-op has not committed yet) while `is_leader()` already
/// reports true, and every entry in a leader's log commits moments later
/// under that no-op. Gating on `commit_index` would leave exactly that
/// window open, which is the window a restart lands in.
///
/// The gate is "applied has caught up with the tip", NOT "an entry exists".
/// A brand-new node — and any node nobody has proposed to yet — has
/// `last_applied == log_tip == 0` and passes immediately, seeding epoch 0
/// from an empty state machine. Requiring an entry first would be a deadlock:
/// the only thing that puts the first entry in the sequencer log is this
/// node proposing under the very seed it is waiting for.
///
/// The state machine's history origin decides whether the seed is exact.
/// A state built from the log start, or restored from a snapshot of complete
/// history, holds every committed epoch. Its next epoch is exact, including
/// the first epoch when none committed. A state whose log start was
/// discarded with no snapshot holds only the retained entries. Epochs rise
/// along the log, so a retained epoch still bounds every discarded one.
/// With no retained epoch there is nothing to derive from, and the seed
/// refuses.
///
/// Returns `None` while a replay is in flight or the history is unknown with
/// no retained epoch. The caller then defers minting for that tick (and only
/// minting — every leader duty that stamps no new identity still runs), so
/// submissions stay queued rather than being sequenced under a colliding
/// epoch.
pub(super) fn derive_epoch_seed(
    node_id: u64,
    multi_raft: &Mutex<MultiRaft>,
    state_machine: &Mutex<SequencerStateMachine>,
) -> Option<EpochCursor> {
    // Read the Raft-side watermarks and release the lock before taking the
    // state machine's: the two are never held together anywhere, and this
    // is the only site that needs both.
    let (last_applied, log_tip, term) = {
        let mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        (
            mr.last_applied(SEQUENCER_GROUP_ID),
            mr.last_log_index(SEQUENCER_GROUP_ID),
            mr.group_leader_at_term(SEQUENCER_GROUP_ID).1,
        )
    };
    let (Some(last_applied), Some(log_tip)) = (last_applied, log_tip) else {
        warn!(
            node_id,
            "sequencer group is not mounted on this node; cannot derive an epoch seed"
        );
        return None;
    };
    if last_applied < log_tip {
        debug!(
            node_id,
            last_applied, log_tip, "sequencer group still replaying; deferring epoch seed"
        );
        return None;
    }

    let state_machine = state_machine.lock().unwrap_or_else(|p| p.into_inner());
    // Refusing to propose is a visible stall. Minting 0 over discarded
    // history is silent loss of every batch that follows.
    let history = state_machine.history_origin();
    if !history.is_known() && state_machine.last_applied_epoch().is_none() {
        warn!(
            node_id,
            ?history,
            "sequencer history before the retained log is unknown and no retained \
             entry carries an epoch; refusing to propose rather than mint an epoch \
             that may collide with discarded history"
        );
        return None;
    }
    let epoch = state_machine.next_epoch();
    drop(state_machine);

    info!(
        node_id,
        epoch,
        log_tip,
        term,
        ?history,
        "sequencer epoch seed derived from the replayed sequencer log"
    );
    Some(EpochCursor { term, next: epoch })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::calvin::CalvinCompletionRegistry;
    use crate::calvin::sequencer::config::SEQUENCER_GROUP_ID;
    use crate::calvin::sequencer::entry::SequencerEntry;
    use crate::calvin::sequencer::service::core::tests::{elect, epoch_batch_bytes, make_harness};
    use crate::calvin::sequencer::state_machine::{HistoryOrigin, SequencerStateMachine};

    fn detached() -> SequencerStateMachine {
        SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached())
    }

    /// A snapshot taken before any epoch committed holds complete history.
    /// The leader that restored it mints the first epoch.
    #[test]
    fn seed_after_an_epoch_free_snapshot_mints_the_first_epoch() {
        let mut harness = make_harness();
        let snapshot = detached().capture_snapshot(5);
        {
            let mut sm = harness
                .state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            sm.restore_snapshot(snapshot);
            assert_eq!(sm.last_applied_epoch(), None);
            assert_eq!(sm.history_origin(), HistoryOrigin::Snapshot { through: 5 });
        }
        assert_eq!(harness.service.ensure_epoch_seeded(), Some(0));
    }

    /// A snapshot that holds epoch `n` seeds epoch `n + 1`.
    #[test]
    fn seed_after_a_snapshot_with_an_epoch_mints_the_next_epoch() {
        let mut harness = make_harness();
        let mut leader = detached();
        for epoch in 0..5u64 {
            leader.apply(epoch + 1, &epoch_batch_bytes(epoch));
        }
        let snapshot = leader.capture_snapshot(5);
        harness
            .state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .restore_snapshot(snapshot);
        assert_eq!(harness.service.ensure_epoch_seeded(), Some(5));
    }

    /// Unknown history with no retained epoch refuses the seed. A retained
    /// epoch bounds every discarded one, so it seeds above it.
    #[test]
    fn unknown_history_without_a_retained_epoch_refuses_the_seed() {
        let mut harness = make_harness();
        harness
            .state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .mark_history_unknown();
        assert_eq!(harness.service.ensure_epoch_seeded(), None);
        assert!(harness.service.current_epoch.is_none());

        let floor = zerompk::to_msgpack_vec(&SequencerEntry::EpochFloor {
            next_epoch: 8,
            epoch_system_ms: 0,
        })
        .expect("encode");
        harness
            .state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .apply(1, &floor);
        assert_eq!(harness.service.ensure_epoch_seeded(), Some(8));
    }

    /// A cursor seeded in an earlier term trails the epochs another leader
    /// minted since. A new term derives the seed again.
    #[test]
    fn a_term_change_re_derives_the_seed() {
        let mut harness = make_harness();
        assert_eq!(harness.service.ensure_epoch_seeded(), Some(0));
        let first_term = harness.service.current_epoch.expect("seeded").term;

        // Another leader minted epochs 0 and 1, applied here.
        {
            let mut sm = harness
                .state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            sm.apply(1, &epoch_batch_bytes(0));
            sm.apply(2, &epoch_batch_bytes(1));
        }
        elect(&harness.multi_raft);
        {
            let mut mr = harness.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let tip = mr
                .last_log_index(SEQUENCER_GROUP_ID)
                .expect("group is mounted");
            mr.advance_applied(SEQUENCER_GROUP_ID, tip)
                .expect("advance applied");
        }

        assert_eq!(harness.service.ensure_epoch_seeded(), Some(2));
        let cursor = harness.service.current_epoch.expect("seeded");
        assert!(cursor.term > first_term);
    }

    /// A tick that finds this node not leading drops the cursor.
    #[test]
    fn losing_leadership_drops_the_epoch_cursor() {
        let mut harness = make_harness();
        assert_eq!(harness.service.ensure_epoch_seeded(), Some(0));
        assert!(harness.service.current_epoch.is_some());
        harness.service.tick();
        assert!(harness.service.current_epoch.is_none());
    }
}
