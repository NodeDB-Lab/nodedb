// SPDX-License-Identifier: BUSL-1.1

//! Sequencer-fan-out catch-up drain for the Calvin scheduler.
//!
//! The sequencer state machine fans each committed `SchedulerInput` out to the
//! per-vShard scheduler channels with a bounded `try_send`. On a Full/Closed
//! channel the input is DROPPED (only bookkept, never blocking — `apply` shares
//! its call stack with every Raft group and must not stall node-wide
//! heartbeats). A dropped input will otherwise permanently diverge this
//! replica's lock table from its peers, since the lock table is a local
//! projection every replica rebuilds from the byte-identical sequencer Raft log.
//!
//! [`Scheduler::drain_catch_up`] closes that gap: it takes the earliest dropped
//! Raft index recorded for this vShard, replays the committed sequencer log
//! range through the SAME `process_scheduler_input` path the live fan-out feeds,
//! and thereby reconstructs the missed input deterministically. Replay is
//! idempotent — `process_new_txn`'s in-flight guard turns an already-in-flight
//! Txn into a no-op, and Reserve/Release re-application is a lock-manager no-op.
//!
//! One drain reads at most [`SchedulerConfig::catch_up_window`] log entries.
//!
//! [`SchedulerConfig::catch_up_window`]: super::super::config::SchedulerConfig::catch_up_window

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::calvin::types::SchedulerInput;

use super::scheduler::Scheduler;

/// Result of one [`Scheduler::drain_catch_up`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) enum CatchUpDrain {
    /// Nothing is left to replay now. No catch-up is armed, nothing is
    /// committed at the armed index yet, the log read failed and the entry
    /// stays armed for a later tick, or the armed range is fully replayed.
    Settled,
    /// The drain stopped before the committed index: the window filled or
    /// the intake gate closed. Catch-up stays armed at the first unprocessed
    /// index, and the next drain can resume at once.
    Remaining,
}

impl Scheduler {
    /// Replay any sequencer-fan-out inputs dropped on this replica.
    ///
    /// Run on the periodic stall tick. O(1) in the common case (no pending
    /// catch-up → one map probe and return).
    ///
    /// Reads and replays at most `catch_up_window` log entries from the armed
    /// index. Stops feeding at the first input after which the intake gate is
    /// closed.
    ///
    /// # Lock discipline (deadlock-safety)
    ///
    /// The two shared mutexes — the sequencer state machine and MultiRaft — are
    /// each acquired in an ISOLATED scope and NEVER nested. The Raft apply loop
    /// holds the SM lock while fanning out but never takes MultiRaft underneath
    /// it; this drain takes them strictly one-at-a-time (SM → release → MultiRaft
    /// → release → SM → release), so the two paths can never form a lock cycle.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn drain_catch_up(
        &mut self,
    ) -> CatchUpDrain {
        // 1. SM-lock scope: PEEK the earliest armed index for this vShard.
        //    `None` (the common case) means no catch-up is pending — return O(1).
        //    Otherwise pair it with the committed-index watermark as the replay
        //    upper bound. We PEEK rather than TAKE: the entry is cleared only
        //    after a confirmed replay (step 4), so a tick that cannot complete
        //    the replay (committed index not yet known, transient log-read
        //    fault) leaves the catch-up armed for the next tick instead of
        //    silently dropping it. The same scope asks how far the replay
        //    cache already holds the window decoded. Release the SM lock before
        //    the MultiRaft read.
        let window = self.config.catch_up_window.max(1);
        let (lo, hi, end, kept_through) = {
            let sm = self
                .sequencer_state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let Some(lo) = sm.peek_catch_up_from(self.vshard_id) else {
                return CatchUpDrain::Settled;
            };
            let Some(hi) = sm.current_committed_index() else {
                // Armed but nothing applied yet — leave it armed and retry once
                // an entry is applied and `hi` is known.
                return CatchUpDrain::Settled;
            };
            if lo > hi {
                // Armed ahead of the committed watermark (e.g. spawn-armed from
                // the first available index before any entry applied on this
                // replica). Nothing to replay yet; stay armed.
                return CatchUpDrain::Settled;
            }
            // Last index this drain replays: the window end, capped at `hi`.
            let end = lo.saturating_add(window - 1).min(hi);
            (lo, hi, end, sm.replay_kept_through(lo, end))
        };

        // 2. MultiRaft-lock scope: read the committed sequencer log from the
        //    first index the replay cache lacks. No SM lock is held here (see
        //    the lock-discipline note above). The read runs under the
        //    MultiRaft lock alone; the result is handled after the lock drops.
        let read_from = kept_through.map_or(Some(lo), |kept| {
            kept.checked_add(1).filter(|&next| next <= end)
        });
        let read = match read_from {
            Some(read_from) => self
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .read_committed_entries(SEQUENCER_GROUP_ID, read_from, end),
            None => Ok(Vec::new()),
        };
        let entries = {
            match read {
                Ok(entries) => entries,
                Err(nodedb_cluster::error::ClusterError::Raft(
                    nodedb_raft::RaftError::LogCompacted { .. },
                )) => {
                    // The armed index is below the retained log: an input
                    // this replica never applied is gone, and only a
                    // data-group snapshot brings the vShard's Calvin state
                    // back. The compaction floor keeps every armed index, so
                    // the log reached this node compacted already: a
                    // sequencer snapshot installed under the armed index.
                    self.metrics.record_catch_up_log_compacted();
                    tracing::error!(
                        vshard = self.vshard_id,
                        lo,
                        "calvin catch-up: the sequencer log no longer holds the armed index; \
                         the vShard's data group takes a snapshot"
                    );
                    self.lose_calvin_base();
                    self.sequencer_state_machine
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .clear_catch_up_up_to(self.vshard_id, hi);
                    return CatchUpDrain::Settled;
                }
                Err(e) => {
                    // Transient infra fault (e.g. group transiently absent).
                    // Leave the catch-up ARMED (we peeked, did not take) so a
                    // later drain retries it. Surface it rather than swallow.
                    tracing::warn!(
                        vshard = self.vshard_id,
                        lo,
                        end,
                        error = %e,
                        "calvin catch-up: failed to read committed sequencer entries"
                    );
                    return CatchUpDrain::Settled;
                }
            }
        };

        // 3. SM-lock scope: project the window into this vShard's
        //    `SchedulerInput` stream, each input with the Raft index it came
        //    from. The replay cache decodes each entry once per node, so a
        //    vShard the entry does not target pays no decode. `None` means
        //    the cache evicted an index of the window after step 1: the
        //    catch-up stays armed, and the next drain reads it again.
        let inputs: Vec<(u64, SchedulerInput)> = {
            let sm = self
                .sequencer_state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            match sm.replay_window_for_vshard(&entries, self.vshard_id, lo, end) {
                Some(inputs) => inputs,
                None => return CatchUpDrain::Remaining,
            }
        };

        // 4. Feed each replayed input through the SAME live processing path — no
        //    lock held. Determinism: identical inputs through identical code.
        //    The in-flight guard makes an overlapping already-in-flight Txn a
        //    no-op; Reserve/Release re-application is idempotent.
        //
        //    An input after which the intake gate is closed stops the feed: a
        //    dispatch deferred at capacity, or a full in-flight backlog. The
        //    next drain resumes at the first input not yet processed.
        let mut replayed: u64 = 0;
        let mut resume_from: Option<u64> = None;
        // The replayed transactions, which the sequencer log keeps until
        // this scheduler made them durable.
        let mut received: Vec<(u64, u64, u32)> = Vec::new();
        let mut feed = inputs.into_iter().peekable();
        while let Some((index, input)) = feed.next() {
            if let SchedulerInput::Txn(txn) = &input {
                received.push((index, txn.epoch, txn.position));
            }
            self.process_scheduler_input(input);
            replayed += 1;
            if self.intake_closure().is_some() {
                resume_from = feed.peek().map(|(index, _)| *index);
                break;
            }
        }
        // A window that ends before `hi` resumes at the first index past it.
        let resume_from = resume_from.or(end.checked_add(1).filter(|&next| next <= hi));

        {
            let mut sm = self
                .sequencer_state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            for (index, epoch, position) in received {
                sm.note_replayed_txn(index, self.vshard_id, epoch, position);
            }
            match resume_from {
                // Stopped early: re-arm exactly at the first unprocessed input's
                // index. The arm widens the range to `next`, and the clear
                // below it moves the start forward. The vShard stays armed
                // throughout, so the replay cache keeps the entries from
                // `next`. Both run under one SM lock.
                Some(next) => {
                    sm.arm_catch_up_from(self.vshard_id, next);
                    if let Some(below) = next.checked_sub(1) {
                        sm.clear_catch_up_up_to(self.vshard_id, below);
                    }
                }
                // Replay of `lo ..= hi` is complete (`end == hi` here): clear
                // the armed catch-up, but only up to `hi` — a concurrent drop
                // recorded at an index `> hi` while this replay ran is
                // preserved for the next drain. This is the CONFIRM step the
                // peek-not-take at the top defers to; a transient failure
                // above returned early and left the entry armed.
                None => sm.clear_catch_up_up_to(self.vshard_id, hi),
            }
        }

        if replayed > 0 {
            self.metrics.record_catch_up_replayed(replayed);
            tracing::info!(
                vshard = self.vshard_id,
                lo,
                end,
                hi,
                replayed,
                "calvin catch-up: replayed dropped sequencer inputs from committed log"
            );
        }
        if resume_from.is_some() {
            CatchUpDrain::Remaining
        } else {
            CatchUpDrain::Settled
        }
    }
}

// ── Catch-up drain + in-flight guard (sequencer fan-out reliability) ────────
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use nodedb_cluster::calvin::types::{EpochBatch, SchedulerInput, SequencedTxn};
    use nodedb_cluster::calvin::{CalvinCompletionRegistry, SequencerEntry};
    use nodedb_types::TenantId;
    use nodedb_types::id::DatabaseId;

    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_physical::physical_plan::meta::MetaOp;

    use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide};
    use crate::bridge::envelope::{StageVote, Status};
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::lead_data_group;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler, build_test_scheduler_with_data_side, fill_tenant_inflight,
        make_sequenced_txn, make_validate_only_txn, release_filler, staged_response,
        test_coll_vshard,
    };
    use crate::control::cluster::calvin::scheduler::lock_manager::{AcquireOutcome, TxnId};
    use crate::control::cluster::calvin::scheduler::metrics::intake_closure_reason;
    use crate::types::RequestId;

    #[tokio::test]
    async fn drain_catch_up_is_noop_when_no_drop_recorded() {
        // Fresh sequencer state machine: no fan-out was ever dropped, so
        // `take_catch_up_from` returns `None` and the drain returns O(1) without
        // touching MultiRaft, replaying anything, or hitting the compacted path.
        let (mut scheduler, _dir) = build_test_scheduler(0);

        scheduler.drain_catch_up();

        assert_eq!(
            scheduler.metrics.catch_up_replayed.load(Ordering::Relaxed),
            0,
            "no inputs should be replayed when no drop was recorded"
        );
        assert_eq!(
            scheduler
                .metrics
                .catch_up_log_compacted
                .load(Ordering::Relaxed),
            0,
            "the compacted path must not be reached on the no-drop common case"
        );
    }

    // ── END-TO-END catch-up drain (real committed sequencer Raft log) ───────────
    //
    // The two tests below close the loop: they stand up a REAL single-node
    // sequencer Raft group, COMMIT epoch batches to it, force the live fan-out to
    // actually DROP under a full channel, then run the real `drain_catch_up` —
    // which reads the genuine committed log via `read_committed_entries`, decodes
    // it through `replay_window_for_vshard`, and feeds each input through
    // `process_scheduler_input` — and assert the dropped input's effect lands in
    // the scheduler's lock table. This proves the whole mechanism closes the
    // fan-out gap, not only each half.

    /// Ensure the sequencer Raft group exists on this scheduler's `MultiRaft` and
    /// that this single node is its leader, so a proposal commits once its disk
    /// holds it.
    fn ensure_sequencer_leader(scheduler: &Scheduler) {
        let mut mr = scheduler
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if !mr.contains_group(nodedb_cluster::calvin::SEQUENCER_GROUP_ID) {
            mr.add_group(nodedb_cluster::calvin::SEQUENCER_GROUP_ID, vec![])
                .unwrap();
        }
        // Force the election timeout to fire on the next tick so the single voter
        // campaigns and wins immediately (majority = itself).
        if let Some(node) = mr
            .groups_mut()
            .get_mut(&nodedb_cluster::calvin::SEQUENCER_GROUP_ID)
        {
            // no-determinism: test-only forced election deadline so the single voter campaigns immediately.
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        for _ in 0..20 {
            mr.tick().unwrap();
            if mr.is_group_leader(nodedb_cluster::calvin::SEQUENCER_GROUP_ID) {
                return;
            }
        }
        panic!("sequencer group did not reach single-node leadership");
    }

    /// Encode `batch` and propose it to the committed sequencer Raft log, returning
    /// its committed Raft index and the encoded bytes (reused to drive `apply`, so
    /// the index handed to `apply` is the SAME real committed index the drain will
    /// read back). A single-voter group commits once its disk holds the entry,
    /// on the next tick.
    fn commit_epoch_batch(scheduler: &Scheduler, batch: EpochBatch) -> (u64, Vec<u8>) {
        let bytes = zerompk::to_msgpack_vec(&SequencerEntry::EpochBatch { batch })
            .expect("encode epoch batch");
        let index = {
            let mut mr = scheduler
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let index = mr
                .propose_to_group(nodedb_cluster::calvin::SEQUENCER_GROUP_ID, bytes.clone())
                .expect("propose epoch batch to sequencer group");
            mr.wait_all_durable_blocking();
            mr.tick().expect("tick");
            index
        };
        (index, bytes)
    }

    /// A single-position `EpochBatch` at `epoch` carrying `txn`.
    fn make_batch(epoch: u64, txn: &SequencedTxn) -> EpochBatch {
        EpochBatch {
            epoch,
            txns: vec![txn.clone()],
            epoch_system_ms: txn.epoch_system_ms,
        }
    }

    /// Register a capacity-1, pre-filled (hence permanently Full) fan-out channel
    /// for `vshard` on the shared sequencer state machine, then `apply` the
    /// committed entry `bytes` (Raft index `index`). The live `try_send` fan-out
    /// hits `Full` and DROPS, recording the catch-up index — the exact drop the
    /// drain must repair. The pre-fill payload keeps the receiver end (`_full_rx`)
    /// alive so the channel reports `Full` (not `Closed`).
    fn apply_with_full_channel(
        scheduler: &Scheduler,
        vshard: u32,
        index: u64,
        bytes: &[u8],
        fill: &SequencedTxn,
    ) {
        let (full_tx, full_rx) = tokio::sync::mpsc::channel(1);
        full_tx
            .try_send(SchedulerInput::Txn(Box::new(fill.clone())))
            .expect("pre-fill the capacity-1 channel");
        // Keep the receiver alive for the duration of `apply` so the sender reports
        // Full rather than Closed (either records a catch-up index, but Full is the
        // backpressure case this test targets).
        let _full_rx = full_rx;
        let mut sm = scheduler
            .sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        sm.set_vshard_sender(vshard, full_tx);
        sm.apply(index, bytes);
    }

    /// END-TO-END: a fan-out drop under a full channel, then a real `drain_catch_up`
    /// that reads the committed sequencer Raft log, replays the dropped input, and
    /// applies it to this scheduler's lock table.
    ///
    /// This is the whole-mechanism proof the piece-wise unit tests cannot give: the
    /// drain reads a GENUINE committed Raft entry (not a hand-built log slice) and
    /// the replayed input mutates the REAL lock table. A conflicting holder is
    /// pre-seeded so the replayed txn blocks in the lock table — landing observably
    /// in `blocked` without a Data-Plane dispatch (no executor runs in this harness).
    #[tokio::test]
    async fn drain_replays_dropped_input_into_lock_table_end_to_end() {
        // Use the vShard that "test_coll" hashes to, so the batch's fan-out targets —
        // and its replay decodes for — this scheduler's vShard.
        let vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "test_coll")
            .vshard()
            .as_u32();
        let (mut scheduler, _dir) = build_test_scheduler(vshard);
        ensure_sequencer_leader(&scheduler);

        let txn = make_sequenced_txn(0, 0);
        let (committed_index, bytes) = commit_epoch_batch(&scheduler, make_batch(0, &txn));

        // Drive the real live drop: apply the committed entry against a full channel.
        apply_with_full_channel(&scheduler, vshard, committed_index, &bytes, &txn);
        {
            let sm = scheduler
                .sequencer_state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            // Non-consuming proof the drop happened (leaves catch_up_from intact for
            // the drain to TAKE).
            assert!(
                sm.metrics.txns_dropped_backpressure.load(Ordering::Relaxed) >= 1,
                "apply on a full channel must drop and bookkeep the catch-up index"
            );
        }

        // Pre-seed a conflicting exclusive holder so the replayed txn BLOCKS (and so
        // never dispatches to a Data Plane that is not running in this harness).
        let keys = crate::control::cluster::calvin::scheduler::driver::helpers::expand_rw_set(
            &txn,
            crate::control::cluster::calvin::scheduler::driver::helpers::LockSite::On(
                scheduler.vshard_id,
            ),
        );
        assert!(!keys.is_empty(), "txn must expand to at least one lock key");
        let sentinel = TxnId::new(u64::MAX, 0);
        {
            let mut lm = scheduler
                .lock_manager
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert_eq!(
                lm.acquire(sentinel, keys.clone()).outcome,
                AcquireOutcome::Ready
            );
        }

        let replayed_before = scheduler.metrics.catch_up_replayed.load(Ordering::Relaxed);
        let dispatched_before = scheduler.metrics.dispatch_count.load(Ordering::Relaxed);
        assert!(scheduler.blocked.is_empty());

        // THE mechanism under test: read the committed log, replay, re-apply.
        scheduler.drain_catch_up();

        // Exactly the one dropped input was replayed.
        assert_eq!(
            scheduler.metrics.catch_up_replayed.load(Ordering::Relaxed),
            replayed_before + 1,
            "drain must replay exactly the one dropped input from the committed log"
        );
        // The replayed input reached the lock table and queued behind the conflicting
        // holder — proving the dropped input was read from the REAL committed Raft
        // log, decoded, and re-applied through the live `process_scheduler_input`.
        let lock_owner = TxnId::new(0, 0);
        assert!(
            scheduler.blocked.contains_key(&lock_owner),
            "the replayed txn must have acquired-and-blocked in the lock table"
        );
        // Blocked never dispatches, so the whole path touched no executor.
        assert_eq!(
            scheduler.metrics.dispatch_count.load(Ordering::Relaxed),
            dispatched_before,
            "a blocked replayed txn must not dispatch"
        );
        // The catch-up entry was consumed exactly once (TAKE semantics).
        {
            let sm = scheduler
                .sequencer_state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert_eq!(
                sm.take_catch_up_from(vshard),
                None,
                "the drain must have TAKEn the catch-up index"
            );
        }
    }

    /// END-TO-END: when the replay range re-covers an input already delivered live
    /// and still in-flight, the in-flight guard turns the overlapping replay into a
    /// no-op — the input is not processed (or dispatched) twice — while a genuinely
    /// dropped earlier input in the same range IS applied.
    ///
    /// This exercises the guard's real end-to-end behavior: the drain replays from
    /// the earliest dropped index forward (`[idx0 ..= idx1]`), which unavoidably
    /// re-covers a later, non-dropped input. Epoch 0 (dropped) must be applied;
    /// epoch 1 (delivered live, in-flight) must be skipped.
    #[tokio::test]
    async fn drain_skips_in_flight_overlap_no_double_dispatch_end_to_end() {
        let vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "test_coll")
            .vshard()
            .as_u32();
        let (mut scheduler, _dir) = build_test_scheduler(vshard);
        ensure_sequencer_leader(&scheduler);

        let txn0 = make_sequenced_txn(0, 0);
        let txn1 = make_sequenced_txn(1, 0);
        let (idx0, bytes0) = commit_epoch_batch(&scheduler, make_batch(0, &txn0));
        let (idx1, bytes1) = commit_epoch_batch(&scheduler, make_batch(1, &txn1));
        assert!(idx1 > idx0, "second batch commits at a later Raft index");

        // A single conflicting holder on the shared key (both txns write test_coll
        // surrogate 1) so every txn BLOCKS rather than dispatching to a Data Plane
        // that is not running.
        let keys = crate::control::cluster::calvin::scheduler::driver::helpers::expand_rw_set(
            &txn0,
            crate::control::cluster::calvin::scheduler::driver::helpers::LockSite::On(
                scheduler.vshard_id,
            ),
        );
        let sentinel = TxnId::new(u64::MAX, 0);
        {
            let mut lm = scheduler
                .lock_manager
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert_eq!(
                lm.acquire(sentinel, keys.clone()).outcome,
                AcquireOutcome::Ready
            );
        }

        // Deliver epoch 1 LIVE: it acquires, blocks behind the sentinel, and is now
        // in-flight (its (epoch, position) sits in `blocked`). It was NOT dropped.
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(txn1.clone())));
        let live_owner = TxnId::new(1, 0);
        assert!(
            scheduler.blocked.contains_key(&live_owner),
            "live epoch-1 txn must be in-flight (blocked) before the drain"
        );

        // Drop BOTH committed entries through the full-channel fan-out so the drain's
        // replay range spans `[idx0 ..= idx1]` (min-collapse keeps idx0; last
        // committed index advances to idx1), re-covering the live epoch-1 input.
        apply_with_full_channel(&scheduler, vshard, idx0, &bytes0, &txn0);
        apply_with_full_channel(&scheduler, vshard, idx1, &bytes1, &txn1);

        let dispatched_before = scheduler.metrics.dispatch_count.load(Ordering::Relaxed);
        let blocked_before = scheduler.blocked.len();
        assert_eq!(blocked_before, 1, "only the live epoch-1 txn is in-flight");

        scheduler.drain_catch_up();

        // Epoch 0 (genuinely dropped, new to this scheduler) was applied → it now
        // blocks behind the sentinel too.
        let dropped_owner = TxnId::new(0, 0);
        assert!(
            scheduler.blocked.contains_key(&dropped_owner),
            "the genuinely-dropped epoch-0 input must be applied by the drain"
        );
        // Epoch 1 was already in-flight; the guard must have skipped its replay — no
        // duplicate entry, no second dispatch. Exactly the two distinct owners remain.
        assert_eq!(
            scheduler.blocked.len(),
            2,
            "the in-flight overlap must not create a duplicate blocked entry"
        );
        assert!(scheduler.blocked.contains_key(&live_owner));
        assert_eq!(
            scheduler.metrics.dispatch_count.load(Ordering::Relaxed),
            dispatched_before,
            "the guarded overlap must not cause a second dispatch"
        );
    }

    /// Commit two single-txn batches at epochs 0 and 1, each a validate-only
    /// txn that stages on this scheduler, and drop both through a full
    /// fan-out channel. Returns the two committed Raft indexes.
    fn arm_two_dropped_stage_batches(scheduler: &Scheduler) -> (u64, u64) {
        ensure_sequencer_leader(scheduler);
        let txn0 = make_validate_only_txn(0, 0);
        let txn1 = make_validate_only_txn(1, 0);
        let (idx0, bytes0) = commit_epoch_batch(scheduler, make_batch(0, &txn0));
        let (idx1, bytes1) = commit_epoch_batch(scheduler, make_batch(1, &txn1));
        assert!(idx1 > idx0, "second batch commits at a later Raft index");
        apply_with_full_channel(scheduler, scheduler.vshard_id, idx0, &bytes0, &txn0);
        apply_with_full_channel(scheduler, scheduler.vshard_id, idx1, &bytes1, &txn1);
        (idx0, idx1)
    }

    /// Draining a replay range against a dispatcher at tenant capacity marks
    /// no replayed position applied.
    #[tokio::test]
    async fn drain_against_full_dispatcher_marks_no_replayed_position_applied() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        lead_data_group(&mut scheduler);
        arm_two_dropped_stage_batches(&scheduler);
        let shared = std::sync::Arc::clone(&scheduler.shared);
        fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));

        scheduler.drain_catch_up();

        assert!(
            !scheduler.applied.is_applied(0, 0),
            "the refused replayed txn must stay unapplied"
        );
        assert!(
            !scheduler.applied.is_applied(1, 0),
            "a replayed txn after the refusal must stay unapplied"
        );
    }

    /// Draining against a dispatcher at tenant capacity stops at the first
    /// refusal and leaves catch-up armed from the first input it did not
    /// process.
    #[tokio::test]
    async fn drain_against_full_dispatcher_stays_armed_from_first_unprocessed_input() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        lead_data_group(&mut scheduler);
        let (_idx0, idx1) = arm_two_dropped_stage_batches(&scheduler);
        let shared = std::sync::Arc::clone(&scheduler.shared);
        fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));

        scheduler.drain_catch_up();

        let armed = scheduler
            .sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .peek_catch_up_from(scheduler.vshard_id);
        assert_eq!(
            armed,
            Some(idx1),
            "catch-up must stay armed from the input after the refused one"
        );
    }

    /// A drain over a range longer than its window replays exactly the
    /// window and stays armed at the first index past it. The next drain
    /// continues from there.
    #[tokio::test]
    async fn drain_replays_one_window_then_resumes_past_it() {
        let vshard = test_coll_vshard();
        let (mut scheduler, _dir) = build_test_scheduler(vshard);
        scheduler.config.catch_up_window = 1;
        ensure_sequencer_leader(&scheduler);

        let txn0 = make_sequenced_txn(0, 0);
        let txn1 = make_sequenced_txn(1, 0);
        let (idx0, bytes0) = commit_epoch_batch(&scheduler, make_batch(0, &txn0));
        let (idx1, bytes1) = commit_epoch_batch(&scheduler, make_batch(1, &txn1));
        assert_eq!(idx1, idx0 + 1, "the two batches commit at adjacent indexes");
        apply_with_full_channel(&scheduler, vshard, idx0, &bytes0, &txn0);
        apply_with_full_channel(&scheduler, vshard, idx1, &bytes1, &txn1);

        // A conflicting holder on the shared key makes each replayed txn
        // block, so nothing dispatches.
        let keys = crate::control::cluster::calvin::scheduler::driver::helpers::expand_rw_set(
            &txn0,
            crate::control::cluster::calvin::scheduler::driver::helpers::LockSite::On(
                scheduler.vshard_id,
            ),
        );
        {
            let mut lm = scheduler
                .lock_manager
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert_eq!(
                lm.acquire(TxnId::new(u64::MAX, 0), keys).outcome,
                AcquireOutcome::Ready
            );
        }
        let armed = |scheduler: &Scheduler| {
            scheduler
                .sequencer_state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .peek_catch_up_from(vshard)
        };

        let first = scheduler.drain_catch_up();

        assert_eq!(first, CatchUpDrain::Remaining);
        assert!(scheduler.blocked.contains_key(&TxnId::new(0, 0)));
        assert!(
            !scheduler.blocked.contains_key(&TxnId::new(1, 0)),
            "the entry past the window must not be replayed"
        );
        assert_eq!(
            armed(&scheduler),
            Some(idx1),
            "catch-up stays armed at the first index past the window"
        );

        let second = scheduler.drain_catch_up();

        assert_eq!(second, CatchUpDrain::Settled);
        assert!(
            scheduler.blocked.contains_key(&TxnId::new(1, 0)),
            "the next drain replays the entry past the first window"
        );
        assert_eq!(armed(&scheduler), None, "the armed range is fully replayed");
    }

    /// One run-loop pass of the rebuild: re-send parked requests, then
    /// replay the armed log range while the intake gate is open.
    fn rebuild_pass(scheduler: &mut Scheduler) {
        if scheduler.resends_deferred() {
            scheduler.redispatch_deferred();
        }
        if scheduler.refresh_intake_gate() {
            scheduler.drain_catch_up();
        }
        scheduler.refresh_intake_gate();
    }

    /// The first index armed for catch-up on `scheduler`'s vShard.
    fn armed_index(scheduler: &Scheduler) -> Option<u64> {
        scheduler
            .sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .peek_catch_up_from(scheduler.vshard_id)
    }

    /// Pop the next request off the Data Plane side. It must be a Calvin
    /// request that `wanted` accepts. Returns its id.
    fn pop_calvin_request(
        data_side: &mut CoreChannelDataSide,
        wanted: impl Fn(&MetaOp) -> bool,
    ) -> RequestId {
        let request = data_side
            .request_rx
            .try_pop()
            .expect("a request reached the Data Plane");
        match &request.inner.plan {
            PhysicalPlan::Meta(op) if wanted(op) => request.inner.request_id,
            other => panic!("unexpected request on the Data Plane: {other:?}"),
        }
    }

    /// Answer `request_id` `Ok` on the Data Plane side, route the answer
    /// back, and hand its completion to the scheduler.
    async fn answer(
        scheduler: &mut Scheduler,
        data_side: &mut CoreChannelDataSide,
        request_id: RequestId,
        stage_vote: Option<StageVote>,
    ) {
        let mut response = staged_response(Status::Ok, stage_vote);
        response.request_id = request_id;
        data_side
            .response_tx
            .try_push(BridgeResponse { inner: response })
            .expect("response ring has room");
        assert!(scheduler.shared.poll_and_route_responses() >= 1);
        let (txn_id, routed, response) =
            tokio::time::timeout(Duration::from_secs(5), scheduler.completion_rx.recv())
                .await
                .expect("the completion arrives")
                .expect("the completion channel is open");
        assert_eq!(routed, request_id);
        scheduler.handle_completion(txn_id, routed, response);
    }

    /// Drive the read-only staged txn `txn_id` from its stage request to
    /// completion: the stage votes commit, the verdict commits, and the
    /// slice drops.
    async fn complete_staged(
        scheduler: &mut Scheduler,
        data_side: &mut CoreChannelDataSide,
        txn_id: TxnId,
    ) {
        let stage = pop_calvin_request(data_side, |op| {
            matches!(op, MetaOp::CalvinExecuteStatic { epoch, position, .. }
                if *epoch == txn_id.epoch && *position == txn_id.position)
        });
        answer(scheduler, data_side, stage, Some(StageVote::Commit)).await;
        scheduler.resume_on_verdict(txn_id, true);
        let drop = pop_calvin_request(data_side, |op| {
            matches!(op, MetaOp::CalvinDrop { epoch, position }
                if *epoch == txn_id.epoch && *position == txn_id.position)
        });
        answer(scheduler, data_side, drop, None).await;
    }

    /// The rebuild re-applies two epochs while the txns' tenant holds its
    /// whole in-flight cap. The first replayed stage defers: its txn stays
    /// pending, nothing halts or completes, and catch-up stays armed behind
    /// it. Rebuild passes while saturated refuse once per attempt and close
    /// the intake gate once. Once capacity frees, both epochs apply and the
    /// scheduler reports caught up.
    #[tokio::test]
    async fn rebuild_against_a_saturated_tenant_defers_then_catches_up() {
        const SATURATED_PASSES: u64 = 32;
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        lead_data_group(&mut scheduler);
        scheduler.rebuild_target_epoch = 1;
        let vshard = scheduler.vshard_id;
        let shared = std::sync::Arc::clone(&scheduler.shared);
        let (_idx0, idx1) = arm_two_dropped_stage_batches(&scheduler);
        let fillers = fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));
        let tenant_scope = crate::DispatchCapacityScope::TenantInflight {
            tenant_id: TenantId::new(1),
            inflight: 0,
            cap: 0,
        };
        let busy = || {
            shared
                .system_metrics
                .as_ref()
                .expect("the test state carries system metrics")
                .dispatch_capacity
                .refusals(&tenant_scope)
        };
        let busy_before = busy();
        let answers = || {
            crate::control::metrics::OverloadProtocol::ALL
                .map(|protocol| crate::control::metrics::overload_responses().sent(protocol))
        };
        let answers_before = answers();
        let first = TxnId::new(0, 0);
        let second = TxnId::new(1, 0);

        rebuild_pass(&mut scheduler);

        assert!(
            scheduler.has_deferred_for(first),
            "the replayed stage defers"
        );
        assert!(scheduler.pending.contains_key(&first));
        assert!(
            !scheduler.pending.contains_key(&second) && !scheduler.blocked.contains_key(&second),
            "the next epoch waits behind the closed gate"
        );
        assert_eq!(armed_index(&scheduler), Some(idx1));

        for _ in 0..SATURATED_PASSES {
            rebuild_pass(&mut scheduler);
        }

        let metrics = &scheduler.metrics;
        assert!(
            !scheduler.is_apply_halted(),
            "a capacity refusal never halts"
        );
        assert_eq!(scheduler.deferred_dispatch_len(), 1);
        assert_eq!(scheduler.pending.len(), 1);
        assert!(!scheduler.applied.is_applied(0, 0));
        assert_eq!(armed_index(&scheduler), Some(idx1));
        assert_eq!(metrics.completed_count.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.executor_error_count.load(Ordering::Relaxed), 0);
        assert_eq!(
            metrics.dispatch_deferred_count.load(Ordering::Relaxed),
            1 + SATURATED_PASSES,
            "each attempt refuses once"
        );
        assert_eq!(busy() - busy_before, 1 + SATURATED_PASSES);
        assert_eq!(
            answers(),
            answers_before,
            "a refusal the scheduler absorbs reaches no client"
        );
        assert_eq!(
            metrics.intake_gate_closed_counts[intake_closure_reason::DEFERRED_DISPATCH]
                .load(Ordering::Relaxed),
            1,
            "the stall closes the intake gate once"
        );
        assert!(!scheduler.is_caught_up());
        assert_eq!(shared.calvin.caught_up.lagging(), vec![vshard]);

        for filler in fillers {
            release_filler(&shared, &mut data_side, filler);
        }
        rebuild_pass(&mut scheduler);

        assert!(!scheduler.has_deferred_dispatch(), "the stage is re-sent");
        assert_eq!(
            armed_index(&scheduler),
            None,
            "the rebuild range is replayed"
        );
        assert!(
            scheduler.blocked.contains_key(&second),
            "the next epoch waits for the first one's locks"
        );

        complete_staged(&mut scheduler, &mut data_side, first).await;
        complete_staged(&mut scheduler, &mut data_side, second).await;

        assert!(scheduler.applied.is_applied(0, 0));
        assert!(scheduler.applied.is_applied(1, 0));
        assert!(scheduler.pending.is_empty());
        assert!(!scheduler.is_apply_halted());
        assert_eq!(
            scheduler
                .metrics
                .executor_error_count
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(scheduler.metrics.completed_count.load(Ordering::Relaxed), 2);
        assert!(scheduler.is_caught_up());
        assert!(
            shared.calvin.caught_up.lagging().is_empty(),
            "the readiness gate opens"
        );
    }
}
