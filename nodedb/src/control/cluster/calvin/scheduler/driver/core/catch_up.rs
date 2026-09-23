// SPDX-License-Identifier: BUSL-1.1

//! Sequencer-fan-out catch-up drain for the Calvin scheduler.
//!
//! The sequencer state machine fans each committed `SchedulerInput` out to the
//! per-vShard scheduler channels with a bounded `try_send`. On a Full/Closed
//! channel the input is DROPPED (only bookkept, never blocking — `apply` shares
//! its call stack with every Raft group and must not stall node-wide
//! heartbeats). A dropped input would otherwise permanently diverge this
//! replica's lock table from its peers, since the lock table is a local
//! projection every replica rebuilds from the byte-identical sequencer Raft log.
//!
//! [`Scheduler::drain_catch_up`] closes that gap: it takes the earliest dropped
//! Raft index recorded for this vShard, replays the committed sequencer log
//! range through the SAME `process_scheduler_input` path the live fan-out feeds,
//! and thereby reconstructs the missed input deterministically. Replay is
//! idempotent — `process_new_txn`'s in-flight guard turns an already-in-flight
//! Txn into a no-op, and Reserve/Release re-application is a lock-manager no-op.

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;

use super::scheduler::Scheduler;

impl Scheduler {
    /// Replay any sequencer-fan-out inputs dropped on this replica.
    ///
    /// Run on the periodic stall tick. O(1) in the common case (no pending
    /// catch-up → one map probe and return).
    ///
    /// # Lock discipline (deadlock-safety)
    ///
    /// The two shared mutexes — the sequencer state machine and MultiRaft — are
    /// each acquired in an ISOLATED scope and NEVER nested. The Raft apply loop
    /// holds the SM lock while fanning out but never takes MultiRaft underneath
    /// it; this drain takes them strictly one-at-a-time (SM → release → MultiRaft
    /// → release → SM → release), so the two paths can never form a lock cycle.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn drain_catch_up(&mut self) {
        // 1. SM-lock scope: PEEK the earliest armed index for this vShard.
        //    `None` (the common case) means no catch-up is pending — return O(1).
        //    Otherwise pair it with the committed-index watermark as the replay
        //    upper bound. We PEEK rather than TAKE: the entry is cleared only
        //    after a confirmed replay (step 4), so a tick that cannot complete
        //    the replay (committed index not yet known, transient log-read
        //    fault) leaves the catch-up armed for the next tick instead of
        //    silently dropping it. Release the SM lock before the MultiRaft read.
        let (lo, hi) = {
            let sm = self
                .sequencer_state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let Some(lo) = sm.peek_catch_up_from(self.vshard_id) else {
                return;
            };
            let Some(hi) = sm.current_committed_index() else {
                // Armed but nothing applied yet — leave it armed and retry once
                // an entry is applied and `hi` is known.
                return;
            };
            if lo > hi {
                // Armed ahead of the committed watermark (e.g. spawn-armed from
                // the first available index before any entry applied on this
                // replica). Nothing to replay yet; stay armed.
                return;
            }
            (lo, hi)
        };

        // 2. MultiRaft-lock scope: read the committed sequencer log range. No SM
        //    lock is held here (see the lock-discipline note above).
        let entries = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            match mr.read_committed_entries(SEQUENCER_GROUP_ID, lo, hi) {
                Ok(entries) => entries,
                Err(nodedb_cluster::error::ClusterError::Raft(
                    nodedb_raft::RaftError::LogCompacted { .. },
                )) => {
                    // The armed index has been compacted below the retained log.
                    // The sequencer-group compaction hold-down (floored at
                    // `min_catch_up_from`) is meant to make this unreachable for
                    // an armed catch-up; if it is nonetheless hit (e.g. a
                    // snapshot-install resync that already subsumes this index),
                    // no replay is owed. Escalate non-silently, CLEAR the entry
                    // to avoid an infinite retry against a permanently-compacted
                    // index, and return.
                    self.metrics.record_catch_up_log_compacted();
                    tracing::error!(
                        vshard = self.vshard_id,
                        lo,
                        "calvin catch-up: sequencer log compacted below armed index; \
                         state is snapshot-covered"
                    );
                    self.sequencer_state_machine
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .clear_catch_up_up_to(self.vshard_id, hi);
                    return;
                }
                Err(e) => {
                    // Transient infra fault (e.g. group transiently absent).
                    // Leave the catch-up ARMED (we peeked, did not take) so a
                    // later drain retries it. Surface it rather than swallow.
                    tracing::warn!(
                        vshard = self.vshard_id,
                        lo,
                        hi,
                        error = %e,
                        "calvin catch-up: failed to read committed sequencer entries"
                    );
                    return;
                }
            }
        };

        // 3. SM-lock scope: decode the raw log entries into this vShard's
        //    `SchedulerInput` stream (a pure `&self` read — no side effects).
        let inputs = {
            let sm = self
                .sequencer_state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            sm.replay_epochs_for_vshard(&entries, self.vshard_id, 0, u64::MAX)
        };

        // 4. Feed each replayed input through the SAME live processing path — no
        //    lock held. Determinism: identical inputs through identical code.
        //    The in-flight guard makes an overlapping already-in-flight Txn a
        //    no-op; Reserve/Release re-application is idempotent.
        let replayed = inputs.len() as u64;
        for input in inputs {
            self.process_scheduler_input(input);
        }

        // Replay of `lo ..= hi` is complete: clear the armed catch-up, but only
        // up to `hi` — a concurrent drop recorded at an index `> hi` while this
        // replay ran is preserved for the next drain. This is the CONFIRM step
        // the peek-not-take at the top defers to; a transient failure above
        // returned early and left the entry armed.
        self.sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear_catch_up_up_to(self.vshard_id, hi);

        if replayed > 0 {
            self.metrics.record_catch_up_replayed(replayed);
            tracing::info!(
                vshard = self.vshard_id,
                lo,
                hi,
                replayed,
                "calvin catch-up: replayed dropped sequencer inputs from committed log"
            );
        }
    }
}

// ── Catch-up drain + in-flight guard (sequencer fan-out reliability) ────────
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use nodedb_cluster::calvin::SequencerEntry;
    use nodedb_cluster::calvin::types::{EpochBatch, SchedulerInput, SequencedTxn};
    use nodedb_types::id::{DatabaseId, VShardId};

    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler, make_sequenced_txn,
    };
    use crate::control::cluster::calvin::scheduler::lock_manager::{AcquireOutcome, TxnId};

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
    // it through `replay_epochs_for_vshard`, and feeds each input through
    // `process_scheduler_input` — and assert the dropped input's effect lands in
    // the scheduler's lock table. This proves the whole mechanism closes the
    // fan-out gap, not just each half.

    /// Ensure the sequencer Raft group exists on this scheduler's `MultiRaft` and
    /// that this single node is its leader, so proposals commit immediately.
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
    /// read back). Single-voter groups commit on propose.
    fn commit_epoch_batch(scheduler: &Scheduler, batch: EpochBatch) -> (u64, Vec<u8>) {
        let bytes = zerompk::to_msgpack_vec(&SequencerEntry::EpochBatch { batch })
            .expect("encode epoch batch");
        let index = {
            let mut mr = scheduler
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            mr.propose_to_group(nodedb_cluster::calvin::SEQUENCER_GROUP_ID, bytes.clone())
                .expect("propose epoch batch to sequencer group")
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
            .try_send(SchedulerInput::Txn(fill.clone()))
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
        let vshard =
            VShardId::from_collection_in_database(DatabaseId::DEFAULT, "test_coll").as_u32();
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
        let keys = crate::control::cluster::calvin::scheduler::driver::helpers::expand_rw_set(&txn);
        assert!(!keys.is_empty(), "txn must expand to at least one lock key");
        let sentinel = TxnId::new(u64::MAX, 0);
        {
            let mut lm = scheduler
                .lock_manager
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert_eq!(lm.acquire(sentinel, keys.clone()), AcquireOutcome::Ready);
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
        let vshard =
            VShardId::from_collection_in_database(DatabaseId::DEFAULT, "test_coll").as_u32();
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
        let keys =
            crate::control::cluster::calvin::scheduler::driver::helpers::expand_rw_set(&txn0);
        let sentinel = TxnId::new(u64::MAX, 0);
        {
            let mut lm = scheduler
                .lock_manager
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert_eq!(lm.acquire(sentinel, keys.clone()), AcquireOutcome::Ready);
        }

        // Deliver epoch 1 LIVE: it acquires, blocks behind the sentinel, and is now
        // in-flight (its (epoch, position) sits in `blocked`). It was NOT dropped.
        scheduler.process_scheduler_input(SchedulerInput::Txn(txn1.clone()));
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
}
