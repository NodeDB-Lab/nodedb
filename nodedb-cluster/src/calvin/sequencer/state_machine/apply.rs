// SPDX-License-Identifier: BUSL-1.1

//! The sequencer state machine's apply path.
//!
//! Runs on every replica (including the leader) as `SequencerEntry` records
//! commit to the sequencer Raft group: decode, check the epoch ordering (see
//! [`crate::calvin::sequencer::epoch_guard`]), fan the batch out to the
//! per-vShard scheduler channels, and advance the watermarks.
//!
//! Synchronous throughout — it runs on the Raft tick thread, so it must never
//! block or do I/O.

use std::sync::atomic::Ordering;

use tokio::sync::mpsc;
use tracing::{error, warn};

use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::sequencer::epoch_guard::{EpochCheck, SequencerHalt, classify};
use crate::calvin::types::SchedulerInput;

use super::core::SequencerStateMachine;

impl SequencerStateMachine {
    /// Apply a committed Raft log entry.
    ///
    /// Decodes the `SequencerEntry`, checks epoch monotonicity, fans out to
    /// per-vshard channels, and advances `last_applied_epoch`.
    ///
    /// `index` is the Raft log index of the committed entry, threaded so drop
    /// bookkeeping can record where the scheduler must catch up from and the
    /// committed-index watermark can advance.
    ///
    /// This method is synchronous (no `.await`). It MUST NOT block or do I/O.
    pub fn apply(&mut self, index: u64, data: &[u8]) {
        // RE-DELIVERY IS NORMAL, NOT DIVERGENCE.
        //
        // Raft collects committed entries from the applied watermark forward,
        // and that watermark only advances once the applier has run. Any commit
        // that lands while a batch is still being applied therefore re-collects
        // the entries already in flight, and the node meets them a second time.
        // A restart is where this actually bites: the whole retained sequencer
        // log replays in one long apply, and a single `Verdict` / reservation
        // proposal landing during it re-delivers every epoch batch in the
        // replayed prefix.
        //
        // Every effect below already ran for this index, so a re-delivery is a
        // no-op. It is emphatically NOT an epoch regression: judging it by the
        // epoch alone reads ordinary restart traffic as a committed epoch being
        // re-minted and halts a replica that is perfectly healthy. The Raft
        // index is what separates the two — a genuine regression arrives at an
        // index this replica has NEVER applied, carrying an epoch it already
        // consumed, and is still caught below.
        //
        // `current_committed_index()` (not the raw field) is deliberate: a
        // freshly constructed state machine has applied nothing, so nothing is
        // "already applied" and a full replay from the top runs in full.
        if self
            .current_committed_index()
            .is_some_and(|applied| index <= applied)
        {
            self.metrics
                .entries_redelivered
                .fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Advance the committed-index watermark for EVERY committed entry, even
        // ones that fail to decode or are skipped as gaps — the entry is durably
        // committed at `index` regardless, so it is a safe replay upper bound.
        self.last_committed_index = index;

        // A membership change of the sequencer group commits in its log too.
        // It is no sequencer entry, and the Raft layer applied it already.
        if crate::conf_change::ConfChange::is_conf_change(data) {
            return;
        }

        let entry: SequencerEntry = match zerompk::from_msgpack(data) {
            Ok(e) => e,
            Err(err) => {
                error!(error = %err, "sequencer state machine: failed to decode entry; skipping");
                return;
            }
        };

        match entry {
            SequencerEntry::EpochBatch { mut batch } => {
                // Re-derive the participating_vshards field which is skipped
                // during serialization (it is computed from write_set collection names).
                for txn in &mut batch.txns {
                    txn.tx_class.restore_derived();
                }

                // A halted state machine has already diverged from the log;
                // resuming fan-out mid-divergence is how a detected fault turns
                // into corrupted lock-table and completion state.
                if self.halted {
                    error!(
                        epoch = batch.epoch,
                        raft_index = index,
                        "sequencer state machine is halted on an epoch regression; \
                         refusing to apply further epoch batches"
                    );
                    return;
                }

                let expected = self.next_epoch();
                let check = classify(expected, batch.epoch);
                match check {
                    EpochCheck::InOrder => {}
                    // Entries are missing on THIS replica, but the batch in
                    // hand is intact and self-describing. Dropping it would
                    // add fresh data loss on top of the entries already
                    // missed, so it is fanned out and the hole is reported —
                    // the scheduler recovers the missed range by replaying the
                    // sequencer Raft log.
                    EpochCheck::Ahead => {
                        error!(
                            epoch = batch.epoch,
                            expected,
                            raft_index = index,
                            "sequencer state machine: epoch gap detected; this node missed \
                             entries. Fanning out the batch in hand; the skipped epochs must \
                             be recovered by log replay."
                        );
                        self.metrics
                            .epochs_skipped_gap
                            .fetch_add(1, Ordering::Relaxed);
                        crate::diag::sequencer_epoch_gap(
                            expected,
                            batch.epoch,
                            check.direction(),
                            batch.txns.len(),
                            index,
                        );
                    }
                    // A NEW log entry (an index never applied here — the
                    // re-delivery guard at the top of `apply` already returned
                    // for the ones that were) carrying an already-consumed
                    // epoch. Every `(epoch, position)` in this batch aliases one
                    // that has already run here, so fanning it out would collide
                    // with live lock-table and completion entries — and dropping
                    // it would silently discard committed writes. Neither is
                    // acceptable: halt and escalate.
                    EpochCheck::Behind => {
                        error!(
                            epoch = batch.epoch,
                            expected,
                            raft_index = index,
                            txns = batch.txns.len(),
                            "sequencer state machine: epoch regression; a committed epoch was \
                             proposed a second time. Halting the sequencer state machine \
                             rather than aliasing committed transaction identities."
                        );
                        self.metrics
                            .epochs_refused_regression
                            .fetch_add(1, Ordering::Relaxed);
                        crate::diag::sequencer_epoch_gap(
                            expected,
                            batch.epoch,
                            check.direction(),
                            batch.txns.len(),
                            index,
                        );
                        self.halted = true;
                        if let Some(hook) = self.unrecoverable_hook.as_ref() {
                            hook(SequencerHalt {
                                expected_epoch: expected,
                                found_epoch: batch.epoch,
                                txns_in_batch: batch.txns.len(),
                                raft_index: index,
                            });
                        }
                        return;
                    }
                }

                let mut fanned_out = 0u64;
                let mut dropped = 0u64;
                // Collected for a single end-of-call diagnostics report,
                // never emitted per-txn — a sustained backpressure storm can
                // drop many positions in one apply() call and per-txn
                // emission would report-storm.
                let mut drop_pairs: Vec<(u32, &'static str)> = Vec::new();

                // Per-vShard count of how many of this epoch's positions target
                // each vShard. Delivered to each scheduler so it knows how many
                // positions of the epoch it must apply before the epoch is fully
                // applied on its vShard — the input to its per-`(epoch, position)`
                // applied gate and fully-applied watermark. Every position of an
                // epoch targeting a given vShard is stamped with the same count.
                // Shared with the replay path via `compute_vshard_txn_counts` so
                // the two paths can never drift.
                let vshard_txn_counts =
                    crate::calvin::sequencer::replay::compute_vshard_txn_counts(&batch);
                for txn in &batch.txns {
                    // Seed the expected vote-participant count deterministically on
                    // EVERY replica (not just the epoch's originating leader), so a
                    // post-failover sequencer leader can still detect vote
                    // completeness and aggregate the verdict.
                    self.completion_registry.seed_expected(
                        crate::calvin::TxnId::new(batch.epoch, txn.position),
                        txn.tx_class.participating_vshards().len(),
                    );
                }

                for txn in &batch.txns {
                    // Build a per-shard copy with epoch_system_ms stamped from
                    // the batch. This is the deterministic time anchor that engine
                    // handlers use instead of reading the wall clock themselves.
                    let mut txn_with_ts = txn.clone();
                    txn_with_ts.epoch_system_ms = batch.epoch_system_ms;

                    // Fan out only to vshards that participate in this txn.
                    let vshards = txn.tx_class.participating_vshards();
                    for vshard_id in vshards {
                        let vshard = vshard_id.as_u32();
                        if let Some(sender) = self.vshard_senders.get(&vshard) {
                            // Stamp the per-vShard position count for the vShard
                            // this copy is delivered to.
                            let mut per_vshard = txn_with_ts.clone();
                            per_vshard.epoch_vshard_txn_count =
                                vshard_txn_counts.get(&vshard).copied().unwrap_or(0);
                            match sender.try_send(SchedulerInput::Txn(per_vshard)) {
                                Ok(()) => {
                                    fanned_out += 1;
                                }
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    warn!(
                                        epoch = batch.epoch,
                                        position = txn.position,
                                        vshard,
                                        "sequencer apply: vshard channel full (backpressure); \
                                         dropping txn. Scheduler will catch up via log replay."
                                    );
                                    self.record_catch_up(vshard, index);
                                    dropped += 1;
                                    drop_pairs.push((vshard, "full"));
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    warn!(
                                        vshard,
                                        epoch = batch.epoch,
                                        "sequencer apply: vshard sender gone; \
                                         scheduler may have exited"
                                    );
                                    self.record_catch_up(vshard, index);
                                    dropped += 1;
                                    drop_pairs.push((vshard, "closed"));
                                }
                            }
                        }
                        // If no sender registered for this vshard, silently skip —
                        // this node may not host that vshard.
                    }
                }

                if dropped > 0 {
                    crate::diag::sequencer_backpressure_drop(batch.epoch, dropped, &drop_pairs);
                }

                self.metrics
                    .txns_fanned_out
                    .fetch_add(fanned_out, Ordering::Relaxed);
                self.metrics
                    .txns_dropped_backpressure
                    .fetch_add(dropped, Ordering::Relaxed);
                self.metrics.epochs_applied.fetch_add(1, Ordering::Relaxed);
                self.last_applied_epoch = batch.epoch;
            }
            SequencerEntry::CompletionAck {
                epoch,
                position,
                vshard_id,
            } => {
                let txn = crate::calvin::TxnId::new(epoch, position);
                self.completion_registry.note_completion_ack(txn, vshard_id);
                self.completion_registry
                    .applied_acks
                    .record(crate::calvin::AppliedCompletionAck {
                        index,
                        txn,
                        vshard_id,
                    });
            }
            // Broadcast the OLLP predicate-mismatch signal to ALL replicas so the
            // coordinator's registry fires wherever it lives (including remote nodes).
            SequencerEntry::OllpMismatch { epoch, position } => {
                self.completion_registry
                    .note_ollp_mismatch(crate::calvin::TxnId::new(epoch, position));
            }
            // Broadcast the terminal routing-failure signal to ALL replicas so
            // the coordinator's registry fires wherever it lives (including
            // remote nodes), mirroring `OllpMismatch`.
            SequencerEntry::TxnRoutingFailed {
                epoch,
                position,
                detail,
            } => {
                self.completion_registry
                    .note_routing_failed(crate::calvin::TxnId::new(epoch, position), detail);
            }
            // Durable per-participant commit vote for a staged cross-shard txn.
            // The registry tallies votes per vshard; once every participant has
            // voted the leader aggregates them into the global verdict that gates
            // the cross-shard commit barrier (flush on commit, drop on abort).
            SequencerEntry::Vote {
                epoch,
                position,
                vshard,
                commit,
            } => {
                self.completion_registry.note_vote(
                    crate::calvin::TxnId::new(epoch, position),
                    vshard,
                    if commit {
                        crate::calvin::ParticipantVote::Commit
                    } else {
                        // A pre-existing abort vote records no reason.
                        crate::calvin::ParticipantVote::Abort(None)
                    },
                );
            }
            // Durable per-participant ABORT vote carrying its cause. Split from
            // `Vote` so the reason reaches the coordinator without changing
            // `Vote`'s wire shape.
            SequencerEntry::AbortVote {
                epoch,
                position,
                vshard,
                reason,
            } => {
                self.completion_registry.note_vote(
                    crate::calvin::TxnId::new(epoch, position),
                    vshard,
                    crate::calvin::ParticipantVote::Abort(Some(reason)),
                );
            }
            // Authoritative commit/abort verdict for a staged cross-shard txn,
            // proposed by the leader once every participant voted. Applied on
            // ALL replicas to store the durable decision, which releases every
            // participant parked at the cross-shard commit barrier into its
            // flush (commit) or drop (abort).
            SequencerEntry::Verdict {
                epoch,
                position,
                commit,
            } => {
                // A pre-existing abort verdict records no reason; `Abort(None)`
                // is exactly that unknown cause.
                let outcome = if commit {
                    crate::calvin::VerdictOutcome::Commit
                } else {
                    crate::calvin::VerdictOutcome::Abort(None)
                };
                self.completion_registry
                    .note_verdict(crate::calvin::TxnId::new(epoch, position), outcome);
            }
            // Authoritative ABORT verdict carrying the winning participant
            // reason. Split from `Verdict` for the same wire-shape reason as
            // `AbortVote`.
            SequencerEntry::AbortVerdict {
                epoch,
                position,
                reason,
            } => {
                self.completion_registry.note_verdict(
                    crate::calvin::TxnId::new(epoch, position),
                    crate::calvin::VerdictOutcome::Abort(Some(reason)),
                );
            }
            // Fan a hot-key read reservation out to its owning vShard's scheduler,
            // which installs the SHARED lock. Same `try_send` backpressure
            // discipline as the epoch-batch fan-out: a full/closed channel logs
            // and drops (this node may not host the vShard, in which case there is
            // simply no sender registered).
            SequencerEntry::ReserveRead { owner, vshard, key } => {
                if let Some(sender) = self.vshard_senders.get(&vshard) {
                    match sender.try_send(SchedulerInput::Reserve { owner, key }) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!(
                                vshard,
                                owner_epoch = owner.epoch,
                                owner_position = owner.position,
                                "sequencer apply: vshard channel full (backpressure); \
                                 dropping read reservation"
                            );
                            self.record_catch_up(vshard, index);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            warn!(
                                vshard,
                                "sequencer apply: vshard sender gone; \
                                 scheduler may have exited (reservation)"
                            );
                            self.record_catch_up(vshard, index);
                        }
                    }
                }
            }
            // Fan a backup's cut marker out to every vShard scheduler this
            // node hosts. Same `try_send` discipline as `ReserveRead`: a
            // dropped marker is recovered by the scheduler's catch-up drain,
            // which replays it in log order.
            SequencerEntry::CutMarker { hlc } => {
                for (&vshard, sender) in &self.vshard_senders {
                    match sender.try_send(SchedulerInput::CutMarker { hlc }) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!(
                                vshard,
                                hlc,
                                "sequencer apply: vshard channel full (backpressure); \
                                 dropping cut marker"
                            );
                            self.record_catch_up(vshard, index);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            warn!(
                                vshard,
                                "sequencer apply: vshard sender gone; \
                                 scheduler may have exited (cut marker)"
                            );
                            self.record_catch_up(vshard, index);
                        }
                    }
                }
            }
            // Fan a reservation release out to its owning vShard's scheduler.
            // Same `try_send` discipline as `ReserveRead`.
            SequencerEntry::ReleaseReservation {
                owner,
                vshard,
                reason,
            } => {
                if let Some(sender) = self.vshard_senders.get(&vshard) {
                    match sender.try_send(SchedulerInput::Release { owner, reason }) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!(
                                vshard,
                                owner_epoch = owner.epoch,
                                owner_position = owner.position,
                                "sequencer apply: vshard channel full (backpressure); \
                                 dropping reservation release"
                            );
                            self.record_catch_up(vshard, index);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            warn!(
                                vshard,
                                "sequencer apply: vshard sender gone; \
                                 scheduler may have exited (reservation release)"
                            );
                            self.record_catch_up(vshard, index);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::calvin::CalvinCompletionRegistry;
    use crate::calvin::types::{
        EngineKeySet, EpochBatch, ReadWriteSet, SequencedTxn, SortedVec, TxClass,
    };
    use nodedb_types::{
        TenantId,
        id::{DatabaseId, VShardId},
    };

    fn find_two_distinct_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..512 {
            let name = format!("col_{i}");
            let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
            if let Some((ref fname, fv)) = first {
                if fv != vshard {
                    return (fname.clone(), name);
                }
            } else {
                first = Some((name, vshard));
            }
        }
        panic!("could not find two distinct-vshard collections in 512 tries");
    }

    fn make_tx_class_for_vshards(vshard_a: u32, vshard_b: u32) -> (TxClass, u32, u32) {
        // Find collections that map to the given vshards.
        // Since we can't control the hash, we use the known pattern from the type:
        // participating_vshards() is derived from collection names.
        // We'll use find_two_distinct_collections and use whatever vshards they hash to.
        let (col_a, col_b) = find_two_distinct_collections();
        let _ = (vshard_a, vshard_b); // actual vshard ids come from the collection hash
        let real_va = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_a).as_u32();
        let real_vb = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &col_b).as_u32();
        let write_set = ReadWriteSet::new(vec![
            EngineKeySet::Document {
                collection: col_a,
                surrogates: SortedVec::new(vec![1]),
            },
            EngineKeySet::Document {
                collection: col_b,
                surrogates: SortedVec::new(vec![2]),
            },
        ]);
        let tx_class = TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![],
            TenantId::new(1),
            None,
            crate::calvin::types::VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        (tx_class, real_va, real_vb)
    }

    fn make_batch_with_two_vshards() -> (EpochBatch, u32, u32) {
        let (tx_class, va, vb) = make_tx_class_for_vshards(0, 1);
        let batch = EpochBatch {
            epoch: 0,
            txns: vec![SequencedTxn {
                epoch: 0,
                position: 0,
                tx_class,
                epoch_system_ms: 1_700_000_000_000,
                epoch_vshard_txn_count: 1,
                lock_owner: None,
            }],
            epoch_system_ms: 1_700_000_000_000,
        };
        (batch, va, vb)
    }

    fn encode_entry(entry: &SequencerEntry) -> Vec<u8> {
        zerompk::to_msgpack_vec(entry).expect("encode")
    }

    #[test]
    fn apply_on_fresh_state_increments_last_applied_epoch() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, _) = mpsc::channel(64);
        let (tx_b, _) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());
        assert_eq!(sm.last_applied_epoch(), None);

        let data = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(1, &data);

        assert_eq!(sm.last_applied_epoch(), Some(0));
        assert_eq!(sm.metrics.epochs_applied.load(Ordering::Relaxed), 1);
    }

    /// A forward gap still trips the detector — but the batch in hand is intact,
    /// so it is fanned out rather than dropped. Only the epochs BETWEEN the two
    /// went missing, and those are recovered by log replay.
    #[test]
    fn forward_gap_is_detected_and_the_batch_in_hand_is_still_fanned_out() {
        let (mut batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, mut rx_a) = mpsc::channel(64);
        let (tx_b, mut rx_b) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        // Apply epoch 0.
        let data0 = encode_entry(&SequencerEntry::EpochBatch {
            batch: batch.clone(),
        });
        sm.apply(1, &data0);
        assert_eq!(sm.last_applied_epoch(), Some(0));
        assert!(rx_a.try_recv().is_ok());
        assert!(rx_b.try_recv().is_ok());

        // Apply epoch 2 (skip epoch 1 → gap).
        batch.epoch = 2;
        for txn in &mut batch.txns {
            txn.epoch = 2;
        }
        let data2 = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(2, &data2);

        // The detector fired...
        assert_eq!(sm.metrics.epochs_skipped_gap.load(Ordering::Relaxed), 1);
        // ...and the epoch advanced to the one received.
        assert_eq!(sm.last_applied_epoch(), Some(2));
        // ...but epoch 2's transactions were NOT dropped: dropping them would
        // add fresh loss on top of the entries this replica already missed.
        assert!(
            rx_a.try_recv().is_ok(),
            "the intact batch must still reach vshard A"
        );
        assert!(
            rx_b.try_recv().is_ok(),
            "the intact batch must still reach vshard B"
        );
        // A forward gap is recoverable, so it must NOT halt the state machine.
        assert!(!sm.is_halted());
        assert_eq!(
            sm.metrics.epochs_refused_regression.load(Ordering::Relaxed),
            0
        );
    }

    /// An already-consumed epoch arriving a second time is the restart-collision
    /// shape: its `(epoch, position)` identities alias committed ones, so it can
    /// neither be applied nor silently dropped. The state machine halts and
    /// escalates to the host's fail-stop hook.
    #[test]
    fn epoch_regression_halts_and_escalates_instead_of_dropping_the_batch() {
        let (mut batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, mut rx_a) = mpsc::channel(64);
        let (tx_b, mut rx_b) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);

        let halts: Arc<std::sync::Mutex<Vec<SequencerHalt>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = Arc::clone(&halts);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached())
            .with_unrecoverable_hook(Arc::new(move |halt| {
                sink.lock().unwrap_or_else(|p| p.into_inner()).push(halt);
            }));

        // Committed history: epochs 0 and 1.
        for epoch in 0..=1u64 {
            batch.epoch = epoch;
            for txn in &mut batch.txns {
                txn.epoch = epoch;
            }
            let data = encode_entry(&SequencerEntry::EpochBatch {
                batch: batch.clone(),
            });
            sm.apply(epoch + 1, &data);
        }
        assert_eq!(sm.next_epoch(), 2);
        while rx_a.try_recv().is_ok() {}
        while rx_b.try_recv().is_ok() {}

        // A restarted leader re-mints epoch 0 and proposes it after that history.
        batch.epoch = 0;
        for txn in &mut batch.txns {
            txn.epoch = 0;
        }
        let duplicate = encode_entry(&SequencerEntry::EpochBatch {
            batch: batch.clone(),
        });
        sm.apply(3, &duplicate);

        assert!(sm.is_halted(), "an epoch regression must halt the replica");
        assert_eq!(
            sm.metrics.epochs_refused_regression.load(Ordering::Relaxed),
            1
        );
        // Not counted as a forward gap — the two are different bugs.
        assert_eq!(sm.metrics.epochs_skipped_gap.load(Ordering::Relaxed), 0);
        // Colliding identities must never reach a scheduler.
        assert!(rx_a.try_recv().is_err());
        assert!(rx_b.try_recv().is_err());

        // The host was told, with the facts it needs to fail loudly.
        let recorded = halts.lock().unwrap_or_else(|p| p.into_inner()).clone();
        assert_eq!(
            recorded,
            vec![SequencerHalt {
                expected_epoch: 2,
                found_epoch: 0,
                txns_in_batch: 1,
                raft_index: 3,
            }]
        );

        // Once halted, further epoch batches are refused rather than half-applied.
        batch.epoch = 2;
        for txn in &mut batch.txns {
            txn.epoch = 2;
        }
        let after = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(4, &after);
        assert!(rx_a.try_recv().is_err());
        assert_eq!(sm.metrics.epochs_applied.load(Ordering::Relaxed), 2);
        // The committed-index watermark still advances: the entry IS committed
        // at that index regardless of this replica refusing to act on it.
        assert_eq!(sm.current_committed_index(), Some(4));
        // Exactly one escalation — the halt is latched, not re-fired per entry.
        assert_eq!(halts.lock().unwrap_or_else(|p| p.into_inner()).len(), 1);
    }

    /// Re-delivery of an already-applied entry is what a restart replay
    /// overlapping a concurrent proposal produces: Raft re-collects from the
    /// applied watermark, so the epochs already in flight arrive a second time.
    /// That must be an idempotent no-op — no second fan-out, no epoch movement,
    /// and above all no halt, because halting here takes a healthy node out on
    /// ordinary restart traffic.
    #[test]
    fn epoch_redelivered_after_restart_is_a_no_op_and_does_not_halt() {
        let (mut batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, mut rx_a) = mpsc::channel(64);
        let (tx_b, mut rx_b) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);

        let halts: Arc<std::sync::Mutex<Vec<SequencerHalt>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = Arc::clone(&halts);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached())
            .with_unrecoverable_hook(Arc::new(move |halt| {
                sink.lock().unwrap_or_else(|p| p.into_inner()).push(halt);
            }));

        // Restart replay of a retained log holding epochs 0..=2 at indexes 1..=3.
        for epoch in 0..=2u64 {
            batch.epoch = epoch;
            for txn in &mut batch.txns {
                txn.epoch = epoch;
            }
            let data = encode_entry(&SequencerEntry::EpochBatch {
                batch: batch.clone(),
            });
            sm.apply(epoch + 1, &data);
        }
        assert_eq!(sm.last_applied_epoch(), Some(2));
        while rx_a.try_recv().is_ok() {}
        while rx_b.try_recv().is_ok() {}

        // The SAME committed prefix arrives again — every index already applied.
        for epoch in 0..=2u64 {
            batch.epoch = epoch;
            for txn in &mut batch.txns {
                txn.epoch = epoch;
            }
            let data = encode_entry(&SequencerEntry::EpochBatch {
                batch: batch.clone(),
            });
            sm.apply(epoch + 1, &data);
        }

        assert!(
            !sm.is_halted(),
            "a re-delivered committed entry is normal Raft behaviour, not a regression"
        );
        assert!(halts.lock().unwrap_or_else(|p| p.into_inner()).is_empty());
        assert_eq!(
            sm.metrics.epochs_refused_regression.load(Ordering::Relaxed),
            0
        );
        assert_eq!(sm.metrics.entries_redelivered.load(Ordering::Relaxed), 3);
        // No second fan-out: the schedulers already ran these positions, and a
        // duplicate delivery would re-enter them under identities in flight.
        assert!(rx_a.try_recv().is_err());
        assert!(rx_b.try_recv().is_err());
        // Watermarks are unmoved — neither advanced nor rewound.
        assert_eq!(sm.last_applied_epoch(), Some(2));
        assert_eq!(sm.current_committed_index(), Some(3));
        assert_eq!(sm.metrics.epochs_applied.load(Ordering::Relaxed), 3);

        // The node is still sequencing: the next genuinely new entry applies.
        batch.epoch = 3;
        for txn in &mut batch.txns {
            txn.epoch = 3;
        }
        let data = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(4, &data);
        assert_eq!(sm.last_applied_epoch(), Some(3));
        assert!(rx_a.try_recv().is_ok());
    }

    /// The re-delivery guard must not swallow the fault it sits in front of: a
    /// NEW log entry re-minting a consumed epoch is still unrecoverable.
    #[test]
    fn regression_at_a_new_index_still_halts_after_a_redelivery() {
        let (mut batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, _rx_a) = mpsc::channel(64);
        let (tx_b, _rx_b) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        for epoch in 0..=1u64 {
            batch.epoch = epoch;
            for txn in &mut batch.txns {
                txn.epoch = epoch;
            }
            let data = encode_entry(&SequencerEntry::EpochBatch {
                batch: batch.clone(),
            });
            sm.apply(epoch + 1, &data);
        }

        // A benign re-delivery of index 1 first — absorbed, no halt.
        batch.epoch = 0;
        for txn in &mut batch.txns {
            txn.epoch = 0;
        }
        let replayed = encode_entry(&SequencerEntry::EpochBatch {
            batch: batch.clone(),
        });
        sm.apply(1, &replayed);
        assert!(!sm.is_halted());

        // A restarted leader re-minting epoch 0 lands at a NEW index. Same
        // epoch, different meaning — this one must halt.
        let duplicate = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(3, &duplicate);
        assert!(sm.is_halted());
        assert_eq!(
            sm.metrics.epochs_refused_regression.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn per_vshard_fanout_sends_only_to_participating_vshards() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, mut rx_a) = mpsc::channel(64);
        let (tx_b, mut rx_b) = mpsc::channel(64);
        // A third vshard with no txns.
        let (tx_c, mut rx_c) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        senders.insert(999, tx_c);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        let data = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(1, &data);

        // Both participating vshards should have received the txn.
        assert!(rx_a.try_recv().is_ok(), "vshard A should have received txn");
        assert!(rx_b.try_recv().is_ok(), "vshard B should have received txn");
        // The unrelated vshard should be empty.
        assert!(
            rx_c.try_recv().is_err(),
            "vshard C should not have received txn"
        );
    }

    #[test]
    fn try_send_on_full_channel_logs_and_drops_without_blocking() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        // Capacity 0 is not allowed; use capacity 1 and fill it first.
        let (tx_a, _rx_a) = mpsc::channel(1);
        let (tx_b, _rx_b) = mpsc::channel(1);
        // Pre-fill channel A so it is full.
        let pre_fill: SequencedTxn = batch.txns[0].clone();
        let _ = tx_a.try_send(SchedulerInput::Txn(pre_fill));
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        let data = encode_entry(&SequencerEntry::EpochBatch { batch });
        // Must not panic or block.
        sm.apply(1, &data);

        // At least one drop was recorded (vshard A was full).
        assert!(sm.metrics.txns_dropped_backpressure.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn next_epoch_is_zero_on_fresh_state_machine() {
        let sm =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());
        assert_eq!(sm.next_epoch(), 0);
    }

    #[test]
    fn next_epoch_increments_after_apply() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, _) = mpsc::channel(64);
        let (tx_b, _) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        let data = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(1, &data);

        assert_eq!(sm.next_epoch(), 1);
    }

    #[tokio::test]
    async fn apply_txn_routing_failed_dispatches_to_completion_registry() {
        let registry = CalvinCompletionRegistry::new_detached();
        let mut sm = SequencerStateMachine::new(HashMap::new(), Arc::clone(&registry));

        let data = encode_entry(&SequencerEntry::TxnRoutingFailed {
            epoch: 5,
            position: 2,
            detail: "unroutable plan".to_owned(),
        });
        sm.apply(1, &data);

        // The registry's waiter (registered AFTER apply) must still observe
        // the failure — `note_routing_failed` persists it on the entry.
        let rx = registry.register_completion(crate::calvin::TxnId::new(5, 2), 1);
        let outcome = rx.await.expect("routing failure fires");
        assert_eq!(
            outcome,
            crate::calvin::AttemptOutcome::Failed {
                detail: "unroutable plan".to_owned()
            }
        );
        // TxnRoutingFailed is not an EpochBatch, so it must not perturb the
        // epoch counter (mirrors OllpMismatch's non-effect on last_applied_epoch).
        assert_eq!(sm.last_applied_epoch(), None);
    }

    #[tokio::test]
    async fn apply_verdict_stores_decision_without_perturbing_epoch() {
        let registry = CalvinCompletionRegistry::new_detached();
        let mut sm = SequencerStateMachine::new(HashMap::new(), Arc::clone(&registry));
        let txn = crate::calvin::TxnId::new(9, 4);

        let data = encode_entry(&SequencerEntry::Verdict {
            epoch: 9,
            position: 4,
            commit: true,
        });
        sm.apply(1, &data);

        // The verdict is stored authoritatively on every replica.
        assert_eq!(registry.verdict(txn), Some(true));
        // Verdict is not an EpochBatch, so it must not perturb the epoch counter
        // (mirrors OllpMismatch/TxnRoutingFailed's non-effect).
        assert_eq!(sm.last_applied_epoch(), None);
    }

    #[tokio::test]
    async fn apply_abort_verdict_reports_its_reason_to_the_coordinator() {
        let registry = CalvinCompletionRegistry::new_detached();
        let mut sm = SequencerStateMachine::new(HashMap::new(), Arc::clone(&registry));
        let txn = crate::calvin::TxnId::new(9, 5);

        let data = encode_entry(&SequencerEntry::AbortVerdict {
            epoch: 9,
            position: 5,
            reason: crate::calvin::AbortReason::ParticipantError,
        });
        sm.apply(1, &data);

        // The participant gate still reads a plain abort...
        assert_eq!(registry.verdict(txn), Some(false));
        // ...and the coordinator gets the cause, not a blanket conflict.
        let rx = registry.register_completion(txn, 1);
        registry.note_completion_ack(txn, 3);
        assert_eq!(
            rx.await.expect("completion fires"),
            crate::calvin::AttemptOutcome::Aborted {
                reason: Some(crate::calvin::AbortReason::ParticipantError)
            }
        );
        assert_eq!(sm.last_applied_epoch(), None);
    }

    #[tokio::test]
    async fn apply_abort_vote_tallies_with_its_reason() {
        let registry = CalvinCompletionRegistry::new_detached();
        let mut sm = SequencerStateMachine::new(HashMap::new(), Arc::clone(&registry));
        let txn = crate::calvin::TxnId::new(9, 6);
        registry.seed_expected(txn, 1);

        let data = encode_entry(&SequencerEntry::AbortVote {
            epoch: 9,
            position: 6,
            vshard: 3,
            reason: crate::calvin::AbortReason::ParticipantError,
        });
        sm.apply(1, &data);

        assert_eq!(
            registry.vote_tally(txn).and_then(|t| t.get(&3).copied()),
            Some(crate::calvin::ParticipantVote::Abort(Some(
                crate::calvin::AbortReason::ParticipantError
            )))
        );
    }

    #[tokio::test]
    async fn apply_legacy_abort_vote_tallies_without_a_reason() {
        // A `Vote { commit: false }` durable before abort reasons existed must
        // still decode and tally — with an unknown cause, never an invented one.
        let registry = CalvinCompletionRegistry::new_detached();
        let mut sm = SequencerStateMachine::new(HashMap::new(), Arc::clone(&registry));
        let txn = crate::calvin::TxnId::new(9, 7);

        let data = encode_entry(&SequencerEntry::Vote {
            epoch: 9,
            position: 7,
            vshard: 3,
            commit: false,
        });
        sm.apply(1, &data);

        assert_eq!(
            registry.vote_tally(txn).and_then(|t| t.get(&3).copied()),
            Some(crate::calvin::ParticipantVote::Abort(None))
        );
    }

    #[test]
    fn catch_up_from_records_dropped_index_and_min_collapses() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        // Capacity 1, pre-filled → vshard A is full and every fan-out drops.
        let (tx_a, _rx_a) = mpsc::channel(1);
        // vshard B has room and a live receiver → never drops.
        let (tx_b, _rx_b) = mpsc::channel(64);
        let _ = tx_a.try_send(SchedulerInput::Txn(batch.txns[0].clone()));
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        // First drop for vshard A at Raft index 4.
        let data0 = encode_entry(&SequencerEntry::EpochBatch {
            batch: batch.clone(),
        });
        sm.apply(4, &data0);

        // Second drop for the SAME vshard at a LATER Raft index. Min-collapse
        // must keep the EARLIER index — replay has to start at the first miss,
        // not the most recent one. (Raft delivers indexes in increasing order,
        // so a later drop is always the higher index.)
        let mut batch1 = batch.clone();
        batch1.epoch = 1;
        for txn in &mut batch1.txns {
            txn.epoch = 1;
        }
        let data1 = encode_entry(&SequencerEntry::EpochBatch { batch: batch1 });
        sm.apply(10, &data1);

        // The recorded catch-up index is the SMALLEST dropped index (4), and the
        // repeated drops for one vShard did not grow the map (a single entry that
        // min-collapsed). vshard B never dropped, so it has no entry.
        assert_eq!(sm.take_catch_up_from(vb), None);
        assert_eq!(sm.take_catch_up_from(va), Some(4));
        // TAKE semantics: the entry is cleared, so a second take returns None.
        assert_eq!(sm.take_catch_up_from(va), None);
    }

    /// PEEK must not consume: the scheduler drain reads the armed index, and
    /// only clears it after a confirmed replay. A take-then-early-return (the
    /// old shape) silently lost the miss when the replay could not complete.
    #[test]
    fn peek_catch_up_from_does_not_consume() {
        let (batch, va, _vb) = make_batch_with_two_vshards();
        let (tx_a, _rx_a) = mpsc::channel(1);
        let _ = tx_a.try_send(SchedulerInput::Txn(batch.txns[0].clone()));
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        sm.apply(9, &encode_entry(&SequencerEntry::EpochBatch { batch }));

        // Repeated peeks keep returning the same armed index.
        assert_eq!(sm.peek_catch_up_from(va), Some(9));
        assert_eq!(sm.peek_catch_up_from(va), Some(9));
    }

    /// Clearing is bounded by the replayed upper bound: a miss covered by the
    /// replay is cleared, one recorded ABOVE it survives for the next drain.
    #[test]
    fn clear_catch_up_up_to_respects_replayed_upper_bound() {
        let senders = HashMap::new();
        let sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());
        let v = 42u32;

        // Armed at 5, replay covered through 10 → cleared.
        sm.arm_catch_up_from(v, 5);
        sm.clear_catch_up_up_to(v, 10);
        assert_eq!(sm.peek_catch_up_from(v), None);

        // Armed at 20, replay only covered through 10 → still armed.
        sm.arm_catch_up_from(v, 20);
        sm.clear_catch_up_up_to(v, 10);
        assert_eq!(sm.peek_catch_up_from(v), Some(20));
    }

    /// The sequencer-log compaction hold-down floors on the LOWEST armed index
    /// across all vShards, so no replica's replay range is compacted away.
    #[test]
    fn min_catch_up_from_is_lowest_armed_index_across_vshards() {
        let senders = HashMap::new();
        let sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());
        assert_eq!(sm.min_catch_up_from(), None);

        sm.arm_catch_up_from(1, 30);
        sm.arm_catch_up_from(2, 12);
        sm.arm_catch_up_from(3, 25);
        assert_eq!(sm.min_catch_up_from(), Some(12));

        // Draining the lowest lifts the floor to the next outstanding miss.
        sm.clear_catch_up_up_to(2, 12);
        assert_eq!(sm.min_catch_up_from(), Some(25));

        sm.clear_catch_up_up_to(1, 30);
        sm.clear_catch_up_up_to(3, 25);
        assert_eq!(sm.min_catch_up_from(), None);
    }

    #[test]
    fn catch_up_from_records_dropped_index_on_closed_channel() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, rx_a) = mpsc::channel(64);
        let (tx_b, _rx_b) = mpsc::channel(64);
        // Close vshard A's receiver → the sender reports Closed on try_send.
        drop(rx_a);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        let data = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(7, &data);

        // The Closed drop is recorded at the entry's index for the closed vShard.
        assert_eq!(sm.take_catch_up_from(va), Some(7));
        assert_eq!(sm.take_catch_up_from(vb), None);
    }

    #[test]
    fn current_committed_index_advances_for_every_applied_entry() {
        let mut sm =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());
        assert_eq!(sm.current_committed_index(), None);

        // A non-EpochBatch entry still advances the committed-index watermark.
        let data = encode_entry(&SequencerEntry::Verdict {
            epoch: 1,
            position: 0,
            commit: true,
        });
        sm.apply(42, &data);
        assert_eq!(sm.current_committed_index(), Some(42));
    }
}
