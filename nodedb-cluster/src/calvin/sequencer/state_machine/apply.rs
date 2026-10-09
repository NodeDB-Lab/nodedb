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

use tracing::{error, warn};

use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::sequencer::epoch_guard::{EpochCheck, SequencerHalt, classify};
use crate::calvin::types::{EpochBatch, SchedulerInput, TxnIdWire};
use crate::calvin::{ParticipantVote, TxnId, VerdictOutcome};

use super::core::{Delivery, SequencerStateMachine};

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
            SequencerEntry::EpochBatch { batch } => self.apply_epoch_batch(index, batch),
            SequencerEntry::CompletionAck {
                epoch,
                position,
                vshard_id,
                result,
                from_node,
            } => self.apply_completion_ack(
                index,
                TxnId::new(epoch, position),
                vshard_id,
                from_node,
                result,
            ),
            // Durable per-participant votes for a staged cross-shard txn. The
            // registry tallies them per vshard. Once every participant voted,
            // the leader aggregates them into the global verdict that gates the
            // cross-shard commit barrier (flush on commit, drop on abort).
            SequencerEntry::Vote {
                epoch,
                position,
                vshard,
            } => self.completion_registry.note_vote(
                TxnId::new(epoch, position),
                vshard,
                ParticipantVote::Commit,
            ),
            SequencerEntry::AbortVote {
                epoch,
                position,
                vshard,
                reason,
            } => self.completion_registry.note_vote(
                TxnId::new(epoch, position),
                vshard,
                ParticipantVote::Abort(reason),
            ),
            // Authoritative verdict for a staged cross-shard txn, proposed by
            // the leader once every participant voted. Applied on ALL replicas
            // to store the durable decision, which releases every participant
            // parked at the cross-shard commit barrier into its flush (commit)
            // or drop (abort).
            SequencerEntry::Verdict { epoch, position } => self
                .completion_registry
                .note_verdict(TxnId::new(epoch, position), VerdictOutcome::Commit),
            SequencerEntry::AbortVerdict {
                epoch,
                position,
                reason,
            } => self
                .completion_registry
                .note_verdict(TxnId::new(epoch, position), VerdictOutcome::Abort(reason)),
            // The owning vShard's scheduler installs the SHARED lock.
            SequencerEntry::ReserveRead { owner, vshard, key } => self.forward_reservation(
                index,
                vshard,
                owner,
                SchedulerInput::Reserve { owner, key },
                "read reservation",
            ),
            // The owning vShard's scheduler releases every shared lock of `owner`.
            SequencerEntry::ReleaseReservation {
                owner,
                vshard,
                reason,
            } => self.forward_reservation(
                index,
                vshard,
                owner,
                SchedulerInput::Release { owner, reason },
                "reservation release",
            ),
            SequencerEntry::EpochFloor {
                next_epoch,
                epoch_system_ms,
            } => self.apply_epoch_floor(next_epoch, epoch_system_ms),
            SequencerEntry::CutMarker { hlc, restore_point } => {
                self.apply_cut_marker(index, hlc, restore_point)
            }
            SequencerEntry::TxnPart {
                epoch,
                position,
                index: part,
                first_task,
                targets,
                plans,
                chunk,
            } => self.apply_txn_part(
                index,
                TxnId::new(epoch, position),
                super::parts::PartEntry {
                    index: part,
                    first_task,
                    targets,
                    plans,
                    chunk,
                },
            ),
            SequencerEntry::TxnPartsAbandoned { epoch, position } => {
                self.apply_parts_abandoned(index, TxnId::new(epoch, position))
            }
        }
    }

    /// Apply a committed epoch batch: re-derive each txn's participants,
    /// check the epoch order, then fan the batch out.
    fn apply_epoch_batch(&mut self, index: u64, mut batch: EpochBatch) {
        // Re-derive the participating_vshards field which is skipped
        // during serialization (it is computed from write_set collection names).
        // A class whose participants cannot be derived makes the entry
        // as unusable as one that fails to decode, so it is skipped the
        // same way.
        for txn in &mut batch.txns {
            if let Err(err) = txn.tx_class.restore_derived() {
                error!(
                    epoch = batch.epoch,
                    raft_index = index,
                    error = %err,
                    "sequencer state machine: epoch batch carries a transaction \
                     with underivable participants; skipping entry"
                );
                crate::diag::sequencer_participants_underivable(
                    batch.epoch,
                    index,
                    &err.to_string(),
                );
                return;
            }
        }
        if self.epoch_in_order(index, &batch) {
            self.last_epoch_system_ms = Some(
                self.last_epoch_system_ms
                    .map_or(batch.epoch_system_ms, |seen| {
                        seen.max(batch.epoch_system_ms)
                    }),
            );
            self.open_multi_parts(index, &batch);
            self.fan_out_epoch_batch(index, batch);
        }
    }

    /// Whether `batch` may be fanned out. `false` when the state machine is
    /// halted or the batch re-mints a consumed epoch. A forward gap is
    /// reported and still returns `true`.
    fn epoch_in_order(&mut self, index: u64, batch: &EpochBatch) -> bool {
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
            return false;
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
                return false;
            }
        }
        true
    }

    /// Fan each txn of `batch` out to the scheduler of every participating
    /// vShard this node hosts, then advance the applied epoch.
    fn fan_out_epoch_batch(&mut self, index: u64, mut batch: EpochBatch) {
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
        let vshard_txn_counts = crate::calvin::sequencer::replay::compute_vshard_txn_counts(&batch);
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

        let epoch_system_ms = batch.epoch_system_ms;
        for txn in &mut batch.txns {
            // Stamp epoch_system_ms from the batch. This is the
            // deterministic time anchor that engine handlers use
            // instead of reading the wall clock themselves.
            txn.epoch_system_ms = epoch_system_ms;

            // Fan out only to vshards that participate in this txn.
            let vshards = txn.tx_class.participating_vshards();
            for vshard_id in vshards {
                let vshard = vshard_id.as_u32();
                // This node may not host the vShard: then nothing is sent.
                if !self.vshard_senders.contains_key(&vshard) {
                    continue;
                }
                // Stamp the per-vShard position count for the vShard this
                // copy is delivered to.
                let mut per_vshard = txn.clone();
                per_vshard.epoch_vshard_txn_count =
                    vshard_txn_counts.get(&vshard).copied().unwrap_or(0);
                match self.deliver(index, vshard, SchedulerInput::Txn(Box::new(per_vshard))) {
                    Delivery::NotHosted => {}
                    Delivery::Sent => {
                        fanned_out += 1;
                        self.undurable
                            .note(index, vshard, batch.epoch, txn.position);
                    }
                    // The armed catch-up replays it in log order.
                    Delivery::Deferred => {
                        dropped += 1;
                        drop_pairs.push((vshard, "catch_up"));
                    }
                    Delivery::DroppedFull => {
                        warn!(
                            epoch = batch.epoch,
                            position = txn.position,
                            vshard,
                            "sequencer apply: vshard channel full (backpressure); \
                             txn left to the catch-up replay"
                        );
                        dropped += 1;
                        drop_pairs.push((vshard, "full"));
                    }
                    Delivery::DroppedClosed => {
                        warn!(
                            vshard,
                            epoch = batch.epoch,
                            "sequencer apply: vshard sender gone; scheduler may have exited"
                        );
                        dropped += 1;
                        drop_pairs.push((vshard, "closed"));
                    }
                }
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

    /// Record `vshard_id`'s completion ack for `txn`, with its apply result,
    /// and log the ack at its Raft `index`.
    ///
    /// The first ack of a vShard in log order answers for it, on every node
    /// alike: the result depends on the log alone. Only the vShard's
    /// data-group leader proposes an ack, so a node that left the group adds
    /// none. `from_node` names the proposer for tracing.
    fn apply_completion_ack(
        &self,
        index: u64,
        txn: TxnId,
        vshard_id: u32,
        from_node: u64,
        result: Vec<u8>,
    ) {
        tracing::trace!(
            epoch = txn.epoch,
            position = txn.position,
            vshard_id,
            from_node,
            "sequencer apply: completion ack"
        );
        self.completion_registry
            .note_completion_ack_with(txn, vshard_id, result);
        self.completion_registry
            .applied_acks
            .record(crate::calvin::AppliedCompletionAck {
                index,
                txn,
                vshard_id,
            });
    }

    /// Send a reservation `input` for `owner` to `vshard`'s scheduler.
    ///
    /// Same delivery as the epoch-batch fan-out: an armed vShard, or a full
    /// or closed channel, leaves the input to the catch-up replay. This node
    /// may not host the vShard. Then nothing is sent. `what` names the input
    /// in the warnings.
    fn forward_reservation(
        &self,
        index: u64,
        vshard: u32,
        owner: TxnIdWire,
        input: SchedulerInput,
        what: &'static str,
    ) {
        match self.deliver(index, vshard, input) {
            Delivery::NotHosted | Delivery::Sent | Delivery::Deferred => {}
            Delivery::DroppedFull => warn!(
                vshard,
                owner_epoch = owner.epoch,
                owner_position = owner.position,
                what,
                "sequencer apply: vshard channel full (backpressure); \
                 reservation input left to the catch-up replay"
            ),
            Delivery::DroppedClosed => warn!(
                vshard,
                what, "sequencer apply: vshard sender gone; scheduler may have exited"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use super::*;
    use crate::calvin::CalvinCompletionRegistry;
    use crate::calvin::types::{
        EngineKeySet, EpochBatch, ReadWriteSet, SequencedTxn, SortedVec, TxClass,
    };
    use nodedb_types::{
        TenantId,
        id::{CollectionKey, DatabaseId},
    };

    fn find_two_distinct_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..512 {
            let name = format!("col_{i}");
            let vshard = CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
                .vshard()
                .as_u32();
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
        let real_va = CollectionKey::from_bare(DatabaseId::DEFAULT, &col_a)
            .vshard()
            .as_u32();
        let real_vb = CollectionKey::from_bare(DatabaseId::DEFAULT, &col_b)
            .vshard()
            .as_u32();
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
        let _ = tx_a.try_send(SchedulerInput::Txn(Box::new(pre_fill)));
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
    async fn apply_verdict_stores_decision_without_perturbing_epoch() {
        let registry = CalvinCompletionRegistry::new_detached();
        let mut sm = SequencerStateMachine::new(HashMap::new(), Arc::clone(&registry));
        let txn = crate::calvin::TxnId::new(9, 4);

        let data = encode_entry(&SequencerEntry::Verdict {
            epoch: 9,
            position: 4,
        });
        sm.apply(1, &data);

        // The verdict is stored authoritatively on every replica.
        assert_eq!(registry.verdict(txn), Some(true));
        // Verdict is not an EpochBatch, so it must not perturb the epoch counter.
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
                reason: crate::calvin::AbortReason::ParticipantError
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
            Some(crate::calvin::ParticipantVote::Abort(
                crate::calvin::AbortReason::ParticipantError
            ))
        );
    }

    #[tokio::test]
    async fn apply_vote_tallies_a_commit_vote() {
        let registry = CalvinCompletionRegistry::new_detached();
        let mut sm = SequencerStateMachine::new(HashMap::new(), Arc::clone(&registry));
        let txn = crate::calvin::TxnId::new(9, 7);

        let data = encode_entry(&SequencerEntry::Vote {
            epoch: 9,
            position: 7,
            vshard: 3,
        });
        sm.apply(1, &data);

        assert_eq!(
            registry.vote_tally(txn).and_then(|t| t.get(&3).copied()),
            Some(crate::calvin::ParticipantVote::Commit)
        );
    }

    /// Every txn a replica fans out carries the batch's `epoch_system_ms`,
    /// whatever value the proposer left on the txn itself.
    #[test]
    fn fanned_out_txns_carry_the_batch_epoch_system_ms() {
        let (mut batch, va, vb) = make_batch_with_two_vshards();
        batch.epoch_system_ms = 1_800_000_000_000;
        for txn in &mut batch.txns {
            txn.epoch_system_ms = 0;
        }
        let (tx_a, mut rx_a) = mpsc::channel(64);
        let (tx_b, mut rx_b) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        sm.apply(1, &encode_entry(&SequencerEntry::EpochBatch { batch }));

        for rx in [&mut rx_a, &mut rx_b] {
            match rx.try_recv() {
                Ok(SchedulerInput::Txn(txn)) => {
                    assert_eq!(txn.epoch_system_ms, 1_800_000_000_000);
                    assert_eq!(txn.epoch_vshard_txn_count, 1);
                }
                _ => panic!("each participating vShard receives the txn"),
            }
        }
    }

    /// The state machine keeps the highest epoch instant it applied, the
    /// floor a leader seeded from it mints above.
    #[test]
    fn applied_epoch_instant_is_the_highest_applied() {
        let mut sm =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());
        assert_eq!(sm.last_epoch_system_ms(), None);
        for (index, (epoch, ms)) in [(0u64, 5_000i64), (1, 4_000), (2, 6_000)]
            .into_iter()
            .enumerate()
        {
            let (mut batch, _, _) = make_batch_with_two_vshards();
            batch.epoch = epoch;
            for txn in &mut batch.txns {
                txn.epoch = epoch;
            }
            batch.epoch_system_ms = ms;
            sm.apply(
                index as u64 + 1,
                &encode_entry(&SequencerEntry::EpochBatch { batch }),
            );
        }
        assert_eq!(sm.last_epoch_system_ms(), Some(6_000));
    }

    #[test]
    fn catch_up_from_records_dropped_index_and_min_collapses() {
        let (batch, va, vb) = make_batch_with_two_vshards();
        // Capacity 1, pre-filled → vshard A is full and every fan-out drops.
        let (tx_a, _rx_a) = mpsc::channel(1);
        // vshard B has room and a live receiver → never drops.
        let (tx_b, _rx_b) = mpsc::channel(64);
        let _ = tx_a.try_send(SchedulerInput::Txn(Box::new(batch.txns[0].clone())));
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

    /// An armed vShard takes no live input, even with room on its channel:
    /// a later input overtaking an earlier dropped one would reach the
    /// scheduler out of log order. The replay delivers both.
    #[test]
    fn an_armed_vshard_defers_every_later_input_to_the_replay() {
        let (batch, va, _vb) = make_batch_with_two_vshards();
        let (tx_a, mut rx_a) = mpsc::channel(1);
        let _ = tx_a.try_send(SchedulerInput::Txn(Box::new(batch.txns[0].clone())));
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        sm.apply(
            4,
            &encode_entry(&SequencerEntry::EpochBatch {
                batch: batch.clone(),
            }),
        );
        // The scheduler reads the pre-filled input: the channel has room.
        assert!(rx_a.try_recv().is_ok());
        let mut later = batch;
        later.epoch = 1;
        for txn in &mut later.txns {
            txn.epoch = 1;
        }
        sm.apply(
            7,
            &encode_entry(&SequencerEntry::EpochBatch { batch: later }),
        );

        assert!(rx_a.try_recv().is_err(), "the later input is not sent live");
        assert_eq!(sm.peek_catch_up_from(va), Some(4));
        // A replay through 5 leaves index 7 owed: still armed, from 6.
        sm.clear_catch_up_up_to(va, 5);
        assert_eq!(sm.peek_catch_up_from(va), Some(6));
        sm.clear_catch_up_up_to(va, 7);
        assert_eq!(sm.peek_catch_up_from(va), None);
    }

    /// PEEK must not consume: the scheduler drain reads the armed index, and
    /// only clears it after a confirmed replay. A take-then-early-return (the
    /// old shape) silently lost the miss when the replay could not complete.
    #[test]
    fn peek_catch_up_from_does_not_consume() {
        let (batch, va, _vb) = make_batch_with_two_vshards();
        let (tx_a, _rx_a) = mpsc::channel(1);
        let _ = tx_a.try_send(SchedulerInput::Txn(Box::new(batch.txns[0].clone())));
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
        let mut senders = HashMap::new();
        let mut receivers = Vec::new();
        for vshard in 1..=3 {
            let (tx, rx) = mpsc::channel(4);
            senders.insert(vshard, tx);
            receivers.push(rx);
        }
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

    /// A vShard that leaves this node takes its catch-up with it. Its armed
    /// index never holds sequencer compaction down afterward.
    #[test]
    fn a_retired_vshard_releases_the_compaction_floor() {
        let (tx, _rx) = mpsc::channel(4);
        let mut sm = SequencerStateMachine::new(
            HashMap::from([(7, tx)]),
            CalvinCompletionRegistry::new_detached(),
        );
        sm.arm_catch_up_from(7, 1);
        assert_eq!(sm.min_catch_up_from(), Some(1));

        sm.remove_vshard_sender(7);
        assert_eq!(sm.min_catch_up_from(), None);
        assert_eq!(sm.peek_catch_up_from(7), None);
    }

    /// An exiting scheduler can arm after its sender is gone. That arm never
    /// counts toward the floor while no sender is registered.
    #[test]
    fn an_arm_without_a_sender_never_holds_the_floor() {
        let mut sm =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());
        sm.arm_catch_up_from(9, 40);
        assert_eq!(sm.min_catch_up_from(), None);

        let (tx, _rx) = mpsc::channel(4);
        sm.set_vshard_sender(9, tx);
        assert_eq!(sm.min_catch_up_from(), Some(40));
    }

    /// Replacing a vShard's sender keeps its pending catch-up. Only a vShard
    /// that leaves this node drops it.
    #[test]
    fn a_replaced_sender_keeps_the_pending_catch_up() {
        let (tx, _rx) = mpsc::channel(4);
        let mut sm = SequencerStateMachine::new(
            HashMap::from([(5, tx)]),
            CalvinCompletionRegistry::new_detached(),
        );
        sm.arm_catch_up_from(5, 12);
        let (tx2, _rx2) = mpsc::channel(4);
        sm.set_vshard_sender(5, tx2);
        assert_eq!(sm.peek_catch_up_from(5), Some(12));
        assert_eq!(sm.min_catch_up_from(), Some(12));
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
        });
        sm.apply(42, &data);
        assert_eq!(sm.current_committed_index(), Some(42));
    }
}
