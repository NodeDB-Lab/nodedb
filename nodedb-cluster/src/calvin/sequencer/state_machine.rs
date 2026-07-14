// SPDX-License-Identifier: BUSL-1.1

//! Calvin sequencer Raft state machine.
//!
//! [`SequencerStateMachine`] is called on every replica (including the leader)
//! when a `SequencerEntry` is committed to the Raft log. It:
//!
//! 1. Decodes the `SequencerEntry` from the raw log bytes.
//! 2. Checks epoch monotonicity to detect log gaps (a gap means the apply path
//!    is broken and the node should not fan out — it logs an error and skips).
//! 3. Fans the `EpochBatch` out to per-vshard output channels. Uses `try_send`
//!    so the apply loop is never blocked. A full channel logs and drops — the
//!    scheduler's log-replay path will catch up.
//! 4. Advances `last_applied_epoch`.
//!
//! The `last_applied_epoch` counter is kept in memory only. On node restart the
//! sequencer group's Raft log is replayed from the beginning (or from the
//! latest snapshot), and the counter is rebuilt monotonically. This is safe
//! because the state machine is deterministic.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;
use tracing::{error, warn};

use crate::calvin::CalvinCompletionRegistry;
use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::types::SequencedTxn;

/// Atomic counters for the sequencer state machine apply path.
pub struct StateMachineMetrics {
    /// Total epoch batches successfully applied.
    pub epochs_applied: AtomicU64,
    /// Total transactions fanned out to vshard channels.
    pub txns_fanned_out: AtomicU64,
    /// Transactions dropped because the vshard channel was full.
    pub txns_dropped_backpressure: AtomicU64,
    /// Epochs skipped because of a gap in the epoch sequence.
    pub epochs_skipped_gap: AtomicU64,
}

impl StateMachineMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            epochs_applied: AtomicU64::new(0),
            txns_fanned_out: AtomicU64::new(0),
            txns_dropped_backpressure: AtomicU64::new(0),
            epochs_skipped_gap: AtomicU64::new(0),
        })
    }
}

impl Default for StateMachineMetrics {
    fn default() -> Self {
        Self {
            epochs_applied: AtomicU64::new(0),
            txns_fanned_out: AtomicU64::new(0),
            txns_dropped_backpressure: AtomicU64::new(0),
            epochs_skipped_gap: AtomicU64::new(0),
        }
    }
}

/// The Calvin sequencer Raft state machine.
///
/// One instance per replica (including leader). Applied on every `CommitApplier`
/// callback for the sequencer Raft group.
pub struct SequencerStateMachine {
    /// Last successfully applied epoch. Used for gap detection.
    /// The first valid epoch is 0; `last_applied_epoch = u64::MAX` means nothing
    /// has been applied yet (using `u64::MAX` avoids a separate `Option` and
    /// makes the "nothing applied" state explicit).
    last_applied_epoch: u64,
    /// Per-vshard output channels. The scheduler subscribes on the other end.
    vshard_senders: HashMap<u32, mpsc::Sender<SequencedTxn>>,
    pub metrics: Arc<StateMachineMetrics>,
    completion_registry: Arc<CalvinCompletionRegistry>,
}

const NOT_YET_APPLIED: u64 = u64::MAX;

impl SequencerStateMachine {
    /// Construct a fresh state machine with no applied epochs.
    pub fn new(
        vshard_senders: HashMap<u32, mpsc::Sender<SequencedTxn>>,
        completion_registry: Arc<CalvinCompletionRegistry>,
    ) -> Self {
        Self {
            last_applied_epoch: NOT_YET_APPLIED,
            vshard_senders,
            metrics: StateMachineMetrics::new(),
            completion_registry,
        }
    }

    /// The last epoch number that was successfully applied, or `None` if no
    /// epoch has been applied yet.
    pub fn last_applied_epoch(&self) -> Option<u64> {
        if self.last_applied_epoch == NOT_YET_APPLIED {
            None
        } else {
            Some(self.last_applied_epoch)
        }
    }

    /// The epoch number that the next proposal should use.
    pub fn next_epoch(&self) -> u64 {
        if self.last_applied_epoch == NOT_YET_APPLIED {
            0
        } else {
            self.last_applied_epoch + 1
        }
    }

    /// Register (or replace) the output sender for a vshard.
    ///
    /// Call this when a scheduler subscribes for a vshard hosted on this node.
    pub fn set_vshard_sender(&mut self, vshard: u32, sender: mpsc::Sender<SequencedTxn>) {
        self.vshard_senders.insert(vshard, sender);
    }

    /// Remove the output sender for a vshard (e.g. when a vshard is migrated
    /// away from this node).
    pub fn remove_vshard_sender(&mut self, vshard: u32) {
        self.vshard_senders.remove(&vshard);
    }

    /// The highest epoch number that has been committed and applied on this
    /// replica, or `None` if no epoch has been applied yet.
    ///
    /// Used by the Calvin scheduler's rebuild path: the scheduler captures
    /// this value before processing the Raft log to determine the upper bound
    /// of the rebuild range (`E+1 ..= current_committed_epoch`).
    pub fn current_committed_epoch(&self) -> Option<u64> {
        self.last_applied_epoch()
    }

    /// Decode committed Raft log entries and return the `SequencedTxn`s for a
    /// specific vshard in epoch order.
    ///
    /// The caller (the Calvin scheduler's rebuild path) passes in raw Raft log
    /// entries obtained via `MultiRaft::read_committed_entries`.  This method
    /// decodes each entry as a `SequencerEntry`, filters to the given vshard
    /// and epoch range `[from_epoch, to_epoch]`, and returns the matching txns
    /// in `(epoch, position)` order.
    ///
    /// Entries that fail to decode are logged and skipped (same policy as the
    /// live apply path).
    pub fn replay_epochs_for_vshard(
        &self,
        entries: &[nodedb_raft::LogEntry],
        vshard_id: u32,
        from_epoch: u64,
        to_epoch: u64,
    ) -> Vec<SequencedTxn> {
        let mut result = std::collections::BTreeMap::<(u64, u32), SequencedTxn>::new();

        for entry in entries {
            if entry.data.is_empty() {
                // No-op entry (newly elected leader heartbeat).
                continue;
            }
            let seq_entry: SequencerEntry = match zerompk::from_msgpack(&entry.data) {
                Ok(e) => e,
                Err(err) => {
                    tracing::warn!(
                        raft_index = entry.index,
                        error = %err,
                        "calvin rebuild: failed to decode sequencer entry; skipping"
                    );
                    continue;
                }
            };
            match seq_entry {
                SequencerEntry::EpochBatch { mut batch } => {
                    if batch.epoch < from_epoch || batch.epoch > to_epoch {
                        continue;
                    }
                    for txn in &mut batch.txns {
                        txn.tx_class.restore_derived();
                        let participates = txn
                            .tx_class
                            .participating_vshards()
                            .iter()
                            .any(|v| v.as_u32() == vshard_id);
                        if participates {
                            result.insert((txn.epoch, txn.position), txn.clone());
                        }
                    }
                }
                SequencerEntry::CompletionAck { .. } => {}
                // Mismatch signals are coordinator-side notifications only; they
                // carry no txn data to replay for a vshard.
                SequencerEntry::OllpMismatch { .. } => {}
                // Routing-failure signals are coordinator-side notifications
                // only, like `OllpMismatch`; they carry no txn data to replay.
                SequencerEntry::TxnRoutingFailed { .. } => {}
                // Vote signals are coordinator-side notifications only, like
                // `OllpMismatch`/`TxnRoutingFailed`; they carry no txn data to
                // replay. Replay re-derives tallies via live `apply` instead.
                SequencerEntry::Vote { .. } => {}
                // Verdict signals carry no txn data to replay, like `Vote`; the
                // stored verdict is re-derived via live `apply`.
                SequencerEntry::Verdict { .. } => {}
            }
        }

        result.into_values().collect()
    }

    /// Apply a committed Raft log entry.
    ///
    /// Decodes the `SequencerEntry`, checks epoch monotonicity, fans out to
    /// per-vshard channels, and advances `last_applied_epoch`.
    ///
    /// This method is synchronous (no `.await`). It MUST NOT block or do I/O.
    pub fn apply(&mut self, data: &[u8]) {
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

                let expected = self.next_epoch();
                if batch.epoch != expected {
                    error!(
                        epoch = batch.epoch,
                        expected,
                        "sequencer state machine: epoch gap detected; \
                         this node may have missed entries. Skipping batch."
                    );
                    self.metrics
                        .epochs_skipped_gap
                        .fetch_add(1, Ordering::Relaxed);
                    // Advance anyway to the received epoch so we don't
                    // permanently stall on a gap. The scheduler will need to
                    // replay from the Raft log to recover.
                    self.last_applied_epoch = batch.epoch;
                    return;
                }

                let mut fanned_out = 0u64;
                let mut dropped = 0u64;

                // Per-vShard count of how many of this epoch's positions target
                // each vShard. Delivered to each scheduler so it knows how many
                // positions of the epoch it must apply before the epoch is fully
                // applied on its vShard — the input to its per-`(epoch, position)`
                // applied gate and fully-applied watermark. Every position of an
                // epoch targeting a given vShard is stamped with the same count.
                let mut vshard_txn_counts: HashMap<u32, u32> = HashMap::new();
                for txn in &batch.txns {
                    let participating = txn.tx_class.participating_vshards();
                    for vshard_id in participating {
                        *vshard_txn_counts.entry(vshard_id.as_u32()).or_insert(0) += 1;
                    }
                    // Seed the expected vote-participant count deterministically on
                    // EVERY replica (not just the epoch's originating leader), so a
                    // post-failover sequencer leader can still detect vote
                    // completeness and aggregate the verdict.
                    self.completion_registry.seed_expected(
                        crate::calvin::TxnId::new(batch.epoch, txn.position),
                        participating.len(),
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
                            match sender.try_send(per_vshard) {
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
                                    dropped += 1;
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    warn!(
                                        vshard,
                                        epoch = batch.epoch,
                                        "sequencer apply: vshard sender gone; \
                                         scheduler may have exited"
                                    );
                                    dropped += 1;
                                }
                            }
                        }
                        // If no sender registered for this vshard, silently skip —
                        // this node may not host that vshard.
                    }
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
                self.completion_registry
                    .note_completion_ack(crate::calvin::TxnId::new(epoch, position), vshard_id);
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
                    commit,
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
                self.completion_registry
                    .note_verdict(crate::calvin::TxnId::new(epoch, position), commit);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        sm.apply(&data);

        assert_eq!(sm.last_applied_epoch(), Some(0));
        assert_eq!(sm.metrics.epochs_applied.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn gap_detection_rejects_out_of_order_epochs() {
        let (mut batch, va, vb) = make_batch_with_two_vshards();
        let (tx_a, _) = mpsc::channel(64);
        let (tx_b, _) = mpsc::channel(64);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        // Apply epoch 0.
        let data0 = encode_entry(&SequencerEntry::EpochBatch {
            batch: batch.clone(),
        });
        sm.apply(&data0);
        assert_eq!(sm.last_applied_epoch(), Some(0));

        // Apply epoch 2 (skip epoch 1 → gap).
        batch.epoch = 2;
        for txn in &mut batch.txns {
            txn.epoch = 2;
        }
        let data2 = encode_entry(&SequencerEntry::EpochBatch { batch });
        sm.apply(&data2);

        assert_eq!(sm.metrics.epochs_skipped_gap.load(Ordering::Relaxed), 1);
        // Epoch advances to 2 to avoid permanent stall.
        assert_eq!(sm.last_applied_epoch(), Some(2));
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
        sm.apply(&data);

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
        let _ = tx_a.try_send(pre_fill);
        let mut senders = HashMap::new();
        senders.insert(va, tx_a);
        senders.insert(vb, tx_b);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached());

        let data = encode_entry(&SequencerEntry::EpochBatch { batch });
        // Must not panic or block.
        sm.apply(&data);

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
        sm.apply(&data);

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
        sm.apply(&data);

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
        sm.apply(&data);

        // The verdict is stored authoritatively on every replica.
        assert_eq!(registry.verdict(txn), Some(true));
        // Verdict is not an EpochBatch, so it must not perturb the epoch counter
        // (mirrors OllpMismatch/TxnRoutingFailed's non-effect).
        assert_eq!(sm.last_applied_epoch(), None);
    }
}
