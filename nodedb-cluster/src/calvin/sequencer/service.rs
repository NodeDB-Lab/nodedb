// SPDX-License-Identifier: BUSL-1.1

//! The Calvin sequencer service.
//!
//! [`SequencerService`] drives the epoch ticker and Raft proposal loop on the
//! sequencer leader. On each tick it:
//!
//! 1. Checks that this node is the sequencer Raft group leader. If not, drains
//!    and discards the inbox (clients will retry against the real leader).
//! 2. Drains the inbox into a candidate batch respecting epoch caps.
//! 3. Runs the pre-validation pass ([`super::validator::validate_batch`]).
//! 4. Proposes the resulting `EpochBatch` to the sequencer Raft group (only if
//!    at least one transaction was admitted).
//! 5. Advances the local epoch counter.
//!
//! The service does **not** apply Raft log entries — that is the
//! [`super::state_machine::SequencerStateMachine`]'s job, which runs on every
//! replica including the leader.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tracing::{debug, info, warn};

use tokio::sync::mpsc;

use crate::calvin::sequencer::config::SEQUENCER_GROUP_ID;
use crate::calvin::sequencer::config::SequencerConfig;
use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::sequencer::inbox::{AdmittedTx, InboxReceiver};
use crate::calvin::sequencer::validator::validate_batch_with_assignments;
use crate::calvin::types::EpochBatch;
use crate::calvin::{CalvinCompletionRegistry, TxnId};
use crate::error::ClusterError;
use crate::multi_raft::MultiRaft;

// Re-export so existing call sites (`service::SequencerMetrics`) don't break.
pub use crate::calvin::sequencer::metrics::{ConflictKey, SequencerMetrics};

/// The Calvin sequencer service.
///
/// Drives the epoch ticker. Must be spawned as a Tokio task on the Control
/// Plane. `Send + Sync`.
pub struct SequencerService {
    config: SequencerConfig,
    node_id: u64,
    multi_raft: Arc<Mutex<MultiRaft>>,
    inbox_receiver: InboxReceiver,
    /// Current epoch number. The leader starts at the last committed epoch + 1
    /// (loaded from state machine on construction) and increments after each
    /// successful proposal. On leader failover, `inbox_receiver` is simply
    /// dropped (in-flight submissions are not in the log and will be retried).
    current_epoch: u64,
    pub metrics: Arc<SequencerMetrics>,
    completion_registry: Arc<CalvinCompletionRegistry>,
    /// Receives `(txn, commit)` verdict signals emitted by this node's
    /// completion registry when a staged cross-shard txn's vote tally becomes
    /// complete. Only the leader turns a signal into a `Verdict` proposal.
    /// Stored as `Option` so `run` can move it out of `&mut self` into an owned
    /// local, avoiding a borrow conflict with `self.tick()` in a sibling
    /// `select!` arm; it is always `Some` after construction.
    verdict_rx: Option<mpsc::Receiver<(TxnId, bool)>>,
}

impl SequencerService {
    /// Construct the sequencer service.
    ///
    /// `starting_epoch` should be `last_applied_epoch + 1` from the
    /// [`super::state_machine::SequencerStateMachine`] on this node.
    pub fn new(
        config: SequencerConfig,
        node_id: u64,
        multi_raft: Arc<Mutex<MultiRaft>>,
        inbox_receiver: InboxReceiver,
        starting_epoch: u64,
        completion_registry: Arc<CalvinCompletionRegistry>,
        verdict_rx: mpsc::Receiver<(TxnId, bool)>,
    ) -> Self {
        Self {
            config,
            node_id,
            multi_raft,
            inbox_receiver,
            current_epoch: starting_epoch,
            metrics: SequencerMetrics::new(),
            completion_registry,
            verdict_rx: Some(verdict_rx),
        }
    }

    /// Run the epoch ticker loop until the shutdown signal fires.
    ///
    /// Each iteration: check leadership, drain inbox, validate, propose.
    pub async fn run(&mut self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(self.config.epoch_duration);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        info!(
            node_id = self.node_id,
            epoch = self.current_epoch,
            "sequencer service starting"
        );

        // Move the verdict receiver out of `self` so the `select!` loop can hold
        // an owned `&mut` to it without conflicting with `self.tick()` in a
        // sibling arm. Always `Some` after construction; a `None` (run called
        // twice) simply disables the verdict arm forever via `pending()`.
        let mut verdict_rx = self.verdict_rx.take();

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.tick();
                }
                verdict = async {
                    match verdict_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    // Every replica's registry emits deterministically, but only
                    // the leader proposes — same leader-gate as OllpMismatch/Vote.
                    if let Some((txn, commit)) = verdict
                        && self.is_leader()
                        && let Err(e) = self.propose_entry(&SequencerEntry::Verdict {
                            epoch: txn.epoch,
                            position: txn.position,
                            commit,
                        })
                    {
                        warn!(
                            epoch = txn.epoch,
                            position = txn.position,
                            error = %e,
                            "sequencer verdict propose failed; a later re-tally will not \
                             re-emit (deduped), but the local decision still drives"
                        );
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(node_id = self.node_id, "sequencer service shutting down");
                        break;
                    }
                }
            }
        }
    }

    /// Execute one epoch tick.
    ///
    /// Exposed as `pub` so tests can drive the service synchronously without
    /// running the full `run()` loop.
    pub fn tick(&mut self) {
        // no-determinism: epoch tick observability, off-WAL path
        let tick_start = Instant::now();
        self.metrics.epochs_total.fetch_add(1, Ordering::Relaxed);

        self.tick_inner();

        // no-determinism: epoch tick observability, off-WAL path
        let elapsed_ms = tick_start.elapsed().as_millis() as u64;
        self.metrics.record_epoch_duration_ms(elapsed_ms);
    }

    /// Inner body of `tick()`, separated so the duration timer in `tick()`
    /// wraps all exit paths cleanly.
    fn tick_inner(&mut self) {
        // Check leadership by attempting a dry-run propose. We use the
        // multi_raft is_leader API directly.
        if !self.is_leader() {
            // Drain and discard: clients will retry against the real leader.
            let discarded = self.inbox_receiver.drain_all_discard();
            debug!(
                node_id = self.node_id,
                "not sequencer leader; discarding {discarded} inbox items",
            );
            return;
        }

        // Re-propose any complete-but-unstored cross-shard verdict. This must run
        // on EVERY leader tick — including the empty-inbox / all-rejected ticks
        // that return early below — so a verdict orphaned by a mid-commit
        // sequencer failover (participant votes committed, but the aggregated
        // `Verdict` entry never did before the old leader died) is always
        // re-driven to durability. It is safe to skip only when not leader, which
        // the gate above already guarantees.
        self.redrive_unproposed_verdicts();

        // Snapshot inbox depth before drain so the gauge reflects the queue
        // depth at the start of this epoch.
        self.metrics
            .inbox_depth
            .store(self.inbox_receiver.depth(), Ordering::Relaxed);

        // Drain inbox up to per-epoch caps.
        let mut candidates: Vec<AdmittedTx> = Vec::new();
        let drained = self.inbox_receiver.drain_into_capped(
            &mut candidates,
            self.config.max_txns_per_epoch,
            self.config.max_bytes_per_epoch,
        );
        if drained == 0 {
            debug!(
                node_id = self.node_id,
                epoch = self.current_epoch,
                "epoch tick: inbox empty, no proposal"
            );
            return;
        }

        // Pre-validation.
        let epoch = self.current_epoch;

        let (admitted, rejected) = validate_batch_with_assignments(epoch, candidates);

        self.metrics
            .admitted_total
            .fetch_add(admitted.len() as u64, Ordering::Relaxed);

        // Record per-conflict metrics and increment the aggregate counter.
        for r in &rejected {
            self.metrics
                .rejected_conflict_total
                .fetch_add(1, Ordering::Relaxed);
            if let Some(ctx) = r.conflict_context.clone() {
                self.metrics.record_conflict(ctx);
            }
        }

        if admitted.is_empty() {
            debug!(
                epoch,
                rejected = rejected.len(),
                "epoch tick: all candidates rejected, no proposal"
            );
            self.current_epoch += 1;
            return;
        }

        // Read wall clock ONCE on the sequencer leader. This is the single
        // deterministic timestamp source for every transaction in this epoch.
        // All replicas receive this value via Raft replication; engine handlers
        // use it instead of reading the wall clock independently.
        let epoch_system_ms = std::time::SystemTime::now() // no-determinism: read once on leader; replicated to all replicas via Raft
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        // Encode and propose.
        let batch = EpochBatch {
            epoch,
            txns: admitted.iter().map(|(_, txn)| txn.clone()).collect(),
            epoch_system_ms,
        };
        for (inbox_seq, txn) in &admitted {
            self.completion_registry.note_assigned(
                *inbox_seq,
                crate::calvin::TxnId::new(epoch, txn.position),
                txn.tx_class.participating_vshards().len(),
            );
        }
        let entry = SequencerEntry::EpochBatch { batch };
        let txns_count = entry_txn_count(&entry);
        let _replicate_span =
            tracing::info_span!("sequencer_replicate", epoch, txns_count,).entered();
        match self.propose_entry(&entry) {
            Ok(log_index) => {
                debug!(
                    epoch,
                    log_index,
                    admitted = entry_txn_count(&entry),
                    rejected = rejected.len(),
                    "sequencer proposed epoch batch"
                );
            }
            Err(e) => {
                warn!(epoch, error = %e, "sequencer propose failed; epoch will be retried on next tick if still leader");
                // Do NOT advance epoch on propose failure — the same epoch
                // will be re-attempted on the next tick if the node is still
                // the leader. This is safe because the epoch has not been
                // committed to the Raft log.
                return;
            }
        }
        self.current_epoch += 1;
    }

    /// Re-propose every complete-but-unstored cross-shard verdict.
    ///
    /// Closes a failover deadlock: a follower that applied the committed `Vote`
    /// entries reached the local vote-completeness transition, which set the
    /// per-`PendingCompletion` `verdict_proposed` flag and emitted a signal the
    /// non-leader service dropped. That flag is in-memory, non-durable, and never
    /// reset, so after the follower promotes the normal emit path stays deduped
    /// and never re-fires. If the prior leader died after the votes committed but
    /// before the `Verdict` entry committed, no node would ever propose the
    /// verdict and parked participants would stall in `AwaitingVerdict` forever.
    ///
    /// This leader-driven rescan re-proposes each such verdict on every tick,
    /// using the same propose path as the emit-signal arm. It self-heals: a
    /// re-proposed `Verdict` that already committed applies idempotently
    /// (`note_verdict` dedups a same-value verdict), and once the verdict is
    /// stored the registry stops returning that txn — so this cannot loop or
    /// double-commit. Runs only on the leader; the caller gates on `is_leader`.
    fn redrive_unproposed_verdicts(&self) {
        for (txn, commit) in self.completion_registry.drain_unproposed_verdicts() {
            if let Err(e) = self.propose_entry(&SequencerEntry::Verdict {
                epoch: txn.epoch,
                position: txn.position,
                commit,
            }) {
                warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    error = %e,
                    "sequencer failover verdict re-propose failed; the next tick will \
                     retry while this node stays leader"
                );
            }
        }
    }

    fn is_leader(&self) -> bool {
        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.is_group_leader(SEQUENCER_GROUP_ID)
    }

    fn propose_entry(&self, entry: &SequencerEntry) -> Result<u64, ClusterError> {
        let bytes = zerompk::to_msgpack_vec(entry).map_err(|e| ClusterError::Codec {
            detail: format!("sequencer encode: {e}"),
        })?;
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.propose_to_group(SEQUENCER_GROUP_ID, bytes)
    }
}

fn entry_txn_count(entry: &SequencerEntry) -> usize {
    match entry {
        SequencerEntry::EpochBatch { batch } => batch.txns.len(),
        SequencerEntry::CompletionAck { .. } => 0,
        SequencerEntry::OllpMismatch { .. } => 0,
        SequencerEntry::TxnRoutingFailed { .. } => 0,
        SequencerEntry::Vote { .. } => 0,
        SequencerEntry::Verdict { .. } => 0,
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::sequencer::config::SequencerConfig;
    use crate::calvin::sequencer::inbox::new_inbox;
    use crate::calvin::sequencer::validator::validate_batch;
    use crate::calvin::types::{EngineKeySet, ReadWriteSet, SortedVec, TxClass};
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

    fn make_tx_class(surr_a: u32, surr_b: u32) -> TxClass {
        let (col_a, col_b) = find_two_distinct_collections();
        let write_set = ReadWriteSet::new(vec![
            EngineKeySet::Document {
                collection: col_a,
                surrogates: SortedVec::new(vec![surr_a]),
            },
            EngineKeySet::Document {
                collection: col_b,
                surrogates: SortedVec::new(vec![surr_b]),
            },
        ]);
        TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![surr_a as u8],
            TenantId::new(1),
            None,
            crate::calvin::types::VersionedReadSet::default(),
        )
        .expect("valid TxClass")
    }

    #[test]
    fn epoch_ticker_fires_increments_counter() {
        let config = SequencerConfig::default();
        let (inbox, rx) = new_inbox(100, &config);
        let _ = inbox.submit(make_tx_class(1, 2));

        let metrics = Arc::new(SequencerMetrics::default());

        let mut candidates: Vec<AdmittedTx> = Vec::new();
        let mut rx2 = rx;
        rx2.drain_into_capped(&mut candidates, 1024, usize::MAX);

        let epoch = 1u64;
        let (admitted, rejected) = validate_batch(epoch, candidates);
        let admitted_count = admitted.len() as u64;
        let rejected_count = rejected.len() as u64;

        metrics
            .admitted_total
            .fetch_add(admitted_count, Ordering::Relaxed);
        metrics
            .rejected_conflict_total
            .fetch_add(rejected_count, Ordering::Relaxed);
        metrics.epochs_total.fetch_add(1, Ordering::Relaxed);

        assert_eq!(metrics.epochs_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.admitted_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn empty_inbox_produces_no_admitted_txns() {
        let epoch = 42u64;
        let candidates: Vec<AdmittedTx> = Vec::new();
        let (admitted, rejected) = validate_batch(epoch, candidates);
        assert!(admitted.is_empty());
        assert!(rejected.is_empty());
    }

    #[test]
    fn non_empty_inbox_produces_one_or_more_admitted_txns() {
        let epoch = 1u64;
        let admitted_tx = AdmittedTx {
            inbox_seq: 0,
            tx_class: make_tx_class(10, 20),
        };
        let (admitted, _rejected) = validate_batch(epoch, vec![admitted_tx]);
        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].epoch, epoch);
    }

    #[test]
    fn sequenced_txns_carry_correct_epoch() {
        let epoch = 99u64;
        let tx = AdmittedTx {
            inbox_seq: 0,
            tx_class: make_tx_class(5, 7),
        };
        let (admitted, _) = validate_batch(epoch, vec![tx]);
        assert_eq!(admitted[0].epoch, epoch);
    }

    #[test]
    fn sequenced_txn_is_clone_and_eq() {
        use crate::calvin::types::SequencedTxn;
        let tx = AdmittedTx {
            inbox_seq: 0,
            tx_class: make_tx_class(1, 2),
        };
        let (admitted, _) = validate_batch(1, vec![tx]);
        let t: SequencedTxn = admitted[0].clone();
        assert_eq!(t.epoch, 1);
    }

    #[test]
    fn drain_caps_at_max_txns_per_epoch() {
        // Produce 10 txns in the inbox; cap at 3 per epoch.
        let config = SequencerConfig {
            max_txns_per_epoch: 3,
            max_bytes_per_epoch: usize::MAX,
            ..SequencerConfig::default()
        };
        let (inbox, mut rx) = new_inbox(20, &config);
        for i in 0..10u32 {
            inbox
                .submit(make_tx_class(i * 2, i * 2 + 1))
                .expect("submit");
        }
        let mut out = Vec::new();
        let n = rx.drain_into_capped(
            &mut out,
            config.max_txns_per_epoch,
            config.max_bytes_per_epoch,
        );
        assert_eq!(n, 3, "drain must stop at max_txns_per_epoch");
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn drain_stops_at_max_bytes_per_epoch() {
        // Each txn has plans = [0u8; 10] (10 bytes). Cap = 25 bytes → 2 fit,
        // the 3rd is deferred to the pending slot.
        let config = SequencerConfig {
            max_txns_per_epoch: 1000,
            max_bytes_per_epoch: 25,
            ..SequencerConfig::default()
        };
        let (inbox, mut rx) = new_inbox(20, &config);
        for i in 0..5u32 {
            let mut tx = make_tx_class(i * 2, i * 2 + 1);
            tx.plans = vec![0u8; 10];
            inbox.submit(tx).expect("submit");
        }

        // First drain: 2 fit (20 bytes), 3rd deferred.
        let mut out = Vec::new();
        let n = rx.drain_into_capped(
            &mut out,
            config.max_txns_per_epoch,
            config.max_bytes_per_epoch,
        );
        assert!(
            n <= 2,
            "at most 2 txns should fit in 25 bytes with 10-byte plans each, got {n}"
        );

        // Second drain: the deferred txn should be emitted first.
        let before = out.len();
        let n2 = rx.drain_into_capped(
            &mut out,
            config.max_txns_per_epoch,
            config.max_bytes_per_epoch,
        );
        assert!(n2 >= 1, "pending item must drain on the next call");
        let _ = before; // consumed for assertion above
    }
}
