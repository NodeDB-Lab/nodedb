// SPDX-License-Identifier: BUSL-1.1

//! `Scheduler` struct definition, constructor, and main run loop.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tracing::info;

use nodedb_cluster::MultiRaft;
use nodedb_cluster::calvin::types::SequencedTxn;
use nodedb_cluster::calvin::{
    CalvinCompletionRegistry, SEQUENCER_GROUP_ID, SequencerEntry, VerdictSignal,
};

use super::super::barrier::{PendingDependentBarrier, ReadResultEvent};
use super::super::config::SchedulerConfig;
use super::super::types::{BlockedTxn, PendingTxn};
use crate::bridge::envelope::Response;
use crate::control::cluster::calvin::scheduler::AppliedGate;
use crate::control::cluster::calvin::scheduler::lock_manager::{LockManager, TxnId};
use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
use crate::control::shutdown::ShutdownReceiver;
use crate::control::state::SharedState;
use crate::types::RequestId;

/// Outcome of an executor response bridge task.
///
/// `None` means the executor response channel was closed before a response
/// arrived (infra error).
pub(in crate::control::cluster::calvin::scheduler::driver::core) type CompletionItem =
    (TxnId, RequestId, Option<Response>);

/// The Calvin scheduler for one vshard.
///
/// Owns the in-memory lock table and orchestrates lock acquisition, dispatch,
/// and response handling for both static-set and dependent-read transactions.
///
/// `Send` — runs as a Tokio task on the Control Plane.
pub struct Scheduler {
    /// Vshard this scheduler is responsible for.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) vshard_id: u32,
    /// Incoming sequenced transactions from the sequencer fan-out.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) receiver:
        mpsc::Receiver<SequencedTxn>,
    /// Shared control-plane state used for dispatch, response tracking, WAL,
    /// and request-id allocation.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) shared: Arc<SharedState>,
    /// Handle to MultiRaft so completion acknowledgements can be proposed to
    /// the sequencer group.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) multi_raft:
        Arc<Mutex<MultiRaft>>,
    /// Deterministic lock manager for this vshard. Shared (via `Arc<Mutex<_>>`)
    /// with the Control-Plane write-admission gate through
    /// `SharedState.calvin_lock_managers`, so a fast-path point write contends
    /// on the SAME lock table this scheduler validates against. The scheduler
    /// still runs single-threaded per vShard, so the mutex is uncontended except
    /// for the brief probe the gate takes.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) lock_manager:
        Arc<Mutex<LockManager>>,
    /// In-flight static/active transactions awaiting executor response.
    /// `BTreeMap` ensures deterministic iteration order.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) pending:
        BTreeMap<TxnId, PendingTxn>,
    /// Blocked transactions awaiting lock release.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) blocked:
        BTreeMap<TxnId, BlockedTxn>,
    /// Dependent-read barriers awaiting passive read results.
    /// `BTreeMap` for determinism.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) dependent_barrier:
        BTreeMap<TxnId, PendingDependentBarrier>,
    /// Channel receiving `CalvinReadResult` Raft apply events from the
    /// per-vshard data Raft apply loop. Bounded.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) read_result_rx:
        mpsc::Receiver<ReadResultEvent>,
    /// Exactly-once applied gate: the fully-applied watermark plus the set of
    /// applied `(epoch, position)` pairs above it. Replaces a bare per-epoch
    /// counter so a multi-position epoch is never marked applied on the strength
    /// of its first completing position.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) applied: AppliedGate,
    /// Rebuild target epoch (highest applied epoch from the initial recovery
    /// scan).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) rebuild_target_epoch: u64,
    /// Scheduler configuration.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) config: SchedulerConfig,
    /// Metrics.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) metrics: Arc<SchedulerMetrics>,
    /// Fan-in receiver for executor responses.
    ///
    /// Each dispatched transaction spawns a lightweight bridge task that
    /// awaits the per-request `mpsc::Receiver<Response>` and forwards the
    /// result here as a [`CompletionItem`]. The scheduler's `select!` loop
    /// includes this channel as a first-class arm so it wakes the moment
    /// any executor response is ready — no polling, no sleep.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) completion_rx:
        mpsc::Receiver<CompletionItem>,
    /// Sender half of the completion fan-in channel, cloned per dispatch.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) completion_tx:
        mpsc::Sender<CompletionItem>,
    /// Receiver for lock promotions performed by a Control-Plane fast-path
    /// [`WriteAdmissionGuard`] drop. When a fast-path write releases an
    /// uncontended key that one of THIS scheduler's transactions had since queued
    /// behind, `LockManager::release` promotes that txn to holder but cannot
    /// dispatch it (the release runs off-task, on the Control Plane). The guard
    /// forwards the promoted `TxnId`s here; the `select!` loop drains them and
    /// runs the same promotion -> dispatch path `on_txn_complete` uses.
    ///
    /// [`WriteAdmissionGuard`]: crate::control::server::shared::write_admission::WriteAdmissionGuard
    pub(in crate::control::cluster::calvin::scheduler::driver::core) promotion_rx:
        mpsc::UnboundedReceiver<Vec<TxnId>>,
    /// Shared cross-node completion registry. The scheduler PROBES it
    /// (`registry.verdict(txn)`) when parking a staged txn on the cross-shard
    /// commit barrier and again on each stall sweep — the durable, replicated
    /// source of truth for the global commit/abort verdict. `Send + Sync`; it is
    /// the same registry the sequencer state machine and completion waiters
    /// share, so holding an `Arc` here crosses no plane boundary.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) registry:
        Arc<CalvinCompletionRegistry>,
    /// Push channel for durable verdicts. `note_verdict` (on this node's
    /// registry) broadcasts a [`VerdictSignal`] here the instant a verdict is
    /// stored; the `select!` loop resumes the matching parked txn with low
    /// latency. The probe-on-park and stall re-probe sweep backstop any dropped
    /// push, so a full/closed channel is never a correctness hazard.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) verdict_rx:
        mpsc::Receiver<VerdictSignal>,
}

/// Parameters for [`Scheduler::new`].
pub struct SchedulerParams {
    pub vshard_id: u32,
    pub receiver: mpsc::Receiver<SequencedTxn>,
    pub shared: Arc<SharedState>,
    pub multi_raft: Arc<Mutex<MultiRaft>>,
    /// Fully-applied watermark seed from the recovery scan.
    pub fully_applied_epoch: u64,
    /// Applied `(epoch, position)` pairs seed from the recovery scan.
    pub applied_tail: std::collections::BTreeSet<(u64, u32)>,
    pub rebuild_target_epoch: u64,
    pub config: SchedulerConfig,
    pub metrics: Arc<SchedulerMetrics>,
    pub read_result_rx: mpsc::Receiver<ReadResultEvent>,
    /// The shared lock table for this vShard. Constructed by
    /// `reconcile_vshard_schedulers` and registered in
    /// `SharedState.calvin_lock_managers` under the SAME `Arc` passed here.
    pub lock_manager: Arc<Mutex<LockManager>>,
    /// Receiver for gate-side lock promotions. Constructed by
    /// `reconcile_vshard_schedulers`; its `UnboundedSender` is registered in
    /// `SharedState.calvin_promotion_senders` for this same vShard so a fast-path
    /// guard drop can hand promoted waiters back to this scheduler.
    pub promotion_rx: mpsc::UnboundedReceiver<Vec<TxnId>>,
    /// Shared completion registry for verdict probes on the commit barrier.
    pub registry: Arc<CalvinCompletionRegistry>,
    /// Verdict-push receiver. Constructed by `reconcile_vshard_schedulers`; its
    /// `Sender` is registered on `registry` for this same vShard so a stored
    /// verdict is pushed here immediately.
    pub verdict_rx: mpsc::Receiver<VerdictSignal>,
}

impl Scheduler {
    /// Construct a scheduler.
    pub fn new(params: SchedulerParams) -> Self {
        let SchedulerParams {
            vshard_id,
            receiver,
            shared,
            multi_raft,
            fully_applied_epoch,
            applied_tail,
            rebuild_target_epoch,
            config,
            metrics,
            read_result_rx,
            lock_manager,
            promotion_rx,
            registry,
            verdict_rx,
        } = params;

        // Capacity: at most one completion per inflight txn. Use the incoming
        // channel capacity as a proxy for the max concurrent pending count.
        let completion_cap = config.channel_capacity;
        let (completion_tx, completion_rx) = mpsc::channel(completion_cap);

        Self {
            vshard_id,
            receiver,
            shared,
            multi_raft,
            lock_manager,
            pending: BTreeMap::new(),
            blocked: BTreeMap::new(),
            dependent_barrier: BTreeMap::new(),
            read_result_rx,
            applied: AppliedGate::new(fully_applied_epoch, applied_tail),
            rebuild_target_epoch,
            config,
            metrics,
            completion_rx,
            completion_tx,
            promotion_rx,
            registry,
            verdict_rx,
        }
    }

    /// Whether the scheduler has caught up to the rebuild target epoch.
    pub fn is_caught_up(&self) -> bool {
        self.applied.fully_applied_epoch() >= self.rebuild_target_epoch
    }

    /// Publish an advanced fully-applied watermark to the metrics gauge and the
    /// shared cross-shard snapshot anchor.
    ///
    /// `BEGIN` reads `SharedState::last_applied_calvin_epoch` to anchor a
    /// session's cross-shard snapshot version, so it MUST reflect the
    /// FULLY-applied epoch — never an epoch that has only some of its positions
    /// committed, which would let a session anchor on a torn epoch. `fetch_max`
    /// keeps it monotonic across all per-vShard schedulers writing the counter.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn publish_watermark(
        &self,
        watermark: u64,
    ) {
        self.metrics.update_last_applied_epoch(watermark);
        self.shared
            .last_applied_calvin_epoch
            .fetch_max(watermark, std::sync::atomic::Ordering::Release);
    }

    /// Spawn a bridge task that awaits a single executor response and forwards
    /// it to the scheduler's fan-in completion channel.
    ///
    /// The bridge task is cancel-safe: it holds only a cloned sender and the
    /// per-request receiver. Dropping the scheduler's `completion_rx` causes
    /// the bridge's `send` to fail silently, which is fine on shutdown.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn spawn_response_bridge(
        &self,
        txn_id: TxnId,
        request_id: RequestId,
        mut response_rx: mpsc::Receiver<Response>,
    ) {
        let tx = self.completion_tx.clone();
        tokio::spawn(async move {
            let result = response_rx.recv().await;
            // Ignore send error: scheduler has shut down.
            let _ = tx.send((txn_id, request_id, result)).await;
        });
    }

    /// Run the scheduler event loop until shutdown is signaled.
    pub async fn run(mut self, mut shutdown: ShutdownReceiver) {
        info!(
            vshard_id = self.vshard_id,
            fully_applied_epoch = self.applied.fully_applied_epoch(),
            rebuild_target_epoch = self.rebuild_target_epoch,
            "calvin scheduler starting"
        );

        // Low-frequency liveness timer so the top-of-loop stall/barrier sweeps
        // run even on an otherwise-idle vShard. Without it, a dropped verdict
        // push plus zero further events for this vShard would leave a parked txn
        // never re-probing the durable verdict. A fraction of the stall-warn
        // window re-probes well within it.
        let mut stall_tick = tokio::time::interval(self.config.verdict_stall_warn() / 4);
        stall_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            self.check_dependent_barrier_timeouts();
            self.check_awaiting_verdict_stalls();

            tokio::select! {
                biased;

                _ = shutdown.wait_cancelled() => {
                    info!(vshard_id = self.vshard_id, "calvin scheduler shutting down");
                    break;
                }

                maybe_completion = self.completion_rx.recv() => {
                    if let Some((txn_id, request_id, resp_opt)) = maybe_completion {
                        self.handle_completion(txn_id, request_id, resp_opt);
                    }
                }

                maybe_verdict = self.verdict_rx.recv() => {
                    if let Some(signal) = maybe_verdict {
                        // A durable global verdict landed: resume the matching
                        // parked txn into its flush (commit) or drop (abort).
                        self.handle_verdict_signal(signal);
                    }
                }

                maybe_event = self.read_result_rx.recv() => {
                    if let Some(event) = maybe_event {
                        self.handle_read_result(event);
                    }
                }

                maybe_promoted = self.promotion_rx.recv() => {
                    if let Some(promoted) = maybe_promoted {
                        // A fast-path write-admission guard released an uncontended
                        // key that one of this scheduler's txns had queued behind;
                        // `release` already promoted it to holder. Run the normal
                        // promotion -> dispatch path so it stops being a stalled
                        // holder in `blocked` and actually executes.
                        self.dispatch_promoted(promoted);
                    }
                }

                maybe_txn = self.receiver.recv() => {
                    match maybe_txn {
                        Some(txn) => self.process_new_txn(txn),
                        None => {
                            info!(
                                vshard_id = self.vshard_id,
                                "calvin scheduler: receiver channel closed; exiting"
                            );
                            break;
                        }
                    }
                }

                _ = stall_tick.tick() => {
                    // Empty body on purpose: the top-of-loop
                    // check_awaiting_verdict_stalls / check_dependent_barrier_timeouts
                    // do the work on every wake. This arm only guarantees the loop
                    // wakes to run them when no other event arrives.
                }
            }
        }
    }

    /// Encode `entry` as MessagePack and propose it to the sequencer Raft group.
    ///
    /// Logs a warning on encode failure or propose failure; never panics.
    /// `op_name` is a short human-readable label used in warning messages
    /// (e.g. `"completion ack"`, `"OLLP mismatch signal"`).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn propose_sequencer_entry(
        &self,
        entry: SequencerEntry,
        txn_id: TxnId,
        op_name: &str,
    ) {
        match zerompk::to_msgpack_vec(&entry) {
            Ok(bytes) => {
                if let Err(e) = self
                    .multi_raft
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .propose_to_group(SEQUENCER_GROUP_ID, bytes)
                {
                    tracing::warn!(
                        vshard_id = self.vshard_id,
                        epoch = txn_id.epoch,
                        position = txn_id.position,
                        error = %e,
                        "calvin: failed to propose {op_name}",
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    error = %e,
                    "calvin: failed to encode {op_name}",
                );
            }
        }
    }

    /// Allocate a fresh request ID for a dispatch.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn next_request_id(
        &self,
    ) -> RequestId {
        self.shared.next_request_id()
    }
}
