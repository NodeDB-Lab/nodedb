// SPDX-License-Identifier: BUSL-1.1

//! `Scheduler` struct definition, constructor, and main run loop.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, mpsc};
use tracing::info;

use nodedb_cluster::MultiRaft;
use nodedb_cluster::calvin::types::SchedulerInput;
use nodedb_cluster::calvin::{CalvinCompletionRegistry, SequencerStateMachine, VerdictSignal};

use super::super::barrier::{PendingDependentBarrier, ReadResultEvent};
use super::super::config::SchedulerConfig;
use super::super::types::{BlockedTxn, PendingTxn};
use super::catch_up::CatchUpDrain;
use super::deferred::DeferredQueue;
use super::halt::HaltLatch;
use super::intake::IntakeGate;
use super::owed::OwedEntries;
use super::sequencer_proposer::SequencerProposer;
use crate::bridge::envelope::Response;
use crate::control::cluster::calvin::scheduler::lock_manager::{LockManager, TxnId};
use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
use crate::control::cluster::calvin::scheduler::{AppliedGate, NOT_YET_APPLIED_EPOCH};
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
    /// Incoming scheduler inputs from the sequencer fan-out (sequenced txns and
    /// shared-reservation install/release directives).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) receiver:
        mpsc::Receiver<SchedulerInput>,
    /// Shared control-plane state used for dispatch, response tracking, WAL,
    /// and request-id allocation.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) shared: Arc<SharedState>,
    /// Handle to MultiRaft for the data-group leader check and the catch-up
    /// read of the sequencer log.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) multi_raft:
        Arc<Mutex<MultiRaft>>,
    /// Hands sequencer entries to the sequencer group, locally on its leader
    /// and by forward from any other node.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) sequencer_proposer:
        Arc<dyn SequencerProposer>,
    /// Sequencer entries proposed and not yet seen applied. See
    /// [`super::owed`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) owed: OwedEntries,
    /// Shared handle to the sequencer state machine. The state machine records,
    /// per vShard, the earliest Raft index whose fan-out `try_send` was DROPPED
    /// (channel Full/Closed) so a dropped `SchedulerInput` never permanently
    /// diverges this replica's lock table from its peers. The catch-up drain
    /// (`drain_catch_up`, run on the periodic stall tick) TAKEs that index,
    /// replays the committed sequencer log range through the SAME
    /// `process_scheduler_input` path, and thereby reconstructs the missed input.
    /// Shared `Arc<Mutex<_>>` with the Raft apply loop; both are Control Plane,
    /// so holding it crosses no plane boundary.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) sequencer_state_machine:
        Arc<Mutex<SequencerStateMachine>>,
    /// Deterministic lock manager for this vshard. Shared (via `Arc<Mutex<_>>`)
    /// with the Control-Plane write-admission gate through
    /// `SharedState.calvin.lock_managers`, so a fast-path point write contends
    /// on the SAME lock table this scheduler validates against. The scheduler
    /// still runs single-threaded per vShard, so the mutex is uncontended except
    /// for the brief probe the gate takes.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) lock_manager:
        Arc<Mutex<LockManager>>,
    /// In-flight static/active transactions awaiting executor response,
    /// including those whose request waits in `deferred` for capacity.
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
    /// Highest replicated epoch observed across all scheduler inputs so far.
    /// Advances monotonically as `process_scheduler_input` sees new inputs; the
    /// lease-based reservation reap uses it (minus `LEASE_EPOCHS`) as the
    /// deterministic threshold below which an orphaned shared reservation is
    /// released. Purely a function of replicated input order — no wall clock.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) max_input_epoch: u64,
    /// Shared mirror of `applied`, read by authorization coverage.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) applied_mirror:
        Arc<crate::control::cluster::calvin::scheduler::AppliedMirror>,
    /// Scheduler configuration.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) config: SchedulerConfig,
    /// Metrics.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) metrics: Arc<SchedulerMetrics>,
    /// Fan-in receiver for executor responses.
    ///
    /// Each dispatched transaction spawns a lightweight bridge task that
    /// awaits the per-request `ResponseReceiver` and forwards the
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
    /// Requests the bridge dispatcher refused at capacity, in refusal order.
    /// Each txn stays in flight and holds its locks until its request is
    /// re-sent. Holds at most one step per in-flight txn, plus one
    /// write-version record per committed txn.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) deferred: DeferredQueue,
    /// The bridge dispatcher's capacity-freed signal, cloned once at
    /// construction. The run loop waits on it while requests are deferred.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) capacity_freed: Arc<Notify>,
    /// Last observed intake gate state. See [`super::intake`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) intake: IntakeGate,
    /// First halt cause, once set. See [`super::halt`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) halt: HaltLatch,
}

/// Parameters for [`Scheduler::new`].
pub struct SchedulerParams {
    pub vshard_id: u32,
    pub receiver: mpsc::Receiver<SchedulerInput>,
    pub shared: Arc<SharedState>,
    pub multi_raft: Arc<Mutex<MultiRaft>>,
    /// The node's sequencer proposer. Production passes one
    /// `RaftSequencerProposer` shared by every scheduler on the node.
    pub sequencer_proposer: Arc<dyn SequencerProposer>,
    /// Shared sequencer state machine, source of the per-vShard catch-up index
    /// the drain replays from. Same `Arc` the Raft apply loop drives.
    pub sequencer_state_machine: Arc<Mutex<SequencerStateMachine>>,
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
    /// `SharedState.calvin.lock_managers` under the SAME `Arc` passed here.
    pub lock_manager: Arc<Mutex<LockManager>>,
    /// Receiver for gate-side lock promotions. Constructed by
    /// `reconcile_vshard_schedulers`; its `UnboundedSender` is registered in
    /// `SharedState.calvin.promotion_senders` for this same vShard so a fast-path
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
            sequencer_proposer,
            sequencer_state_machine,
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

        let applied_mirror = shared.authorization_fence.calvin_mirrors().register(
            vshard_id,
            fully_applied_epoch,
            &applied_tail,
        );

        let capacity_freed = shared
            .dispatcher
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .capacity_freed();

        Self {
            vshard_id,
            receiver,
            shared,
            multi_raft,
            sequencer_proposer,
            owed: OwedEntries::new(),
            sequencer_state_machine,
            lock_manager,
            pending: BTreeMap::new(),
            blocked: BTreeMap::new(),
            dependent_barrier: BTreeMap::new(),
            read_result_rx,
            applied: AppliedGate::new(fully_applied_epoch, applied_tail),
            applied_mirror,
            rebuild_target_epoch,
            max_input_epoch: 0,
            config,
            metrics,
            completion_rx,
            completion_tx,
            promotion_rx,
            registry,
            verdict_rx,
            deferred: DeferredQueue::new(),
            capacity_freed,
            intake: IntakeGate::default(),
            halt: HaltLatch::default(),
        }
    }

    /// Whether the scheduler has caught up to the rebuild target epoch.
    ///
    /// `rebuild_target_epoch` is seeded from `AppliedRecovery::max_applied_epoch`
    /// (see `recovery.rs`): [`NOT_YET_APPLIED_EPOCH`] means the WAL scan found NO
    /// `CalvinApplied` marker for this vShard at all — a greenfield node with no
    /// Calvin history — never a real epoch (epoch 0 with markers reports
    /// `max_applied_epoch == 0`, distinct from the sentinel). With nothing to
    /// rebuild, such a node is trivially caught up.
    ///
    /// `fully_applied_epoch()` is conservatively seeded to the same sentinel by
    /// recovery (the watermark only advances once the sequencer's re-fan-out
    /// supplies per-epoch expected-position counts) — it does NOT mean "nothing
    /// left to apply". Naively comparing `u64::MAX >= rebuild_target_epoch` would
    /// therefore report caught-up before a single epoch was actually
    /// re-applied. So: sentinel `fully_applied_epoch` is caught-up ONLY when
    /// there is genuinely no rebuild target; otherwise it must NOT be treated as
    /// "ahead of everything".
    pub fn is_caught_up(&self) -> bool {
        if self.rebuild_target_epoch == NOT_YET_APPLIED_EPOCH {
            // No Calvin history for this vShard — nothing to rebuild.
            return true;
        }
        let fully_applied = self.applied.fully_applied_epoch();
        if fully_applied == NOT_YET_APPLIED_EPOCH {
            // Nothing proven fully-applied yet, but a real target exists.
            return false;
        }
        fully_applied >= self.rebuild_target_epoch
    }

    /// Publish an advanced fully-applied watermark to the metrics gauge and the
    /// shared cross-shard snapshot anchor.
    ///
    /// `BEGIN` reads `CalvinLocalState::last_applied_epoch` to anchor a
    /// session's cross-shard snapshot version, so it MUST reflect the
    /// FULLY-applied epoch — never an epoch that has only some of its positions
    /// committed, which would let a session anchor on a torn epoch. `fetch_max`
    /// keeps it monotonic across all per-vShard schedulers writing the counter.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn publish_watermark(
        &self,
        watermark: u64,
    ) {
        self.metrics.update_last_applied_epoch(watermark);
        self.applied_mirror.fold(watermark);
        self.shared
            .calvin
            .last_applied_epoch
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
        mut response_rx: crate::control::ResponseReceiver,
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

        // Woken when a routed Data-Plane response frees dispatcher capacity.
        let capacity_freed = Arc::clone(&self.capacity_freed);
        // Set when a tick left armed catch-up unreplayed. The next open-gate
        // pass fires the tick at once to resume it.
        let mut catch_up_resume = false;

        loop {
            // Register for the capacity wake BEFORE the re-send pass. A
            // response routed after a refusal but before this point is
            // covered by the pass itself. One routed after it wakes the arm.
            let capacity_notified = capacity_freed.notified();
            tokio::pin!(capacity_notified);
            capacity_notified.as_mut().enable();
            if self.resends_deferred() {
                self.redispatch_deferred();
            }

            self.check_dependent_barrier_timeouts();
            self.check_awaiting_verdict_stalls();

            let intake_open = self.refresh_intake_gate();
            if intake_open && catch_up_resume {
                catch_up_resume = false;
                stall_tick.reset_immediately();
            }

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

                _ = &mut capacity_notified, if self.resends_deferred() => {
                    // Capacity freed: the next loop pass re-sends deferred
                    // requests in FIFO order.
                }

                maybe_txn = self.receiver.recv(), if intake_open => {
                    match maybe_txn {
                        Some(input) => self.process_scheduler_input(input),
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
                    // Replay any sequencer-fan-out inputs dropped on this replica
                    // (channel Full/Closed) so a missed `SchedulerInput` never
                    // permanently diverges this vShard's lock table from its peers.
                    // O(1) common case (no pending catch-up). See `drain_catch_up`.
                    // A closed intake gate skips the drain until it opens.
                    catch_up_resume =
                        !intake_open || self.drain_catch_up() == CatchUpDrain::Remaining;
                    // Propose again every owed sequencer entry not yet applied.
                    self.retry_owed_sequencer_entries();
                    // The top-of-loop check_awaiting_verdict_stalls /
                    // check_dependent_barrier_timeouts and the deferred re-send
                    // pass run on every wake; this arm guarantees the loop wakes
                    // to run them (and the drain) when no other event arrives.
                }
            }
        }
        // Every txn still pending stays unapplied on this replica.
        self.hold_all_redo_records();
    }

    /// Allocate a fresh request ID for a dispatch.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn next_request_id(
        &self,
    ) -> RequestId {
        self.shared.next_request_id()
    }
}

// ── `is_caught_up` sentinel handling ─────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use crate::control::cluster::calvin::scheduler::driver::core::test_support::build_test_scheduler;

    /// A freshly-recovered scheduler (`fully_applied_epoch` still the
    /// `NOT_YET_APPLIED_EPOCH` sentinel) with a REAL, non-zero rebuild target must
    /// NOT report caught-up. Naively comparing `u64::MAX >= rebuild_target_epoch`
    /// (the bug) would say "caught up" before a single epoch was re-applied.
    #[tokio::test]
    async fn is_caught_up_false_when_fully_applied_is_sentinel_and_target_is_real() {
        let (mut scheduler, _dir) = build_test_scheduler(0);
        scheduler.rebuild_target_epoch = 5;
        assert_eq!(
            scheduler.applied.fully_applied_epoch(),
            NOT_YET_APPLIED_EPOCH
        );

        assert!(
            !scheduler.is_caught_up(),
            "sentinel fully_applied_epoch with a real rebuild target must not be caught up"
        );
    }

    /// Once the applied watermark advances to (or past) a real rebuild target,
    /// the scheduler correctly reports caught-up.
    #[tokio::test]
    async fn is_caught_up_true_once_fully_applied_reaches_target() {
        let (mut scheduler, _dir) = build_test_scheduler(0);
        scheduler.rebuild_target_epoch = 5;

        scheduler.applied = AppliedGate::new(5, BTreeSet::new());
        assert!(
            scheduler.is_caught_up(),
            "fully_applied_epoch == rebuild_target_epoch must be caught up"
        );

        scheduler.applied = AppliedGate::new(7, BTreeSet::new());
        assert!(
            scheduler.is_caught_up(),
            "fully_applied_epoch > rebuild_target_epoch must be caught up"
        );
    }

    /// A greenfield node with NO Calvin history at all: `read_applied_recovery`
    /// seeds `max_applied_epoch` (hence `rebuild_target_epoch`) to
    /// `NOT_YET_APPLIED_EPOCH` too (see `recovery.rs`'s
    /// `greenfield_returns_sentinel_and_empty_tail` test) — this is distinct from
    /// a real target of epoch 0 (which would report `max_applied_epoch == 0`).
    /// With nothing to rebuild, the scheduler is trivially caught up even though
    /// `fully_applied_epoch` is still the sentinel.
    #[tokio::test]
    async fn is_caught_up_true_when_no_rebuild_target_exists() {
        let (mut scheduler, _dir) = build_test_scheduler(0);
        // `build_test_scheduler` defaults `rebuild_target_epoch` to `0` (a REAL
        // target) for its own catch-up-drain tests; set it to the sentinel here to
        // model the actual greenfield-recovery value.
        scheduler.rebuild_target_epoch = NOT_YET_APPLIED_EPOCH;
        assert_eq!(
            scheduler.applied.fully_applied_epoch(),
            NOT_YET_APPLIED_EPOCH
        );

        assert!(
            scheduler.is_caught_up(),
            "no rebuild target (greenfield node) must report caught-up"
        );
    }
}
