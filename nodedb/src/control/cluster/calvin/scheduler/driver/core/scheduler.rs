// SPDX-License-Identifier: BUSL-1.1

//! `Scheduler` struct definition, constructor, and main run loop.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::mpsc;
use tracing::{debug, info};

use nodedb_cluster::MultiRaft;
use nodedb_cluster::calvin::types::SchedulerInput;
use nodedb_cluster::calvin::{
    CalvinCompletionRegistry, SEQUENCER_GROUP_ID, SequencerEntry, SequencerStateMachine,
    VerdictSignal,
};

use super::super::barrier::{PendingDependentBarrier, ReadResultEvent};
use super::super::config::SchedulerConfig;
use super::super::types::{BlockedTxn, PendingTxn};
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
    /// Handle to MultiRaft so completion acknowledgements can be proposed to
    /// the sequencer group.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) multi_raft:
        Arc<Mutex<MultiRaft>>,
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
    /// Pacing gate for the catch-up drain while dispatch capacity is saturated
    /// (issue 352): the stall tick defers its re-drive until this instant.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) dispatch_busy_until:
        Option<Instant>,
    /// Consecutive capacity-busy dispatch outcomes; drives the bounded backoff.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) dispatch_busy_attempts: u32,
    /// Highest replicated epoch observed across all scheduler inputs so far.
    /// Advances monotonically as `process_scheduler_input` sees new inputs; the
    /// lease-based reservation reap uses it (minus `LEASE_EPOCHS`) as the
    /// deterministic threshold below which an orphaned shared reservation is
    /// released. Purely a function of replicated input order — no wall clock.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) max_input_epoch: u64,
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
    pub receiver: mpsc::Receiver<SchedulerInput>,
    pub shared: Arc<SharedState>,
    pub multi_raft: Arc<Mutex<MultiRaft>>,
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

        Self {
            vshard_id,
            receiver,
            shared,
            multi_raft,
            sequencer_state_machine,
            lock_manager,
            pending: BTreeMap::new(),
            blocked: BTreeMap::new(),
            dependent_barrier: BTreeMap::new(),
            read_result_rx,
            applied: AppliedGate::new(fully_applied_epoch, applied_tail),
            rebuild_target_epoch,
            dispatch_busy_until: None,
            dispatch_busy_attempts: 0,
            max_input_epoch: 0,
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
                    // Issue 352: while dispatch capacity is saturated, defer the
                    // catch-up drain's re-drive instead of spinning on a full
                    // per-tenant queue. The next tick after the backoff retries.
                    // no-determinism: the drain gate reads the wall clock; not Calvin WAL data
                    let now = Instant::now();
                    if self.dispatch_busy_until.map(|t| now < t).unwrap_or(false) {
                        debug!(
                            vshard_id = self.vshard_id,
                            "calvin scheduler: drain deferred (capacity busy)"
                        );
                    } else {
                        self.dispatch_busy_until = None;
                        // Replay any sequencer-fan-out inputs dropped on this
                        // replica (channel Full/Closed) so a missed
                        // `SchedulerInput` never permanently diverges this
                        // vShard's lock table from its peers. O(1) common case
                        // (no pending catch-up). See `drain_catch_up`.
                        self.drain_catch_up();
                    }
                    // The top-of-loop check_awaiting_verdict_stalls /
                    // check_dependent_barrier_timeouts do the stall work on every
                    // wake; this arm guarantees the loop wakes to run them (and the
                    // drain) when no other event arrives.
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

// ── `is_caught_up` sentinel handling ─────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeSet, HashMap};
    use std::time::Duration;

    use nodedb_cluster::RoutingTable;

    use crate::bridge::dispatch::Dispatcher;

    /// Build a minimally-wired `Scheduler` for driver-level unit tests. The Data
    /// Plane is NOT started — tests exercise Control-Plane routing, guards, and
    /// request dispatch only, so no core loop is needed. The returned `TempDir`
    /// must be kept alive for the scheduler's lifetime (backs the WAL and
    /// Raft storage).
    fn build_test_scheduler(vshard_id: u32) -> (Scheduler, tempfile::TempDir) {
        let registry = CalvinCompletionRegistry::new_detached();
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap(),
        );
        let (dispatcher, mut data_sides) = Dispatcher::new(1, 64);
        let _data_side = data_sides
            .pop()
            .expect("one configured core has one data side");
        let shared = SharedState::new(dispatcher, wal).unwrap();

        let rt = RoutingTable::uniform(1, &[1], 1);
        let multi_raft = Arc::new(Mutex::new(MultiRaft::new(1, rt, dir.path().to_path_buf())));

        let sequencer_state_machine = Arc::new(Mutex::new(SequencerStateMachine::new(
            HashMap::new(),
            Arc::clone(&registry),
        )));

        let (_tx, receiver) = mpsc::channel(16);
        let (_rr_tx, read_result_rx) = mpsc::channel(16);
        let (_prom_tx, promotion_rx) = mpsc::unbounded_channel();
        let (verdict_tx, verdict_rx) = mpsc::channel(16);
        registry.register_verdict_signal_sender(vshard_id, verdict_tx);

        let lock_manager = Arc::new(Mutex::new(LockManager::new()));

        let scheduler = Scheduler::new(SchedulerParams {
            vshard_id,
            receiver,
            shared,
            multi_raft,
            sequencer_state_machine,
            // A freshly-built scheduler has applied nothing, so its watermark is the
            // not-yet-applied sentinel (matching `read_applied_recovery` for a clean
            // node). Hardcoding `0` here would instead claim epoch 0 is fully applied,
            // making the exactly-once gate (`AppliedGate::is_applied`) short-circuit
            // every epoch-0 replay before it reaches the lock table — silently
            // defeating the end-to-end drain tests below.
            fully_applied_epoch: NOT_YET_APPLIED_EPOCH,
            applied_tail: BTreeSet::new(),
            rebuild_target_epoch: 0,
            config: SchedulerConfig::default(),
            metrics: SchedulerMetrics::new(),
            read_result_rx,
            lock_manager,
            promotion_rx,
            registry,
            verdict_rx,
        });
        (scheduler, dir)
    }

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

    #[test]
    fn busy_backoff_is_bounded_and_jittered() {
        assert_eq!(Scheduler::busy_backoff(0, 0, 0), Duration::from_millis(5));
        assert_eq!(Scheduler::busy_backoff(3, 0, 0), Duration::from_millis(40));
        assert!(Scheduler::busy_backoff(30, 0, 0) <= Duration::from_millis(257));
        // jitter varies with vshard + epoch and stays inside the 0..=6 ms band
        assert_ne!(
            Scheduler::busy_backoff(2, 1, 0),
            Scheduler::busy_backoff(2, 1, 1)
        );
        assert!(Scheduler::busy_backoff(2, 0, 0) >= Duration::from_millis(20));
        assert!(Scheduler::busy_backoff(2, 0, 6) <= Duration::from_millis(26));
    }
}
