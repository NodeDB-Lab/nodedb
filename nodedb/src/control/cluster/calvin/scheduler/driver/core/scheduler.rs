// SPDX-License-Identifier: BUSL-1.1

//! `Scheduler` struct definition and constructor.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, mpsc};

use nodedb_cluster::MultiRaft;
use nodedb_cluster::calvin::types::SchedulerInput;
use nodedb_cluster::calvin::{CalvinCompletionRegistry, SequencerStateMachine, VerdictSignal};

use super::super::barrier::PendingDependentBarrier;
use super::super::config::SchedulerConfig;
use super::super::types::{BlockedTxn, PendingTxn};
use super::deferred::DeferredQueue;
use super::halt::HaltLatch;
use super::intake::IntakeGate;
use super::owed::OwedEntries;
use super::sequencer_proposer::SequencerProposer;
use crate::bridge::envelope::Response;
use crate::control::cluster::calvin::scheduler::AppliedGate;
use crate::control::cluster::calvin::scheduler::lock_manager::{LockManager, TxnId};
use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
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
    /// Handle to MultiRaft for the data-group role, the redo proposals, and
    /// the catch-up read of the sequencer log.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) multi_raft:
        Arc<Mutex<MultiRaft>>,
    /// The data groups' leadership, read each pass without the `MultiRaft`
    /// lock.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) leader_board:
        nodedb_cluster::multi_raft::LeaderBoard,
    /// The routing table `multi_raft` shares, for the vShard-to-group lookup.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) routing:
        Arc<std::sync::RwLock<nodedb_cluster::routing::RoutingTable>>,
    /// This node's role in the vShard's data group. See [`super::role`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) role: super::role::StageGate,
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
    /// Granted transactions that have not finished: staged ones on the
    /// leader, held ones on a follower, including those whose request waits
    /// in `deferred` for capacity. `BTreeMap` ensures deterministic
    /// iteration order.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) pending:
        BTreeMap<TxnId, PendingTxn>,
    /// Installs the apply loop reported for txns still waiting for their
    /// locks. Each completes when its locks are granted.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) early_applied:
        BTreeMap<TxnId, super::redo_applied::AppliedRedo>,
    /// This scheduler's registered inbox: how the data-group apply loop
    /// concluded each stamped redo of the vShard.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) inbox:
        crate::control::cluster::calvin::scheduler::InboxHandle,
    /// Blocked transactions awaiting lock release.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) blocked:
        BTreeMap<TxnId, BlockedTxn>,
    /// Dependent-read barriers this leader holds open. `BTreeMap` for
    /// determinism.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) dependent_barrier:
        BTreeMap<TxnId, PendingDependentBarrier>,
    /// The barrier entries of the data-group log, folded per dependent txn
    /// this scheduler holds: at a barrier, or in `pending`. A barrier of the
    /// txn decides from them. Each entry leaves when its txn completes.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) barrier_logs:
        BTreeMap<TxnId, crate::control::cluster::calvin::scheduler::BarrierLog>,
    /// This vShard's read-result buffer: the barrier entries the apply loop
    /// applied for txns this scheduler did not hold yet.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) read_results:
        Arc<crate::control::cluster::calvin::scheduler::VShardReadResults>,
    /// Committed txns whose stage failed on this leader, with their
    /// restage count and backoff. See [`super::restage`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) restages:
        BTreeMap<TxnId, super::restage::Restage>,
    /// Exactly-once applied gate: the fully-applied watermark plus the set of
    /// applied `(epoch, position)` pairs above it. Replaces a bare per-epoch
    /// counter so a multi-position epoch is never marked applied on the strength
    /// of its first completing position.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) applied: AppliedGate,
    /// Rebuild target epoch: the highest applied epoch of the ledger when
    /// the scheduler started.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) rebuild_target_epoch: u64,
    /// Highest replicated epoch observed across all scheduler inputs so far.
    /// Advances monotonically as `process_scheduler_input` sees new inputs; the
    /// lease-based reservation reap uses it (minus `LEASE_EPOCHS`) as the
    /// deterministic threshold below which an orphaned shared reservation is
    /// released. Purely a function of replicated input order — no wall clock.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) max_input_epoch: u64,
    /// Backup cut markers this scheduler received: the commit HLC floors they
    /// set and the markers not yet reported.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) cut_floors:
        crate::control::cluster::calvin::scheduler::cut_floor::CutFloors,
    /// Ordered cuts whose barrier this vShard's later redo waits for.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) cut_holds:
        crate::control::cluster::calvin::scheduler::cut_hold::CutHolds,
    /// This vShard's applied ledger: the gate's seed, and the shared record
    /// of each position this scheduler finishes.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) ledger:
        Arc<crate::control::cluster::calvin::scheduler::CalvinAppliedLedger>,
    /// This scheduler's caught-up entry, read by the startup readiness gate.
    /// Set once the watermark reaches `rebuild_target_epoch`.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) caught_up:
        crate::control::cluster::calvin::scheduler::CaughtUpHandle,
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
    /// re-sent. Holds at most one awaited step per in-flight txn, plus the
    /// discards it issued.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) deferred: DeferredQueue,
    /// The bridge dispatcher's capacity-freed signal, cloned once at
    /// construction. The run loop waits on it while requests are deferred.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) capacity_freed: Arc<Notify>,
    /// Last observed intake gate state. See [`super::intake`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) intake: IntakeGate,
    /// First halt cause, once set. See [`super::halt`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) halt: HaltLatch,
    /// The sequenced txn waiting for this node's metadata apply. See
    /// [`super::metadata_hold`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) metadata_hold:
        Option<super::metadata_hold::MetadataHold>,
    /// The multi-part transactions this vShard participates in and has not
    /// staged yet. See [`super::parts`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) parts:
        super::parts::PartsState,
    /// The vShard base this scheduler started under. See
    /// [`super::install_gate`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) install_gate:
        super::install_gate::InstallGate,
    /// The `RETURNING` evaluations a commit vote waits for, and the
    /// transcripts each txn keeps. See [`super::returning_eval`].
    pub(in crate::control::cluster::calvin::scheduler::driver::core) returning_evals:
        super::returning_eval::ReturningEvals,
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
    /// This vShard's applied ledger. The applied gate starts from its
    /// state.
    pub ledger: Arc<crate::control::cluster::calvin::scheduler::CalvinAppliedLedger>,
    /// The epoch the scheduler must fully apply before it reports caught
    /// up. [`NOT_YET_APPLIED_EPOCH`](crate::control::cluster::calvin::scheduler::NOT_YET_APPLIED_EPOCH)
    /// means nothing to rebuild.
    pub rebuild_target_epoch: u64,
    pub config: SchedulerConfig,
    pub metrics: Arc<SchedulerMetrics>,
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
            ledger,
            rebuild_target_epoch,
            config,
            metrics,
            lock_manager,
            promotion_rx,
            registry,
            verdict_rx,
        } = params;

        // Capacity: at most one completion per inflight txn. Use the incoming
        // channel capacity as a proxy for the max concurrent pending count.
        let completion_cap = config.channel_capacity;
        let (completion_tx, completion_rx) = mpsc::channel(completion_cap);

        let (fully_applied_epoch, applied_tail) = ledger.snapshot();
        let caught_up = shared.calvin.caught_up.register(vshard_id);

        // A backup's cut waits on every scheduler this node runs.
        shared.calvin.cuts.register(vshard_id);
        let install_gate = super::install_gate::InstallGate::new(&shared, vshard_id);
        let inbox = shared
            .calvin
            .inboxes
            .register(vshard_id, config.max_inflight_backlog);
        let read_results = shared.calvin.read_results.vshard(vshard_id);

        let capacity_freed = shared
            .dispatcher
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .capacity_freed();

        // Taken after this vShard's lock table is registered: every write the
        // gate admitted with no table, or with a retired one, is earlier.
        let start_fence = shared.calvin.admission_tickets.start_fence(vshard_id);
        let (leader_board, routing) = {
            let mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            (mr.leader_board(), mr.routing())
        };
        let scheduler = Self {
            vshard_id,
            receiver,
            shared,
            multi_raft,
            leader_board,
            routing,
            role: super::role::StageGate::fenced(start_fence),
            sequencer_proposer,
            owed: OwedEntries::new(),
            sequencer_state_machine,
            lock_manager,
            pending: BTreeMap::new(),
            early_applied: BTreeMap::new(),
            inbox,
            blocked: BTreeMap::new(),
            dependent_barrier: BTreeMap::new(),
            barrier_logs: BTreeMap::new(),
            read_results,
            restages: BTreeMap::new(),
            applied: AppliedGate::new(fully_applied_epoch, applied_tail),
            cut_floors: Default::default(),
            cut_holds: Default::default(),
            ledger,
            caught_up,
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
            metadata_hold: None,
            parts: Default::default(),
            install_gate,
            returning_evals: super::returning_eval::ReturningEvals::new(),
        };
        // A scheduler with nothing to rebuild reports caught up at once.
        scheduler.publish_caught_up();
        scheduler
    }
}
