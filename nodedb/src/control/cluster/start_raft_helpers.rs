// SPDX-License-Identifier: BUSL-1.1

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, RwLock};

use nodedb_cluster::calvin::{CalvinCompletionRegistry, SequencerStateMachine};

use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
use crate::control::cluster::calvin::{
    RaftSequencerProposer, Scheduler, SchedulerConfig, SchedulerParams, SequencerProposer,
};
use crate::control::cluster::calvin_snapshot;
use crate::control::cluster::handle::ClusterHandle;
use crate::control::state::SharedState;

/// The vShards whose scheduler this node started and has not stopped.
type ServedVShards = Arc<Mutex<BTreeSet<u32>>>;

/// The vShards this node currently hosts: the union of `vshards_for_group` over
/// every Raft group whose member set includes this node, read from the live
/// routing table.
fn hosted_vshards(routing: &RwLock<nodedb_cluster::RoutingTable>, node_id: u64) -> Vec<u32> {
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let mut vshards = Vec::new();
    for (group_id, info) in routing.group_members() {
        if info.members.contains(&node_id) {
            vshards.extend(routing.vshards_for_group(*group_id));
        }
    }
    vshards.sort_unstable();
    vshards.dedup();
    vshards
}

/// Stop the scheduler of every vShard this node served and either no longer
/// hosts or holds a new base for.
///
/// A node that left a vShard's group holds none of its state. Its scheduler
/// will go on applying the vShard's slices against stale data and acking
/// them. A snapshot install replaced the state a running scheduler started
/// from. Dropping the vShard's sequencer sender closes the scheduler's
/// intake, and the scheduler exits. Its other channels and its lock table go
/// too. It hands the barrier events of the txns it did not finish back to
/// the vShard's read-result buffer. The next reconcile that finds the vShard hosted spawns a new
/// scheduler from the vShard's base.
fn retire_vshard_schedulers(
    hosted: &[u32],
    shared: &Arc<SharedState>,
    sequencer_state_machine: &Arc<Mutex<SequencerStateMachine>>,
    served_vshards: &ServedVShards,
    calvin_completion_registry: &Arc<CalvinCompletionRegistry>,
) {
    let (left, rebased): (Vec<u32>, Vec<u32>) = {
        let mut served_set = served_vshards.lock().unwrap_or_else(|p| p.into_inner());
        let served: Vec<u32> = served_set.iter().copied().collect();
        let left: Vec<u32> = served
            .iter()
            .copied()
            .filter(|vshard| hosted.binary_search(vshard).is_err())
            .collect();
        let rebased: Vec<u32> = calvin_snapshot::rebased(shared, &served)
            .into_iter()
            .filter(|vshard| !left.contains(vshard))
            .collect();
        for vshard in left.iter().chain(&rebased) {
            served_set.remove(vshard);
        }
        (left, rebased)
    };
    calvin_snapshot::forget_left(shared, &left);
    // A vShard that left this node keeps no barrier events here, in memory
    // or stored. A rebased vShard keeps them for the scheduler that starts
    // again.
    for &vshard in &left {
        if let Err(e) =
            crate::control::cluster::calvin::scheduler::barrier_store::forget_vshard(shared, vshard)
        {
            // The rows stay. Boot marks them stored for a vShard with no
            // scheduler, and a snapshot install of the vShard replaces them.
            tracing::warn!(
                vshard_id = vshard,
                error = %e,
                "calvin: the barrier rows of a vShard that left were not removed"
            );
            crate::diag::calvin_barrier_log_store_failed(vshard, None, "remove", &e);
        }
    }
    for &vshard in &rebased {
        tracing::info!(
            vshard_id = vshard,
            "calvin: a snapshot replaced the vShard's state; its scheduler starts again"
        );
    }
    let left: Vec<u32> = left.into_iter().chain(rebased).collect();
    if left.is_empty() {
        return;
    }
    {
        let mut sm = sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        for &vshard in &left {
            sm.remove_vshard_sender(vshard);
        }
    }
    for &vshard in &left {
        shared
            .calvin
            .lock_managers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&vshard);
        shared
            .calvin
            .promotion_senders
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&vshard);
        // The write gate's holds on the stopped lock table order nothing.
        shared
            .calvin
            .admission_holds
            .release_vshard(crate::types::VShardId::new(vshard));
        calvin_completion_registry.unregister_verdict_signal_sender(vshard);
        // A cut on this node waits only on the schedulers it runs.
        shared.calvin.cuts.unregister(vshard);
        tracing::info!(vshard_id = vshard, "calvin: the vShard's scheduler stops");
    }
}

/// Parameters for [`reconcile_vshard_schedulers`].
struct ReconcileSchedulersParams<'a> {
    node_id: u64,
    routing: &'a Arc<RwLock<nodedb_cluster::RoutingTable>>,
    shared: &'a Arc<SharedState>,
    raft_loop_handle: &'a Arc<Mutex<nodedb_cluster::multi_raft::MultiRaft>>,
    /// One proposer per node, so its forward limit bounds the whole node.
    sequencer_proposer: &'a Arc<dyn SequencerProposer>,
    sequencer_state_machine: &'a Arc<Mutex<SequencerStateMachine>>,
    served_vshards: &'a ServedVShards,
    calvin_completion_registry: &'a Arc<CalvinCompletionRegistry>,
    scheduler_config: &'a SchedulerConfig,
}

/// Idempotently ensure a Calvin `Scheduler` runs for exactly the vShards this
/// node currently hosts.
///
/// A vShard is already served when `served_vshards` holds it. A served
/// vShard this node no longer hosts has its scheduler stopped first (see
/// [`retire_vshard_schedulers`]). Only newly-hosted vShards get a fresh
/// scheduler — this pass never double-spawns. Returns the number of NEW
/// schedulers started.
fn reconcile_vshard_schedulers(params: ReconcileSchedulersParams<'_>) -> crate::Result<usize> {
    let ReconcileSchedulersParams {
        node_id,
        routing,
        shared,
        raft_loop_handle,
        sequencer_proposer,
        sequencer_state_machine,
        served_vshards,
        calvin_completion_registry,
        scheduler_config,
    } = params;

    let hosted = hosted_vshards(routing, node_id);
    retire_vshard_schedulers(
        &hosted,
        shared,
        sequencer_state_machine,
        served_vshards,
        calvin_completion_registry,
    );
    calvin_snapshot::retain_mounted(shared, raft_loop_handle, routing);
    crate::control::cluster::redo_stream_membership::reconcile_redo_streams(
        shared,
        raft_loop_handle,
    );
    let mut spawned = 0usize;
    let mut kept = Vec::new();
    for vshard_id in hosted {
        // Already-served vShards keep their running scheduler untouched.
        if served_vshards
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(&vshard_id)
        {
            continue;
        }

        // The earliest committed sequencer index still in the retained log — the
        // lower bound the spawn-time catch-up (below) arms from. `None` while
        // the log has no known start.
        let sequencer_start = raft_loop_handle
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .sequencer_log_start();
        let group_id = routing
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .group_for_vshard(vshard_id)
            .ok();
        // A scheduler starts only from Calvin state that reaches the log it
        // catches up from. The sequencer log keeps that range meanwhile.
        if !calvin_snapshot::may_start(
            shared,
            raft_loop_handle,
            vshard_id,
            group_id,
            sequencer_start,
        ) {
            continue;
        }
        let Some(first_available) = sequencer_start else {
            continue;
        };

        // The vShard's applied ledger: boot recovery or a snapshot install
        // filled it, and a vShard with no Calvin history here gets an empty
        // one. The scheduler rebuilds up to its highest applied epoch.
        let ledger = shared.calvin.applied.get_or_create(vshard_id);
        let rebuild_target_epoch = ledger.max_applied_epoch();
        let (sequenced_tx, sequenced_rx) =
            tokio::sync::mpsc::channel(scheduler_config.channel_capacity);

        {
            let mut sm = sequencer_state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            sm.set_vshard_sender(vshard_id, sequenced_tx);
            // Arm a spawn-time catch-up BEFORE the scheduler runs. A scheduler
            // subscribes only once this node's membership in the vShard's data
            // group lands (a late, reconcile-driven event on a forming cluster),
            // by which point the sequencer can have already committed — and
            // fanned out to a then-absent sender, i.e. SILENTLY skipped — epochs
            // for this vShard. A fresh replica has nothing durably applied to
            // rebuild from, so it will otherwise consider itself caught up and
            // never receive those txns (cross-shard graph edges among them),
            // losing them permanently after it becomes leader. Arming from the
            // first available index makes the scheduler's drain replay every
            // committed sequencer entry for this vShard applied before it
            // subscribed; replay is idempotent (in-flight guard + Reserve/Release
            // no-ops).
            sm.arm_catch_up_from(vshard_id, first_available);
        }
        // The armed catch-up holds the log from here, so the base is kept.
        kept.push((vshard_id, first_available));

        served_vshards
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(vshard_id);

        // The deterministic lock table is shared between this scheduler and the
        // Control-Plane write-admission gate: build it once and register the
        // SAME `Arc` in `CalvinLocalState::lock_managers` so a fast-path point write and
        // this scheduler's validation contend on one mutex.
        let lock_manager = Arc::new(Mutex::new(
            crate::control::cluster::calvin::scheduler::lock_manager::LockManager::new(),
        ));
        shared
            .calvin
            .lock_managers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(vshard_id, Arc::clone(&lock_manager));

        // Promotion channel: when a Control-Plane fast-path write-admission guard
        // drops and `release` promotes a waiter this scheduler enqueued behind the
        // fast-path key, the guard forwards the promoted `TxnId`s over this
        // unbounded sender. Register the sender for the SAME vShard so the gate can
        // find it, and hand the receiver to the scheduler's run loop. Unbounded is
        // safe: promotions are bounded by in-flight txns and the send runs from a
        // synchronous `Drop` that must not block.
        let (promotion_tx, promotion_rx) = tokio::sync::mpsc::unbounded_channel();
        shared
            .calvin
            .promotion_senders
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(vshard_id, promotion_tx);

        // Verdict-push channel: `note_verdict` (on this node's completion
        // registry) broadcasts a `VerdictSignal` to every registered per-vShard
        // scheduler the instant a cross-shard verdict becomes durable, so a txn
        // parked on the commit barrier resumes with low latency. Bounded like the
        // other scheduler channels; a dropped push is backstopped by the
        // scheduler's probe-on-park and stall re-probe sweep.
        let (verdict_tx, verdict_rx) =
            tokio::sync::mpsc::channel(scheduler_config.channel_capacity);
        calvin_completion_registry.register_verdict_signal_sender(vshard_id, verdict_tx);

        let scheduler = Scheduler::new(SchedulerParams {
            vshard_id,
            receiver: sequenced_rx,
            shared: Arc::clone(shared),
            multi_raft: raft_loop_handle.clone(),
            sequencer_proposer: Arc::clone(sequencer_proposer),
            sequencer_state_machine: Arc::clone(sequencer_state_machine),
            ledger,
            rebuild_target_epoch,
            config: scheduler_config.clone(),
            metrics: SchedulerMetrics::new(),
            lock_manager,
            promotion_rx,
            registry: Arc::clone(calvin_completion_registry),
            verdict_rx,
        });
        // Route through `spawn_loop_no_abort` so the Control Plane drain
        // waits for the scheduler to exit (dropping its captured
        // `Arc<SharedState>` deterministically) but NEVER force-aborts it: a
        // Calvin `Scheduler` advances a replicated state machine and a
        // mid-epoch `.abort()` will diverge this node from its peers.
        // `Scheduler::run` already breaks at an epoch-safe boundary (its
        // `biased` shutdown arm sits at the top of the select loop), so on a
        // signal it exits well within the shutdown deadline.
        //
        // The scheduler dispatches to the Data Plane, so it drains at
        // `DrainingControlPlane`, before the enqueue gate closes.
        //
        // Fixed `&'static str` name: N schedulers register under one key.
        // `LoopRegistry::register` stores handles in a `Vec` with no de-dup,
        // so duplicate names are tolerated and each handle is joined
        // independently. Per-vShard identity is carried by the `vshard_id`
        // field the scheduler logs on start/stop.
        crate::control::shutdown::spawn_loop_no_abort(
            &shared.loop_registry,
            &shared.shutdown,
            "calvin_scheduler",
            crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
            move |shutdown| async move {
                scheduler.run(shutdown).await;
            },
        );
        spawned += 1;
    }
    calvin_snapshot::note_kept(shared, &kept);
    Ok(spawned)
}

/// Parameters for [`spawn_vshard_schedulers`].
pub(super) struct SpawnVshardSchedulersParams<'a> {
    pub(super) handle: &'a ClusterHandle,
    pub(super) shared: &'a Arc<SharedState>,
    pub(super) raft_loop_handle: Arc<Mutex<nodedb_cluster::multi_raft::MultiRaft>>,
    pub(super) sequencer_state_machine: &'a Arc<Mutex<SequencerStateMachine>>,
    pub(super) calvin_completion_registry: &'a Arc<CalvinCompletionRegistry>,
    pub(super) scheduler_config: &'a SchedulerConfig,
}

/// Spawn Calvin `Scheduler` tasks for this node's vShards and keep the set in
/// sync with cluster membership.
///
/// Scheduler ownership is derived from the routing table's group membership, but
/// a JOINING node's membership is established AFTER `start_raft` runs (it
/// propagates via conf-change once the node is admitted to each data group). A
/// one-shot snapshot at startup therefore misses every vShard on a freshly
/// joined node, leaving cross-shard Calvin transactions whose participants live
/// there permanently un-dispatched (no completion ack → submit times out).
///
/// To make scheduler registration correct regardless of when membership lands —
/// and resilient to later ownership changes — this runs an initial reconcile
/// (covers the bootstrap node, which already sees its membership) and then
/// spawns a background task that re-reconciles on a short interval until
/// shutdown. Reconcile is idempotent: it starts schedulers for vShards this
/// node gained and stops those of vShards it left. Each pass also runs the
/// membership pass over this node's chunked redo streams (see
/// [`crate::control::cluster::redo_stream_membership`]).
pub(super) fn spawn_vshard_schedulers(
    params: SpawnVshardSchedulersParams<'_>,
) -> crate::Result<()> {
    let SpawnVshardSchedulersParams {
        handle,
        shared,
        raft_loop_handle,
        sequencer_state_machine,
        calvin_completion_registry,
        scheduler_config,
    } = params;

    let node_id = handle.node_id;
    let served_vshards: ServedVShards = Arc::new(Mutex::new(BTreeSet::new()));
    let routing = Arc::clone(&handle.routing);
    let sequencer_proposer: Arc<dyn SequencerProposer> = Arc::new(RaftSequencerProposer::new(
        node_id,
        Arc::clone(&raft_loop_handle),
        shared,
    ));
    // A backup's cut proposes its marker through the same proposer.
    if shared
        .calvin
        .sequencer_proposer
        .set(Arc::clone(&sequencer_proposer))
        .is_err()
    {
        tracing::warn!("calvin: the sequencer proposer was set already; keeping the first");
    }

    // Initial reconcile: schedulers for vShards this node already knows it hosts.
    reconcile_vshard_schedulers(ReconcileSchedulersParams {
        node_id,
        routing: &routing,
        shared,
        raft_loop_handle: &raft_loop_handle,
        sequencer_proposer: &sequencer_proposer,
        sequencer_state_machine,
        served_vshards: &served_vshards,
        calvin_completion_registry,
        scheduler_config,
    })?;

    // Background reconcile: pick up vShards whose membership lands after startup
    // (joiner admission) or shifts later (rebalancing). The routing table has no
    // change-notification, so a short fixed-interval reconcile is the simplest
    // self-healing mechanism; each pass is a cheap routing read + map probe and
    // a no-op once the set has converged.
    let shared_task = Arc::clone(shared);
    let sm_task = Arc::clone(sequencer_state_machine);
    let served_task = Arc::clone(&served_vshards);
    let registry_task = Arc::clone(calvin_completion_registry);
    let cfg_task = scheduler_config.clone();
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "calvin_vshard_reconcile",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {
                        if let Err(e) = reconcile_vshard_schedulers(ReconcileSchedulersParams {
                            node_id,
                            routing: &routing,
                            shared: &shared_task,
                            raft_loop_handle: &raft_loop_handle,
                            sequencer_proposer: &sequencer_proposer,
                            sequencer_state_machine: &sm_task,
                            served_vshards: &served_task,
                            calvin_completion_registry: &registry_task,
                            scheduler_config: &cfg_task,
                        }) {
                            tracing::warn!(node_id, error = %e, "calvin scheduler reconcile pass failed");
                        }
                    }
                }
            }
        },
    );

    Ok(())
}
