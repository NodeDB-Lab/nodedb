// SPDX-License-Identifier: BUSL-1.1

use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};

use nodedb_cluster::calvin::{CalvinCompletionRegistry, SequencerStateMachine};
use nodedb_cluster::distributed_array::{ArrayLocalExecutor, handle_array_shard_rpc};
use nodedb_cluster::vshard_handler::{DispatchTarget, dispatch_by_type};
use nodedb_cluster::wire::VShardEnvelope;

use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
use crate::control::cluster::calvin::scheduler::read_applied_recovery;
use crate::control::cluster::calvin::{
    ReadResultEvent, Scheduler, SchedulerConfig, SchedulerParams,
};
use crate::control::cluster::handle::ClusterHandle;
use crate::control::state::SharedState;

/// Build the `VShardEnvelopeHandler` closure used by `RaftLoop`.
///
/// The closure receives raw envelope bytes from the QUIC transport layer,
/// dispatches based on `msg_type`, and returns a serialized response.
pub(super) fn build_vshard_handler(
    array_executor: Arc<dyn ArrayLocalExecutor>,
) -> nodedb_cluster::VShardEnvelopeHandler {
    Arc::new(move |bytes: Vec<u8>| {
        let executor = array_executor.clone();
        let fut: Pin<
            Box<dyn std::future::Future<Output = nodedb_cluster::error::Result<Vec<u8>>> + Send>,
        > = Box::pin(async move {
            let envelope = VShardEnvelope::from_bytes(&bytes).ok_or_else(|| {
                nodedb_cluster::error::ClusterError::Codec {
                    detail: "vshard_handler: failed to deserialize VShardEnvelope".into(),
                }
            })?;

            let target = dispatch_by_type(&envelope);
            match target {
                DispatchTarget::ArrayShard => {
                    let opcode = envelope.msg_type as u32;
                    let resp_payload = handle_array_shard_rpc(
                        opcode,
                        envelope.vshard_id,
                        &envelope.payload,
                        &executor,
                    )
                    .await?;

                    // Response opcode = request opcode + 1 for all array shard RPCs.
                    // Resolve the msg_type variant via a minimal scratch envelope parse
                    // (avoids any unsafe transmute — the `from_bytes` mapping in wire.rs
                    // is the canonical source of truth for the opcode→variant table).
                    let resp_opcode = opcode + 1;
                    let resp_msg_type = resolve_vshard_msg_type(resp_opcode)?;
                    let resp_envelope = VShardEnvelope::new(
                        resp_msg_type,
                        envelope.target_node,
                        envelope.source_node,
                        envelope.vshard_id,
                        resp_payload,
                    );
                    Ok(resp_envelope.to_bytes())
                }

                other => Err(nodedb_cluster::error::ClusterError::Transport {
                    detail: format!(
                        "vshard_handler: no handler registered for dispatch target {other:?}"
                    ),
                }),
            }
        });
        fut
    })
}

/// Type alias for the shared per-vShard read-result sender registry.
type ReadResultSenders =
    Arc<Mutex<std::collections::BTreeMap<u32, tokio::sync::mpsc::Sender<ReadResultEvent>>>>;

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

/// Parameters for [`reconcile_vshard_schedulers`].
struct ReconcileSchedulersParams<'a> {
    node_id: u64,
    routing: &'a Arc<RwLock<nodedb_cluster::RoutingTable>>,
    shared: &'a Arc<SharedState>,
    raft_loop_handle: &'a Arc<Mutex<nodedb_cluster::multi_raft::MultiRaft>>,
    sequencer_state_machine: &'a Arc<Mutex<SequencerStateMachine>>,
    calvin_read_result_senders: &'a ReadResultSenders,
    calvin_completion_registry: &'a Arc<CalvinCompletionRegistry>,
    scheduler_config: &'a SchedulerConfig,
}

/// Idempotently ensure a Calvin `Scheduler` is running for every vShard this
/// node currently hosts.
///
/// A vShard is considered already-served iff it has a registered read-result
/// sender (the schedulers' presence registry). Only newly-hosted vShards get a
/// fresh scheduler — this pass never double-spawns. Returns the number of NEW
/// schedulers started.
///
/// This is `add-only`: it never tears down a scheduler for a vShard that has
/// left this node. vShard removal happens via migration / decommission, which
/// own their own teardown path; wiring scheduler removal into that lifecycle is
/// tracked as a separate follow-up.
fn reconcile_vshard_schedulers(params: ReconcileSchedulersParams<'_>) -> crate::Result<usize> {
    let ReconcileSchedulersParams {
        node_id,
        routing,
        shared,
        raft_loop_handle,
        sequencer_state_machine,
        calvin_read_result_senders,
        calvin_completion_registry,
        scheduler_config,
    } = params;

    let mut spawned = 0usize;
    for vshard_id in hosted_vshards(routing, node_id) {
        // Already-served vShards keep their running scheduler untouched.
        if calvin_read_result_senders
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&vshard_id)
        {
            continue;
        }

        let recovery = read_applied_recovery(&shared.wal, vshard_id)?;
        let (sequenced_tx, sequenced_rx) =
            tokio::sync::mpsc::channel(scheduler_config.channel_capacity);
        sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .set_vshard_sender(vshard_id, sequenced_tx);

        let (read_result_tx, read_result_rx) =
            tokio::sync::mpsc::channel(scheduler_config.channel_capacity);
        calvin_read_result_senders
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(vshard_id, read_result_tx);

        // The deterministic lock table is shared between this scheduler and the
        // Control-Plane write-admission gate: build it once and register the
        // SAME `Arc` in `calvin_lock_managers` so a fast-path point write and
        // this scheduler's validation contend on one mutex.
        let lock_manager = Arc::new(Mutex::new(
            crate::control::cluster::calvin::scheduler::lock_manager::LockManager::new(),
        ));
        shared
            .calvin_lock_managers
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
            .calvin_promotion_senders
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
            fully_applied_epoch: recovery.fully_applied_epoch,
            applied_tail: recovery.applied_tail,
            rebuild_target_epoch: recovery.max_applied_epoch,
            config: scheduler_config.clone(),
            metrics: SchedulerMetrics::new(),
            read_result_rx,
            lock_manager,
            promotion_rx,
            registry: Arc::clone(calvin_completion_registry),
            verdict_rx,
        });
        // Route through `spawn_loop_no_abort` so `LoopRegistry::shutdown_all`
        // waits for the scheduler to exit (dropping its captured
        // `Arc<SharedState>` deterministically) but NEVER force-aborts it: a
        // Calvin `Scheduler` advances a replicated state machine and a
        // mid-epoch `.abort()` would diverge this node from its peers.
        // `Scheduler::run` already breaks at an epoch-safe boundary (its
        // `biased` shutdown arm sits at the top of the select loop), so on a
        // signal it exits well within the shutdown deadline.
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
            move |shutdown| async move {
                scheduler.run(shutdown).await;
            },
        );
        spawned += 1;
    }
    Ok(spawned)
}

/// Parameters for [`spawn_vshard_schedulers`].
pub(super) struct SpawnVshardSchedulersParams<'a> {
    pub(super) handle: &'a ClusterHandle,
    pub(super) shared: &'a Arc<SharedState>,
    pub(super) raft_loop_handle: Arc<Mutex<nodedb_cluster::multi_raft::MultiRaft>>,
    pub(super) sequencer_state_machine: &'a Arc<Mutex<SequencerStateMachine>>,
    pub(super) calvin_read_result_senders: &'a ReadResultSenders,
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
/// shutdown. Reconcile is idempotent and add-only.
pub(super) fn spawn_vshard_schedulers(
    params: SpawnVshardSchedulersParams<'_>,
) -> crate::Result<()> {
    let SpawnVshardSchedulersParams {
        handle,
        shared,
        raft_loop_handle,
        sequencer_state_machine,
        calvin_read_result_senders,
        calvin_completion_registry,
        scheduler_config,
    } = params;

    let node_id = handle.node_id;
    let routing = Arc::clone(&handle.routing);

    // Initial reconcile: schedulers for vShards this node already knows it hosts.
    reconcile_vshard_schedulers(ReconcileSchedulersParams {
        node_id,
        routing: &routing,
        shared,
        raft_loop_handle: &raft_loop_handle,
        sequencer_state_machine,
        calvin_read_result_senders,
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
    let rr_task = Arc::clone(calvin_read_result_senders);
    let registry_task = Arc::clone(calvin_completion_registry);
    let cfg_task = scheduler_config.clone();
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "calvin_vshard_reconcile",
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
                            sequencer_state_machine: &sm_task,
                            calvin_read_result_senders: &rr_task,
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

/// Resolve a raw opcode `u32` to a `VShardMessageType` variant.
///
/// Uses `VShardEnvelope::from_bytes` as the canonical opcode→variant mapping
/// so this helper stays in sync with the wire format without duplicating the
/// match table.
pub(super) fn resolve_vshard_msg_type(
    opcode: u32,
) -> nodedb_cluster::error::Result<nodedb_cluster::wire::VShardMessageType> {
    let mut scratch = [0u8; 26];
    scratch[0..2].copy_from_slice(&1u16.to_le_bytes()); // version
    scratch[2..4].copy_from_slice(&(opcode as u16).to_le_bytes()); // msg_type

    VShardEnvelope::from_bytes(&scratch)
        .map(|e| e.msg_type)
        .ok_or_else(|| nodedb_cluster::error::ClusterError::Codec {
            detail: format!("resolve_vshard_msg_type: unknown opcode {opcode}"),
        })
}
