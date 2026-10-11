// SPDX-License-Identifier: BUSL-1.1

//! Phase 3 of `start_raft`: build the `RaftLoop` from the phase-1/2
//! dependencies, consume `pending_subsystems`, build the Calvin sequencer
//! service, spawn the vShard schedulers, and start the cluster subsystems
//! (health/gossip/etc.) that share the loop's `MultiRaft`.

use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use nodedb_cluster::calvin::{
    CalvinCompletionRegistry, SequencerConfig, SequencerReceivers, SequencerService,
    SequencerStateMachine, new_inbox, new_reservation_inbox,
};

use crate::control::cluster::calvin::SchedulerConfig;
use crate::control::cluster::calvin::executor::ollp::OllpConfig;
use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;
use crate::control::cluster::handle::ClusterHandle;
use crate::control::cluster::start_raft_helpers::{
    SpawnVshardSchedulersParams, spawn_vshard_schedulers,
};
use crate::control::distributed_applier::{ApplyBatch, ProposeTracker};
use crate::control::state::SharedState;

use super::group_setup::GroupSetup;
use super::hooks::Hooks;

pub(super) type RaftLoopType = nodedb_cluster::RaftLoop<
    crate::control::cluster::spsc_applier::SpscCommitApplier,
    crate::control::LocalPlanExecutor,
>;

/// Everything phase 3 produces: the running `RaftLoop`, the Calvin
/// sequencer service (not yet spawned — the caller decides shutdown
/// wiring), and the phase-1 values later phases (proposer wiring,
/// observability) still need.
pub(super) struct LoopBuild {
    pub(super) raft_loop: Arc<RaftLoopType>,
    pub(super) sequencer_service: SequencerService,
    pub(super) sequencer_metrics: Arc<nodedb_cluster::calvin::SequencerMetrics>,
    pub(super) sequencer_inbox: nodedb_cluster::calvin::Inbox,
    pub(super) reservation_inbox:
        nodedb_cluster::calvin::sequencer::reservation_inbox::ReservationInbox,
    pub(super) ollp_orchestrator: Arc<OllpOrchestrator>,
    pub(super) tracker: Arc<ProposeTracker>,
    pub(super) apply_rx: mpsc::Receiver<ApplyBatch>,
    pub(super) calvin_completion_registry: Arc<CalvinCompletionRegistry>,
    pub(super) token_state: nodedb_cluster::SharedTokenStateMirror,
    /// Shared with the compactor wiring so sequencer-group log compaction can
    /// be held down below any armed scheduler catch-up index.
    pub(super) sequencer_state_machine: Arc<Mutex<SequencerStateMachine>>,
}

/// Build the `RaftLoop`, consume `pending_subsystems`, build the sequencer
/// service + OLLP orchestrator, spawn the vShard schedulers, and start the
/// cluster subsystems that share the loop's `MultiRaft`.
pub(super) async fn build_raft_loop(
    handle: &ClusterHandle,
    shared: &Arc<SharedState>,
    data_dir: &std::path::Path,
    mut multi_raft: nodedb_cluster::multi_raft::MultiRaft,
    setup: GroupSetup,
    hooks: Hooks,
) -> crate::Result<LoopBuild> {
    // Metadata entries this node stamps as leader carry its HLC. The clock
    // starts above every stamp this node applied, so a stamp it takes rises
    // above entries compacted out of its log.
    crate::control::cluster::metadata_stamp::fold_metadata_stamp_hwm(shared)?;
    multi_raft.set_metadata_clock(Arc::clone(&shared.hlc_clock));
    let GroupSetup {
        tracker,
        data_applier,
        apply_rx,
        calvin_completion_registry,
        calvin_verdict_rx,
        sequencer_state_machine,
        metadata_applier,
        token_state,
        plan_executor,
        vshard_handler,
        tick_interval,
        snapshot_chunk_bytes,
        orphan_partial_max_age_secs,
        replication_factor,
    } = setup;

    // The authorization lease runs on the Raft timing of this node.
    let lease_timing = crate::control::security::auth_lease::LeaseTiming::from_raft(
        multi_raft.election_timeout_min(),
        multi_raft.heartbeat_interval(),
    )?;
    if !shared.authorization_fence.install_timing(lease_timing) {
        tracing::warn!(
            "authorization lease timing already set — start_raft appears to have run twice"
        );
    }
    let lease_service = Arc::new(
        crate::control::security::auth_lease::LeaderLeaseService::new(
            Arc::downgrade(shared),
            lease_timing,
        ),
    );
    if !shared
        .authorization_fence
        .install_leader(Arc::clone(&lease_service))
    {
        tracing::warn!(
            "authorization lease service already set — start_raft appears to have run twice"
        );
    }
    // Authorization coverage of the sequencer group settles each completion
    // ack against this node's schedulers.
    calvin_completion_registry.applied_acks.enable();

    // The sequencer log compacts at the threshold every group runs with,
    // once its state machine's capture is durable.
    let data_applier = data_applier.with_sequencer_compaction(
        crate::control::cluster::sequencer_compaction::SequencerCompaction::new(
            Arc::clone(&hooks.sequencer_snapshots),
            shared,
            multi_raft.log_compaction_threshold(),
        ),
    );

    let raft_loop = Arc::new(
        nodedb_cluster::RaftLoop::new(
            multi_raft,
            handle.transport.clone(),
            handle.topology.clone(),
            data_applier,
        )
        .with_plan_executor(plan_executor)
        .with_metadata_applier(metadata_applier)
        .with_metadata_cache(shared.metadata_cache.clone())
        .with_lease_holder_liveness(Arc::clone(&shared.lease_runtime.holder_liveness))
        .with_vshard_handler(vshard_handler)
        .with_tick_interval(tick_interval)
        // The routing table, the cluster epoch and a join's topology are
        // saved to the same catalog the node restarts from.
        .with_catalog(Arc::clone(&handle.catalog))
        .with_group_watchers(handle.group_watchers.clone())
        .with_snapshot_quarantine_hook(hooks.quarantine_hook)
        .with_snapshot_builder(hooks.snapshot_builder)
        .with_snapshot_applier(hooks.snapshot_applier)
        .with_shuffle_receiver(hooks.shuffle_receiver)
        .with_shuffle_producer(hooks.shuffle_producer)
        .with_shuffle_consumer(hooks.shuffle_consumer)
        .with_shuffle_aggregator(hooks.shuffle_aggregator)
        .with_assign_remote_surrogate(hooks.assign_remote_surrogate)
        .with_calvin_submit(hooks.calvin_submit)
        .with_calvin_submit_inbox(hooks.calvin_submit_inbox)
        .with_reserve_read(hooks.reserve_read)
        .with_release_reservation(hooks.release_reservation)
        .with_data_propose_gate(hooks.data_propose_gate)
        .with_auth_lease(lease_service)
        .with_data_dir(data_dir.to_path_buf())
        .with_snapshot_chunk_bytes(snapshot_chunk_bytes)
        .with_orphan_partial_max_age_secs(orphan_partial_max_age_secs)
        .with_replication_factor(replication_factor),
    );

    // Spawn cluster subsystems only after the loop owns `MultiRaft`.
    // They share the same `Arc<Mutex<MultiRaft>>` the loop holds, so
    // shutdown is symmetric (subsystems are torn down before the
    // loop's strong ref drops). See `nodedb_cluster::start_cluster`
    // doc for the two-phase startup rationale.
    let pending = handle
        .pending_subsystems
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take()
        .ok_or_else(|| crate::Error::Config {
            detail: "start_raft called twice: pending_subsystems already consumed".into(),
        })?;
    let raft_loop_handle = raft_loop.multi_raft_handle();
    crate::control::pitr::spawn_metadata_log_archiver(shared, raft_loop_handle.clone());

    let sequencer_config = SequencerConfig::default();
    sequencer_config
        .validate()
        .map_err(|e| crate::Error::Config {
            detail: e.to_string(),
        })?;
    // Each scheduler runs on the epoch duration of the sequencer feeding it.
    let scheduler_config = SchedulerConfig::from_tuning(&shared.tuning.calvin, &sequencer_config);
    let (sequencer_inbox, sequencer_inbox_rx) = new_inbox(10_000, &sequencer_config);
    let (reservation_inbox, reservation_inbox_rx) = new_reservation_inbox(10_000);
    let ollp_orchestrator = Arc::new(OllpOrchestrator::new(OllpConfig::default()));
    let sequencer_service = SequencerService::new(
        sequencer_config,
        handle.node_id,
        raft_loop_handle.clone(),
        SequencerReceivers {
            inbox: sequencer_inbox_rx,
            reservations: reservation_inbox_rx,
        },
        // The state machine, NOT a starting epoch read here: at this point in
        // startup the Raft loop below has not been spawned, so nothing has
        // replayed the sequencer group's committed log into it and its epoch
        // counter still reads 0 on every restart. The service derives its seed
        // lazily on the first leader tick that finds the group replayed —
        // see `SequencerService::ensure_epoch_seeded`.
        Arc::clone(&sequencer_state_machine),
        Arc::clone(&calvin_completion_registry),
        calvin_verdict_rx,
    );
    let sequencer_metrics = Arc::clone(&sequencer_service.metrics);

    spawn_vshard_schedulers(SpawnVshardSchedulersParams {
        handle,
        shared,
        raft_loop_handle: raft_loop_handle.clone(),
        sequencer_state_machine: &sequencer_state_machine,
        calvin_completion_registry: &calvin_completion_registry,
        scheduler_config: &scheduler_config,
    })?;

    let running = nodedb_cluster::start_cluster_subsystems(
        &pending.config,
        nodedb_cluster::SubsystemHandles {
            topology: Arc::clone(&handle.topology),
            routing: Arc::clone(&handle.routing),
            transport: Arc::clone(&handle.transport),
            raft_multi_raft: raft_loop_handle,
        },
        &handle.catalog,
        nodedb_cluster::SwimWiring {
            transport: Arc::clone(&pending.swim_transport),
            subscribers: vec![Arc::clone(&shared.lease_runtime.holder_liveness)
                as Arc<dyn nodedb_cluster::MembershipSubscriber>],
        },
        Arc::clone(&handle.migration_tracker),
    )
    .await
    .map_err(|e| crate::Error::Config {
        detail: format!("cluster subsystem start: {e}"),
    })?;

    // Bridge this node's own decommission into the process's graceful
    // shutdown path before handing `running` off — `nodedb-cluster` has no
    // process-shutdown primitive of its own, so this is where the two sides
    // meet. See `crate::control::cluster::decommission_bridge`.
    crate::control::cluster::spawn_decommission_shutdown_bridge(
        &shared.loop_registry,
        &shared.shutdown,
        running.decommission_signal(),
    );

    *handle
        .running_cluster
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = Some(running);

    Ok(LoopBuild {
        raft_loop,
        sequencer_service,
        sequencer_metrics,
        sequencer_inbox,
        reservation_inbox,
        ollp_orchestrator,
        tracker,
        apply_rx,
        calvin_completion_registry,
        token_state,
        sequencer_state_machine,
    })
}
