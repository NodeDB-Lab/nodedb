// SPDX-License-Identifier: BUSL-1.1

//! Phase 3 of `start_raft`: build the `RaftLoop` from the phase-1/2
//! dependencies, consume `pending_subsystems`, build the Calvin sequencer
//! service, spawn the vShard schedulers, and start the cluster subsystems
//! (health/gossip/etc.) that share the loop's `MultiRaft`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::{self, Sender};

use nodedb_cluster::calvin::{
    CalvinCompletionRegistry, SequencerConfig, SequencerService, new_inbox,
};

use crate::control::cluster::calvin::executor::ollp::OllpConfig;
use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;
use crate::control::cluster::calvin::{ReadResultEvent, SchedulerConfig};
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
    pub(super) ollp_orchestrator: Arc<OllpOrchestrator>,
    pub(super) tracker: Arc<ProposeTracker>,
    pub(super) apply_rx: mpsc::Receiver<ApplyBatch>,
    pub(super) calvin_read_result_senders: Arc<Mutex<BTreeMap<u32, Sender<ReadResultEvent>>>>,
    pub(super) calvin_completion_registry: Arc<CalvinCompletionRegistry>,
}

/// Build the `RaftLoop`, consume `pending_subsystems`, build the sequencer
/// service + OLLP orchestrator, spawn the vShard schedulers, and start the
/// cluster subsystems that share the loop's `MultiRaft`.
pub(super) fn build_raft_loop(
    handle: &ClusterHandle,
    shared: &Arc<SharedState>,
    data_dir: &std::path::Path,
    multi_raft: nodedb_cluster::multi_raft::MultiRaft,
    setup: GroupSetup,
    hooks: Hooks,
) -> crate::Result<LoopBuild> {
    let GroupSetup {
        tracker,
        data_applier,
        apply_rx,
        calvin_completion_registry,
        calvin_verdict_rx,
        sequencer_state_machine,
        calvin_read_result_senders,
        metadata_applier,
        plan_executor,
        vshard_handler,
        tick_interval,
        snapshot_chunk_bytes,
        orphan_partial_max_age_secs,
        replication_factor,
    } = setup;

    let raft_loop = Arc::new(
        nodedb_cluster::RaftLoop::new(
            multi_raft,
            handle.transport.clone(),
            handle.topology.clone(),
            data_applier,
        )
        .with_plan_executor(plan_executor)
        .with_metadata_applier(metadata_applier)
        .with_vshard_handler(vshard_handler)
        .with_tick_interval(tick_interval)
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
        .with_data_dir(data_dir.to_path_buf())
        .with_snapshot_chunk_bytes(snapshot_chunk_bytes)
        .with_orphan_partial_max_age_secs(orphan_partial_max_age_secs)
        .with_replication_factor(replication_factor),
    );

    // Spawn cluster subsystems now that the loop owns `MultiRaft`.
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

    let sequencer_config = SequencerConfig::default();
    let (sequencer_inbox, sequencer_inbox_rx) = new_inbox(10_000, &sequencer_config);
    let ollp_orchestrator = Arc::new(OllpOrchestrator::new(OllpConfig::default()));
    let sequencer_service = SequencerService::new(
        sequencer_config,
        handle.node_id,
        raft_loop_handle.clone(),
        sequencer_inbox_rx,
        sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .next_epoch(),
        Arc::clone(&calvin_completion_registry),
        calvin_verdict_rx,
    );
    let sequencer_metrics = Arc::clone(&sequencer_service.metrics);

    let scheduler_config = SchedulerConfig::default();
    spawn_vshard_schedulers(SpawnVshardSchedulersParams {
        handle,
        shared,
        raft_loop_handle: raft_loop_handle.clone(),
        sequencer_state_machine: &sequencer_state_machine,
        calvin_read_result_senders: &calvin_read_result_senders,
        calvin_completion_registry: &calvin_completion_registry,
        scheduler_config: &scheduler_config,
    })?;

    let running = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(nodedb_cluster::start_cluster_subsystems(
            &pending.config,
            Arc::clone(&handle.topology),
            Arc::clone(&handle.routing),
            Arc::clone(&handle.transport),
            raft_loop_handle,
        ))
    })
    .map_err(|e| crate::Error::Config {
        detail: format!("cluster subsystem start: {e}"),
    })?;
    *handle
        .running_cluster
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = Some(running);

    Ok(LoopBuild {
        raft_loop,
        sequencer_service,
        sequencer_metrics,
        sequencer_inbox,
        ollp_orchestrator,
        tracker,
        apply_rx,
        calvin_read_result_senders,
        calvin_completion_registry,
    })
}
