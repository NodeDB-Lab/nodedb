// SPDX-License-Identifier: BUSL-1.1

//! Phase 5 (final) of `start_raft`: publish Calvin/OLLP state, the cluster
//! observer, live Raft leader-status, and the metadata-Raft proposer handle
//! onto `SharedState`; install and start the surrogate reservation refiller;
//! subscribe the boot-ready watch; register loop metrics; and spawn the
//! Raft tick loop, sequencer service, RPC server, and health monitor.

use std::sync::Arc;

use tracing::info;

use nodedb_cluster::calvin::CalvinCompletionRegistry;

use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;
use crate::control::cluster::handle::ClusterHandle;
use crate::control::state::SharedState;

use super::loop_build::RaftLoopType;

/// Everything the final phase needs beyond `handle`/`shared`.
pub(super) struct ObservabilityInputs {
    pub(super) sequencer_inbox: nodedb_cluster::calvin::Inbox,
    pub(super) reservation_inbox:
        nodedb_cluster::calvin::sequencer::reservation_inbox::ReservationInbox,
    pub(super) sequencer_metrics: Arc<nodedb_cluster::calvin::SequencerMetrics>,
    pub(super) calvin_completion_registry: Arc<CalvinCompletionRegistry>,
    pub(super) ollp_orchestrator: Arc<OllpOrchestrator>,
    pub(super) sequencer_service: nodedb_cluster::calvin::SequencerService,
}

/// Publish observability handles, spawn the surrogate refiller, and start
/// the Raft tick loop / sequencer service / RPC server / health monitor.
/// Returns the boot-ready watch receiver `start_raft` hands back to its
/// caller (`main.rs` awaits it before binding client-facing listeners).
pub(super) fn finish_observability(
    handle: &ClusterHandle,
    shared: &Arc<SharedState>,
    transport_tuning: &nodedb_types::config::tuning::ClusterTransportTuning,
    raft_loop: Arc<RaftLoopType>,
    inputs: ObservabilityInputs,
) -> tokio::sync::watch::Receiver<bool> {
    let ObservabilityInputs {
        sequencer_inbox,
        reservation_inbox,
        sequencer_metrics,
        calvin_completion_registry,
        ollp_orchestrator,
        sequencer_service,
    } = inputs;

    let _ = shared.sequencer_inbox.set(sequencer_inbox);
    let _ = shared.reservation_inbox.set(reservation_inbox);
    let _ = shared.sequencer_metrics.set(sequencer_metrics);
    let _ = shared
        .calvin_completion_registry
        .set(calvin_completion_registry);
    let _ = shared.ollp_orchestrator.set(ollp_orchestrator);

    // Publish the cluster observability handle to SharedState before
    // any listener starts serving.
    let observer = Arc::new(nodedb_cluster::ClusterObserver::new(
        handle.node_id,
        handle.lifecycle.clone(),
        handle.topology.clone(),
        handle.routing.clone(),
        // Weak: `SharedState` owns the observer, and the observer must not
        // keep `raft_loop` alive or the two form a strong reference cycle
        // that pins `SharedState` forever. The loop's spawned tasks keep it
        // alive during normal operation.
        Arc::downgrade(
            &(raft_loop.clone() as Arc<dyn nodedb_cluster::GroupStatusProvider + Send + Sync>),
        ),
    ));
    if shared.cluster_observer.set(observer).is_err() {
        tracing::warn!("cluster_observer already set — start_raft appears to have run twice");
    }

    // Publish a live Raft leader-status snapshot fn so routing (gateway +
    // graph scatter) resolves group leadership from CURRENT Raft state
    // rather than the (lagging) routing-table hint. Wraps the raft loop's
    // `group_statuses()` snapshot.
    // Weak for the same cycle-breaking reason as `cluster_observer` above.
    // A dropped loop (only reachable post-shutdown) yields an empty status
    // snapshot, which every consumer already treats as "no cluster groups"
    // — identical to the single-node case where this fn is never installed.
    let raft_loop_for_status = Arc::downgrade(&raft_loop);
    if shared
        .raft_status_fn
        .set(Arc::new(move || {
            raft_loop_for_status
                .upgrade()
                .map(|rl| rl.group_statuses())
                .unwrap_or_default()
        }))
        .is_err()
    {
        tracing::warn!("raft_status_fn already set — start_raft appears to have run twice");
    }

    // Publish the leadership confirmer so a linearizable read served on this
    // node proves against a quorum that it is still the leader. Holds the
    // same coordinator mutex the loop ticks, and the loop only weakly, to ask
    // a group leader for a read index from a follower.
    let gate: Arc<dyn crate::control::cluster::read_index::RaftReadGate> =
        Arc::new(crate::control::cluster::read_index::MultiRaftReadGate::new(
            raft_loop.multi_raft_handle(),
            Arc::downgrade(&raft_loop),
        ));
    if shared.raft_read_gate.set(gate).is_err() {
        tracing::warn!("raft_read_gate already set — start_raft appears to have run twice");
    }

    // Renew this node's authorization lease with the metadata leader. Its
    // confirmed coverage needs the read gate above.
    if let Some(timing) = shared.authorization_fence.timing() {
        let renew_state = Arc::clone(shared);
        crate::control::shutdown::spawn_loop(
            &shared.loop_registry,
            &shared.shutdown,
            "auth_lease_renew",
            crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
            move |shutdown| {
                crate::control::security::auth_lease::renew_loop::run_renew_loop(
                    renew_state,
                    timing,
                    shutdown,
                )
            },
        );
    }

    // Publish this node's cluster-epoch state so the routing gate can tell
    // whether this node has missed a topology transition before it coordinates
    // work on a view of the cluster that may already be superseded.
    if shared
        .cluster_epoch
        .set(raft_loop.cluster_epoch_handle())
        .is_err()
    {
        tracing::warn!("cluster_epoch already set — start_raft appears to have run twice");
    }

    // Publish the raft loop handle into SharedState so the metadata
    // proposer can reach it. The handle is type-erased behind a
    // trait object to keep the SharedState field concrete.
    let proposer_handle: Arc<dyn crate::control::metadata_proposer::MetadataRaftHandle> =
        Arc::new(crate::control::metadata_proposer::RaftLoopProposerHandle::new(raft_loop.clone()));
    if shared.metadata_raft.set(proposer_handle).is_err() {
        tracing::warn!("metadata_raft already set — start_raft appears to have run twice");
    }

    // Allow the surrogate assigner's flush path to propose
    // `SurrogateAlloc` entries to the Raft group so followers advance
    // their in-memory HWM on every checkpoint.
    shared
        .surrogate_assigner
        .install_shared(Arc::downgrade(shared));

    // Spawn the per-node surrogate reservation refiller. It owns ALL batch
    // reservation so the latency-critical `assign` insert path never blocks
    // on the metadata-Raft round-trip in steady state: it eagerly reserves
    // the first batch on its first iteration (before inserts arrive) and
    // tops the batch up whenever the hot path nudges it below the
    // low-watermark. The loop self-gates on the registry's static
    // Local/Cluster mode (decided at process start from whether
    // `config.cluster` is present, before this function runs) — this
    // function only runs at all when it is, so the registry is always
    // `Cluster` here; the gate is defense-in-depth, not the primary check.
    // Same lifetime/shutdown pattern as the sequencer ticker below.
    let refiller = shared.surrogate_assigner.clone();
    let refiller_shared = Arc::downgrade(shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "surrogate_refill_loop",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            tokio::select! {
                _ = refiller.run_refill_loop(refiller_shared) => {}
                _ = shutdown.wait_cancelled() => {}
            }
            info!("surrogate refill loop stopped");
        },
    );

    // Subscribe to the boot-time readiness watch BEFORE spawning the
    // tick loop so we cannot miss the first transition. The receiver
    // is returned to `main.rs`, which awaits it before binding any
    // client-facing listener.
    let ready_rx = raft_loop.subscribe_ready();

    // Register the raft-tick loop's standardized metrics so the
    // `/metrics` route can expose them alongside every other driver.
    shared
        .loop_metrics_registry
        .register(raft_loop.loop_metrics());

    // Start the Raft tick loop. `RaftLoop::run` takes a raw
    // `watch::Receiver<bool>` and drives shutdown internally, so it gets one
    // from the canonical watch; the `spawn_loop` receiver is unused. Routing
    // through `spawn_loop` registers the join handle so the Control Plane
    // drain waits for it (dropping its captured `Arc<RaftLoopType>`
    // deterministically). It drains there because a committed entry it
    // delivers is applied through a Data Plane dispatch.
    let rl_run = raft_loop.clone();
    let raft_raw_shutdown = shared.shutdown.raw_receiver();
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "raft_tick_loop",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |_shutdown| async move {
            rl_run.run(raft_raw_shutdown).await;
            info!("raft loop stopped");
        },
    );

    let seq_raw_shutdown = shared.shutdown.raw_receiver();
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "raft_sequencer",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |_shutdown| async move {
            let mut sequencer_service = sequencer_service;
            sequencer_service.run(seq_raw_shutdown).await;
            info!("sequencer service stopped");
        },
    );

    // Start the RPC server (accepts inbound QUIC connections).
    let transport_serve = handle.transport.clone();
    let rl_handler = raft_loop.clone();
    let serve_raw_shutdown = shared.shutdown.raw_receiver();
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "raft_rpc_serve",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |_shutdown| async move {
            if let Err(e) = transport_serve.serve(rl_handler, serve_raw_shutdown).await {
                tracing::error!(error = %e, "raft RPC server failed");
            }
        },
    );

    // Wire version of every node is now carried on the live
    // `NodeInfo` in `cluster_topology`. Log the derived view for observability.
    {
        let view = shared.cluster_version_view();
        let compat = crate::control::rolling_upgrade::should_compat_mode(&view);
        info!(
            node_id = handle.node_id,
            nodes = view.node_count,
            min_version = view.min_version,
            max_version = view.max_version,
            mixed = view.is_mixed_version(),
            compat_mode = compat,
            "cluster version view derived from topology"
        );
    }

    // Start the health monitor (periodic pings, failure detection,
    // topology re-broadcast).
    let health_config = nodedb_cluster::HealthConfig {
        ping_interval: std::time::Duration::from_secs(transport_tuning.health_ping_interval_secs),
        failure_threshold: transport_tuning.health_failure_threshold,
    };
    let health_monitor = Arc::new(nodedb_cluster::HealthMonitor::new(
        handle.node_id,
        handle.transport.clone(),
        handle.topology.clone(),
        handle.catalog.clone(),
        health_config,
    ));
    shared
        .loop_metrics_registry
        .register(health_monitor.loop_metrics());
    if shared.health_monitor.set(health_monitor.clone()).is_err() {
        tracing::warn!("health_monitor already set — start_raft appears to have run twice");
    }
    let health_raw_shutdown = shared.shutdown.raw_receiver();
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "raft_health_monitor",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |_shutdown| async move {
            health_monitor.run(health_raw_shutdown).await;
        },
    );

    info!(node_id = handle.node_id, "raft loop and RPC server started");

    ready_rx
}
