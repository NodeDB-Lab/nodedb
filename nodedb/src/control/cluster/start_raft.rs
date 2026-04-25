//! Start the Raft event loop, RPC server, and both appliers.

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use nodedb_types::config::tuning::ClusterTransportTuning;

use crate::control::cluster::handle::ClusterHandle;
use crate::control::cluster::metadata_applier::MetadataCommitApplier;
use crate::control::cluster::spsc_applier::SpscCommitApplier;
use crate::control::state::SharedState;

/// Start the Raft event loop and RPC server.
///
/// Must be called after `SharedState` is constructed (needs the WAL and
/// dispatcher for the `SpscCommitApplier`). Moves the `MultiRaft` out of
/// `handle.multi_raft` into the `RaftLoop`; must be called **exactly
/// once** per handle.
pub fn start_raft(
    handle: &ClusterHandle,
    shared: Arc<SharedState>,
    _data_dir: &std::path::Path,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    transport_tuning: &ClusterTransportTuning,
) -> crate::Result<tokio::sync::watch::Receiver<bool>> {
    // Move the MultiRaft constructed by `start_cluster` into this
    // function. Rebuilding it here from the routing table would lose
    // learner membership for joining nodes and would double-open
    // per-group redb log files.
    let multi_raft = handle
        .multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take()
        .ok_or_else(|| crate::Error::Config {
            detail: "start_raft called twice: cluster multi_raft already consumed".into(),
        })?;

    let data_applier = SpscCommitApplier::new(shared.clone());

    // Production metadata applier: writes to the shared cache,
    // writes back to the `SystemCatalog` redb so every non-cache
    // reader observes the change, bumps the applied-index watcher,
    // broadcasts `CatalogChangeEvent`, and spawns Data Plane
    // `Register` dispatches on committed `CollectionDdl::Create`.
    let metadata_applier_concrete = Arc::new(MetadataCommitApplier::new(
        handle.metadata_cache.clone(),
        handle.applied_index_watcher.clone(),
        shared.catalog_change_tx.clone(),
        shared.credentials.clone(),
    ));
    // Install the Weak<SharedState> before the raft loop starts
    // ticking so no commit can reach the applier without it.
    metadata_applier_concrete.install_shared(Arc::downgrade(&shared));
    let metadata_applier: Arc<dyn nodedb_cluster::MetadataApplier> =
        metadata_applier_concrete.clone();

    // LocalPlanExecutor is the C-β physical-plan execution path (C-δ.6: sole execution path).
    let plan_executor = Arc::new(crate::control::LocalPlanExecutor::new(shared.clone()));

    let tick_interval = Duration::from_millis(transport_tuning.raft_tick_interval_ms);
    let raft_loop = Arc::new(
        nodedb_cluster::RaftLoop::new(
            multi_raft,
            handle.transport.clone(),
            handle.topology.clone(),
            data_applier,
        )
        .with_plan_executor(plan_executor)
        .with_metadata_applier(metadata_applier)
        .with_tick_interval(tick_interval),
    );

    // Publish the cluster observability handle to SharedState before
    // any listener starts serving.
    let observer = Arc::new(nodedb_cluster::ClusterObserver::new(
        handle.node_id,
        handle.lifecycle.clone(),
        handle.topology.clone(),
        handle.routing.clone(),
        raft_loop.clone() as Arc<dyn nodedb_cluster::GroupStatusProvider + Send + Sync>,
    ));
    if shared.cluster_observer.set(observer).is_err() {
        tracing::warn!("cluster_observer already set — start_raft appears to have run twice");
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
    // their in-memory HWM on every checkpoint. Mirrors the pattern
    // used by `MetadataCommitApplier::install_shared`.
    shared
        .surrogate_assigner
        .install_shared(Arc::downgrade(&shared));

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

    // Start the Raft tick loop.
    let rl_run = raft_loop.clone();
    let sr_raft = shutdown_rx.clone();
    tokio::spawn(async move {
        rl_run.run(sr_raft).await;
        info!("raft loop stopped");
    });

    // Start the RPC server (accepts inbound QUIC connections).
    let transport_serve = handle.transport.clone();
    let rl_handler = raft_loop.clone();
    let sr_serve = shutdown_rx.clone();
    tokio::spawn(async move {
        if let Err(e) = transport_serve.serve(rl_handler, sr_serve).await {
            tracing::error!(error = %e, "raft RPC server failed");
        }
    });

    // Wire version of every node is now carried on the live
    // `NodeInfo` in `cluster_topology`, stamped by the joiner on
    // join_request and the self-register path on bootstrap — no
    // shadow map to populate here. Log the derived view for
    // observability.
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
    // topology re-broadcast). Without this, topology updates are
    // only propagated via the fire-and-forget broadcast during the
    // join flow — if that single broadcast is lost (peer QUIC server
    // not yet accepting), the peer never converges.
    let health_config = nodedb_cluster::HealthConfig {
        ping_interval: Duration::from_secs(transport_tuning.health_ping_interval_secs),
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
    let sr_health = shutdown_rx;
    tokio::spawn(async move {
        health_monitor.run(sr_health).await;
    });

    info!(node_id = handle.node_id, "raft loop and RPC server started");

    Ok(ready_rx)
}
