// SPDX-License-Identifier: BUSL-1.1

//! The one-node cluster boot every in-process harness runs.
//!
//! A server with no `[cluster]` section boots a one-node Calvin cluster:
//! `init_single_node_calvin`, `wire_cluster_handle`, `start_raft`, the
//! descriptor lease loop, then `await_cluster_ready`. A harness runs the same
//! production functions in the same order:
//!
//! 1. [`init`] synthesizes the cluster before `SharedState` is shared.
//! 2. [`wire`] installs its handles on the state before anything clones it.
//! 3. The harness starts its cores, response poller and Event Plane.
//! 4. [`start`] starts Raft and the lease loop, then waits for the boot
//!    readiness production waits for before it opens a listener.
//! 5. The harness binds its listeners.
//!
//! [`OneNodeRaft::shutdown`] stops what [`start`] started.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use nodedb::control::cluster::ClusterHandle;
use nodedb::control::startup::{StartupPhase, StartupSequencer};
use nodedb::control::state::SharedState;
use nodedb_types::config::tuning::ClusterTransportTuning;

/// Error type of every boot step.
pub type BootError = Box<dyn std::error::Error + Send + Sync>;

/// How long each shutdown step can take.
const SHUTDOWN_STEP: Duration = Duration::from_secs(5);

/// Transport tuning for a cluster whose only voter is this node.
///
/// A sole voter never waits on a peer's vote, so a short election timeout
/// elects it at once. The boot vote fence refuses votes to other nodes only,
/// so it never delays a sole voter.
pub fn tuning() -> ClusterTransportTuning {
    ClusterTransportTuning {
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        ..ClusterTransportTuning::default()
    }
}

/// Synthesize the one-node cluster rooted at `data_dir`, as boot does for a
/// server with no `[cluster]` section. A directory that already holds a
/// cluster catalog restarts that cluster.
pub async fn init(data_dir: &Path) -> Result<ClusterHandle, BootError> {
    Ok(nodedb::control::cluster::init_single_node_calvin(data_dir, &tuning()).await?)
}

/// Install `handle` on `state` and load the host tables from the catalog.
///
/// Runs before `state` is shared and before the Event Plane spawns: the
/// Event Plane starts the cross-shard drain only when its sender is wired.
pub fn wire(
    state: &mut SharedState,
    handle: &ClusterHandle,
    data_dir: &Path,
) -> Result<(), BootError> {
    nodedb::bootstrap::state_wiring::wire_cluster_handle(state, handle, data_dir)?;
    nodedb::control::cluster::metadata_applier::seed_host_tables(state)?;
    Ok(())
}

/// The Raft side of a running one-node cluster.
pub struct OneNodeRaft {
    running: Option<nodedb_cluster::RunningCluster>,
    lease_renewal: Option<tokio::task::JoinHandle<()>>,
    lease_shutdown_tx: tokio::sync::watch::Sender<bool>,
}

/// Start Raft and the descriptor lease loop on `shared`, then wait for the
/// readiness production boot waits for before it opens a listener.
///
/// The cores and the response poller run already: the readiness steps
/// dispatch to them. The wait covers the metadata group's first apply, the
/// data groups' replay, the schema rehydration and the first authorization
/// lease.
pub async fn start(
    handle: &ClusterHandle,
    shared: &Arc<SharedState>,
    data_dir: &Path,
) -> Result<OneNodeRaft, BootError> {
    let tuning = tuning();
    let ready_rx =
        nodedb::control::cluster::start_raft(handle, Arc::clone(shared), data_dir, &tuning).await?;
    let running = handle
        .running_cluster
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take();
    let (lease_shutdown_tx, lease_shutdown_rx) = tokio::sync::watch::channel(false);
    let (lease_renewal, lease_metrics) = nodedb::control::lease::LeaseRenewalLoop::spawn(
        Arc::clone(shared),
        &tuning,
        lease_shutdown_rx,
    )?;
    shared.loop_metrics_registry.register(lease_metrics);
    let raft = OneNodeRaft {
        running,
        lease_renewal: Some(lease_renewal),
        lease_shutdown_tx,
    };

    let (sequencer, _gate) = StartupSequencer::new();
    let gates = nodedb::bootstrap::cluster_ready::ClusterReadyGates {
        raft_gate: sequencer.register_gate(StartupPhase::RaftMetadataReplay, "raft"),
        schema_gate: sequencer.register_gate(StartupPhase::SchemaCacheWarmup, "schema"),
        sanity_gate: sequencer.register_gate(StartupPhase::CatalogSanityCheck, "sanity"),
        data_groups_gate: sequencer.register_gate(StartupPhase::DataGroupsReplay, "data-groups"),
        transport_gate: sequencer.register_gate(StartupPhase::TransportBind, "transport"),
        warm_peers_gate: sequencer.register_gate(StartupPhase::WarmPeers, "warm-peers"),
        health_loop_gate: sequencer.register_gate(StartupPhase::HealthLoopStart, "health-loop"),
        gateway_enable_gate: sequencer.register_gate(StartupPhase::GatewayEnable, "gateway"),
    };
    // The harness cores replay their WAL before they report ready, so no
    // replay receiver is owed here. The harness runs without a config file and
    // pins short bounds, so a stuck group fails the test in seconds. The
    // shipped defaults wait minutes for a replay backlog this harness never has.
    let startup = nodedb_types::config::tuning::StartupTuning {
        raft_ready_timeout_ms: 30_000,
        data_group_recovery_timeout_ms: 60_000,
    };
    nodedb::bootstrap::cluster_ready::await_cluster_ready(
        shared,
        ready_rx,
        Vec::new(),
        gates,
        startup.raft_ready_timeout(),
        startup.data_group_recovery_timeout(),
    )
    .await?;
    Ok(raft)
}

impl OneNodeRaft {
    /// Stop the lease loop and the cluster subsystems, then close the
    /// transport. `shared.shutdown` stops the Raft loops: the caller signals
    /// it first.
    pub async fn shutdown(&mut self, shared: &SharedState) {
        let _ = self.lease_shutdown_tx.send(true);
        if let Some(handle) = self.lease_renewal.take() {
            handle.abort();
            let _ = handle.await;
        }
        if let Some(running) = self.running.take() {
            let errors = running.shutdown_all(SHUTDOWN_STEP).await;
            if !errors.is_empty() {
                eprintln!("one-node cluster: subsystem shutdown errors: {errors:?}");
            }
        }
        if let Some(transport) = shared.cluster_transport.clone() {
            // A one-node cluster has no peer to acknowledge the close.
            let _ = transport.close(SHUTDOWN_STEP).await;
        }
    }

    /// Stop the lease loop without waiting, for a harness dropped without
    /// its async shutdown.
    pub fn abort(&self) {
        let _ = self.lease_shutdown_tx.send(true);
        if let Some(handle) = self.lease_renewal.as_ref() {
            handle.abort();
        }
    }
}
