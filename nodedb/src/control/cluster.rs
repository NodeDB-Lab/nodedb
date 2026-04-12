//! Cluster mode startup and integration.
//!
//! Bridges `nodedb-cluster` (Raft, transport, routing) into the main server.
//! When cluster mode is enabled via `[cluster]` config, this module:
//!
//! 1. Creates the QUIC transport (nexar + quinn)
//! 2. Opens the cluster catalog (persistent topology + routing)
//! 3. Bootstraps, joins, or restarts the cluster
//! 4. Starts the Raft event loop
//! 5. Wires Raft propose/commit into SharedState for distributed writes
//!
//! ## Architecture
//!
//! ```text
//! Control Plane (Tokio)
//!   ├── RaftLoop::run()       ← drives ticks, dispatches messages
//!   ├── NexarTransport::serve() ← accepts inbound QUIC RPCs
//!   └── CommitApplier          ← applies committed entries to SPSC bridge
//! ```

use std::sync::{Arc, RwLock};
use std::time::Duration;

use tracing::info;

use nodedb_types::config::tuning::ClusterTransportTuning;

use crate::config::server::ClusterSettings;
use crate::control::state::SharedState;

/// Everything needed to run the cluster after startup.
pub struct ClusterHandle {
    /// The QUIC transport (for serving and sending RPCs).
    pub transport: Arc<nodedb_cluster::NexarTransport>,
    /// Cluster topology (shared with SharedState).
    pub topology: Arc<RwLock<nodedb_cluster::ClusterTopology>>,
    /// Cluster routing table (shared with SharedState).
    pub routing: Arc<RwLock<nodedb_cluster::RoutingTable>>,
    /// This node's ID.
    pub node_id: u64,
}

/// Initialize the cluster: create transport, open catalog, bootstrap/join/restart.
///
/// Returns the cluster handle and a function to start the Raft loop (called
/// after SharedState is fully constructed, since the CommitApplier needs it).
pub async fn init_cluster(
    config: &ClusterSettings,
    data_dir: &std::path::Path,
    transport_tuning: &ClusterTransportTuning,
) -> crate::Result<ClusterHandle> {
    // 1. Create QUIC transport, configured from ClusterTransportTuning.
    let transport = Arc::new(
        nodedb_cluster::NexarTransport::with_tuning(
            config.node_id,
            config.listen,
            transport_tuning,
        )
        .map_err(|e| crate::Error::Config {
            detail: format!("cluster transport: {e}"),
        })?,
    );

    info!(
        node_id = config.node_id,
        addr = %transport.local_addr(),
        "cluster QUIC transport bound"
    );

    // 2. Open cluster catalog.
    let catalog_path = data_dir.join("cluster.redb");
    let catalog =
        nodedb_cluster::ClusterCatalog::open(&catalog_path).map_err(|e| crate::Error::Config {
            detail: format!("cluster catalog: {e}"),
        })?;

    // 3. Bootstrap, join, or restart.
    let cluster_config = nodedb_cluster::ClusterConfig {
        node_id: config.node_id,
        listen_addr: config.listen,
        seed_nodes: config.seed_nodes.clone(),
        num_groups: config.num_groups,
        replication_factor: config.replication_factor,
        data_dir: data_dir.to_path_buf(),
        force_bootstrap: config.force_bootstrap,
    };

    let state = nodedb_cluster::start_cluster(&cluster_config, &catalog, &transport)
        .await
        .map_err(|e| crate::Error::Config {
            detail: format!("cluster start: {e}"),
        })?;

    info!(
        node_id = config.node_id,
        nodes = state.topology.node_count(),
        groups = state.routing.num_groups(),
        "cluster initialized"
    );

    let topology = Arc::new(RwLock::new(state.topology));
    let routing = Arc::new(RwLock::new(state.routing));

    Ok(ClusterHandle {
        transport,
        topology,
        routing,
        node_id: config.node_id,
    })
}

/// Start the Raft event loop and RPC server.
///
/// Must be called after SharedState is constructed (needs the WAL and dispatcher
/// for the CommitApplier).
pub fn start_raft(
    handle: &ClusterHandle,
    shared: Arc<SharedState>,
    data_dir: &std::path::Path,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    transport_tuning: &ClusterTransportTuning,
) -> crate::Result<()> {
    // Reconstruct MultiRaft from routing table.
    let routing = handle.routing.read().unwrap_or_else(|p| p.into_inner());
    let mut multi_raft =
        nodedb_cluster::MultiRaft::new(handle.node_id, routing.clone(), data_dir.to_path_buf());

    for (group_id, info) in routing.group_members() {
        if info.members.contains(&handle.node_id) {
            let peers: Vec<u64> = info
                .members
                .iter()
                .copied()
                .filter(|&id| id != handle.node_id)
                .collect();
            if let Err(e) = multi_raft.add_group(*group_id, peers) {
                tracing::warn!(
                    group_id,
                    error = %e,
                    "failed to add Raft group (may already exist)"
                );
            }
        }
    }
    drop(routing);

    // Create the CommitApplier that bridges Raft commits to the SPSC bridge.
    let applier = SpscCommitApplier {
        shared: shared.clone(),
    };

    // Create the LocalForwarder for executing forwarded queries on this node's
    // local Data Plane. This is the canonical path for leader-side execution
    // of queries forwarded by non-leader nodes via QUIC RPC.
    let query_ctx = crate::control::planner::context::QueryContext::new();
    let forwarder = Arc::new(crate::control::LocalForwarder::new(
        shared.clone(),
        query_ctx,
    ));

    // Create the Raft loop with real forwarder (handles ForwardRequest RPCs).
    // Override the tick interval from ClusterTransportTuning (default: 10ms).
    let tick_interval = Duration::from_millis(transport_tuning.raft_tick_interval_ms);
    let raft_loop = Arc::new(
        nodedb_cluster::RaftLoop::with_forwarder(
            multi_raft,
            handle.transport.clone(),
            handle.topology.clone(),
            applier,
            forwarder,
        )
        .with_tick_interval(tick_interval),
    );

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
    let sr_serve = shutdown_rx;
    tokio::spawn(async move {
        if let Err(e) = transport_serve.serve(rl_handler, sr_serve).await {
            tracing::error!(error = %e, "raft RPC server failed");
        }
    });

    // Register this node's wire version in the rolling upgrade tracker.
    // Other nodes' versions are reported via topology updates and heartbeats.
    {
        // Recover from poisoned mutex: cluster version state is non-critical
        // metadata that shouldn't prevent node startup if a prior thread panicked.
        let mut vs = shared
            .cluster_version_state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        vs.report_version(handle.node_id, crate::version::WIRE_FORMAT_VERSION);

        // Seed from current topology: assume all existing nodes run the same version
        // until they report otherwise via heartbeat.
        if let Ok(topo) = handle.topology.read() {
            for node in topo.active_nodes() {
                if node.node_id != handle.node_id {
                    vs.report_version(node.node_id, crate::version::WIRE_FORMAT_VERSION);
                }
            }
        }
        let compat = crate::control::rolling_upgrade::should_compat_mode(&vs);
        info!(
            node_id = handle.node_id,
            nodes = vs.node_count,
            mixed = vs.is_mixed_version(),
            compat_mode = compat,
            "cluster version state initialized"
        );
    }

    info!(node_id = handle.node_id, "raft loop and RPC server started");

    Ok(())
}

/// CommitApplier that dispatches committed Raft entries to the SPSC bridge.
///
/// When a Raft entry is committed across the quorum, the leader's applier
/// dispatches the write to the Data Plane via the same SPSC path used for
/// single-node writes. The entry data is a serialized `ReplicatedEntry`.
pub struct SpscCommitApplier {
    shared: Arc<SharedState>,
}

impl nodedb_cluster::CommitApplier for SpscCommitApplier {
    fn apply_committed(&self, _group_id: u64, entries: &[nodedb_raft::message::LogEntry]) -> u64 {
        let mut last_applied = 0u64;

        for entry in entries {
            if entry.data.is_empty() {
                // No-op entry (leader election marker) — nothing to apply.
                last_applied = entry.index;
                continue;
            }

            // Deserialize the entry as a ReplicatedEntry and dispatch to Data Plane.
            match crate::control::wal_replication::from_replicated_entry(&entry.data) {
                Some((tenant_id, vshard_id, plan)) => {
                    // WAL append (already done by the proposer before Raft).
                    // Dispatch to Data Plane via SPSC.
                    let request = crate::bridge::envelope::Request {
                        request_id: crate::types::RequestId::new(entry.index),
                        tenant_id,
                        vshard_id,
                        plan,
                        deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
                        priority: crate::bridge::envelope::Priority::Normal,
                        trace_id: 0,
                        consistency: crate::types::ReadConsistency::Strong,
                        idempotency_key: Some(entry.index),
                        event_source: crate::event::EventSource::RaftFollower,
                        user_roles: Vec::new(),
                    };

                    match self.shared.dispatcher.lock() {
                        Ok(mut d) => {
                            if let Err(e) = d.dispatch(request) {
                                tracing::warn!(
                                    index = entry.index,
                                    error = %e,
                                    "failed to dispatch committed entry to data plane"
                                );
                            }
                        }
                        Err(p) => {
                            if let Err(e) = p.into_inner().dispatch(request) {
                                tracing::warn!(
                                    index = entry.index,
                                    error = %e,
                                    "failed to dispatch committed entry (poisoned lock)"
                                );
                            }
                        }
                    }
                }
                None => {
                    // ConfChange or unrecognized entry — skip (ConfChanges are
                    // handled by the RaftLoop before calling the applier).
                    tracing::debug!(
                        index = entry.index,
                        data_len = entry.data.len(),
                        "skipping non-data entry"
                    );
                }
            }

            last_applied = entry.index;
        }

        last_applied
    }
}
