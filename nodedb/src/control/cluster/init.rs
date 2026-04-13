//! Cluster startup: create transport, open catalog, bootstrap/join/restart.

use std::sync::{Arc, Mutex, RwLock};

use tracing::info;

use nodedb_types::config::tuning::ClusterTransportTuning;

use crate::config::server::ClusterSettings;
use crate::control::cluster::applied_index_watcher::AppliedIndexWatcher;
use crate::control::cluster::handle::ClusterHandle;

/// Initialize the cluster: create transport, open catalog, bootstrap/join/restart.
///
/// Returns the cluster handle; the caller must then call
/// [`super::start_raft::start_raft`] after `SharedState` is constructed
/// so the applier has the dispatcher / WAL it needs.
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

    let lifecycle = nodedb_cluster::ClusterLifecycleTracker::new();
    let state = nodedb_cluster::start_cluster(&cluster_config, &catalog, &transport, &lifecycle)
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
    let metadata_cache = Arc::new(RwLock::new(nodedb_cluster::MetadataCache::new()));
    let applied_index_watcher = Arc::new(AppliedIndexWatcher::new());

    Ok(ClusterHandle {
        transport,
        topology,
        routing,
        lifecycle,
        metadata_cache,
        applied_index_watcher,
        node_id: config.node_id,
        multi_raft: Mutex::new(Some(state.multi_raft)),
    })
}
