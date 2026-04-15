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

    init_cluster_with_transport(config, transport, data_dir).await
}

/// Initialize the cluster using a pre-bound QUIC transport.
///
/// Used by multi-node integration tests that need to learn a node's
/// ephemeral port **before** building the seed list for peer nodes
/// — by the time `init_cluster`'s own `NexarTransport::with_tuning`
/// has run the port is known, but the same call wants it as input via
/// `ClusterSettings.listen`. Tests pre-bind with
/// `NexarTransport::new(node_id, "127.0.0.1:0")`, read the real
/// `local_addr()`, patch it into the config, and call this function.
///
/// Production uses [`init_cluster`] above.
pub async fn init_cluster_with_transport(
    config: &ClusterSettings,
    transport: Arc<nodedb_cluster::NexarTransport>,
    data_dir: &std::path::Path,
) -> crate::Result<ClusterHandle> {
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
        join_retry: join_retry_policy_from_env(),
        swim_udp_addr: None,
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

/// Build the join retry policy, honouring two optional environment
/// variables for test/CI overrides:
///
/// - `NODEDB_JOIN_RETRY_MAX_ATTEMPTS` — total attempts (default 8)
/// - `NODEDB_JOIN_RETRY_MAX_BACKOFF_SECS` — per-attempt ceiling
///   (default 32 s)
///
/// Production deployments leave both unset and get the production
/// schedule. The integration test harness sets both to small values
/// so a join-retry path doesn't spend ~1 minute sleeping in CI.
fn join_retry_policy_from_env() -> nodedb_cluster::JoinRetryPolicy {
    let mut policy = nodedb_cluster::JoinRetryPolicy::default();
    if let Ok(v) = std::env::var("NODEDB_JOIN_RETRY_MAX_ATTEMPTS")
        && let Ok(n) = v.parse::<u32>()
        && n > 0
    {
        policy.max_attempts = n;
    }
    if let Ok(v) = std::env::var("NODEDB_JOIN_RETRY_MAX_BACKOFF_SECS")
        && let Ok(n) = v.parse::<u64>()
        && n > 0
    {
        policy.max_backoff_secs = n;
    }
    policy
}
