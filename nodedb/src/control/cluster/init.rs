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
    // 1a. Resolve TLS credentials (mandatory mTLS unless explicitly opted out).
    let credentials = crate::control::cluster::tls::resolve_credentials(config, data_dir)?;

    // 1b. Create QUIC transport, configured from ClusterTransportTuning.
    let transport = Arc::new(
        nodedb_cluster::NexarTransport::with_tuning(
            config.node_id,
            config.listen,
            transport_tuning,
            credentials,
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

    init_cluster_with_transport(config, transport, data_dir, transport_tuning).await
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
    transport_tuning: &ClusterTransportTuning,
) -> crate::Result<ClusterHandle> {
    // 2. Open cluster catalog.
    let catalog_path = data_dir.join("cluster.redb");
    let catalog = Arc::new(
        nodedb_cluster::ClusterCatalog::open(&catalog_path).map_err(|e| crate::Error::Config {
            detail: format!("cluster catalog: {e}"),
        })?,
    );

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
        election_timeout_min: std::time::Duration::from_secs(
            transport_tuning.election_timeout_min_secs,
        ),
        election_timeout_max: std::time::Duration::from_secs(
            transport_tuning.election_timeout_max_secs,
        ),
    };

    let lifecycle = nodedb_cluster::ClusterLifecycleTracker::new();
    let state = nodedb_cluster::start_cluster(
        &cluster_config,
        &catalog,
        Arc::clone(&transport),
        &lifecycle,
    )
    .await
    .map_err(|e| crate::Error::Config {
        detail: format!("cluster start: {e}"),
    })?;

    info!(
        node_id = config.node_id,
        nodes = state.topology.read().map(|t| t.node_count()).unwrap_or(0),
        groups = state.routing.read().map(|r| r.num_groups()).unwrap_or(0),
        "cluster initialized"
    );

    // ClusterState carries Arc<RwLock<T>> fields — use them directly.
    let topology = state.topology;
    let routing = state.routing;
    let metadata_cache = Arc::new(RwLock::new(nodedb_cluster::MetadataCache::new()));
    let applied_index_watcher = Arc::new(AppliedIndexWatcher::new());

    // `start_cluster` does not start any subsystems, so the `Arc<Mutex<MultiRaft>>`
    // it returns has exactly one strong owner (this scope). `try_unwrap`
    // succeeds and we hand the inner `MultiRaft` to the cluster handle for
    // `start_raft` to move into the `RaftLoop`.
    let multi_raft_inner = Arc::try_unwrap(state.multi_raft)
        .map_err(|_| crate::Error::Config {
            detail: "MultiRaft Arc has unexpected extra owners after start_cluster; \
                     this should be impossible — subsystems are spawned only after \
                     RaftLoop::new in start_raft, which clones the loop's own Arc"
                .into(),
        })?
        .into_inner()
        .unwrap_or_else(|p| p.into_inner());

    Ok(ClusterHandle {
        transport,
        topology,
        routing,
        lifecycle,
        metadata_cache,
        applied_index_watcher,
        node_id: config.node_id,
        multi_raft: Mutex::new(Some(multi_raft_inner)),
        catalog,
        running_cluster: Mutex::new(None),
        pending_subsystems: Mutex::new(Some(crate::control::cluster::handle::PendingSubsystems {
            config: cluster_config,
        })),
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
