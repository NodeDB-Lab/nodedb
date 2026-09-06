// SPDX-License-Identifier: BUSL-1.1

//! Cluster startup: create transport, open catalog, bootstrap/join/restart.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, RwLock};

use tracing::info;

use nodedb_types::config::tuning::ClusterTransportTuning;

use nodedb_cluster::GroupAppliedWatchers;

use crate::config::server::ClusterSettings;
use crate::control::cluster::handle::ClusterHandle;

/// Node id for the synthesized single-node Calvin deployment. A standalone
/// server is a cluster of one, so the id is fixed and non-zero.
const SINGLE_NODE_CALVIN_NODE_ID: u64 = 1;

/// Raft group count for the synthesized single-node Calvin deployment. This
/// node is the sole member of every group, so the value only affects how the
/// vShard space is partitioned into groups — never placement (this node hosts
/// every vShard regardless).
const SINGLE_NODE_CALVIN_NUM_GROUPS: u64 = 4;

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
    let credentials = crate::control::cluster::tls::resolve_credentials(config, data_dir).await?;

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
        join_retry: nodedb_cluster::JoinRetryPolicy {
            max_attempts: config.join_retry_max_attempts,
            max_backoff_secs: config.join_retry_max_backoff_secs,
        },
        swim_udp_addr: None,
        election_timeout_min: std::time::Duration::from_millis(
            transport_tuning.effective_election_timeout_min_ms(),
        ),
        election_timeout_max: std::time::Duration::from_millis(
            transport_tuning.effective_election_timeout_max_ms(),
        ),
        install_snapshot_chunk_bytes: 4 * 1024 * 1024,
        orphan_partial_max_age_secs: 300,
        log_compaction_threshold: config.log_compaction_threshold,
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

    transport.install_identity_topology(Arc::clone(&state.topology));

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
    let group_watchers = Arc::new(GroupAppliedWatchers::new());

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
        group_watchers,
        node_id: config.node_id,
        multi_raft: Mutex::new(Some(multi_raft_inner)),
        catalog,
        running_cluster: Mutex::new(None),
        pending_subsystems: Mutex::new(Some(crate::control::cluster::handle::PendingSubsystems {
            config: cluster_config,
        })),
    })
}

/// Initialize a flag-gated single-node Calvin deployment on a standalone server.
///
/// Synthesizes a one-node cluster configuration — this node as its own sole
/// seed, replication factor 1 — and drives the SAME cluster startup a real
/// deployment uses ([`init_cluster_with_transport`]). The QUIC transport binds
/// to an ephemeral loopback port and never dials a peer; a single-member Raft
/// group is the deterministic bootstrapper and self-elects, committing locally.
/// The single node therefore hosts every vShard, so the sequencer group and
/// per-vShard schedulers all come up and `calvin_available` becomes true.
///
/// The caller must still call [`super::start_raft::start_raft`] after
/// `SharedState` is constructed, exactly as for [`init_cluster`].
///
/// Only reached when `server.single_node_calvin` is set and `[cluster]` is
/// absent; when the flag is off (the default) the standalone boot path never
/// calls this and no Calvin stack is started.
pub async fn init_single_node_calvin(
    data_dir: &std::path::Path,
    transport_tuning: &ClusterTransportTuning,
) -> crate::Result<ClusterHandle> {
    // Bind a loopback QUIC transport on an ephemeral port. Channel
    // authentication is disabled: the listener is loopback-only and never
    // dials a peer. The bound port is known only after binding, so it is read
    // back below to build a self-referential seed list.
    let listen_placeholder = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let transport = Arc::new(
        nodedb_cluster::NexarTransport::with_tuning(
            SINGLE_NODE_CALVIN_NODE_ID,
            listen_placeholder,
            transport_tuning,
            nodedb_cluster::TransportCredentials::Insecure,
        )
        .map_err(|e| crate::Error::Config {
            detail: format!("single-node calvin transport: {e}"),
        })?,
    );
    let listen = transport.local_addr();

    info!(
        node_id = SINGLE_NODE_CALVIN_NODE_ID,
        addr = %listen,
        "single-node Calvin transport bound"
    );

    // This node is its own sole seed → the deterministic bootstrapper of a
    // one-node cluster. Replication factor 1: the single node hosts every
    // vShard, so a scheduler spawns here for each.
    let settings = ClusterSettings {
        node_id: SINGLE_NODE_CALVIN_NODE_ID,
        listen,
        seed_nodes: vec![listen],
        num_groups: SINGLE_NODE_CALVIN_NUM_GROUPS,
        replication_factor: 1,
        force_bootstrap: false,
        tls: None,
        max_active_sessions: 0,
        login_attempts_per_ip_per_min: 0,
        login_attempts_per_user_per_min: 0,
        insecure_transport: true,
        log_compaction_threshold: None,
        join_retry_max_attempts: 8,
        join_retry_max_backoff_secs: 32,
    };

    init_cluster_with_transport(&settings, transport, data_dir, transport_tuning).await
}
