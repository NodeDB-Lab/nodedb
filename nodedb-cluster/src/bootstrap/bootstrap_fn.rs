// SPDX-License-Identifier: BUSL-1.1

//! Bootstrap path: the founding member of a new cluster.

use std::sync::{Arc, Mutex, RwLock};

use tracing::info;

use crate::catalog::{ClusterCatalog, ClusterSettings};
use crate::error::Result;
use crate::multi_raft::MultiRaft;
use crate::routing::RoutingTable;
use crate::topology::{ClusterTopology, NodeInfo, NodeState};

use super::config::{ClusterConfig, ClusterState};

/// Bootstrap a new cluster: this node is the founding member.
///
/// `local_spki_pin` is the SHA-256 SPKI fingerprint of this node's own TLS
/// leaf certificate; `None` in insecure transport mode.  When present it is
/// stored in the founding node's `NodeInfo` so that joining peers can pin our
/// identity immediately.
pub(super) fn bootstrap(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    local_spki_pin: Option<[u8; 32]>,
) -> Result<ClusterState> {
    info!(
        node_id = config.node_id,
        addr = %config.listen_addr,
        groups = config.num_groups,
        "bootstrapping new cluster"
    );

    // Create topology with this node.
    let mut topology = ClusterTopology::new();
    topology.add_node(
        NodeInfo::new(config.node_id, config.listen_addr, NodeState::Active)
            .with_spki_pin(local_spki_pin)
            .with_swim_addr(config.swim_udp_addr),
    );

    // Create routing table: all groups on this single node. The configured
    // replication factor is passed through as-is; `RoutingTable::uniform`
    // clamps each group's voter set to the number of available nodes (just
    // this one at bootstrap), so a single founding node always starts with
    // one voter per group. The configured RF is persisted in `ClusterSettings`
    // and governs how many learners get promoted to voters as peers join.
    // ONE shared routing handle: MultiRaft and ClusterState read/write the
    // same table so committed Raft conf-changes converge the data-plane view.
    let routing = Arc::new(RwLock::new(RoutingTable::uniform(
        config.num_groups,
        &[config.node_id],
        config.replication_factor,
    )));

    // Create MultiRaft with all groups (single-node, no peers).
    let mut multi_raft = MultiRaft::new_with_shared_routing(
        config.node_id,
        routing.clone(),
        config.data_dir.clone(),
    )
    .with_election_timeout(config.election_timeout_min, config.election_timeout_max)
    .with_log_compaction_threshold(config.log_compaction_threshold);
    let group_ids = routing
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_ids();
    for group_id in group_ids {
        multi_raft.add_group(group_id, vec![])?;
    }

    // The bootstrap seed is the only voter in every group, so the first
    // tick of `RaftLoop::run` elects it leader of each. A `JoinRequest`
    // that arrives before an election timeout fires then finds a leader.
    multi_raft.elect_sole_voter_groups_now();

    // Generate cluster ID and persist everything.
    let cluster_id = generate_cluster_id();
    catalog.save_cluster_id(cluster_id)?;
    catalog.save_cluster_settings(&ClusterSettings::from_config(config))?;
    catalog.save_topology(&topology)?;
    catalog.save_routing(&routing.read().unwrap_or_else(|p| p.into_inner()))?;

    info!(
        node_id = config.node_id,
        cluster_id,
        groups = config.num_groups,
        "cluster bootstrapped"
    );

    Ok(ClusterState {
        topology: Arc::new(RwLock::new(topology)),
        routing,
        multi_raft: Arc::new(Mutex::new(multi_raft)),
    })
}

/// Generate a unique cluster ID (random u64).
fn generate_cluster_id() -> u64 {
    use rand::RngExt;
    rand::rng().random::<u64>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ClusterCatalog;
    use std::time::Duration;

    fn temp_catalog() -> (tempfile::TempDir, ClusterCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        let catalog = ClusterCatalog::open(&path).unwrap();
        (dir, catalog)
    }

    #[test]
    fn bootstrap_creates_cluster() {
        let (_dir, catalog) = temp_catalog();
        let config = ClusterConfig {
            node_id: 1,
            listen_addr: "127.0.0.1:9400".parse().unwrap(),
            seed_nodes: vec!["127.0.0.1:9400".parse().unwrap()],
            num_groups: 4,
            replication_factor: 1,
            data_dir: _dir.path().to_path_buf(),
            force_bootstrap: false,
            join_retry: Default::default(),
            swim_udp_addr: None,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            install_snapshot_chunk_bytes: 4 * 1024 * 1024,
            orphan_partial_max_age_secs: 300,
            log_compaction_threshold: None,
            wire_build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
        };

        let state = bootstrap(&config, &catalog, None).unwrap();
        let topo = state.topology.read().unwrap();
        let routing = state.routing.read().unwrap();
        let multi_raft = state.multi_raft.lock().unwrap();

        assert_eq!(topo.node_count(), 1);
        assert_eq!(topo.active_nodes().len(), 1);
        // num_groups() includes the metadata group (id 0); 4 data groups + metadata = 5.
        assert_eq!(routing.num_groups(), 5);
        assert_eq!(multi_raft.group_count(), 5);

        assert!(catalog.is_bootstrapped().unwrap());
        let settings = catalog.load_cluster_settings().unwrap().unwrap();
        assert_eq!(settings.replication_factor, 1); // config RF is 1 here
        let loaded_topo = catalog.load_topology().unwrap().unwrap();
        assert_eq!(loaded_topo.node_count(), 1);
        let loaded_rt = catalog.load_routing().unwrap().unwrap();
        assert_eq!(loaded_rt.num_groups(), 5);
    }

    #[test]
    fn bootstrap_preserves_configured_replication_factor() {
        // A founding node configured with RF=3 must persist RF=3 in
        // ClusterSettings (so learners get promoted to voters as peers join),
        // while its routing table starts with a single voter per group because
        // it is the only node present.
        let (_dir, catalog) = temp_catalog();
        let config = ClusterConfig {
            node_id: 1,
            listen_addr: "127.0.0.1:9401".parse().unwrap(),
            seed_nodes: vec!["127.0.0.1:9401".parse().unwrap()],
            num_groups: 4,
            replication_factor: 3,
            data_dir: _dir.path().to_path_buf(),
            force_bootstrap: false,
            join_retry: Default::default(),
            swim_udp_addr: None,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            install_snapshot_chunk_bytes: 4 * 1024 * 1024,
            orphan_partial_max_age_secs: 300,
            log_compaction_threshold: None,
            wire_build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
        };

        let state = bootstrap(&config, &catalog, None).unwrap();
        let routing = state.routing.read().unwrap();

        // Configured RF survives bootstrap unchanged.
        let settings = catalog.load_cluster_settings().unwrap().unwrap();
        assert_eq!(settings.replication_factor, 3);

        // But every group has exactly one voter — the sole founding node —
        // because routing clamps the voter set to the node count.
        for group_id in routing.group_ids() {
            let members = &routing.group_info(group_id).unwrap().members;
            assert_eq!(
                members.len(),
                1,
                "group {group_id} must start with a single voter at bootstrap"
            );
            assert_eq!(members, &vec![config.node_id]);
        }
    }
}
