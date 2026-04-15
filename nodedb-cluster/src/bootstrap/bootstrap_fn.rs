//! Bootstrap path: the founding member of a new cluster.

use tracing::info;

use crate::catalog::ClusterCatalog;
use crate::error::Result;
use crate::multi_raft::MultiRaft;
use crate::routing::RoutingTable;
use crate::topology::{ClusterTopology, NodeInfo, NodeState};

use super::config::{ClusterConfig, ClusterState};

/// Bootstrap a new cluster: this node is the founding member.
pub(super) fn bootstrap(config: &ClusterConfig, catalog: &ClusterCatalog) -> Result<ClusterState> {
    info!(
        node_id = config.node_id,
        addr = %config.listen_addr,
        groups = config.num_groups,
        "bootstrapping new cluster"
    );

    // Create topology with this node.
    let mut topology = ClusterTopology::new();
    topology.add_node(NodeInfo::new(
        config.node_id,
        config.listen_addr,
        NodeState::Active,
    ));

    // Create routing table: all groups on this single node.
    let routing = RoutingTable::uniform(
        config.num_groups,
        &[config.node_id],
        config.replication_factor.min(1), // Single node → RF=1.
    );

    // Create MultiRaft with all groups (single-node, no peers).
    let mut multi_raft = MultiRaft::new(config.node_id, routing.clone(), config.data_dir.clone());
    for group_id in routing.group_ids() {
        multi_raft.add_group(group_id, vec![])?;
    }

    // Kick every group's election deadline into the past so the very
    // first tick of `RaftLoop::run` elects this node as leader of each
    // group. Otherwise an incoming `JoinRequest` that arrives before the
    // random 150–300 ms election timeout fires would hit a non-leader
    // node and be rejected. The bootstrap seed is by definition the only
    // voter in every group, so self-election is unambiguous.
    let now = std::time::Instant::now();
    for node in multi_raft.groups_mut().values_mut() {
        node.election_deadline_override(now - std::time::Duration::from_millis(1));
    }

    // Generate cluster ID and persist everything.
    let cluster_id = generate_cluster_id();
    catalog.save_cluster_id(cluster_id)?;
    catalog.save_topology(&topology)?;
    catalog.save_routing(&routing)?;

    info!(
        node_id = config.node_id,
        cluster_id,
        groups = config.num_groups,
        "cluster bootstrapped"
    );

    Ok(ClusterState {
        topology,
        routing,
        multi_raft,
    })
}

/// Generate a unique cluster ID (random u64).
fn generate_cluster_id() -> u64 {
    use rand::Rng;
    rand::rng().random::<u64>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ClusterCatalog;

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
        };

        let state = bootstrap(&config, &catalog).unwrap();

        assert_eq!(state.topology.node_count(), 1);
        assert_eq!(state.topology.active_nodes().len(), 1);
        assert_eq!(state.routing.num_groups(), 4);
        assert_eq!(state.multi_raft.group_count(), 4);

        // Verify persistence.
        assert!(catalog.is_bootstrapped().unwrap());
        let loaded_topo = catalog.load_topology().unwrap().unwrap();
        assert_eq!(loaded_topo.node_count(), 1);
        let loaded_rt = catalog.load_routing().unwrap().unwrap();
        assert_eq!(loaded_rt.num_groups(), 4);
    }
}
