//! Restart path: reload topology/routing from catalog after a clean shutdown or crash.

use tracing::info;

use crate::catalog::ClusterCatalog;
use crate::error::{ClusterError, Result};
use crate::multi_raft::MultiRaft;
use crate::transport::NexarTransport;

use super::config::{ClusterConfig, ClusterState};

/// Restart from persisted state — load topology and routing from catalog.
pub(super) fn restart(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: &NexarTransport,
) -> Result<ClusterState> {
    let topology = catalog
        .load_topology()?
        .ok_or_else(|| ClusterError::Transport {
            detail: "catalog is bootstrapped but topology is missing".into(),
        })?;

    let routing = catalog
        .load_routing()?
        .ok_or_else(|| ClusterError::Transport {
            detail: "catalog is bootstrapped but routing table is missing".into(),
        })?;

    // Reconstruct MultiRaft from routing table.
    let mut multi_raft = MultiRaft::new(config.node_id, routing.clone(), config.data_dir.clone());
    for (group_id, info) in routing.group_members() {
        if info.members.contains(&config.node_id) {
            let peers: Vec<u64> = info
                .members
                .iter()
                .copied()
                .filter(|&id| id != config.node_id)
                .collect();
            multi_raft.add_group(*group_id, peers)?;
        }
    }

    // Register all known peers in the transport.
    for node in topology.all_nodes() {
        if node.node_id != config.node_id
            && let Some(addr) = node.socket_addr()
        {
            transport.register_peer(node.node_id, addr);
        }
    }

    info!(
        node_id = config.node_id,
        nodes = topology.node_count(),
        groups = multi_raft.group_count(),
        "restarted from catalog"
    );

    Ok(ClusterState {
        topology,
        routing,
        multi_raft,
    })
}

#[cfg(test)]
mod tests {
    use super::super::bootstrap_fn::bootstrap;
    use super::*;
    use crate::catalog::ClusterCatalog;

    fn temp_catalog() -> (tempfile::TempDir, ClusterCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        let catalog = ClusterCatalog::open(&path).unwrap();
        (dir, catalog)
    }

    #[tokio::test]
    async fn restart_from_catalog() {
        let (_dir, catalog) = temp_catalog();
        let config = ClusterConfig {
            node_id: 1,
            listen_addr: "127.0.0.1:9400".parse().unwrap(),
            seed_nodes: vec![],
            num_groups: 4,
            replication_factor: 1,
            data_dir: _dir.path().to_path_buf(),
            force_bootstrap: false,
        };

        // Bootstrap first.
        let _ = bootstrap(&config, &catalog).unwrap();

        // Create transport for restart.
        let transport = NexarTransport::new(1, "127.0.0.1:0".parse().unwrap()).unwrap();

        // Restart — should load from catalog.
        let state = restart(&config, &catalog, &transport).unwrap();

        assert_eq!(state.topology.node_count(), 1);
        assert_eq!(state.routing.num_groups(), 4);
        assert_eq!(state.multi_raft.group_count(), 4);
    }
}
