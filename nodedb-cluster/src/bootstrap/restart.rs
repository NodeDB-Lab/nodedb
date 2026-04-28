//! Restart path: reload topology/routing from catalog after a clean shutdown or crash.

use std::sync::{Arc, Mutex, RwLock};

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

    // Reconstruct MultiRaft from routing table. A restarting node
    // may be a voter (`info.members`) OR a learner (`info.learners`)
    // — the latter is the window between an `AddLearner` commit
    // and the follow-up `PromoteLearner` commit during a join. A
    // node that crashes inside that window must still come back
    // as a learner on restart; dropping the group entirely would
    // leave the node permanently without any copy of it and
    // silently broken.
    let mut multi_raft = MultiRaft::new(config.node_id, routing.clone(), config.data_dir.clone())
        .with_election_timeout(config.election_timeout_min, config.election_timeout_max);
    for (group_id, info) in routing.group_members() {
        let is_voter = info.members.contains(&config.node_id);
        let is_learner = info.learners.contains(&config.node_id);

        if is_voter {
            let peers: Vec<u64> = info
                .members
                .iter()
                .copied()
                .filter(|&id| id != config.node_id)
                .collect();
            multi_raft.add_group(*group_id, peers)?;
        } else if is_learner {
            // Voters are the full member set (none of them is
            // self). Other learners catching up alongside us are
            // tracked for replication too.
            let voters = info.members.clone();
            let other_learners: Vec<u64> = info
                .learners
                .iter()
                .copied()
                .filter(|&id| id != config.node_id)
                .collect();
            multi_raft.add_group_as_learner(*group_id, voters, other_learners)?;
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
        topology: Arc::new(RwLock::new(topology)),
        routing: Arc::new(RwLock::new(routing)),
        multi_raft: Arc::new(Mutex::new(multi_raft)),
    })
}

#[cfg(test)]
mod tests {
    use super::super::bootstrap_fn::bootstrap;
    use super::*;
    use crate::catalog::ClusterCatalog;
    use std::time::Duration;

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
            join_retry: Default::default(),
            swim_udp_addr: None,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
        };

        // Bootstrap first.
        let _ = bootstrap(&config, &catalog).unwrap();

        // Create transport for restart.
        use crate::transport::credentials::TransportCredentials;
        let transport = NexarTransport::new(
            1,
            "127.0.0.1:0".parse().unwrap(),
            TransportCredentials::Insecure,
        )
        .unwrap();

        // Restart — should load from catalog.
        let state = restart(&config, &catalog, &transport).unwrap();

        assert_eq!(state.topology.read().unwrap().node_count(), 1);
        // num_groups() counts data groups + metadata group: 4 data + 1 = 5.
        assert_eq!(state.routing.read().unwrap().num_groups(), 5);
        assert_eq!(state.multi_raft.lock().unwrap().group_count(), 5);
    }
}
