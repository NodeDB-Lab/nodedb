// SPDX-License-Identifier: BUSL-1.1

//! The shared 3-node bringup body (`spawn_three_inner`). The post-join
//! convergence barriers live in `ready`.

use std::time::Duration;

use nodedb_types::config::tuning::ClusterTransportTuning;

use super::TestCluster;
use super::types::ClusterSpawnConfig;
use crate::cluster_harness::node::TestClusterNode;

impl TestCluster {
    /// Shared 3-node spawn body. Threads an optional Raft
    /// `log_compaction_threshold` and a Raft `replication_factor` into
    /// every node's spawn; all public `spawn_three_*` entry points funnel
    /// here.
    pub(super) async fn spawn_three_inner(
        tuning: ClusterTransportTuning,
        graph_tuning: nodedb_types::config::tuning::GraphTuning,
        query_tuning: nodedb_types::config::tuning::QueryTuning,
        num_cores: usize,
        log_compaction_threshold: Option<u64>,
        replication_factor: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = ClusterSpawnConfig {
            tuning,
            graph_tuning,
            query_tuning,
            num_cores,
            log_compaction_threshold,
            replication_factor,
            single_node_calvin: false,
        };

        let node1 = TestClusterNode::spawn_with_full_config(1, vec![], &config).await?;

        // Wait until node 1 has bootstrapped (topology shows itself)
        // before peers try to join. The old fixed 200ms sleep was too
        // short under heavy host load (e.g. 500+ parallel unit tests
        // sharing the same CPU pool), causing peers to dial before
        // node 1's transport was ready — failing topology convergence.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while node1.topology_size() < 1 {
            if std::time::Instant::now() >= deadline {
                return Err("node 1 failed to bootstrap within 30s".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let seeds = vec![node1.listen_addr];
        let node2 = TestClusterNode::spawn_with_full_config(2, seeds.clone(), &config).await?;

        // Wait for node 2's join to be reflected before spawning node 3.
        // Under load, spawning both peers simultaneously can overwhelm the
        // bootstrap leader's join handler, causing neither join to complete
        // within the topology convergence deadline.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while node1.topology_size() < 2 {
            if std::time::Instant::now() >= deadline {
                return Err("node 2 failed to join within 30s".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let node3 = TestClusterNode::spawn_with_full_config(3, seeds, &config).await?;

        let cluster = Self {
            nodes: vec![node1, node2, node3],
            spawn_config: config,
        };

        cluster.await_ready().await;

        Ok(cluster)
    }
}
