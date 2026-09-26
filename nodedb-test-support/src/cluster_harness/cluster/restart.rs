// SPDX-License-Identifier: BUSL-1.1

//! Restart every node of a [`TestCluster`] in place.

use super::TestCluster;
use crate::cluster_harness::node::TestClusterNode;

impl TestCluster {
    /// Stop every node, then bring every one back on its node id, listen
    /// address and data directory, and wait until the cluster is ready.
    ///
    /// Every node stops before any restarts, so nothing survives in memory:
    /// what each node serves afterwards comes from its own disk. The nodes
    /// restart together, since each Raft group needs a quorum to elect.
    pub async fn restart_all(self) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let TestCluster {
            nodes,
            spawn_config,
        } = self;
        let mut stopped = Vec::with_capacity(nodes.len());
        for node in nodes {
            stopped.push(node.stop_for_restart().await?);
        }
        let seeds: Vec<std::net::SocketAddr> =
            stopped.iter().map(|node| node.listen_addr()).collect();
        let nodes = futures::future::try_join_all(
            stopped
                .into_iter()
                .map(|node| TestClusterNode::restart(node, seeds.clone(), &spawn_config)),
        )
        .await?;
        let cluster = TestCluster {
            nodes,
            spawn_config,
        };
        cluster.await_ready().await;
        Ok(cluster)
    }
}
