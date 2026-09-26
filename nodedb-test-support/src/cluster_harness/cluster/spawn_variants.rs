// SPDX-License-Identifier: BUSL-1.1

//! Thin `spawn_three*` convenience wrappers over
//! [`super::bringup`]'s `spawn_three_inner`.

use nodedb_types::config::tuning::ClusterTransportTuning;

use super::TestCluster;
use super::types::fast_cluster_tuning;

impl TestCluster {
    /// Spawn a 3-node cluster: node 1 bootstraps, nodes 2 and 3 join
    /// via node 1's pre-bound address. Waits until every node sees
    /// topology_size == 3 (10s deadline).
    pub async fn spawn_three() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_tuning(fast_cluster_tuning()).await
    }

    /// Spawn a 3-node cluster with `num_cores` Data-Plane cores PER NODE,
    /// using the same fast election tuning as [`spawn_three`]. Exercises
    /// multi-core cross-node code paths (the per-core store fan-out).
    pub async fn spawn_three_with_cores(
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_tuning_and_cores(fast_cluster_tuning(), num_cores).await
    }

    /// Spawn a 3-node cluster with the fast election tuning, `num_cores`
    /// Data-Plane cores per node, AND lowered variable-length MATCH expansion
    /// caps. Used to force `[*min..max]` truncation on a small graph and prove
    /// the cross-shard resume pipeline drains to the complete result set.
    pub async fn spawn_three_with_varlen_caps_and_cores(
        num_cores: usize,
        varlen_max_results: usize,
        varlen_max_frontier: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let graph_tuning = nodedb_types::config::tuning::GraphTuning {
            varlen_max_results,
            varlen_max_frontier,
            ..Default::default()
        };
        Self::spawn_three_with_tuning_graph_and_cores(
            fast_cluster_tuning(),
            graph_tuning,
            num_cores,
        )
        .await
    }

    /// Spawn a 3-node cluster with a lowered `columnar_flush_threshold` so
    /// cluster tests can observe flush / segment behaviour on small datasets
    /// (e.g. a handful of rows) without inserting 65k rows per test.
    ///
    /// All other tuning values stay at their defaults. Uses 1 Data-Plane core
    /// per node and the standard fast-election cluster transport tuning.
    pub async fn spawn_three_with_columnar_flush_threshold(
        flush_threshold: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let query_tuning = nodedb_types::config::tuning::QueryTuning {
            columnar_flush_threshold: flush_threshold,
            ..Default::default()
        };
        Self::spawn_three_with_tuning_graph_query_and_cores(
            fast_cluster_tuning(),
            nodedb_types::config::tuning::GraphTuning::default(),
            query_tuning,
            1,
        )
        .await
    }

    /// Spawn a 3-node cluster with a custom `ClusterTransportTuning`.
    /// Used by the descriptor-lease renewal tests to drive the
    /// renewal loop on a much faster cadence than the production
    /// 60-second default.
    pub async fn spawn_three_with_tuning(
        tuning: ClusterTransportTuning,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_tuning_and_cores(tuning, 1).await
    }

    /// Spawn a 3-node cluster with a custom `ClusterTransportTuning` and a
    /// specific number of Data-Plane cores per node. Graph tuning defaults
    /// (100k variable-length expansion caps).
    pub async fn spawn_three_with_tuning_and_cores(
        tuning: ClusterTransportTuning,
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_tuning_graph_and_cores(
            tuning,
            nodedb_types::config::tuning::GraphTuning::default(),
            num_cores,
        )
        .await
    }

    /// Spawn a 3-node cluster with custom cluster-transport AND graph engine
    /// tuning plus a specific core count per node. Lets a cluster test lower
    /// the variable-length MATCH expansion caps (`varlen_max_results` /
    /// `varlen_max_frontier`) to force truncation on a small graph and prove
    /// the cross-shard resume pipeline drains to the complete result set.
    pub async fn spawn_three_with_tuning_graph_and_cores(
        tuning: ClusterTransportTuning,
        graph_tuning: nodedb_types::config::tuning::GraphTuning,
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_tuning_graph_query_and_cores(
            tuning,
            graph_tuning,
            nodedb_types::config::tuning::QueryTuning::default(),
            num_cores,
        )
        .await
    }

    /// Spawn a 3-node cluster with a low Raft `log_compaction_threshold` on
    /// every node, so a handful of writes compacts the leader's data-group
    /// log past the start. Once compacted, a freshly-joined learner cannot be
    /// caught up via `AppendEntries` — the leader must send a real
    /// `InstallSnapshot`. Used by the install-snapshot end-to-end test
    /// together with [`Self::add_learner_node`].
    ///
    /// Uses the standard fast-election tuning and 1 Data-Plane core per node.
    pub async fn spawn_three_with_compaction_threshold(
        threshold: u64,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_inner(
            fast_cluster_tuning(),
            nodedb_types::config::tuning::GraphTuning::default(),
            nodedb_types::config::tuning::QueryTuning::default(),
            1,
            Some(threshold),
            3,
        )
        .await
    }

    /// Spawn a 3-node cluster with a low Raft `log_compaction_threshold`
    /// (see [`Self::spawn_three_with_compaction_threshold`]) AND a custom
    /// Raft `replication_factor`.
    ///
    /// Used by the InstallSnapshot end-to-end test: HRW placement assigns
    /// `take = min(replication_factor, node_count)` nodes to each Raft
    /// group, so with the default `replication_factor = 3` a 4th node added
    /// via `add_learner_node()` is NOT guaranteed to be placed on the
    /// collection's data group at all. Passing `replication_factor` equal
    /// to the POST-JOIN node count (4, for a 3-node cluster plus one
    /// learner) makes `take = min(4, 4) = 4`, so placement deterministically
    /// assigns every node — including the learner — to every group. Without
    /// this, an assertion on the learner's local hosting/snapshot state
    /// could fail (or vacuously pass) depending on unrelated hash placement,
    /// not on whether InstallSnapshot actually ran.
    pub async fn spawn_three_with_compaction_threshold_and_rf(
        threshold: u64,
        replication_factor: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_inner(
            fast_cluster_tuning(),
            nodedb_types::config::tuning::GraphTuning::default(),
            nodedb_types::config::tuning::QueryTuning::default(),
            1,
            Some(threshold),
            replication_factor,
        )
        .await
    }

    /// Spawn a 3-node cluster whose data groups each place
    /// `replication_factor` of the three nodes. With a factor below 3 some
    /// node replicates no copy of a group, so a test can act on a node that
    /// never applied the group's writes.
    ///
    /// Uses the standard fast-election tuning and 1 Data-Plane core per node.
    pub async fn spawn_three_with_replication_factor(
        replication_factor: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_inner(
            fast_cluster_tuning(),
            nodedb_types::config::tuning::GraphTuning::default(),
            nodedb_types::config::tuning::QueryTuning::default(),
            1,
            None,
            replication_factor,
        )
        .await
    }

    /// Spawn a 3-node cluster with custom cluster-transport, graph engine tuning,
    /// query execution tuning, and a specific core count per node.
    ///
    /// This is the lowest-level cluster spawn entry point. Public methods that
    /// tune only a subset of parameters delegate here with appropriate defaults.
    pub async fn spawn_three_with_tuning_graph_query_and_cores(
        tuning: ClusterTransportTuning,
        graph_tuning: nodedb_types::config::tuning::GraphTuning,
        query_tuning: nodedb_types::config::tuning::QueryTuning,
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_inner(tuning, graph_tuning, query_tuning, num_cores, None, 3).await
    }
}
