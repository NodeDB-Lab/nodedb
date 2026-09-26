// SPDX-License-Identifier: BUSL-1.1

//! Thin `spawn*` convenience wrappers over
//! [`super::spawn_full`]'s `spawn_with_full_config`.

use std::net::SocketAddr;
use std::path::PathBuf;

use nodedb_types::config::tuning::ClusterTransportTuning;

use crate::cluster_harness::cluster::ClusterSpawnConfig;

use super::types::TestClusterNode;

impl TestClusterNode {
    /// Spawn a cluster node.
    ///
    /// - `node_id` — non-zero unique id within the cluster.
    /// - `seed_nodes` — empty for the bootstrap node; otherwise the
    ///   pre-bound listen address of at least one already-running
    ///   peer (typically node 1).
    pub async fn spawn(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_with_tuning(node_id, seed_nodes, ClusterTransportTuning::default()).await
    }

    /// Spawn a cluster node with a custom `ClusterTransportTuning`.
    /// Used by tests that need to override the descriptor lease
    /// duration or renewal cadence to drive renewal within a
    /// short test budget.
    pub async fn spawn_with_tuning(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
        tuning: ClusterTransportTuning,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_with_tuning_and_cores(node_id, seed_nodes, tuning, 1).await
    }

    /// Spawn a cluster node with a custom `ClusterTransportTuning` and a
    /// specific number of Data-Plane cores. Used to exercise multi-core
    /// code paths in cluster tests. Graph tuning defaults (100k varlen caps).
    pub async fn spawn_with_tuning_and_cores(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
        tuning: ClusterTransportTuning,
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_with_tuning_graph_and_cores(
            node_id,
            seed_nodes,
            tuning,
            nodedb_types::config::tuning::GraphTuning::default(),
            num_cores,
        )
        .await
    }

    /// Spawn a cluster node with custom cluster-transport AND graph engine
    /// tuning plus a specific core count. The `graph_tuning` knob lets cluster
    /// tests lower the variable-length MATCH expansion caps to drive truncation
    /// (and exercise the cross-shard resume drain) on small graphs.
    pub async fn spawn_with_tuning_graph_and_cores(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
        tuning: ClusterTransportTuning,
        graph_tuning: nodedb_types::config::tuning::GraphTuning,
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_with_tuning_graph_query_and_cores(
            node_id,
            seed_nodes,
            tuning,
            graph_tuning,
            nodedb_types::config::tuning::QueryTuning::default(),
            num_cores,
        )
        .await
    }

    /// Spawn a cluster node with custom cluster-transport, graph engine
    /// tuning, query execution tuning, and a specific core count.
    ///
    /// The `query_tuning` knob lets cluster tests override per-core Data Plane
    /// parameters (e.g. `columnar_flush_threshold`) to exercise flush behaviour
    /// on small datasets.
    ///
    /// Delegates to [`Self::spawn_with_full_config`] with
    /// `log_compaction_threshold = None` (auto-compaction disabled).
    pub async fn spawn_with_tuning_graph_query_and_cores(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
        tuning: ClusterTransportTuning,
        graph_tuning: nodedb_types::config::tuning::GraphTuning,
        query_tuning: nodedb_types::config::tuning::QueryTuning,
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = ClusterSpawnConfig {
            tuning,
            graph_tuning,
            query_tuning,
            num_cores,
            log_compaction_threshold: None,
            replication_factor: 3,
            single_node_calvin: false,
        };
        Self::spawn_with_full_config(node_id, seed_nodes, &config).await
    }

    /// Spawn a standalone node with the flag-gated single-node Calvin stack
    /// (`server.single_node_calvin = true`), exercising the same
    /// `init_single_node_calvin` synthesis the production standalone boot uses.
    ///
    /// The node stands up its own sequencer Raft group and per-vShard
    /// schedulers, so `calvin_available` becomes true and a cross-vShard
    /// transaction traverses the deterministic Calvin path — all on one node.
    /// `num_cores` should be `>= 2` so distinct vShards map to distinct cores.
    pub async fn spawn_single_node_calvin(
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_single_node_calvin_at(num_cores, None).await
    }

    /// Spawn (or reopen) the single-node Calvin stack rooted at
    /// `data_dir_path` instead of an internally-minted `tempdir()`.
    ///
    /// This is the WAL-only-restart entry point: call it once with a fresh
    /// `tempfile::tempdir()`'s path to bring the node up, `exec` some writes,
    /// shut down via `graceful_shutdown_wal_only` (which does NOT delete the
    /// directory — the caller's `TempDir` owns it), then call this again with
    /// the SAME path to reopen against the persisted WAL / redb stores. The
    /// caller must keep its `TempDir` alive across both calls.
    pub async fn spawn_single_node_calvin_on_path(
        num_cores: usize,
        data_dir_path: PathBuf,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_single_node_calvin_at(num_cores, Some(data_dir_path)).await
    }

    async fn spawn_single_node_calvin_at(
        num_cores: usize,
        data_dir_path: Option<PathBuf>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Sub-second election so the lone sequencer voter self-elects quickly
        // under the shared CI CPU pool without spurious re-elections.
        let tuning = ClusterTransportTuning {
            health_ping_interval_secs: 1,
            election_timeout_min_ms: 500,
            election_timeout_max_ms: 1000,
            ..ClusterTransportTuning::default()
        };
        let config = ClusterSpawnConfig {
            tuning,
            graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
            query_tuning: nodedb_types::config::tuning::QueryTuning::default(),
            num_cores,
            log_compaction_threshold: None,
            replication_factor: 1,
            single_node_calvin: true,
        };
        Self::spawn_with_full_config_at(1, vec![], &config, data_dir_path, None).await
    }
}
