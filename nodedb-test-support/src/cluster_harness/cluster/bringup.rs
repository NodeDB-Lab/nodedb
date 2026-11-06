// SPDX-License-Identifier: BUSL-1.1

//! The shared 3-node bringup body (`spawn_three_inner`). The post-join
//! convergence barriers live in `ready`.

use std::time::Duration;

use nodedb_types::config::tuning::ClusterTransportTuning;

use super::TestCluster;
use super::types::{ClusterSpawnConfig, DEFAULT_NUM_GROUPS};
use crate::cluster_harness::node::TestClusterNode;

impl TestCluster {
    /// Spawn a 3-node cluster whose Control Planes run `network`. A test shortens
    /// the snapshot hold with it, as an operator's `[tuning.network]` section does.
    pub async fn spawn_three_with_network_tuning(
        network: nodedb_types::config::tuning::NetworkTuning,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_config(ClusterSpawnConfig {
            tuning: super::types::fast_cluster_tuning(),
            graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
            query_tuning: nodedb_types::config::tuning::QueryTuning::default(),
            num_cores: 1,
            log_compaction_threshold: None,
            replication_factor: 3,
            num_groups: DEFAULT_NUM_GROUPS,
            single_node_calvin: false,
            backup_storage: None,
            pitr: None,
            node_timeseries_tuning: std::collections::HashMap::new(),
            redo_chunk_limits: None,
            network_tuning: Some(network),
        })
        .await
    }

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
            num_groups: DEFAULT_NUM_GROUPS,
            single_node_calvin: false,
            backup_storage: None,
            pitr: None,
            node_timeseries_tuning: std::collections::HashMap::new(),
            redo_chunk_limits: None,
            network_tuning: None,
        };
        Self::spawn_three_with_config(config).await
    }

    /// Spawn a 3-node cluster with `num_groups` data groups, a low Raft
    /// `log_compaction_threshold` and `replication_factor`. More groups give
    /// the rendezvous placement more distinct replica sets to choose from.
    pub async fn spawn_three_with_groups_compaction_threshold_and_rf(
        num_groups: u64,
        threshold: u64,
        replication_factor: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_config(ClusterSpawnConfig {
            tuning: super::types::fast_cluster_tuning(),
            graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
            query_tuning: nodedb_types::config::tuning::QueryTuning::default(),
            num_cores: 1,
            log_compaction_threshold: Some(threshold),
            replication_factor,
            num_groups,
            single_node_calvin: false,
            backup_storage: None,
            pitr: None,
            node_timeseries_tuning: std::collections::HashMap::new(),
            redo_chunk_limits: None,
            network_tuning: None,
        })
        .await
    }

    /// Spawn a 3-node cluster with `num_groups` data groups,
    /// `replication_factor`, and `num_cores` Data-Plane cores per node.
    pub async fn spawn_three_with_groups_rf_and_cores(
        num_groups: u64,
        replication_factor: usize,
        num_cores: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_config(ClusterSpawnConfig {
            tuning: super::types::fast_cluster_tuning(),
            graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
            query_tuning: nodedb_types::config::tuning::QueryTuning::default(),
            num_cores,
            log_compaction_threshold: None,
            replication_factor,
            num_groups,
            single_node_calvin: false,
            backup_storage: None,
            pitr: None,
            node_timeseries_tuning: std::collections::HashMap::new(),
            redo_chunk_limits: None,
            network_tuning: None,
        })
        .await
    }

    /// Spawn a 3-node cluster where node `node_id` runs `timeseries_tuning`
    /// and the other nodes run the default. Uses the standard fast-election
    /// tuning and 1 core per node.
    pub async fn spawn_three_with_node_timeseries_tuning(
        node_id: u64,
        timeseries_tuning: nodedb_types::config::tuning::TimeseriesToning,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_config(ClusterSpawnConfig {
            tuning: super::types::fast_cluster_tuning(),
            graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
            query_tuning: nodedb_types::config::tuning::QueryTuning::default(),
            num_cores: 1,
            log_compaction_threshold: None,
            replication_factor: 3,
            num_groups: DEFAULT_NUM_GROUPS,
            single_node_calvin: false,
            backup_storage: None,
            pitr: None,
            node_timeseries_tuning: std::collections::HashMap::from([(node_id, timeseries_tuning)]),
            redo_chunk_limits: None,
            network_tuning: None,
        })
        .await
    }

    /// Spawn a 3-node cluster whose nodes share one `[backup_storage]`
    /// `local_root`, so a `file://` backup URI names the same file on every
    /// node. Uses the standard fast-election tuning and 1 core per node.
    pub async fn spawn_three_with_backup_root(
        local_root: std::path::PathBuf,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_config(ClusterSpawnConfig {
            tuning: super::types::fast_cluster_tuning(),
            graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
            query_tuning: nodedb_types::config::tuning::QueryTuning::default(),
            num_cores: 1,
            log_compaction_threshold: None,
            replication_factor: 3,
            num_groups: DEFAULT_NUM_GROUPS,
            single_node_calvin: false,
            backup_storage: Some(nodedb::config::server::BackupStorageSettings {
                local_root: Some(local_root),
                ..Default::default()
            }),
            pitr: None,
            node_timeseries_tuning: std::collections::HashMap::new(),
            redo_chunk_limits: None,
            network_tuning: None,
        })
        .await
    }

    /// Spawn a 3-node cluster whose nodes share `pitr`'s cold store,
    /// snapshot store and WAL key. Uses the standard fast-election tuning and
    /// 1 core per node.
    pub async fn spawn_three_with_pitr(
        pitr: crate::cluster_harness::pitr::PitrStorage,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_config(ClusterSpawnConfig {
            tuning: super::types::fast_cluster_tuning(),
            graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
            query_tuning: nodedb_types::config::tuning::QueryTuning::default(),
            num_cores: 1,
            log_compaction_threshold: None,
            replication_factor: 3,
            num_groups: DEFAULT_NUM_GROUPS,
            single_node_calvin: false,
            backup_storage: None,
            pitr: Some(pitr),
            node_timeseries_tuning: std::collections::HashMap::new(),
            redo_chunk_limits: None,
            network_tuning: None,
        })
        .await
    }

    /// Spawn a 3-node cluster whose nodes cap a redo entry at
    /// `max_entry_bytes`, so a commit larger than it travels as a chunked
    /// redo stream. Uses the standard fast-election tuning and 1 core per
    /// node.
    pub async fn spawn_three_with_redo_entry_limit(
        max_entry_bytes: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let calvin = nodedb_types::config::tuning::CalvinTuning::default();
        Self::spawn_three_with_config(ClusterSpawnConfig {
            tuning: super::types::fast_cluster_tuning(),
            graph_tuning: nodedb_types::config::tuning::GraphTuning::default(),
            query_tuning: nodedb_types::config::tuning::QueryTuning::default(),
            num_cores: 1,
            log_compaction_threshold: None,
            replication_factor: 3,
            num_groups: DEFAULT_NUM_GROUPS,
            single_node_calvin: false,
            backup_storage: None,
            pitr: None,
            node_timeseries_tuning: std::collections::HashMap::new(),
            network_tuning: None,
            redo_chunk_limits: Some(
                nodedb::control::wal_replication::transaction_redo::chunks::RedoChunkLimits {
                    max_entry_bytes,
                    max_open_bytes: calvin.max_open_redo_bytes,
                },
            ),
        })
        .await
    }

    /// Spawn node 1, then nodes 2 and 3 joining it, all with `config`.
    async fn spawn_three_with_config(
        config: ClusterSpawnConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
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

        cluster
            .await_ready(super::ready::LeaderBar::Preferred)
            .await;

        Ok(cluster)
    }
}
