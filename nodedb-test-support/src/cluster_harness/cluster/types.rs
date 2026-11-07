// SPDX-License-Identifier: BUSL-1.1

//! [`TestCluster`] + [`ClusterSpawnConfig`] type definitions, and the
//! fast-election tuning shared by every `spawn_three*` entry point.

use nodedb_types::config::tuning::ClusterTransportTuning;

use super::super::node::TestClusterNode;

/// Data Raft groups a test cluster bootstraps with by default.
pub(crate) const DEFAULT_NUM_GROUPS: u64 = 2;

/// The spawn configuration used to bring up every node in a cluster.
///
/// Captured at spawn so that a later [`TestCluster::add_learner_node`]
/// brings the new node up with the *same* tuning — most importantly the
/// same Raft `log_compaction_threshold`, so the learner behaves
/// identically to the original members.
#[derive(Clone)]
pub(crate) struct ClusterSpawnConfig {
    pub(crate) tuning: ClusterTransportTuning,
    pub(crate) graph_tuning: nodedb_types::config::tuning::GraphTuning,
    pub(crate) query_tuning: nodedb_types::config::tuning::QueryTuning,
    pub(crate) num_cores: usize,
    pub(crate) log_compaction_threshold: Option<u64>,
    /// Raft replication factor used for every original member AND any
    /// later `add_learner_node()` call (HRW placement takes
    /// `min(replication_factor, node_count)`). Defaults to 3 for every
    /// spawn entry point except [`TestCluster::spawn_three_with_compaction_threshold_and_rf`].
    pub(crate) replication_factor: usize,
    /// Data Raft groups the cluster bootstraps with. Every spawn entry
    /// point uses [`DEFAULT_NUM_GROUPS`] except
    /// [`TestCluster::spawn_three_with_groups_compaction_threshold_and_rf`].
    pub(crate) num_groups: u64,
    /// When `true`, the node acquires its cluster handle from
    /// `init_single_node_calvin` (the one-node cluster synthesis production
    /// boot runs when `[cluster]` is absent) instead of building explicit
    /// `ClusterSettings` and calling `init_cluster_with_transport`. Only
    /// [`TestClusterNode::spawn_single_node_calvin`] uses it. Every multi-node
    /// spawn path sets `false`.
    pub(crate) single_node_calvin: bool,
    /// `[backup_storage]` installed on every node. `None` leaves every
    /// `file://` backup URI refused.
    pub(crate) backup_storage: Option<nodedb::config::server::BackupStorageSettings>,
    /// Shared PITR storage. `Some` opens every node's WAL encrypted at
    /// `<data_dir>/wal` and wires PITR before its Raft groups start.
    pub(crate) pitr: Option<crate::cluster_harness::pitr::PitrStorage>,
    /// Timeseries tuning of the node with each listed id. Every other node
    /// runs `TimeseriesToning::default()`.
    pub(crate) node_timeseries_tuning:
        std::collections::HashMap<u64, nodedb_types::config::tuning::TimeseriesToning>,
    /// Chunked-redo bounds every node runs. `None` keeps the default
    /// `[tuning.calvin]` bounds.
    pub(crate) redo_chunk_limits:
        Option<nodedb::control::wal_replication::transaction_redo::chunks::RedoChunkLimits>,
    /// The `[tuning.network]` every Control Plane runs. `None` keeps the default.
    pub(crate) network_tuning: Option<nodedb_types::config::tuning::NetworkTuning>,
}

impl ClusterSpawnConfig {
    /// The timeseries tuning node `node_id` runs.
    pub(crate) fn timeseries_tuning_for(
        &self,
        node_id: u64,
    ) -> nodedb_types::config::tuning::TimeseriesToning {
        self.node_timeseries_tuning
            .get(&node_id)
            .cloned()
            .unwrap_or_default()
    }
}

/// An in-process cluster of `TestClusterNode`s.
pub struct TestCluster {
    pub nodes: Vec<TestClusterNode>,
    /// Config used for the original members; reused by `add_learner_node`.
    pub(super) spawn_config: ClusterSpawnConfig,
}

/// Fast election tuning used by [`TestCluster::spawn_three`] and
/// [`TestCluster::spawn_three_with_cores`].
pub(super) fn fast_cluster_tuning() -> ClusterTransportTuning {
    ClusterTransportTuning {
        // Fast health pings so the HealthMonitor re-broadcasts
        // topology within ~1s if the initial join broadcast was missed.
        health_ping_interval_secs: 1,
        // Sub-second election windows. Bootstrap defaults are 150/300ms;
        // we allow significantly more headroom (500/1000ms) because
        // integration tests share the host CPU pool with hundreds of
        // unit tests running in parallel — under that load the Raft
        // tick loop can be starved long enough that aggressive
        // 200/500ms windows trigger spurious re-elections mid-test.
        // 500/1000ms is still ~3× faster than the seconds-floor of
        // 1s/2s but stable under contention.
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 1000,
        ..ClusterTransportTuning::default()
    }
}
