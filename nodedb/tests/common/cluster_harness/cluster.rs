//! Multi-node cluster orchestration.

use std::time::Duration;

use nodedb_types::config::tuning::ClusterTransportTuning;

use super::node::TestClusterNode;
use super::wait::wait_for;

/// An in-process cluster of `TestClusterNode`s.
pub struct TestCluster {
    pub nodes: Vec<TestClusterNode>,
}

impl TestCluster {
    /// Spawn a 3-node cluster: node 1 bootstraps, nodes 2 and 3 join
    /// via node 1's pre-bound address. Waits until every node sees
    /// topology_size == 3 (10s deadline).
    pub async fn spawn_three() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_three_with_tuning(ClusterTransportTuning::default()).await
    }

    /// Spawn a 3-node cluster with a custom `ClusterTransportTuning`.
    /// Used by the descriptor-lease renewal tests to drive the
    /// renewal loop on a much faster cadence than the production
    /// 60-second default.
    pub async fn spawn_three_with_tuning(
        tuning: ClusterTransportTuning,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let node1 = TestClusterNode::spawn_with_tuning(1, vec![], tuning.clone()).await?;

        // Give node 1's transport + raft loop a moment to start
        // accepting before peers dial in.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let seeds = vec![node1.listen_addr];
        let node2 = TestClusterNode::spawn_with_tuning(2, seeds.clone(), tuning.clone()).await?;
        let node3 = TestClusterNode::spawn_with_tuning(3, seeds, tuning).await?;

        let cluster = Self {
            nodes: vec![node1, node2, node3],
        };

        wait_for(
            "all 3 nodes report topology_size == 3",
            Duration::from_secs(10),
            Duration::from_millis(100),
            || cluster.nodes.iter().all(|n| n.topology_size() == 3),
        )
        .await;

        // CRITICAL: wait for every node to exit rolling-upgrade
        // compat mode before letting the test issue any DDL.
        //
        // `metadata_proposer::propose_catalog_entry` consults
        // `cluster_version_view().can_activate_feature(DISTRIBUTED_CATALOG_VERSION)`
        // and, while even one node still reports a lower wire
        // version, returns `Ok(0)` without going through the raft
        // group. The pgwire DDL handlers (CREATE USER, etc.) then
        // fall through to a LEGACY path that writes the record
        // directly on the proposing node — **with zero
        // replication** to followers. Any subsequent
        // `has_active_user` check on a follower returns false and
        // the test flakes.
        //
        // Topology has three members the moment the join request
        // completes, but the `wire_version` field on each node's
        // topology entry is updated asynchronously by the gossip
        // path. That's why `topology_size == 3` converges fast yet
        // `can_activate_feature(...)` can still be false for
        // several hundred milliseconds afterwards. Waiting here
        // closes the window deterministically — no retries, no
        // flakes, no compat-mode fallback silently breaking
        // replication.
        wait_for(
            "all 3 nodes exit rolling-upgrade compat mode",
            Duration::from_secs(10),
            Duration::from_millis(20),
            || {
                cluster.nodes.iter().all(|n| {
                    n.shared.cluster_version_view().can_activate_feature(
                        nodedb::control::rolling_upgrade::DISTRIBUTED_CATALOG_VERSION,
                    )
                })
            },
        )
        .await;

        // CRITICAL: wait for the metadata Raft group to elect a leader
        // and for every node's local view to agree on the same leader id.
        //
        // Topology convergence + rolling-upgrade exit only guarantees
        // membership and wire version are agreed; they say nothing about
        // election state. Under heavy host load (e.g. running this test
        // immediately after another full-suite cluster test exits and
        // the unit-test pool ramps back up), the initial Raft heartbeat
        // window can be missed and the first `acquire`/`propose` issued
        // by the test races a re-election — surfacing as
        // `raft error: not leader (leader hint: None)` from a
        // descriptor-lease or DDL call.
        //
        // Waiting until every node reports the same non-zero leader id
        // closes the window deterministically. Symmetric to the
        // rolling-upgrade wait above: no retries, no flakes, no
        // wasted CI minutes on cleanup of a doomed cluster bringup.
        wait_for(
            "metadata group has stable leader visible on every node",
            Duration::from_secs(10),
            Duration::from_millis(20),
            || {
                let leaders: Vec<u64> = cluster
                    .nodes
                    .iter()
                    .map(|n| n.metadata_group_leader())
                    .collect();
                let first = leaders[0];
                first != 0 && leaders.iter().all(|&l| l == first)
            },
        )
        .await;

        Ok(cluster)
    }

    /// Find a node that will accept the given DDL — retries up to
    /// 10 seconds across all nodes. Non-leader nodes surface
    /// `not metadata-group leader` errors via the pgwire error path;
    /// the retry loop tries the next node on failure so the test
    /// doesn't have to discover the leader explicitly.
    ///
    /// After the DDL is accepted, **blocks until every node's
    /// metadata applier has caught up to the proposer's applied
    /// index**. `propose_catalog_entry` already waits for the entry
    /// to be applied on the proposing node before returning, but
    /// followers apply asynchronously — without this barrier a
    /// subsequent `wait_for("x visible on every node")` would race
    /// the follower appliers and trip its timeout on the cold-start
    /// attempt. Polling the watermark directly is O(applied_index)
    /// and converges as soon as the followers drain their commit
    /// queues, so it's both strictly more correct and strictly
    /// faster than waiting on the visibility check itself.
    pub async fn exec_ddl_on_any_leader(&self, sql: &str) -> Result<usize, String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut last_err = String::new();
        while std::time::Instant::now() < deadline {
            for (idx, node) in self.nodes.iter().enumerate() {
                match node.exec(sql).await {
                    Ok(()) => {
                        self.wait_for_applied_index_convergence(idx).await;
                        return Ok(idx);
                    }
                    Err(e) => last_err = e,
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!(
            "no node accepted DDL within 10s; last error: {last_err}"
        ))
    }

    /// Block until every node's metadata applier has caught up to the
    /// proposer's current applied index. Called after every successful
    /// DDL by `exec_ddl_on_any_leader`.
    async fn wait_for_applied_index_convergence(&self, proposer_idx: usize) {
        let target = self.nodes[proposer_idx]
            .shared
            .applied_index_watcher()
            .current();
        if target == 0 {
            return;
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let all_caught_up = self
                .nodes
                .iter()
                .all(|n| n.shared.applied_index_watcher().current() >= target);
            if all_caught_up {
                return;
            }
            if std::time::Instant::now() >= deadline {
                // Don't panic — the caller's own `wait_for` assertion
                // will report the specific visibility failure with a
                // better error than "convergence timed out".
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Cooperatively shut down every node. Reverse order so peers
    /// observe their neighbours' drop without rejecting inbound
    /// traffic on an already-closed transport.
    pub async fn shutdown(self) {
        let mut nodes = self.nodes;
        while let Some(node) = nodes.pop() {
            node.shutdown().await;
        }
    }
}
