// SPDX-License-Identifier: BUSL-1.1

//! Post-spawn membership growth (`add_learner_node`), DDL dispatch
//! (`exec_ddl_on_any_leader`), the convergence barriers backing both,
//! and cooperative shutdown.

use std::time::Duration;

use super::TestCluster;
use crate::cluster_harness::node::TestClusterNode;
use crate::cluster_harness::wait::wait_for;

impl TestCluster {
    /// Add a fresh node to the running cluster as a learner and return a
    /// reference to it.
    ///
    /// This drives the **production** runtime membership path end to end —
    /// no hand-copied state:
    ///
    /// 1. Spawn a brand-new full node (fresh temp data dir, its own
    ///    ephemeral QUIC + pgwire ports, 1 core, the SAME cluster config as
    ///    the original members — including the low
    ///    `log_compaction_threshold`). Its only seed is node 1's pre-bound
    ///    listen address. Because the node has no prior catalog, the
    ///    cluster-init dispatcher takes the **join** path: the node sends a
    ///    `RaftRpc::JoinRequest` to the seed.
    ///
    /// 2. The seed's `RaftLoop::join_flow` (running on its live transport)
    ///    is the real conf-change driver:
    ///    - registers the joiner's address on the leader's transport
    ///      (`transport.register_peer`) so the leader can immediately
    ///      `AppendEntries` / `InstallSnapshot` to it,
    ///    - admits it into topology, then proposes
    ///      `ConfChange::AddLearner(new_node_id)` on **every** Raft group
    ///      the node is missing (metadata + every data group),
    ///    - waits for each conf-change to commit, persists topology +
    ///      routing, and broadcasts a `TopologyUpdate` to every active peer
    ///      so the *other* existing nodes also learn the joiner's address
    ///      (peer wiring in both directions).
    ///
    ///    The `JoinResponse` carries the post-`AddLearner` routing, and the
    ///    joining node reconstructs its local `MultiRaft` with each data
    ///    group started in the `Learner` role (`add_group_as_learner`).
    ///
    /// 3. The learner now lives in every group as a non-voter. On the next
    ///    leader tick, the leader sees the learner's `next_index` is below
    ///    its compacted `snapshot_index`, so it cannot use `AppendEntries`
    ///    and instead builds a real per-group snapshot via the installed
    ///    `DataPlaneSnapshotBuilder` and streams it with `InstallSnapshot`.
    ///    The learner applies it through the `DataPlaneSnapshotApplier`.
    ///
    /// This method blocks until the new node is visible in every node's
    /// topology and every Raft group has fully propagated (via
    /// [`Self::wait_for_full_apply_convergence`]). It does **not** assert
    /// the data is present — that is the caller's job (query the learner's
    /// own pgwire client).
    ///
    /// Returns a reference to the newly-added node (the last entry in
    /// [`Self::nodes`]).
    pub async fn add_learner_node(
        &mut self,
    ) -> Result<&TestClusterNode, Box<dyn std::error::Error + Send + Sync>> {
        let new_node_id = self.nodes.iter().map(|n| n.node_id).max().unwrap_or(0) + 1;
        let seeds = vec![self.nodes[0].listen_addr];
        let cfg = self.spawn_config.clone();

        let learner = TestClusterNode::spawn_with_full_config(new_node_id, seeds, &cfg).await?;

        self.nodes.push(learner);
        let expected = self.nodes.len();

        // Wait until every node (including the new learner) sees the full
        // membership. The join broadcasts a TopologyUpdate to existing
        // peers, so this converges once that propagates.
        wait_for(
            "every node sees the new learner in topology",
            Duration::from_secs(30),
            Duration::from_millis(50),
            || self.nodes.iter().all(|n| n.topology_size() == expected),
        )
        .await;

        // Wait until every Raft group has fully propagated to every member /
        // learner. For the learner this only completes once the leader's
        // InstallSnapshot has been applied (its data-group log starts beyond
        // the compacted region, so AppendEntries alone cannot advance it).
        self.wait_for_full_apply_convergence(Duration::from_secs(30))
            .await;

        Ok(self.nodes.last().expect("learner just pushed"))
    }

    /// Find a node that will accept the given DDL — retries up to
    /// 10 seconds across all nodes. Non-leader nodes surface
    /// `not metadata-group leader` errors via the pgwire error path;
    /// the retry loop tries the next node on failure so the test
    /// doesn't have to discover the leader explicitly.
    ///
    /// After the DDL is accepted, blocks until every node applied the
    /// metadata group through its commit index. Followers apply
    /// asynchronously, so a later visibility check otherwise races their
    /// appliers. Panics with every node's positions after 10 s. Then blocks
    /// until every node holds an authorization lease, and panics naming the
    /// node that holds none after 15 s.
    pub async fn exec_ddl_on_any_leader(&self, sql: &str) -> Result<usize, String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        // The first error names why the statement was refused at all. Each
        // node's last error names how that node refused it at the deadline:
        // the metadata leader refuses for its own reason, and a follower
        // reports why its forward failed.
        let mut first_err: Option<String> = None;
        let mut last_errs: Vec<Option<String>> = vec![None; self.nodes.len()];
        while std::time::Instant::now() < deadline {
            for (idx, node) in self.nodes.iter().enumerate() {
                match node.exec(sql).await {
                    Ok(()) => {
                        self.wait_for_metadata_apply_convergence(Duration::from_secs(10))
                            .await;
                        // An authorization-bearing DDL is acknowledged once
                        // each lease holder covered it or its lease expired.
                        // A node whose lease expired plans again only after
                        // its next granted renewal.
                        self.wait_for_authorization_leases().await;
                        return Ok(idx);
                    }
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(format!("node {idx}: {e}"));
                        }
                        last_errs[idx] = Some(e);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let per_node: Vec<String> = self
            .nodes
            .iter()
            .zip(&last_errs)
            .enumerate()
            .map(|(idx, (node, err))| {
                format!(
                    "node {idx} (id {}): {}; last error: {}",
                    node.node_id,
                    metadata_group_view(node),
                    err.as_deref().unwrap_or("none")
                )
            })
            .collect();
        Err(format!(
            "no node accepted DDL within 10s; first error: {}; per node: [{}]",
            first_err.unwrap_or_default(),
            per_node.join("; ")
        ))
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

/// `node`'s own Raft view of the metadata group: role, term, the leader it
/// names, commit and apply positions, and membership counts.
fn metadata_group_view(node: &TestClusterNode) -> String {
    let Some(status_fn) = node.shared.raft_status_fn.get() else {
        return "no raft status".to_owned();
    };
    status_fn()
        .into_iter()
        .find(|g| g.group_id == nodedb_cluster::METADATA_GROUP_ID)
        .map_or_else(
            || "hosts no metadata group".to_owned(),
            |g| {
                format!(
                    "metadata group {} term {} leader {} commit {} applied {} voters {} \
                     learners {}",
                    g.role,
                    g.term,
                    g.leader_id,
                    g.commit_index,
                    g.last_applied,
                    g.member_count,
                    g.learner_count
                )
            },
        )
}
