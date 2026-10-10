// SPDX-License-Identifier: BUSL-1.1

//! Raft leadership reads and moves on one [`TestClusterNode`], through the
//! node's own Raft groups.

use crate::cluster_harness::node::lifecycle::TestClusterNode;

impl TestClusterNode {
    /// Whether this node leads `group_id` in its own Raft state now.
    pub fn leads_group(&self, group_id: u64) -> bool {
        self.shared.multi_raft.get().is_some_and(|multi_raft| {
            multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .group_role_is_leader(group_id)
        })
    }

    /// Ask this node, the leader of `group_id`, to hand the leadership to
    /// node `target`. The move finishes once `target` wins the next term.
    /// The error names the node, the group and the target.
    pub fn transfer_group_leadership(&self, group_id: u64, target: u64) -> Result<(), String> {
        let multi_raft = self
            .shared
            .multi_raft
            .get()
            .ok_or_else(|| format!("node {}: its Raft groups are not started", self.node_id))?;
        multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .transfer_leadership(group_id, target)
            .map_err(|e| {
                format!(
                    "node {}: transfer of group {group_id} to node {target}: {e}",
                    self.node_id
                )
            })
    }
}
