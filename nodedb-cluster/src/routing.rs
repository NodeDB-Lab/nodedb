use std::collections::HashMap;

use crate::error::{ClusterError, Result};

/// Number of virtual shards.
pub const VSHARD_COUNT: u16 = 1024;

/// Maps vShards to Raft groups and Raft groups to nodes.
///
/// The 1024 vShards are divided into distinct Raft Groups
/// (e.g., vShards 0-63 managed by Raft Group 1 across Nodes A, B, and C).
///
/// This table is the authoritative routing source. It is updated atomically
/// via Raft state machine when:
/// - A shard migration completes (Phase 3 atomic cut-over)
/// - A Raft group membership changes
/// - A node joins or decommissions
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RoutingTable {
    /// vshard_id → raft_group_id.
    vshard_to_group: Vec<u64>,
    /// raft_group_id → (leader_node, [replica_nodes]).
    group_members: HashMap<u64, GroupInfo>,
}

#[derive(
    Debug,
    Clone,
    Default,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct GroupInfo {
    /// Current leader node ID (0 = no leader known).
    pub leader: u64,
    /// All voting members (including leader).
    pub members: Vec<u64>,
    /// Non-voting learner peers catching up to this group.
    ///
    /// Learners receive log replication but do not vote in elections and
    /// are not counted toward the commit quorum. A learner transitions
    /// into `members` via a second `PromoteLearner` conf-change once the
    /// leader observes it has caught up.
    #[serde(default)]
    pub learners: Vec<u64>,
}

impl RoutingTable {
    /// Create a routing table with uniform distribution of vShards across groups.
    ///
    /// `num_groups` Raft groups are created. vShards are distributed round-robin.
    /// Each group initially contains `nodes_per_group` nodes from the `nodes` list.
    pub fn uniform(num_groups: u64, nodes: &[u64], replication_factor: usize) -> Self {
        assert!(!nodes.is_empty(), "need at least one node");
        assert!(replication_factor > 0, "need at least RF=1");

        let mut vshard_to_group = Vec::with_capacity(VSHARD_COUNT as usize);
        for i in 0..VSHARD_COUNT {
            vshard_to_group.push((i as u64) % num_groups);
        }

        let mut group_members = HashMap::new();
        for group_id in 0..num_groups {
            let rf = replication_factor.min(nodes.len());
            let start = (group_id as usize * rf) % nodes.len();
            let members: Vec<u64> = (0..rf).map(|i| nodes[(start + i) % nodes.len()]).collect();
            let leader = members[0];
            group_members.insert(
                group_id,
                GroupInfo {
                    leader,
                    members,
                    learners: Vec::new(),
                },
            );
        }

        Self {
            vshard_to_group,
            group_members,
        }
    }

    /// Look up which Raft group owns a vShard.
    pub fn group_for_vshard(&self, vshard_id: u16) -> Result<u64> {
        self.vshard_to_group
            .get(vshard_id as usize)
            .copied()
            .ok_or(ClusterError::VShardNotMapped { vshard_id })
    }

    /// Look up the leader node for a vShard.
    pub fn leader_for_vshard(&self, vshard_id: u16) -> Result<u64> {
        let group_id = self.group_for_vshard(vshard_id)?;
        let info = self
            .group_members
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        Ok(info.leader)
    }

    /// Get group info.
    pub fn group_info(&self, group_id: u64) -> Option<&GroupInfo> {
        self.group_members.get(&group_id)
    }

    /// Update the leader for a Raft group.
    pub fn set_leader(&mut self, group_id: u64, leader: u64) {
        if let Some(info) = self.group_members.get_mut(&group_id) {
            info.leader = leader;
        }
    }

    /// Atomically reassign a vShard to a different Raft group.
    /// Used during Phase 3 (atomic cut-over) of shard migration.
    pub fn reassign_vshard(&mut self, vshard_id: u16, new_group_id: u64) {
        if (vshard_id as usize) < self.vshard_to_group.len() {
            self.vshard_to_group[vshard_id as usize] = new_group_id;
        }
    }

    /// All vShards assigned to a given group.
    pub fn vshards_for_group(&self, group_id: u64) -> Vec<u16> {
        self.vshard_to_group
            .iter()
            .enumerate()
            .filter(|(_, gid)| **gid == group_id)
            .map(|(i, _)| i as u16)
            .collect()
    }

    /// Number of Raft groups.
    pub fn num_groups(&self) -> usize {
        self.group_members.len()
    }

    /// All group IDs.
    pub fn group_ids(&self) -> Vec<u64> {
        self.group_members.keys().copied().collect()
    }

    /// Update the voting members of a Raft group (for membership changes).
    pub fn set_group_members(&mut self, group_id: u64, members: Vec<u64>) {
        if let Some(info) = self.group_members.get_mut(&group_id) {
            info.members = members;
        }
    }

    /// Remove a node from a group's voter and learner lists. If the
    /// removed node was the current leader hint, the hint is cleared
    /// so the next query drives a fresh discovery. Returns `true` if
    /// the group existed and anything was actually removed.
    ///
    /// The caller is responsible for safety: dropping below the
    /// configured replication factor must be gated by
    /// `decommission::safety::check_can_decommission`.
    pub fn remove_group_member(&mut self, group_id: u64, node_id: u64) -> bool {
        let Some(info) = self.group_members.get_mut(&group_id) else {
            return false;
        };
        let before_members = info.members.len();
        let before_learners = info.learners.len();
        info.members.retain(|&id| id != node_id);
        info.learners.retain(|&id| id != node_id);
        if info.leader == node_id {
            info.leader = 0;
        }
        info.members.len() != before_members || info.learners.len() != before_learners
    }

    /// Update the learner list for a Raft group.
    pub fn set_group_learners(&mut self, group_id: u64, learners: Vec<u64>) {
        if let Some(info) = self.group_members.get_mut(&group_id) {
            info.learners = learners;
        }
    }

    /// Add a learner to a group if not already present. No-op if the peer
    /// is already a voter or a learner.
    pub fn add_group_learner(&mut self, group_id: u64, peer: u64) {
        if let Some(info) = self.group_members.get_mut(&group_id)
            && !info.members.contains(&peer)
            && !info.learners.contains(&peer)
        {
            info.learners.push(peer);
        }
    }

    /// Promote a learner to a voter within a group. Returns `true` if the
    /// learner was found and promoted.
    pub fn promote_group_learner(&mut self, group_id: u64, peer: u64) -> bool {
        if let Some(info) = self.group_members.get_mut(&group_id)
            && let Some(pos) = info.learners.iter().position(|&id| id == peer)
        {
            info.learners.remove(pos);
            if !info.members.contains(&peer) {
                info.members.push(peer);
            }
            return true;
        }
        false
    }

    /// Access the vshard-to-group mapping (for persistence / wire transfer).
    pub fn vshard_to_group(&self) -> &[u64] {
        &self.vshard_to_group
    }

    /// Access all group members (for persistence / wire transfer).
    pub fn group_members(&self) -> &HashMap<u64, GroupInfo> {
        &self.group_members
    }

    /// Reconstruct a RoutingTable from persisted data.
    pub fn from_parts(vshard_to_group: Vec<u64>, group_members: HashMap<u64, GroupInfo>) -> Self {
        Self {
            vshard_to_group,
            group_members,
        }
    }
}

/// Compute the primary vShard for a collection name.
///
/// Maps a collection name to its vShard ID.
///
/// Must match `VShardId::from_collection()` in the nodedb types module
/// exactly — uses u16 accumulator with multiplier 31.
pub fn vshard_for_collection(collection: &str) -> u16 {
    let hash = collection
        .as_bytes()
        .iter()
        .fold(0u16, |h, &b| h.wrapping_mul(31).wrapping_add(b as u16));
    hash % VSHARD_COUNT
}

/// FNV-1a 64-bit hash for deterministic key partitioning.
///
/// Used by distributed join shuffle and shard split to assign keys
/// to partitions. NOT for vShard routing — use `vshard_for_collection`
/// for that.
pub fn fnv1a_hash(key: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in key.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uniform_distribution() {
        let rt = RoutingTable::uniform(16, &[1, 2, 3], 3);
        assert_eq!(rt.num_groups(), 16);

        // Each group should have ~64 vShards (1024/16).
        for gid in 0..16 {
            let shards = rt.vshards_for_group(gid);
            assert_eq!(shards.len(), 64);
        }
    }

    #[test]
    fn leader_lookup() {
        let rt = RoutingTable::uniform(4, &[10, 20, 30], 3);
        let leader = rt.leader_for_vshard(0).unwrap();
        assert!(leader > 0);
    }

    #[test]
    fn reassign_vshard() {
        let mut rt = RoutingTable::uniform(4, &[1, 2, 3], 3);
        let old_group = rt.group_for_vshard(0).unwrap();
        let new_group = (old_group + 1) % 4;
        rt.reassign_vshard(0, new_group);
        assert_eq!(rt.group_for_vshard(0).unwrap(), new_group);
    }

    #[test]
    fn set_leader() {
        let mut rt = RoutingTable::uniform(2, &[1, 2, 3], 3);
        rt.set_leader(0, 99);
        assert_eq!(rt.leader_for_vshard(0).unwrap(), 99);
    }

    #[test]
    fn remove_group_member_strips_voter_and_clears_leader() {
        let mut rt = RoutingTable::uniform(2, &[1, 2, 3], 3);
        rt.set_leader(0, 2);
        assert!(rt.remove_group_member(0, 2));
        let info = rt.group_info(0).unwrap();
        assert!(!info.members.contains(&2));
        assert_eq!(info.leader, 0, "leader hint should be cleared");
    }

    #[test]
    fn remove_group_member_strips_learner_only() {
        let mut rt = RoutingTable::uniform(2, &[1, 2, 3], 3);
        rt.add_group_learner(0, 9);
        assert!(rt.remove_group_member(0, 9));
        let info = rt.group_info(0).unwrap();
        assert!(!info.learners.contains(&9));
    }

    #[test]
    fn remove_group_member_unknown_group_returns_false() {
        let mut rt = RoutingTable::uniform(1, &[1, 2], 2);
        assert!(!rt.remove_group_member(99, 1));
    }

    #[test]
    fn vshard_not_mapped() {
        let rt = RoutingTable::uniform(2, &[1, 2], 2);
        // All 1024 are mapped, so this shouldn't fail.
        assert!(rt.group_for_vshard(1023).is_ok());
    }
}
