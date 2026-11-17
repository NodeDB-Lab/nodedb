// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashMap;

use nodedb_types::id::{CollectionKey, VShardId};

use crate::error::{ClusterError, Result};

/// Number of virtual shards.
///
/// Re-exports [`VShardId::COUNT`] to keep the cluster routing layer and the
/// types-layer hash function locked to the same constant. Changing the count
/// in only one of the two crates would silently misroute every collection.
pub const VSHARD_COUNT: u32 = VShardId::COUNT;

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
///
/// # Lock order
///
/// The shared `Arc<RwLock<RoutingTable>>` ranks below the `MultiRaft` mutex.
/// Take the `MultiRaft` lock first, then the routing guard.
/// Never call into `MultiRaft` while holding a routing guard.
/// That bans `group_statuses`, `raft_status_fn`, and any `multi_raft.lock()` path.
/// `MultiRaft` reads routing under its own lock, so the nested call re-reads routing.
/// A queued writer blocks that nested read, and the node deadlocks.
/// Take the Raft status first, then the routing guard.
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
    /// vshard_id → (raft_group_id → epoch at which the vShard moved to that
    /// group). A committed `ReassignVShard` metadata entry sets the epoch to
    /// its own metadata log index, so every node derives the same value and a
    /// replay reproduces it. It is the only event that sets an epoch. A vShard
    /// in its initial group has epoch `0`.
    #[serde(default)]
    vshard_epochs: HashMap<u32, HashMap<u64, u64>>,
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
    /// The Raft term `leader` is known at: from this node's own Raft, or
    /// from a leader redirect that named the leader with its term. `0` when
    /// the hint came from a source with no term (the metadata log,
    /// placement). A termed hint at a higher term always replaces the hint.
    /// A term-less hint never replaces a termed one. A clear keeps the term.
    /// Only a confirmation (see [`RoutingTable::confirm_leader`]) fills a
    /// cleared hint at that term.
    #[serde(default)]
    pub leader_term: u64,
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
    /// Intended voter nodes for this group (the placement set), authored
    /// centrally and replicated via the metadata Raft group. `None` = no
    /// explicit placement; consumers fall back to the current `members`.
    /// `Some(_)` = the explicit target voter set used to cap promotion.
    #[serde(default)]
    pub placement: Option<Vec<u64>>,
}

impl RoutingTable {
    /// Create a routing table with uniform distribution of vShards across data groups.
    ///
    /// `num_groups` is the number of **data** Raft groups. vShards are distributed
    /// round-robin across groups `1..=num_groups`. Group 0 is the metadata group and
    /// is always included in `group_members` but is never assigned any vShards —
    /// it is accessed only via `propose_to_metadata_group`, never via
    /// `propose(vshard_id, data)`.
    ///
    /// Each data group initially contains `replication_factor` nodes from `nodes`.
    /// The metadata group (0) receives the same membership as the first data group.
    pub fn uniform(num_groups: u64, nodes: &[u64], replication_factor: usize) -> Self {
        assert!(!nodes.is_empty(), "need at least one node");
        assert!(num_groups > 0, "need at least 1 data group");
        assert!(replication_factor > 0, "need at least RF=1");

        // vShards map to data groups 1..=num_groups, skipping group 0 (metadata).
        let mut vshard_to_group = Vec::with_capacity(VSHARD_COUNT as usize);
        for i in 0..VSHARD_COUNT {
            vshard_to_group.push(1 + (i as u64) % num_groups);
        }

        let mut group_members = HashMap::new();
        // Data groups: 1..=num_groups.
        for idx in 0..num_groups {
            let group_id = idx + 1;
            let rf = replication_factor.min(nodes.len());
            let start = (idx as usize * rf) % nodes.len();
            let members: Vec<u64> = (0..rf).map(|i| nodes[(start + i) % nodes.len()]).collect();
            let leader = members[0];
            group_members.insert(
                group_id,
                GroupInfo {
                    leader,
                    leader_term: 0,
                    members,
                    learners: Vec::new(),
                    placement: None,
                },
            );
        }
        // Metadata group 0: same membership as the first data group.
        let rf = replication_factor.min(nodes.len());
        let meta_members: Vec<u64> = (0..rf).map(|i| nodes[i % nodes.len()]).collect();
        let meta_leader = meta_members[0];
        group_members.insert(
            0,
            GroupInfo {
                leader: meta_leader,
                leader_term: 0,
                members: meta_members,
                learners: Vec::new(),
                placement: None,
            },
        );

        Self {
            vshard_to_group,
            group_members,
            vshard_epochs: HashMap::new(),
        }
    }

    /// Look up which Raft group owns a vShard.
    pub fn group_for_vshard(&self, vshard_id: u32) -> Result<u64> {
        self.vshard_to_group
            .get(vshard_id as usize)
            .copied()
            .ok_or(ClusterError::VShardNotMapped { vshard_id })
    }

    /// Look up the leader node for a vShard.
    pub fn leader_for_vshard(&self, vshard_id: u32) -> Result<u64> {
        let group_id = self.group_for_vshard(vshard_id)?;
        let info = self
            .group_members
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        Ok(info.leader)
    }

    /// The leader hint of the group that owns `vshard_id`, with the term the
    /// hint is known at: `(leader, leader_term)`.
    pub fn leader_at_term_for_vshard(&self, vshard_id: u32) -> Result<(u64, u64)> {
        let group_id = self.group_for_vshard(vshard_id)?;
        let info = self
            .group_members
            .get(&group_id)
            .ok_or(ClusterError::GroupNotFound { group_id })?;
        Ok((info.leader, info.leader_term))
    }

    /// Get group info.
    pub fn group_info(&self, group_id: u64) -> Option<&GroupInfo> {
        self.group_members.get(&group_id)
    }

    /// Set the leader hint of a Raft group from a source that names a
    /// leader without a term. It applies only while the hint holds no term,
    /// so it never replaces a leader some node's Raft observed. Returns
    /// whether the hint changed.
    ///
    /// The callers with no term:
    /// - the metadata log's `RoutingChange` entries, which name a planned
    ///   leaseholder or transfer target, not an elected leader;
    /// - the migration executor's cut-over when no metadata proposer is
    ///   wired, which names the transfer target;
    /// - a migration compensation that restores a hint it replaced;
    /// - routing tables built by tests.
    ///
    /// Every source that knows the leader's term calls
    /// [`Self::observe_leader`] instead.
    pub fn set_leader(&mut self, group_id: u64, leader: u64) -> bool {
        match self.group_members.get_mut(&group_id) {
            Some(info) if info.leader_term == 0 && info.leader != leader => {
                info.leader = leader;
                true
            }
            _ => false,
        }
    }

    /// Forget the leader of a Raft group, as when its leaseholder is
    /// suspected dead. The term stays, so an observation of the same leader
    /// at the same term does not restore it. A new election's higher term
    /// does.
    pub fn clear_leader(&mut self, group_id: u64) {
        if let Some(info) = self.group_members.get_mut(&group_id) {
            info.leader = 0;
        }
    }

    /// Whether [`Self::observe_leader`] would change the hint.
    pub fn leader_observation_is_new(&self, group_id: u64, leader: u64, term: u64) -> bool {
        self.group_members
            .get(&group_id)
            .is_some_and(|info| leader != 0 && term > info.leader_term)
    }

    /// Whether [`Self::confirm_leader`] would change the hint.
    pub fn leader_confirmation_is_new(&self, group_id: u64, leader: u64, term: u64) -> bool {
        self.group_members.get(&group_id).is_some_and(|info| {
            leader != 0
                && (term > info.leader_term || (term == info.leader_term && info.leader == 0))
        })
    }

    /// Record `leader` of `group_id` at `term` from a source that shows the
    /// leader serves that term now:
    /// - this node's Raft, while it leads or the leader's contact is fresh;
    /// - a node that answered as the leader, or named the leader it follows;
    /// - SWIM, when a node it suspected answers again.
    ///
    /// It applies above the hint's term, as [`Self::observe_leader`] does.
    /// It also fills a hint cleared at the same term: a clear is a suspicion,
    /// and this source shows the leader still serves. Returns whether the
    /// hint changed.
    pub fn confirm_leader(&mut self, group_id: u64, leader: u64, term: u64) -> bool {
        if !self.leader_confirmation_is_new(group_id, leader, term) {
            return false;
        }
        match self.group_members.get_mut(&group_id) {
            Some(info) => {
                info.leader = leader;
                info.leader_term = term;
                true
            }
            None => false,
        }
    }

    /// Record `leader` of `group_id` as known at `term`: observed by this
    /// node's Raft, or named by a leader redirect with the redirecting
    /// node's term. Applies only above the hint's term: Raft elects at most
    /// one leader per term, so a higher term is newer. Returns whether the
    /// hint changed.
    pub fn observe_leader(&mut self, group_id: u64, leader: u64, term: u64) -> bool {
        if !self.leader_observation_is_new(group_id, leader, term) {
            return false;
        }
        match self.group_members.get_mut(&group_id) {
            Some(info) => {
                info.leader = leader;
                info.leader_term = term;
                true
            }
            None => false,
        }
    }

    /// Atomically reassign a vShard to a different Raft group, at `epoch`,
    /// the metadata log index of the reassignment.
    pub fn reassign_vshard(&mut self, vshard_id: u32, new_group_id: u64, epoch: u64) {
        if (vshard_id as usize) < self.vshard_to_group.len() {
            self.vshard_to_group[vshard_id as usize] = new_group_id;
            let epochs = self.vshard_epochs.entry(vshard_id).or_default();
            let current = epochs.entry(new_group_id).or_insert(epoch);
            *current = (*current).max(epoch);
        }
    }

    /// The epoch at which `vshard_id` moved to `group_id`: `0` while the
    /// vShard stays in its initial group. An entry the group applies for the
    /// vShard is positioned in this epoch, however late it applies.
    pub fn vshard_epoch(&self, vshard_id: u32, group_id: u64) -> u64 {
        self.vshard_epochs
            .get(&vshard_id)
            .and_then(|epochs| epochs.get(&group_id))
            .copied()
            .unwrap_or(0)
    }

    /// All vShards assigned to a given group.
    pub fn vshards_for_group(&self, group_id: u64) -> Vec<u32> {
        self.vshard_to_group
            .iter()
            .enumerate()
            .filter(|(_, gid)| **gid == group_id)
            .map(|(i, _)| i as u32)
            .collect()
    }

    /// How many vShards each group owns, from one pass over the table. A
    /// group that owns none is absent.
    pub fn group_vshard_counts(&self) -> std::collections::BTreeMap<u64, usize> {
        let mut counts = std::collections::BTreeMap::new();
        for &group_id in &self.vshard_to_group {
            *counts.entry(group_id).or_insert(0) += 1;
        }
        counts
    }

    /// How many vShards `group_id` owns.
    pub fn group_vshard_count(&self, group_id: u64) -> usize {
        self.vshard_to_group
            .iter()
            .filter(|&&gid| gid == group_id)
            .count()
    }

    /// Number of Raft groups.
    pub fn num_groups(&self) -> usize {
        self.group_members.len()
    }

    /// All group IDs, in ascending order.
    pub fn group_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.group_members.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Update the voting members of a Raft group (for membership changes).
    pub fn set_group_members(&mut self, group_id: u64, members: Vec<u64>) {
        if let Some(info) = self.group_members.get_mut(&group_id) {
            info.members = members;
        }
    }

    /// Set the explicit placement (intended voter set) for a group.
    ///
    /// No-op if the group is not present; consistent with `set_group_members`.
    pub fn set_placement(&mut self, group_id: u64, placement: Vec<u64>) {
        if let Some(info) = self.group_members.get_mut(&group_id) {
            info.placement = Some(placement);
        }
    }

    /// The effective target voter set for a group: the explicit placement if
    /// set, else the current members. Never panics; empty for unknown groups.
    pub fn effective_placement(&self, group_id: u64) -> Vec<u64> {
        match self.group_members.get(&group_id) {
            Some(g) => g.placement.clone().unwrap_or_else(|| g.members.clone()),
            None => Vec::new(),
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

    /// Remove a learner from a group's learner list only. Voters (members) are
    /// not touched. Returns `true` if the group existed and the learner was
    /// actually present and removed; `false` for unknown groups or absent peers.
    pub fn remove_group_learner(&mut self, group_id: u64, peer: u64) -> bool {
        if let Some(info) = self.group_members.get_mut(&group_id) {
            let before = info.learners.len();
            info.learners.retain(|&id| id != peer);
            info.learners.len() != before
        } else {
            false
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
            vshard_epochs: HashMap::new(),
        }
    }
}

/// Compute the primary vShard for a collection.
///
/// Delegates to [`VShardId::from_collection`], so the cluster routing layer
/// and the types-layer hash cannot drift. The [`CollectionKey`] carries the
/// database id and the bare catalog name, so a qualified name can never reach
/// the hash.
pub fn vshard_for_collection(key: CollectionKey<'_>) -> u32 {
    VShardId::from_collection(key).as_u32()
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

/// Hash `key` using the algorithm recorded in the cluster's [`PlacementHashId`].
///
/// Callers load the `PlacementHashId` from `ClusterSettings` once at
/// startup and pass it through every shard-split / shuffle operation.
/// The underlying implementations live in
/// [`crate::catalog::placement_hash`]; this function is the routing-layer
/// entry point so callers do not need to import the catalog module directly.
pub fn partition_hash(placement_hash_id: crate::catalog::PlacementHashId, key: &str) -> u64 {
    crate::catalog::placement_hash(placement_hash_id, key.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_table_with_vshard_epochs_round_trips_through_msgpack() {
        let mut rt = RoutingTable::uniform(4, &[1, 2, 3], 3);
        rt.reassign_vshard(7, 3, 41);
        rt.reassign_vshard(7, 2, 58);
        rt.reassign_vshard(9, 4, 60);
        let bytes = zerompk::to_msgpack_vec(&rt).expect("encode routing table");
        let decoded: RoutingTable = zerompk::from_msgpack(&bytes).expect("decode routing table");
        assert_eq!(decoded.vshard_epoch(7, 3), 41);
        assert_eq!(decoded.vshard_epoch(7, 2), 58);
        assert_eq!(decoded.vshard_epoch(9, 4), 60);
        assert_eq!(decoded.vshard_epoch(1, 1), 0);
        assert_eq!(decoded.group_for_vshard(7).ok(), Some(2));
        assert_eq!(decoded.vshard_epochs, rt.vshard_epochs);
    }

    #[test]
    fn uniform_distribution() {
        // 16 data groups → groups 1..=16 for vShards, plus metadata group 0.
        // Total group_members entries = 17, but vShard groups = 16.
        let rt = RoutingTable::uniform(16, &[1, 2, 3], 3);
        // num_groups() returns group_members.len() = 17 (16 data + 1 metadata).
        assert_eq!(rt.num_groups(), 17);

        // Each data group (1..=16) should have ~64 vShards (1024/16).
        for gid in 1..=16u64 {
            let shards = rt.vshards_for_group(gid);
            assert_eq!(shards.len(), 64);
        }

        // Metadata group 0 has no vShards.
        assert_eq!(rt.vshards_for_group(0).len(), 0);
    }

    #[test]
    fn leader_lookup() {
        let rt = RoutingTable::uniform(4, &[10, 20, 30], 3);
        let leader = rt.leader_for_vshard(0).unwrap();
        // vshard 0 maps to data group 1, which has a valid leader.
        assert!(leader > 0);
    }

    #[test]
    fn reassign_vshard() {
        let mut rt = RoutingTable::uniform(4, &[1, 2, 3], 3);
        let old_group = rt.group_for_vshard(0).unwrap();
        // old_group is 1 (first data group); reassign to data group 2.
        let new_group = if old_group < 4 { old_group + 1 } else { 1 };
        rt.reassign_vshard(0, new_group, 17);
        assert_eq!(rt.group_for_vshard(0).unwrap(), new_group);
    }

    #[test]
    fn a_reassignment_raises_the_vshard_epoch_and_a_replay_keeps_it() {
        let mut rt = RoutingTable::uniform(4, &[1, 2, 3], 3);
        let initial = rt.group_for_vshard(0).unwrap();
        assert_eq!(rt.vshard_epoch(0, initial), 0);
        let moved = if initial < 4 { initial + 1 } else { 1 };
        rt.reassign_vshard(0, moved, 40);
        assert_eq!(rt.vshard_epoch(0, moved), 40);
        // The old group keeps its epoch, so its late entries stay below.
        assert_eq!(rt.vshard_epoch(0, initial), 0);
        // A metadata replay applies the same index again.
        rt.reassign_vshard(0, moved, 40);
        assert_eq!(rt.vshard_epoch(0, moved), 40);
    }

    #[test]
    fn set_leader() {
        let mut rt = RoutingTable::uniform(2, &[1, 2, 3], 3);
        // Data group 1 owns vshard 0.
        rt.set_leader(1, 99);
        assert_eq!(rt.leader_for_vshard(0).unwrap(), 99);
    }

    #[test]
    fn a_raft_observation_at_a_higher_term_wins_over_every_other_writer() {
        let mut rt = RoutingTable::uniform(2, &[1, 2, 3], 3);
        // A term-less hint is replaced by the first observation. `uniform`
        // seeds group 1's hint with node 1, so the term-less write names
        // another node to change it.
        assert!(rt.set_leader(1, 3));
        assert_eq!(rt.leader_at_term_for_vshard(0).unwrap(), (3, 0));
        assert!(rt.observe_leader(1, 2, 5));
        assert_eq!(rt.leader_for_vshard(0).unwrap(), 2);

        // SWIM clears the suspected leader. The same leader at the same term
        // is not restored, and an unknown leader is never recorded.
        rt.clear_leader(1);
        assert!(!rt.observe_leader(1, 2, 5));
        assert!(!rt.observe_leader(1, 0, 6));
        assert_eq!(rt.leader_for_vshard(0).unwrap(), 0);

        // The next election's leader is recorded, and an older term never
        // replaces it.
        assert!(rt.observe_leader(1, 3, 6));
        assert!(!rt.observe_leader(1, 2, 5));
        assert_eq!(rt.leader_for_vshard(0).unwrap(), 3);

        // A term-less hint never replaces the observed leader.
        assert!(!rt.set_leader(1, 1));
        assert_eq!(rt.leader_at_term_for_vshard(0).unwrap(), (3, 6));

        // Nor does it fill a cleared hint that holds a term.
        rt.clear_leader(1);
        assert!(!rt.set_leader(1, 1));
        assert_eq!(rt.leader_at_term_for_vshard(0).unwrap(), (0, 6));
    }

    #[test]
    fn a_confirmation_fills_a_hint_cleared_at_its_term_and_nothing_older() {
        let mut rt = RoutingTable::uniform(2, &[1, 2, 3], 3);
        assert!(rt.observe_leader(1, 2, 5));
        rt.clear_leader(1);

        // The leader still serves term 5: the confirmation fills the clear.
        assert!(!rt.leader_observation_is_new(1, 2, 5));
        assert!(rt.confirm_leader(1, 2, 5));
        assert_eq!(rt.leader_at_term_for_vshard(0).unwrap(), (2, 5));

        // It never replaces a live hint at the same term, or any older term.
        assert!(!rt.confirm_leader(1, 3, 5));
        assert!(!rt.confirm_leader(1, 3, 4));
        rt.clear_leader(1);
        assert!(!rt.confirm_leader(1, 3, 4));
        assert!(!rt.confirm_leader(1, 0, 5));

        // A newer term applies as an observation does.
        assert!(rt.confirm_leader(1, 3, 6));
        assert_eq!(rt.leader_at_term_for_vshard(0).unwrap(), (3, 6));
    }

    #[test]
    fn remove_group_member_strips_voter_and_clears_leader() {
        let mut rt = RoutingTable::uniform(2, &[1, 2, 3], 3);
        // Use data group 1 (vshard 0 owner).
        rt.set_leader(1, 2);
        assert!(rt.remove_group_member(1, 2));
        let info = rt.group_info(1).unwrap();
        assert!(!info.members.contains(&2));
        assert_eq!(info.leader, 0, "leader hint should be cleared");
    }

    #[test]
    fn remove_group_member_strips_learner_only() {
        let mut rt = RoutingTable::uniform(2, &[1, 2, 3], 3);
        rt.add_group_learner(1, 9);
        assert!(rt.remove_group_member(1, 9));
        let info = rt.group_info(1).unwrap();
        assert!(!info.learners.contains(&9));
    }

    #[test]
    fn remove_group_member_unknown_group_returns_false() {
        let mut rt = RoutingTable::uniform(1, &[1, 2], 2);
        assert!(!rt.remove_group_member(99, 1));
    }

    #[test]
    fn remove_group_learner_removes_from_learners_only() {
        let mut rt = RoutingTable::uniform(2, &[1, 2, 3], 3);
        rt.add_group_learner(1, 9);
        let members_before = rt.group_info(1).unwrap().members.clone();

        assert!(rt.remove_group_learner(1, 9));

        let info = rt.group_info(1).unwrap();
        assert!(!info.learners.contains(&9), "learner must be removed");
        assert_eq!(info.members, members_before, "voters must not be affected");
    }

    #[test]
    fn remove_group_learner_noop_for_voter_and_absent() {
        let mut rt = RoutingTable::uniform(2, &[1, 2, 3], 3);
        let members_before = rt.group_info(1).unwrap().members.clone();

        // Voter node — remove_group_learner must not touch it.
        assert!(!rt.remove_group_learner(1, members_before[0]));
        assert_eq!(rt.group_info(1).unwrap().members, members_before);

        // Absent learner — returns false without panic.
        assert!(!rt.remove_group_learner(1, 999));
    }

    #[test]
    fn remove_group_learner_unknown_group_returns_false() {
        let mut rt = RoutingTable::uniform(1, &[1, 2], 2);
        assert!(!rt.remove_group_learner(99, 1));
    }

    #[test]
    fn vshard_not_mapped() {
        let rt = RoutingTable::uniform(2, &[1, 2], 2);
        // All 1024 are mapped, so this shouldn't fail.
        assert!(rt.group_for_vshard(1023).is_ok());
    }

    #[test]
    fn partition_hash_fnv1a_vs_xxhash3_differ() {
        use crate::catalog::PlacementHashId;
        let key = "some-partition-key";
        let fnv = partition_hash(PlacementHashId::Fnv1a, key);
        let xx3 = partition_hash(PlacementHashId::XxHash3, key);
        assert_ne!(fnv, xx3, "FNV-1a and XxHash3 must produce distinct values");
    }

    #[test]
    fn vshard_for_collection_matches_types_layer() {
        // The cluster routing layer MUST agree with the types-layer hash
        // for every (database, collection) pair. Drift here silently
        // misroutes data across nodes — once this regresses, collections
        // in any non-DEFAULT database resolve to the wrong vShard on the
        // gateway while the data plane still keys them by the correct
        // hash. This test pins the contract.
        for db_raw in [0u64, 1, 2, 1024, 999_999] {
            let db = nodedb_types::id::DatabaseId::new(db_raw);
            for name in ["users", "orders", "events", "a", "this_is_a_long_name"] {
                let key = CollectionKey::from_bare(db, name);
                assert_eq!(
                    vshard_for_collection(key),
                    VShardId::from_collection(key).as_u32(),
                    "drift detected: db={db_raw} collection={name}"
                );
            }
        }
    }

    #[test]
    fn vshard_for_collection_diverges_across_databases() {
        // Same collection name in different databases must route to
        // different vShards (probabilistic; "users" is a canonical
        // example whose hashes are known to differ across DEFAULT and
        // DatabaseId(1024)).
        use nodedb_types::id::DatabaseId;
        let v_default =
            vshard_for_collection(CollectionKey::from_bare(DatabaseId::DEFAULT, "users"));
        let v_other =
            vshard_for_collection(CollectionKey::from_bare(DatabaseId::new(1024), "users"));
        assert_ne!(
            v_default, v_other,
            "same collection name across databases must route independently"
        );
    }

    #[test]
    fn set_placement_and_effective_placement() {
        let mut rt = RoutingTable::uniform(2, &[1, 2, 3], 3);

        // Before any explicit placement: effective_placement returns members.
        let members = rt.group_info(1).unwrap().members.clone();
        assert_eq!(rt.effective_placement(1), members);
        assert!(rt.group_info(1).unwrap().placement.is_none());

        // After set_placement: effective_placement returns the explicit set.
        rt.set_placement(1, vec![10, 20]);
        assert_eq!(rt.effective_placement(1), vec![10, 20]);
        assert_eq!(rt.group_info(1).unwrap().placement, Some(vec![10, 20]));

        // Unknown group: effective_placement returns empty, no panic.
        assert_eq!(rt.effective_placement(999), Vec::<u64>::new());

        // set_placement on unknown group is a no-op (consistent with set_group_members).
        rt.set_placement(999, vec![1]);
        assert_eq!(rt.effective_placement(999), Vec::<u64>::new());
    }

    #[test]
    fn partition_hash_deterministic() {
        use crate::catalog::PlacementHashId;
        let key = "some-partition-key";
        assert_eq!(
            partition_hash(PlacementHashId::Fnv1a, key),
            partition_hash(PlacementHashId::Fnv1a, key)
        );
        assert_eq!(
            partition_hash(PlacementHashId::XxHash3, key),
            partition_hash(PlacementHashId::XxHash3, key)
        );
    }
}
