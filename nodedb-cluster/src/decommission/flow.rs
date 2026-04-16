//! Decommission flow — emit the full ordered sequence of metadata
//! entries that move a node from `Active` to fully removed.
//!
//! [`plan_full_decommission`] is pure: given a snapshot of topology
//! and routing, it returns the exact list of
//! [`MetadataEntry`](crate::metadata_group::MetadataEntry) values the
//! coordinator will propose through the metadata Raft group, in the
//! order they must commit. The flow is deterministic — two nodes
//! looking at the same snapshot produce byte-identical plans, which
//! means a failed coordinator can be resumed from any consistent
//! snapshot without needing per-plan state to be replicated.

use crate::error::Result;
use crate::metadata_group::{MetadataEntry, RoutingChange, TopologyChange};
use crate::routing::RoutingTable;
use crate::topology::ClusterTopology;

use super::safety::check_can_decommission;

/// Output of [`plan_full_decommission`] — the caller proposes
/// `entries` in order, waiting for each to commit before moving on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecommissionPlan {
    pub node_id: u64,
    pub entries: Vec<MetadataEntry>,
}

/// Build the complete decommission plan for `node_id`.
///
/// Steps (in the order they appear in the returned `entries`):
///
/// 1. `TopologyChange::StartDecommission` — flip the target to
///    `Draining`.
/// 2. `RoutingChange::LeadershipTransfer` — for every group the
///    target currently leads, hand leadership to another voter.
/// 3. `RoutingChange::RemoveMember` — strip the target out of every
///    group's member (and learner) list.
/// 4. `TopologyChange::FinishDecommission` — flip the target to
///    `Decommissioned`.
/// 5. `TopologyChange::Leave` — remove the target from topology
///    entirely so future peer lookups return `NodeNotFound`.
///
/// The safety gate in [`check_can_decommission`] runs first and
/// returns an error without producing a plan if any group would drop
/// below the configured replication factor.
pub fn plan_full_decommission(
    node_id: u64,
    topology: &ClusterTopology,
    routing: &RoutingTable,
    replication_factor: usize,
) -> Result<DecommissionPlan> {
    check_can_decommission(node_id, topology, routing, replication_factor)?;

    let mut entries = Vec::new();
    entries.push(MetadataEntry::TopologyChange(
        TopologyChange::StartDecommission { node_id },
    ));

    // Collect a stable, sorted group_id ordering so the plan is
    // reproducible across HashMap iterations.
    let mut group_ids: Vec<u64> = routing
        .group_members()
        .iter()
        .filter(|(_, info)| info.members.contains(&node_id) || info.learners.contains(&node_id))
        .map(|(gid, _)| *gid)
        .collect();
    group_ids.sort_unstable();

    // 2. Leadership transfers for every group the target currently leads.
    for gid in &group_ids {
        let info = routing
            .group_info(*gid)
            .expect("group id came from routing snapshot");
        if info.leader != node_id {
            continue;
        }
        if let Some(&new_leader) = info.members.iter().find(|&&m| m != node_id) {
            entries.push(MetadataEntry::RoutingChange(
                RoutingChange::LeadershipTransfer {
                    group_id: *gid,
                    new_leader_node_id: new_leader,
                },
            ));
        }
    }

    // 3. Remove the target from every group's member and learner sets.
    for gid in &group_ids {
        entries.push(MetadataEntry::RoutingChange(RoutingChange::RemoveMember {
            group_id: *gid,
            node_id,
        }));
    }

    // 4. Finish decommission (topology state → Decommissioned).
    entries.push(MetadataEntry::TopologyChange(
        TopologyChange::FinishDecommission { node_id },
    ));

    // 5. Leave — remove from topology entirely.
    entries.push(MetadataEntry::TopologyChange(TopologyChange::Leave {
        node_id,
    }));

    Ok(DecommissionPlan { node_id, entries })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{NodeInfo, NodeState};
    use std::net::SocketAddr;

    fn topo(nodes: &[u64]) -> ClusterTopology {
        let mut t = ClusterTopology::new();
        for (i, id) in nodes.iter().enumerate() {
            let addr: SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            t.add_node(NodeInfo::new(*id, addr, NodeState::Active));
        }
        t
    }

    #[test]
    fn plan_shape_matches_spec() {
        let t = topo(&[1, 2, 3]);
        // 2 groups, RF=3 (each group has all 3 nodes). Decommission
        // 1 with RF=2 (the surviving quorum).
        let routing = RoutingTable::uniform(2, &[1, 2, 3], 3);
        let plan = plan_full_decommission(1, &t, &routing, 2).unwrap();
        assert_eq!(plan.node_id, 1);

        // First entry: StartDecommission.
        assert!(matches!(
            plan.entries.first(),
            Some(MetadataEntry::TopologyChange(
                TopologyChange::StartDecommission { node_id: 1 }
            ))
        ));

        // Last two entries: FinishDecommission, Leave.
        let n = plan.entries.len();
        assert!(matches!(
            plan.entries[n - 2],
            MetadataEntry::TopologyChange(TopologyChange::FinishDecommission { node_id: 1 })
        ));
        assert!(matches!(
            plan.entries[n - 1],
            MetadataEntry::TopologyChange(TopologyChange::Leave { node_id: 1 })
        ));

        // Every group the target is in must get a RemoveMember.
        let remove_count = plan
            .entries
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    MetadataEntry::RoutingChange(RoutingChange::RemoveMember { node_id: 1, .. })
                )
            })
            .count();
        assert_eq!(remove_count, 2);
    }

    #[test]
    fn plan_emits_leadership_transfer_when_target_leads() {
        let t = topo(&[1, 2, 3]);
        let mut routing = RoutingTable::uniform(2, &[1, 2, 3], 3);
        routing.set_leader(0, 1);
        routing.set_leader(1, 2);
        let plan = plan_full_decommission(1, &t, &routing, 2).unwrap();
        // Exactly one LeadershipTransfer for group 0.
        let transfers: Vec<_> = plan
            .entries
            .iter()
            .filter_map(|e| match e {
                MetadataEntry::RoutingChange(RoutingChange::LeadershipTransfer {
                    group_id,
                    new_leader_node_id,
                }) => Some((*group_id, *new_leader_node_id)),
                _ => None,
            })
            .collect();
        assert_eq!(transfers.len(), 1);
        assert_eq!(transfers[0].0, 0);
        assert_ne!(transfers[0].1, 1, "new leader must not be the target");
    }

    #[test]
    fn plan_is_deterministic() {
        let t = topo(&[1, 2, 3]);
        let routing = RoutingTable::uniform(4, &[1, 2, 3], 3);
        let p1 = plan_full_decommission(2, &t, &routing, 2).unwrap();
        let p2 = plan_full_decommission(2, &t, &routing, 2).unwrap();
        assert_eq!(p1.entries, p2.entries);
    }

    #[test]
    fn plan_rejected_when_safety_fails() {
        let t = topo(&[1, 2]);
        let routing = RoutingTable::uniform(2, &[1, 2], 2);
        let err = plan_full_decommission(1, &t, &routing, 2).unwrap_err();
        assert!(err.to_string().contains("replication factor"));
    }

    #[test]
    fn plan_skips_groups_target_is_not_in() {
        let t = topo(&[1, 2, 3]);
        let mut routing = RoutingTable::uniform(4, &[1, 2, 3], 3);
        routing.set_group_members(0, vec![2, 3]);
        routing.set_group_members(1, vec![2, 3]);
        routing.set_group_members(2, vec![1, 2, 3]);
        routing.set_group_members(3, vec![1, 2, 3]);
        let plan = plan_full_decommission(1, &t, &routing, 2).unwrap();
        let removes: Vec<u64> = plan
            .entries
            .iter()
            .filter_map(|e| match e {
                MetadataEntry::RoutingChange(RoutingChange::RemoveMember { group_id, .. }) => {
                    Some(*group_id)
                }
                _ => None,
            })
            .collect();
        assert_eq!(removes, vec![2, 3]);
    }
}
