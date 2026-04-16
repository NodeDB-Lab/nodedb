//! Decommission safety gate.
//!
//! Before the coordinator proposes a single metadata entry, it must
//! prove that removing the target node from every Raft group it
//! belongs to will leave each group with at least `replication_factor`
//! voting members. Dropping below RF silently is a data-loss bug —
//! this module is the only place that decision is made.

use crate::error::{ClusterError, Result};
use crate::routing::RoutingTable;
use crate::topology::{ClusterTopology, NodeState};

/// Why a decommission request was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecommissionSafetyError {
    /// The target node id does not exist in the topology.
    NodeNotFound { node_id: u64 },
    /// The node is already past the point of decommission.
    AlreadyDecommissioned { node_id: u64 },
    /// Removing the node would leave this group below `replication_factor`
    /// voters. The decommission must wait until a new voter has been
    /// added to the group (via rebalance / migration executor).
    WouldViolateReplicationFactor {
        node_id: u64,
        group_id: u64,
        current_voters: usize,
        replication_factor: usize,
    },
}

impl std::fmt::Display for DecommissionSafetyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeNotFound { node_id } => {
                write!(f, "node {node_id} not found in topology")
            }
            Self::AlreadyDecommissioned { node_id } => {
                write!(f, "node {node_id} is already decommissioned")
            }
            Self::WouldViolateReplicationFactor {
                node_id,
                group_id,
                current_voters,
                replication_factor,
            } => write!(
                f,
                "removing node {node_id} from group {group_id} \
                 would leave {} voter(s), below replication factor {replication_factor}",
                current_voters.saturating_sub(1)
            ),
        }
    }
}

impl std::error::Error for DecommissionSafetyError {}

impl From<DecommissionSafetyError> for ClusterError {
    fn from(value: DecommissionSafetyError) -> Self {
        ClusterError::Transport {
            detail: value.to_string(),
        }
    }
}

/// Verify that node `node_id` can be safely stripped out of every
/// group it participates in without dropping any group below
/// `replication_factor` voters.
///
/// This check is purely structural — it looks at the current routing
/// table, not the live cluster. Callers must re-run it immediately
/// before proposing each step if the topology may have shifted since
/// the plan was computed.
pub fn check_can_decommission(
    node_id: u64,
    topology: &ClusterTopology,
    routing: &RoutingTable,
    replication_factor: usize,
) -> Result<()> {
    let node = topology
        .get_node(node_id)
        .ok_or(DecommissionSafetyError::NodeNotFound { node_id })?;

    if node.state == NodeState::Decommissioned {
        return Err(DecommissionSafetyError::AlreadyDecommissioned { node_id }.into());
    }

    for (group_id, info) in routing.group_members() {
        if !info.members.contains(&node_id) {
            continue;
        }
        let current_voters = info.members.len();
        // After removal the group would have `current_voters - 1`
        // voters. Require that to be at least `replication_factor`.
        if current_voters.saturating_sub(1) < replication_factor {
            return Err(DecommissionSafetyError::WouldViolateReplicationFactor {
                node_id,
                group_id: *group_id,
                current_voters,
                replication_factor,
            }
            .into());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::NodeInfo;
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
    fn rejects_unknown_node() {
        let t = topo(&[1, 2, 3]);
        let r = RoutingTable::uniform(2, &[1, 2, 3], 3);
        let err = check_can_decommission(99, &t, &r, 2).unwrap_err();
        assert!(err.to_string().contains("99"));
    }

    #[test]
    fn rejects_already_decommissioned() {
        let mut t = topo(&[1, 2, 3]);
        t.set_state(1, NodeState::Decommissioned);
        let r = RoutingTable::uniform(2, &[1, 2, 3], 3);
        let err = check_can_decommission(1, &t, &r, 2).unwrap_err();
        assert!(err.to_string().contains("already decommissioned"));
    }

    #[test]
    fn rejects_when_rf_would_be_violated() {
        let t = topo(&[1, 2]);
        // RF=2 with only 2 nodes → every group has exactly 2 voters.
        // Removing either one would leave 1 voter (< RF=2).
        let r = RoutingTable::uniform(2, &[1, 2], 2);
        let err = check_can_decommission(1, &t, &r, 2).unwrap_err();
        assert!(err.to_string().contains("replication factor"));
    }

    #[test]
    fn accepts_when_extra_voter_available() {
        let t = topo(&[1, 2, 3]);
        // 3 nodes × RF=2 means each group has 2 voters but the third
        // node is a candidate replacement. The safety check doesn't
        // know about replacements — it only checks current state,
        // so we need RF=1 for this to pass without a prior rebalance.
        let r = RoutingTable::uniform(2, &[1, 2, 3], 3);
        check_can_decommission(1, &t, &r, 2).unwrap();
    }

    #[test]
    fn skips_groups_target_is_not_member_of() {
        let t = topo(&[1, 2, 3]);
        // Node 1 is only in group 0, node 2 is only in group 1.
        let mut r = RoutingTable::uniform(2, &[1, 2, 3], 3);
        r.set_group_members(0, vec![1, 3]);
        r.set_group_members(1, vec![2, 3]);
        // Decommission 1 with RF=1 → group 0 drops to [3], group 1
        // untouched.
        check_can_decommission(1, &t, &r, 1).unwrap();
    }
}
