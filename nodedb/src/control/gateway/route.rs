// SPDX-License-Identifier: BUSL-1.1

//! Route decision types for the Gateway.
//!
//! [`TaskRoute`] pairs a sub-plan with where it should be executed.
//! [`RouteDecision`] encodes whether the plan runs on the local node,
//! on a single remote node, or broadcasts to every vShard in a list.

use nodedb_physical::physical_plan::PhysicalPlan;

/// A routing decision for a single physical sub-plan.
#[derive(Debug, Clone)]
pub struct TaskRoute {
    /// The sub-plan to execute.
    pub plan: PhysicalPlan,
    /// Where to execute it.
    pub decision: RouteDecision,
    /// vShard ID that owns this task.
    pub vshard_id: u32,
}

/// Where a task should be executed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    /// Execute on the local node (this node is the leaseholder).
    Local,
    /// Forward via `ExecuteRequest` RPC to a remote node.
    Remote {
        /// Remote node to forward to.
        node_id: u64,
        /// vShard to which this task belongs.
        vshard_id: u64,
    },
    /// Fan-out scan: send to every vShard in the list.
    ///
    /// Used for broadcast scans (SCAN, aggregates, graph traversals)
    /// where data is distributed across all shards.
    Broadcast { vshards: Vec<u64> },
    /// Cluster mode but no leader currently known for this vShard's Raft
    /// group (routing table entry is `0`). Surfaced as `Error::NotLeader`
    /// at dispatch so the gateway retry loop sleeps and re-resolves —
    /// never silently served from a (possibly stale) local replica.
    LeaderUnknown {
        /// vShard whose leader is currently unknown.
        vshard_id: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

    #[test]
    fn route_decision_equality() {
        assert_eq!(RouteDecision::Local, RouteDecision::Local);
        assert_ne!(
            RouteDecision::Remote {
                node_id: 1,
                vshard_id: 0
            },
            RouteDecision::Local
        );
    }

    #[test]
    fn task_route_holds_plan() {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "test",
            ),
            key: b"k".to_vec(),
            rls_filters: vec![],
            surrogate_ceiling: None,
            system_time: nodedb_types::SystemTimeScope::Current,
        });
        let route = TaskRoute {
            plan: plan.clone(),
            decision: RouteDecision::Local,
            vshard_id: 0,
        };
        assert_eq!(route.decision, RouteDecision::Local);
        assert_eq!(route.plan, plan);
    }
}
