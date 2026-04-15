//! Physical plan → `Vec<TaskRoute>` routing.
//!
//! The router consults the local [`RoutingTable`] to decide whether each
//! task runs locally or must be forwarded to a remote node.
//!
//! # Routing rules
//!
//! 1. Compute the vShard for the plan's primary collection via
//!    [`vshard_for_collection`].
//! 2. Look up the Raft group leader for that vShard in the routing table.
//! 3. If the leader is this node (`local_node_id`) → `RouteDecision::Local`.
//! 4. If the leader is another node → `RouteDecision::Remote`.
//! 5. For broadcast-scan plans ([`PhysicalPlan::is_broadcast_scan`]) →
//!    `RouteDecision::Broadcast` listing every vShard in the routing table.
//!
//! In single-node mode (routing table = `None`), all plans route locally.

use nodedb_cluster::routing::{RoutingTable, vshard_for_collection};

use crate::bridge::physical_plan::PhysicalPlan;

use super::route::{RouteDecision, TaskRoute};
use super::version_set::touched_collections;

/// Compute routing decisions for a single `PhysicalPlan`.
///
/// Returns a `Vec<TaskRoute>` — usually one element; multiple elements only
/// for broadcast scans (one route per vShard).
pub fn route_plan(
    plan: PhysicalPlan,
    local_node_id: u64,
    routing: Option<&RoutingTable>,
) -> Vec<TaskRoute> {
    // In single-node mode every plan runs locally.
    let Some(routing) = routing else {
        let vshard_id = primary_vshard(&plan);
        return vec![TaskRoute {
            plan,
            decision: RouteDecision::Local,
            vshard_id,
        }];
    };

    if plan.is_broadcast_scan() {
        return route_broadcast(plan, local_node_id, routing);
    }

    let vshard_id = primary_vshard(&plan);
    let decision = match routing.leader_for_vshard(vshard_id) {
        Ok(leader) if leader == local_node_id || leader == 0 => RouteDecision::Local,
        Ok(leader) => RouteDecision::Remote {
            node_id: leader,
            vshard_id: vshard_id as u64,
        },
        Err(_) => RouteDecision::Local,
    };

    vec![TaskRoute {
        plan,
        decision,
        vshard_id,
    }]
}

/// Build one route per vShard for broadcast-scan plans.
///
/// Returns a mix of `Local` (this node's vShards) and `Remote` routes.
fn route_broadcast(
    plan: PhysicalPlan,
    local_node_id: u64,
    routing: &RoutingTable,
) -> Vec<TaskRoute> {
    use nodedb_cluster::routing::VSHARD_COUNT;

    let mut routes = Vec::with_capacity(VSHARD_COUNT as usize);
    for vshard_id in 0u16..VSHARD_COUNT {
        let decision = match routing.leader_for_vshard(vshard_id) {
            Ok(leader) if leader == local_node_id || leader == 0 => RouteDecision::Local,
            Ok(leader) => RouteDecision::Remote {
                node_id: leader,
                vshard_id: vshard_id as u64,
            },
            Err(_) => RouteDecision::Local,
        };
        routes.push(TaskRoute {
            plan: plan.clone(),
            decision,
            vshard_id,
        });
    }
    routes
}

/// Determine the primary vShard for a plan by hashing the first collection name.
///
/// Falls back to vShard 0 for plans that have no named collection (Meta ops).
fn primary_vshard(plan: &PhysicalPlan) -> u16 {
    touched_collections(plan)
        .into_iter()
        .next()
        .map(|name| vshard_for_collection(&name))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::physical_plan::{DocumentOp, KvOp, PhysicalPlan};

    fn single_node_table() -> RoutingTable {
        RoutingTable::uniform(1, &[1], 1)
    }

    fn two_node_table() -> RoutingTable {
        // Group 0 → leader=1, Group 1 → leader=2.
        // vShards distributed 50/50 across groups.
        RoutingTable::uniform(2, &[1, 2], 1)
    }

    #[test]
    fn single_node_routes_locally() {
        let table = single_node_table();
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: "users".into(),
            key: vec![],
            rls_filters: vec![],
        });
        let routes = route_plan(plan, 1, Some(&table));
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].decision, RouteDecision::Local);
    }

    #[test]
    fn no_routing_table_routes_locally() {
        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: "x".into(),
            key: vec![],
            value: vec![],
            ttl_ms: 0,
        });
        let routes = route_plan(plan, 99, None);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].decision, RouteDecision::Local);
    }

    #[test]
    fn remote_route_when_different_leader() {
        let mut table = two_node_table();
        // Force vShard 0 leader to node 2; we are node 1.
        let group = table.group_for_vshard(0).unwrap();
        table.set_leader(group, 2);

        // Use a collection that hashes to vShard 0.
        // Find one by brute force.
        let collection = find_collection_for_vshard(0);
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection,
            key: vec![],
            rls_filters: vec![],
        });
        let routes = route_plan(plan, 1, Some(&table));
        assert_eq!(routes.len(), 1);
        match &routes[0].decision {
            RouteDecision::Remote { node_id, .. } => assert_eq!(*node_id, 2),
            other => panic!("expected Remote, got {other:?}"),
        }
    }

    #[test]
    fn broadcast_scan_produces_multiple_routes() {
        let table = two_node_table();
        let plan = PhysicalPlan::Document(DocumentOp::Scan {
            collection: "events".into(),
            limit: 100,
            offset: 0,
            sort_keys: vec![],
            filters: vec![],
            distinct: false,
            projection: vec![],
            computed_columns: vec![],
            window_functions: vec![],
        });
        let routes = route_plan(plan, 1, Some(&table));
        // Broadcast should produce VSHARD_COUNT routes.
        assert_eq!(routes.len(), nodedb_cluster::routing::VSHARD_COUNT as usize);
    }

    /// Find a collection name that hashes to the given vShard.
    fn find_collection_for_vshard(target: u16) -> String {
        for i in 0u64.. {
            let name = format!("col_{i}");
            if vshard_for_collection(&name) == target {
                return name;
            }
        }
        unreachable!()
    }
}
