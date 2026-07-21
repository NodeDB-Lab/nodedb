// SPDX-License-Identifier: BUSL-1.1

//! Physical plan → `Vec<TaskRoute>` routing.
//!
//! The router consults the local [`RoutingTable`] to decide whether each
//! task runs locally or must be forwarded to a remote node.
//!
//! # Routing rules
//!
//! 1. Consult the `strategy_fn` closure (backed by the catalog) for the plan's
//!    primary collection to determine its [`PartitionStrategy`]:
//!    - `CollectionHomed` → one vShard derived from [`vshard_for_collection`].
//!    - `KeyPartitioned` → one vShard per distinct key via [`VShardId::from_key`]
//!      (deduplicated; multiple keys mapping to the same vShard share one route).
//! 2. Look up the Raft group leader for each vShard in the routing table.
//! 3. If the leader is this node (`local_node_id`) → `RouteDecision::Local`.
//! 4. If the leader is another node → `RouteDecision::Remote`.
//! 5. For plans wrapped in `QueryOp::Exchange{Gather{..}}` the Exchange wrapper
//!    is stripped and the child is routed by its data distribution: a genuinely
//!    cluster-partitioned child (graph traversal by node-id, array by tile) is
//!    broadcast to every vShard; a single-vShard-homed child (document / kv /
//!    columnar / timeseries / spatial / vector / text, and joins/aggregates over
//!    them) is routed to its ONE owning vShard — broadcasting it would duplicate
//!    rows, since the data-plane scan is not vshard-scoped.
//!
//! In single-node mode (routing table = `None`), all plans route locally.

use nodedb_cluster::routing::{RoutingTable, vshard_for_collection};
use nodedb_types::PartitionStrategy;
use nodedb_types::id::{DatabaseId, VShardId};

use nodedb_physical::physical_plan::PhysicalPlan;

use crate::Result;

use super::key_extractor::KeyExtractor;
use super::route::{RouteDecision, TaskRoute};
use super::version_set::touched_collections;

/// Compute routing decisions for a single `PhysicalPlan`.
///
/// Returns a `Vec<TaskRoute>` — usually one element; multiple elements only
/// for broadcast scans (one route per vShard) or key-partitioned collections
/// (one route per distinct key vShard).
///
/// `database_id` scopes the routing hash so that the same collection name in
/// two different databases resolves to independent vShards.
///
/// `strategy_fn` is called with the primary collection name and returns the
/// [`PartitionStrategy`] for that collection. The caller builds this closure
/// from the catalog; the router stays catalog-agnostic.
///
/// `extractor` is invoked only for `KeyPartitioned` collections. No collection
/// carries that strategy yet, so [`UnwiredKeyExtractor`] is the correct
/// sentinel and this path is unreachable in practice.
///
/// [`UnwiredKeyExtractor`]: super::key_extractor::UnwiredKeyExtractor
pub fn route_plan(
    plan: PhysicalPlan,
    local_node_id: u64,
    routing: Option<&RoutingTable>,
    database_id: DatabaseId,
    strategy_fn: impl Fn(&str) -> PartitionStrategy,
    extractor: &dyn KeyExtractor,
) -> Result<Vec<TaskRoute>> {
    // Commit-time meta-ops (ResolveTxn / TransactionBatch) carry no collection
    // name, so their vShard cannot be derived here — the primary_vshard
    // fallback would silently send them to vShard 0 and durably apply the
    // commit batch on the wrong core (#193). They are dispatched with the
    // task's pre-classified `vshard_id` (see `dispatch_single_shard`), never
    // through the gateway.
    {
        use nodedb_physical::physical_plan::MetaOp;
        if matches!(
            &plan,
            PhysicalPlan::Meta(MetaOp::ResolveTxn { .. } | MetaOp::TransactionBatch { .. })
        ) {
            return Err(crate::Error::Internal {
                detail: "commit meta-op cannot be routed by the gateway; \
                         dispatch it with the task's explicit vshard_id"
                    .to_owned(),
            });
        }
    }

    // In single-node mode every plan runs locally.
    let Some(routing) = routing else {
        let vshard_id = primary_vshard(&plan, database_id);
        return Ok(vec![TaskRoute {
            plan,
            decision: RouteDecision::Local,
            vshard_id,
        }]);
    };

    // A sharded read/aggregate reaches the router wrapped in `Exchange{Gather}`.
    // The coordinator strips the Exchange here: its child is the plan that runs
    // on each vShard, and the per-vShard payloads are fused on return (see
    // `fuse_payloads` in the gateway core). Shipping the Exchange wrapper itself
    // would let it reach a Data-Plane core, which rejects unresolved Exchange
    // nodes ("Exchange must be resolved by the coordinator before dispatch").
    use nodedb_physical::physical_plan::{
        ExchangeMode, ExchangeOp, QueryOp, plan_contains_cluster_partitioned_leaf,
    };
    match plan {
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Gather { .. },
        })) => {
            // Strip the Exchange wrapper and route the child. How depends on the
            // child's data distribution — mirroring `gather_all_vshards`:
            //
            // - A genuinely cluster-partitioned source (graph traversal by
            //   node-id, array by tile) has rows spread across ALL vShards →
            //   broadcast the child to every vShard and fuse the per-vShard
            //   payloads.
            // - A single-vShard-homed source (document/kv/columnar/timeseries/
            //   spatial/vector/text, and joins/aggregates over them) lives on
            //   exactly ONE vShard. Broadcasting it to all 1024 vShards would
            //   return the full result from the owning node once per route that
            //   lands there (the data-plane scan is NOT vshard-scoped) → N-fold
            //   duplication. Route it to its single owning vShard instead. Any
            //   nested build-side data movement is resolved at the dispatch site
            //   (see `dispatch_remote` / `dispatch_to_data_plane`).
            if plan_contains_cluster_partitioned_leaf(&child) {
                Ok(route_broadcast(*child, local_node_id, routing))
            } else {
                Ok(route_single_collection(
                    *child,
                    local_node_id,
                    routing,
                    database_id,
                    &strategy_fn,
                    extractor,
                )?)
            }
        }
        other => Ok(route_single_collection(
            other,
            local_node_id,
            routing,
            database_id,
            &strategy_fn,
            extractor,
        )?),
    }
}

/// Route a plan that touches exactly one primary collection (or none).
///
/// Consults [`PartitionStrategy`] for the primary collection name and produces:
/// - `CollectionHomed` → one route to the collection's owning vShard
///   (byte-identical to the original collection-homed path).
/// - `KeyPartitioned { key }` → one route per distinct vShard derived from the
///   plan's shard keys via [`VShardId::from_key`].
fn route_single_collection(
    plan: PhysicalPlan,
    local_node_id: u64,
    routing: &RoutingTable,
    database_id: DatabaseId,
    strategy_fn: &impl Fn(&str) -> PartitionStrategy,
    extractor: &dyn KeyExtractor,
) -> Result<Vec<TaskRoute>> {
    let primary_name: Option<String> = touched_collections(&plan).into_iter().next();

    let strategy = primary_name.as_deref().map(strategy_fn).unwrap_or_default(); // CollectionHomed for plans with no named collection

    match strategy {
        PartitionStrategy::CollectionHomed => {
            // Byte-identical to the original primary_vshard / resolve_decision path.
            let vshard_id = primary_name
                .as_deref()
                .map(|name| vshard_for_collection(database_id, name))
                .unwrap_or(0);
            let decision = resolve_decision(vshard_id, local_node_id, Some(routing), None);
            Ok(vec![TaskRoute {
                plan,
                decision,
                vshard_id,
            }])
        }
        PartitionStrategy::KeyPartitioned { key: key_spec } => {
            let raw_keys = extractor.extract_keys(&plan, &key_spec)?;
            // Deduplicate vShards: two keys that hash to the same vShard share
            // a single cloned route rather than fanning out unnecessarily.
            let mut seen = std::collections::HashSet::new();
            let mut routes = Vec::new();
            for raw_key in raw_keys {
                let vshard_id = VShardId::from_key(&raw_key).as_u32();
                if seen.insert(vshard_id) {
                    let decision = resolve_decision(vshard_id, local_node_id, Some(routing), None);
                    routes.push(TaskRoute {
                        plan: plan.clone(),
                        decision,
                        vshard_id,
                    });
                }
            }
            Ok(routes)
        }
    }
}

/// Resolve the `RouteDecision` for a single vShard.
///
/// The routing table is a *cached hint*. The authoritative source of
/// truth is the live Raft group status. When `live_leader_for_group` is
/// provided, it overrides the routing table's leader hint for the
/// vShard's group — the routing table can be stale (especially with
/// "leader is me" pointing at a former leader), while live Raft state
/// always reflects the current term's actual leader on this node's view.
///
/// Decision rules (cluster mode):
/// 1. If live Raft says this node is leader for the group → `Local`.
/// 2. If live Raft names a *different* leader → `Remote { that node }`.
/// 3. If neither live Raft nor the routing table know a leader →
///    `LeaderUnknown` (surfaced as `Error::NotLeader` by dispatch so the
///    gateway retry loop sleeps and re-resolves).
///
/// Single-node mode (`routing == None`) always routes locally.
pub fn resolve_decision(
    vshard_id: u32,
    local_node_id: u64,
    routing: Option<&RoutingTable>,
    live_leader_for_group: Option<&dyn Fn(u64) -> u64>,
) -> RouteDecision {
    let Some(routing) = routing else {
        return RouteDecision::Local;
    };
    let unknown = RouteDecision::LeaderUnknown {
        vshard_id: vshard_id as u64,
    };

    // Prefer live Raft state over the routing-table hint when available.
    if let Some(live) = live_leader_for_group
        && let Ok(group_id) = routing.group_for_vshard(vshard_id)
    {
        let live_leader = live(group_id);
        if live_leader == local_node_id {
            return RouteDecision::Local;
        }
        if live_leader != 0 {
            return RouteDecision::Remote {
                node_id: live_leader,
                vshard_id: vshard_id as u64,
            };
        }
        // Live state has no leader for this group yet — fall through to
        // routing-table hint (it may have a stale-but-usable forwarding
        // target from the last term).
    }

    match routing.leader_for_vshard(vshard_id) {
        Ok(0) => unknown,
        Ok(leader) if leader == local_node_id => RouteDecision::Local,
        Ok(leader) => RouteDecision::Remote {
            node_id: leader,
            vshard_id: vshard_id as u64,
        },
        Err(_) => unknown,
    }
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
    for vshard_id in 0u32..VSHARD_COUNT {
        let decision = resolve_decision(vshard_id, local_node_id, Some(routing), None);
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
fn primary_vshard(plan: &PhysicalPlan, database_id: DatabaseId) -> u32 {
    touched_collections(plan)
        .into_iter()
        .next()
        .map(|name| vshard_for_collection(database_id, &name))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};

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
            surrogate_ceiling: None,
        });
        let routes = route_plan(
            plan,
            1,
            Some(&table),
            DatabaseId::DEFAULT,
            |_| PartitionStrategy::CollectionHomed,
            &crate::control::gateway::UnwiredKeyExtractor,
        )
        .expect("route");
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
            surrogate: nodedb_types::Surrogate::ZERO,
        });
        let routes = route_plan(
            plan,
            99,
            None,
            DatabaseId::DEFAULT,
            |_| PartitionStrategy::CollectionHomed,
            &crate::control::gateway::UnwiredKeyExtractor,
        )
        .expect("route");
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
            surrogate_ceiling: None,
        });
        let routes = route_plan(
            plan,
            1,
            Some(&table),
            DatabaseId::DEFAULT,
            |_| PartitionStrategy::CollectionHomed,
            &crate::control::gateway::UnwiredKeyExtractor,
        )
        .expect("route");
        assert_eq!(routes.len(), 1);
        match &routes[0].decision {
            RouteDecision::Remote { node_id, .. } => assert_eq!(*node_id, 2),
            other => panic!("expected Remote, got {other:?}"),
        }
    }

    #[test]
    fn single_homed_gather_routes_to_one_vshard() {
        let table = two_node_table();
        let scan = PhysicalPlan::Document(DocumentOp::Scan {
            collection: "events".into(),
            limit: 100,
            offset: 0,
            sort_keys: vec![],
            filters: vec![],
            distinct: false,
            projection: vec![],
            computed_columns: vec![],
            window_functions: vec![],
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        });
        // A single-vShard-homed read reaches the router wrapped in
        // Exchange{Gather} (the shape `convert()` produces). The router must
        // strip the Exchange and route the child to its ONE owning vShard — NOT
        // broadcast to every vShard. Broadcasting a single-homed source would
        // return the full collection from the owning node once per route that
        // lands there (the data-plane scan is not vshard-scoped) → N-fold
        // duplication.
        let plan = PhysicalPlan::Query(nodedb_physical::physical_plan::QueryOp::Exchange(
            nodedb_physical::physical_plan::ExchangeOp {
                child: Box::new(scan),
                mode: nodedb_physical::physical_plan::ExchangeMode::Gather {
                    as_aggregate: false,
                },
            },
        ));
        let routes = route_plan(
            plan,
            1,
            Some(&table),
            DatabaseId::DEFAULT,
            |_| PartitionStrategy::CollectionHomed,
            &crate::control::gateway::UnwiredKeyExtractor,
        )
        .expect("route");
        // Exactly ONE route — to the collection's owning vShard.
        assert_eq!(
            routes.len(),
            1,
            "single-homed Exchange{{Gather}} must route to one vShard, not broadcast"
        );
        // The route carries the UNWRAPPED child plan, not the Exchange wrapper
        // (a wrapper shipped to a Data-Plane core is rejected as unresolved).
        assert!(
            matches!(
                routes[0].plan,
                PhysicalPlan::Document(DocumentOp::Scan { .. })
            ),
            "route must carry the unwrapped scan child, got {:?}",
            routes[0].plan
        );
        // It routes to the same single vShard a bare scan of the same collection
        // would (the collection's owner).
        assert_eq!(
            routes[0].vshard_id,
            vshard_for_collection(DatabaseId::DEFAULT, "events")
        );
    }

    /// Find a collection name that hashes to the given vShard.
    fn find_collection_for_vshard(target: u32) -> String {
        for i in 0u64.. {
            let name = format!("col_{i}");
            if vshard_for_collection(DatabaseId::DEFAULT, &name) == target {
                return name;
            }
        }
        unreachable!()
    }

    /// Commit-time meta-ops carry no collection name, so the router cannot
    /// derive their vShard — silently falling back to vShard 0 durably applies
    /// the commit batch on the wrong core (#193). They must be rejected here;
    /// callers dispatch them with the task's pre-classified `vshard_id`.
    #[test]
    fn commit_meta_ops_are_rejected() {
        use nodedb_physical::physical_plan::MetaOp;

        for plan in [
            PhysicalPlan::Meta(MetaOp::TransactionBatch {
                plans: vec![],
                txn_id: None,
            }),
            PhysicalPlan::Meta(MetaOp::ResolveTxn {
                txn_id: nodedb_types::id::TxnId::new(7),
                plans: vec![],
            }),
        ] {
            for table in [None, Some(single_node_table())] {
                let result = route_plan(
                    plan.clone(),
                    1,
                    table.as_ref(),
                    DatabaseId::DEFAULT,
                    |_| PartitionStrategy::CollectionHomed,
                    &crate::control::gateway::UnwiredKeyExtractor,
                );
                assert!(
                    result.is_err(),
                    "commit meta-op must not be routable via the gateway: {plan:?}"
                );
            }
        }
    }
}
