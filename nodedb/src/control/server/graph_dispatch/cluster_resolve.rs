// SPDX-License-Identifier: BUSL-1.1

//! Shared cluster-routing helpers for graph scatter paths (`match_scatter` and
//! `bsp_pagerank`): resolve a vShard to a live `RouteDecision`, and fetch the
//! gateway `Arc<SharedState>` used for remote dispatch.
//!
//! Both helpers resolve against LIVE Raft leadership where available so a stale
//! routing-table hint cannot misdirect a scatter. Factored here so the MATCH
//! scatter and the BSP PageRank coordinator share one implementation instead of
//! duplicating the routing-lock + live-leader plumbing.

use std::sync::Arc;

use crate::bridge::envelope::{Payload, PhysicalPlan};
use crate::control::gateway::dispatcher::{DispatchRouteParams, dispatch_route};
use crate::control::gateway::router::resolve_decision;
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::gateway::{RouteDecision, TaskRoute};
use crate::control::server::exchange::execute_plan_all_local_cores;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

/// Resolve a vShard to a `RouteDecision` against live Raft leadership, falling
/// back to the routing-table hint when no live snapshot is available.
pub(super) fn resolve_for_vshard(state: &SharedState, vshard_id: u32) -> RouteDecision {
    let routing_guard = state
        .cluster_routing
        .as_ref()
        .map(|rw| rw.read().unwrap_or_else(|p| p.into_inner()));
    let raft_snapshot: Vec<nodedb_cluster::GroupStatus> =
        state.raft_status_fn.get().map(|f| f()).unwrap_or_default();
    let live_leader = move |group_id: u64| -> u64 {
        raft_snapshot
            .iter()
            .find(|gs| gs.group_id == group_id)
            .map(|gs| gs.leader_id)
            .unwrap_or(0)
    };
    let live_lookup: Option<&dyn Fn(u64) -> u64> = if state.raft_status_fn.get().is_some() {
        Some(&live_leader)
    } else {
        None
    };
    resolve_decision(
        vshard_id,
        state.node_id,
        routing_guard.as_deref(),
        live_lookup,
    )
}

/// Parameters for [`dispatch_superstep_to_node`].
pub(in crate::control::server::graph_dispatch) struct DispatchSuperstepParams<'a> {
    pub(in crate::control::server::graph_dispatch) tenant_id: TenantId,
    pub(in crate::control::server::graph_dispatch) database_id: DatabaseId,
    pub(in crate::control::server::graph_dispatch) deadline_ms: u64,
    pub(in crate::control::server::graph_dispatch) node_id: u64,
    pub(in crate::control::server::graph_dispatch) is_local: bool,
    pub(in crate::control::server::graph_dispatch) route_vshard: u32,
    pub(in crate::control::server::graph_dispatch) plan: PhysicalPlan,
    pub(in crate::control::server::graph_dispatch) version_set: &'a GatewayVersionSet,
}

/// Dispatch a single already-built graph-superstep `plan` to one owner node and
/// return its node-level payload. The LOCAL node fans the plan across all its
/// Data-Plane cores via `execute_plan_all_local_cores` (per-core results merged
/// into one payload); a REMOTE node gets one `RouteDecision::Remote` dispatch via
/// `dispatch_route`. An empty payload denotes a zero-vertex shard — the caller's
/// decoder maps it to its result type's `::default()`. Shared by the PageRank and
/// WCC per-node scatter paths.
pub(in crate::control::server::graph_dispatch) async fn dispatch_superstep_to_node(
    shared_arc: &Arc<SharedState>,
    args: DispatchSuperstepParams<'_>,
) -> crate::Result<Payload> {
    let DispatchSuperstepParams {
        tenant_id,
        database_id,
        deadline_ms,
        node_id,
        is_local,
        route_vshard,
        plan,
        version_set,
    } = args;
    if is_local {
        // Local node: fan across ALL local cores and merge. The per-core
        // owned-node sets are disjoint, so the merged result is correct without
        // dedup. At 1 core/node this is behaviour-identical to a single-core
        // dispatch.
        let node_result = execute_plan_all_local_cores(
            shared_arc.as_ref(),
            tenant_id,
            database_id,
            plan,
            TraceId::ZERO,
            // Cross-node MATCH resolve: the transaction's staged overlay lives
            // on the origin node and is unreachable here, so run committed-only.
            // Cross-node MATCH read-your-own-writes is a separate unit.
            None,
        )
        .await?;
        Ok(Payload::from_vec(node_result.payload))
    } else {
        // Remote node: one dispatch via the gateway.
        let route = TaskRoute {
            plan,
            decision: RouteDecision::Remote {
                node_id,
                vshard_id: route_vshard as u64,
            },
            vshard_id: route_vshard,
        };
        let payloads = dispatch_route(DispatchRouteParams {
            route,
            shared: shared_arc,
            tenant_id,
            database_id,
            trace_id: TraceId::ZERO,
            // Cross-node graph superstep read; no owning interactive txn and a
            // remote route cannot carry txn_id regardless.
            txn_id: None,
            deadline_ms,
            version_set,
        })
        .await?;
        payloads
            .into_iter()
            .next()
            .map(Payload::from_vec)
            .ok_or_else(|| crate::Error::Internal {
                detail: format!("graph superstep: node={node_id} returned no payload"),
            })
    }
}

/// The gateway's `Arc<SharedState>` for the remote dispatch path. In cluster
/// mode the gateway is always wired; failing loudly here beats silently
/// degrading to a local-only (partial) scatter.
pub(super) fn gateway_shared(state: &SharedState) -> crate::Result<&Arc<SharedState>> {
    state
        .gateway
        .as_ref()
        .map(|g| &g.shared)
        .ok_or_else(|| crate::Error::Internal {
            detail: "graph scatter: cluster routing present but gateway unavailable for \
                     remote dispatch"
                .into(),
        })
}
