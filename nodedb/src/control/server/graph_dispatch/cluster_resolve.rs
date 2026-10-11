// SPDX-License-Identifier: BUSL-1.1

//! Shared cluster-dispatch helpers for graph scatter paths (`match_scatter` and
//! `bsp_pagerank`): dispatch a superstep to one owner node, and fetch the
//! gateway `Arc<SharedState>` used for remote dispatch.
//!
//! vShard resolution against live Raft leadership lives in
//! `crate::control::gateway::live_leaders`.

use std::sync::Arc;

use crate::bridge::envelope::{Payload, PhysicalPlan};
use crate::control::gateway::dispatcher::{DispatchRouteParams, dispatch_route};
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::gateway::{RouteDecision, TaskRoute};
use crate::control::server::exchange::execute_plan_all_local_cores;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

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
    /// The superstep reads linearizably: each serving node confirms first.
    pub(in crate::control::server::graph_dispatch) linearizable: bool,
}

/// One owner node's answer to a graph superstep: its payload and the
/// versions its cores reported.
pub(in crate::control::server::graph_dispatch) struct NodeRead {
    pub(in crate::control::server::graph_dispatch) payload: Payload,
    pub(in crate::control::server::graph_dispatch) read_versions: crate::types::ReadVersions,
}

/// Dispatch a single already-built graph-superstep `plan` to one owner node and
/// return its node-level payload and reported versions. The LOCAL node fans the plan across all its
/// Data-Plane cores via `execute_plan_all_local_cores` (per-core results merged
/// into one payload); a REMOTE node gets one `RouteDecision::Remote` dispatch via
/// `dispatch_route`. An empty payload denotes a zero-vertex shard — the caller's
/// decoder maps it to its result type's `::default()`. Shared by the PageRank and
/// WCC per-node scatter paths.
pub(in crate::control::server::graph_dispatch) async fn dispatch_superstep_to_node(
    shared_arc: &Arc<SharedState>,
    args: DispatchSuperstepParams<'_>,
) -> crate::Result<NodeRead> {
    let DispatchSuperstepParams {
        tenant_id,
        database_id,
        deadline_ms,
        node_id,
        is_local,
        route_vshard,
        plan,
        version_set,
        linearizable,
    } = args;
    if is_local {
        // Local node: fan across ALL local cores and merge. The per-core
        // owned-node sets are disjoint, so the merged result is correct without
        // dedup. At 1 core/node this is behaviour-identical to a single-core
        // dispatch. A linearizable graph read confirms the groups the plan
        // reads first.
        if linearizable {
            super::read_groups::confirm_graph_read(shared_arc, database_id, &plan).await?;
        }
        let node_result = execute_plan_all_local_cores(
            shared_arc.as_ref(),
            tenant_id,
            database_id,
            plan,
            TraceId::ZERO,
            // This resolve path carries no session-transaction context.
            None,
        )
        .await?;
        Ok(NodeRead {
            payload: Payload::from_vec(node_result.payload),
            read_versions: node_result.read_versions,
        })
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
        let outcome = dispatch_route(DispatchRouteParams {
            route,
            shared: shared_arc,
            tenant_id,
            database_id,
            trace_id: TraceId::ZERO,
            deadline_ms,
            version_set,
            // This resolve path carries no session-transaction context.
            txn_id: None,
            linearizable,
        })
        .await?;
        let read_versions = outcome.read_versions;
        let payload = outcome
            .payloads
            .into_iter()
            .next()
            .map(Payload::from_vec)
            .ok_or_else(|| crate::Error::Internal {
                detail: format!("graph superstep: node={node_id} returned no payload"),
            })?;
        Ok(NodeRead {
            payload,
            read_versions,
        })
    }
}

/// The gateway's `Arc<SharedState>` for the remote dispatch path. In cluster
/// mode the gateway is always wired; failing loudly here beats silently
/// degrading to a local-only (partial) scatter.
///
/// `pub(crate)` so the in-transaction staging choke points
/// (`session::leader_forward`) can obtain the `Arc<SharedState>` the remote
/// dispatch primitive requires when forwarding a staged write / overlay drop to
/// a remote leader.
pub(crate) fn gateway_shared(state: &SharedState) -> crate::Result<Arc<SharedState>> {
    // Upgrade the gateway's `Weak<SharedState>` back-reference. Always
    // succeeds while the node runs; a `None` (racing full teardown) surfaces
    // as the accessor's own typed shutdown error.
    state.self_arc()
}
