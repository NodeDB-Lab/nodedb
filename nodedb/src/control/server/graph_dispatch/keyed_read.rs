// SPDX-License-Identifier: BUSL-1.1

//! A one-hop graph read of one node, served where that node's edges live.
//!
//! A node's edges, forward and reverse, live on its key vShard
//! (`types/record_home.rs`). The read runs on that vShard's leader: here when
//! this node leads it, through the gateway otherwise. The leader also holds a
//! transaction's staged edges (`shared/session/leader_forward.rs`).

use crate::bridge::envelope::{Payload, PhysicalPlan};
use crate::control::gateway::dispatcher::{DispatchRouteParams, dispatch_route};
use crate::control::gateway::live_leaders::resolve_live_decision;
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::gateway::{RouteDecision, TaskRoute};
use crate::control::server::payload_merge::merge_msgpack_arrays;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, TxnId, VShardId};

use super::cluster_resolve::gateway_shared;
use super::shard_reads::ShardReadLog;

/// Run `plan`, a one-hop read of `node_key`, on the leader of the node's key
/// vShard, and return its merged payload.
pub async fn read_on_key_owner(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    node_key: &str,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
    linearizable: bool,
) -> crate::Result<Payload> {
    let vshard_id = VShardId::from_key(node_key.as_bytes()).as_u32();
    read_on_vshard(
        state,
        VShardRead {
            tenant_id,
            database_id,
            vshard_id,
            txn_id,
            linearizable,
        },
        plan,
    )
    .await
}

/// Where a [`read_on_vshard`] runs, and as what.
#[derive(Debug, Clone, Copy)]
pub struct VShardRead {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: u32,
    pub txn_id: Option<TxnId>,
    pub linearizable: bool,
}

/// Run `plan`, a read of one vShard's state, on the leader of that vShard,
/// and return its merged payload.
pub async fn read_on_vshard(
    state: &SharedState,
    at: VShardRead,
    plan: PhysicalPlan,
) -> crate::Result<Payload> {
    let VShardRead {
        tenant_id,
        database_id,
        vshard_id,
        txn_id,
        linearizable,
    } = at;
    let collection = plan_collection(&plan);
    let mut reads = ShardReadLog::new();
    let payload = match resolve_live_decision(state, vshard_id) {
        RouteDecision::Local => {
            if linearizable {
                super::read_groups::confirm_graph_read(state, database_id, &plan).await?;
            }
            let response = crate::control::server::broadcast::broadcast_to_all_cores_txn(
                state,
                tenant_id,
                database_id,
                plan,
                TraceId::ZERO,
                txn_id,
            )
            .await?;
            reads.note([vshard_id], &response.read_versions);
            response.payload
        }
        RouteDecision::Remote {
            node_id,
            vshard_id: route_vshard,
        } => {
            let shared_arc = gateway_shared(state)?;
            let version_set = GatewayVersionSet::from_pairs(Vec::new());
            let outcome = dispatch_route(DispatchRouteParams {
                route: TaskRoute {
                    plan,
                    decision: RouteDecision::Remote {
                        node_id,
                        vshard_id: route_vshard,
                    },
                    vshard_id,
                },
                shared: &shared_arc,
                tenant_id,
                database_id,
                trace_id: TraceId::ZERO,
                deadline_ms: crate::control::gateway::dispatcher::statement_deadline_ms(state),
                version_set: &version_set,
                txn_id,
                linearizable,
            })
            .await?;
            reads.note([vshard_id], &outcome.read_versions);
            let payloads = outcome.payloads;
            Payload::from_vec(match payloads.len() {
                1 => payloads.into_iter().next().unwrap_or_default(),
                _ => merge_msgpack_arrays(&payloads),
            })
        }
        RouteDecision::LeaderUnknown { vshard_id } => {
            return Err(crate::Error::NotLeader {
                vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
                leader_node: 0,
                leader_addr: String::new(),
                leader_term: 0,
            });
        }
        RouteDecision::Broadcast { .. } => {
            return Err(crate::Error::Internal {
                detail: "graph keyed read: a single vShard resolved to a broadcast".into(),
            });
        }
    };
    // The node's key vShard joins the transaction read-set.
    reads.publish(tenant_id, database_id, collection);
    Ok(payload)
}

/// The database-qualified collection a one-hop graph plan scopes, or `None`
/// when it reads every collection.
fn plan_collection(plan: &PhysicalPlan) -> Option<String> {
    use nodedb_physical::physical_plan::GraphOp;
    match plan {
        PhysicalPlan::Graph(
            GraphOp::Neighbors { collection, .. }
            | GraphOp::NeighborsMulti { collection, .. }
            | GraphOp::Hop { collection, .. },
        ) => collection.as_ref().map(|c| c.as_str().to_owned()),
        PhysicalPlan::Graph(
            GraphOp::TemporalNeighbors { collection, .. }
            | GraphOp::NodePresenceRead { collection, .. },
        ) => Some(collection.as_str().to_owned()),
        _ => None,
    }
}
