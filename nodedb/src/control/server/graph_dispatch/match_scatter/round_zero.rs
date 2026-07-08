// SPDX-License-Identifier: BUSL-1.1

//! Round-0 scatter: local broadcast + one remote dispatch per distinct
//! non-local group leader, issued concurrently.

use std::collections::HashSet;

use futures::future::join_all;

use crate::bridge::envelope::{Payload, PhysicalPlan};
use crate::control::gateway::dispatcher::dispatch_route;
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::gateway::{RouteDecision, TaskRoute};
use crate::control::server::graph_dispatch::cluster_resolve::gateway_shared;
use crate::control::server::graph_dispatch::match_broadcast::{
    broadcast_match_to_all_cores, unwrap_match_envelope,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::GraphOp;

use super::coord::{TaggedShardResult, decode_rows};

/// A distinct remote owner node and one vShard it owns (used as the dispatch
/// target for the round-0 remote `Match`).
pub(super) struct RemoteOwner {
    pub(super) node_id: u64,
    pub(super) vshard_id: u64,
}

/// Round-0 scatter: local broadcast + one remote dispatch per distinct
/// non-local group leader, all issued concurrently.
pub(super) async fn scatter_round_zero(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    query_bytes: &[u8],
    deadline_ms: u64,
) -> crate::Result<Vec<TaggedShardResult>> {
    // Local cores: fan to all and unwrap each `{rows, frontier}` envelope. The
    // cluster MATCH path runs `cluster_mode = true`, under which the pattern
    // engine's transaction-overlay merge is disabled (the durable path must emit
    // the cross-shard frontier instead), so no `txn_id` is threaded here —
    // cross-shard MATCH read-your-own-writes is a separate unit.
    let local_plan = PhysicalPlan::Graph(GraphOp::Match {
        query: query_bytes.to_vec(),
        frontier_bitmap: None,
        cluster_mode: true,
    });
    let local_fut = broadcast_match_to_all_cores(
        state,
        tenant_id,
        database_id,
        local_plan,
        TraceId::ZERO,
        None,
    );

    // Remote owners: one batched dispatch per distinct non-local group leader.
    let remote_owners = distinct_remote_owners(state)?;
    let shared_arc = gateway_shared(state)?;
    let version_set = GatewayVersionSet::from_pairs(Vec::new());
    let remote_futs = remote_owners.into_iter().map(|owner| {
        let plan = PhysicalPlan::Graph(GraphOp::Match {
            query: query_bytes.to_vec(),
            frontier_bitmap: None,
            cluster_mode: true,
        });
        let route = TaskRoute {
            plan,
            decision: RouteDecision::Remote {
                node_id: owner.node_id,
                vshard_id: owner.vshard_id,
            },
            vshard_id: (owner.vshard_id % VShardId::COUNT as u64) as u32,
        };
        let version_set = version_set.clone();
        let node_id = owner.node_id;
        Box::pin(async move {
            let payloads = dispatch_route(
                route,
                shared_arc,
                tenant_id,
                database_id,
                TraceId::ZERO,
                deadline_ms,
                &version_set,
                // Graph scatter reads committed state only (no overlay merge).
                None,
            )
            .await?;
            collect_remote_envelopes(node_id, payloads)
        })
    });

    // Drive local + all remotes concurrently.
    let (local_outcome, remote_results) =
        futures::future::join(local_fut, join_all(remote_futs)).await;

    let mut out: Vec<TaggedShardResult> = Vec::new();
    let local_outcome = local_outcome?;
    out.push(TaggedShardResult {
        emitting_node: state.node_id,
        rows: decode_rows(&local_outcome.rows_payload)?,
        frontier: local_outcome.frontier,
        resume: local_outcome.resume,
    });
    for res in remote_results {
        out.extend(res?);
    }
    Ok(out)
}

/// Enumerate the distinct non-local data-group leaders, each paired with one
/// vShard the group owns. The metadata group (0) holds no vShards and is
/// skipped. Resolution uses LIVE Raft leadership where available so a stale
/// routing hint cannot misdirect the scatter.
fn distinct_remote_owners(state: &SharedState) -> crate::Result<Vec<RemoteOwner>> {
    let Some(routing_lock) = state.cluster_routing.as_ref() else {
        return Ok(Vec::new());
    };
    let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());

    let raft_snapshot: Vec<nodedb_cluster::GroupStatus> =
        state.raft_status_fn.get().map(|f| f()).unwrap_or_default();
    let live_leader = |group_id: u64| -> u64 {
        raft_snapshot
            .iter()
            .find(|gs| gs.group_id == group_id)
            .map(|gs| gs.leader_id)
            .unwrap_or(0)
    };

    let mut seen: HashSet<u64> = HashSet::new();
    let mut owners = Vec::new();
    for group_id in routing.group_ids() {
        // Skip the metadata group — it owns no vShards.
        if group_id == 0 {
            continue;
        }
        let vshards = routing.vshards_for_group(group_id);
        let Some(&vshard_id) = vshards.first() else {
            continue;
        };
        // Prefer live Raft leadership; fall back to the routing-table hint.
        let mut leader = live_leader(group_id);
        if leader == 0 {
            leader = routing.group_info(group_id).map(|g| g.leader).unwrap_or(0);
        }
        if leader == state.node_id {
            // This group is LOCAL — already covered by the local
            // `broadcast_match_to_all_cores`; skip from the remote-owner set.
            continue;
        }
        if leader == 0 {
            // No known leader for this group: fail hard so a leader election
            // surfaces as an explicit error rather than silently omitting
            // every vShard in this group from the round-0 scatter.
            return Err(crate::Error::NotLeader {
                vshard_id: VShardId::new(vshard_id),
                leader_node: 0,
                leader_addr: String::new(),
            });
        }
        if seen.insert(leader) {
            owners.push(RemoteOwner {
                node_id: leader,
                vshard_id: vshard_id as u64,
            });
        }
    }
    Ok(owners)
}

/// Unwrap each remote `{rows, frontier}` envelope payload into one tagged
/// result per payload, all tagged with the emitting (remote) node id.
pub(super) fn collect_remote_envelopes(
    node_id: u64,
    payloads: Vec<Vec<u8>>,
) -> crate::Result<Vec<TaggedShardResult>> {
    let mut out = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let unwrapped = unwrap_match_envelope(&Payload::from_vec(payload))?;
        // Remote truncation is recoverable: it rides INSIDE the envelope bytes as
        // the resume cursor array (the per-frame `partial` flag is collapsed by
        // remote dispatch, so the in-payload cursor is the durable signal).
        out.push(TaggedShardResult {
            emitting_node: node_id,
            rows: decode_rows(&unwrapped.rows_payload)?,
            frontier: unwrapped.frontier,
            resume: unwrapped.resume,
        });
    }
    Ok(out)
}
