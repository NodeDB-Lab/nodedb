// SPDX-License-Identifier: BUSL-1.1

//! Round-0 scatter: local broadcast + one remote dispatch per distinct
//! non-local group leader, issued concurrently.

use std::collections::HashMap;

use futures::future::join_all;

use crate::bridge::envelope::{Payload, PhysicalPlan};
use crate::control::gateway::dispatcher::{DispatchRouteParams, dispatch_route};
use crate::control::gateway::live_leaders::LiveLeaders;
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::gateway::{RouteDecision, TaskRoute};
use crate::control::server::graph_dispatch::cluster_resolve::gateway_shared;
use crate::control::server::graph_dispatch::match_broadcast::{
    GraphRead, broadcast_match_to_all_cores, unwrap_match_envelope,
};
use crate::control::server::graph_dispatch::shard_reads::ShardReadLog;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::GraphOp;

use super::coord::{TaggedShardResult, decode_rows};

/// A distinct remote owner node, one vShard it owns (the dispatch target for
/// the round-0 remote `Match`), and every vShard it leads.
pub(super) struct RemoteOwner {
    pub(super) node_id: u64,
    pub(super) vshard_id: u64,
    pub(super) vshards: Vec<u32>,
}

/// Who leads each data vShard, from one routing snapshot: the vShards this
/// node leads, and one entry per remote leader.
struct RoundZeroOwners {
    local_vshards: Vec<u32>,
    remote: Vec<RemoteOwner>,
}

/// Round-0 scatter: local broadcast + one remote dispatch per distinct
/// non-local group leader, all issued concurrently.
///
/// Every leg walks every vShard its node leads, so round 0 reads every vShard
/// of the graph. A MATCH result depends on every one of them: an edge written
/// to any vShard can add a match. The returned log notes each vShard under the
/// leg it was dispatched to, at the versions that leg reported, from the same
/// routing snapshot that picked the legs. Later rounds read vShards round 0
/// already noted.
pub(super) async fn scatter_round_zero(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    query_bytes: &[u8],
    deadline_ms: u64,
    read: GraphRead,
) -> crate::Result<(Vec<TaggedShardResult>, ShardReadLog)> {
    let GraphRead {
        txn_id,
        linearizable,
    } = read;
    // Local cores: fan to all and unwrap each `{rows, frontier}` envelope. The
    // active `txn_id` is threaded onto this LOCAL leg so each core merges the
    // transaction's staged edge overlay for read-your-own-writes; with the
    // fixed-hop overlay merge un-gated in cluster mode, a bound zero-degree
    // source still emits its cross-shard frontier. The same `txn_id` is
    // forwarded to remote owners below so their leg can resolve the transaction's
    // staged overlay; the staging/forwarding of that overlay to the leader is a
    // separate unit, so the forwarded id is inert until that lands.
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
        read,
    );

    // Remote owners: one batched dispatch per distinct non-local group leader.
    let RoundZeroOwners {
        local_vshards,
        remote: remote_owners,
    } = round_zero_owners(state)?;
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
        let leg_vshards = owner.vshards;
        let shared_arc = shared_arc.clone();
        Box::pin(async move {
            let outcome = dispatch_route(DispatchRouteParams {
                route,
                shared: &shared_arc,
                tenant_id,
                database_id,
                trace_id: TraceId::ZERO,
                deadline_ms,
                version_set: &version_set,
                txn_id,
                linearizable,
            })
            .await?;
            let mut log = ShardReadLog::new();
            log.note(leg_vshards, &outcome.read_versions);
            Ok::<_, crate::Error>((collect_remote_envelopes(node_id, outcome.payloads)?, log))
        })
    });

    // Drive local + all remotes concurrently.
    let (local_outcome, remote_results) =
        futures::future::join(local_fut, join_all(remote_futs)).await;

    let mut out: Vec<TaggedShardResult> = Vec::new();
    let mut log = ShardReadLog::new();
    let local_outcome = local_outcome?;
    log.note(local_vshards, &local_outcome.read_versions);
    out.push(TaggedShardResult {
        emitting_node: state.node_id,
        rows: decode_rows(&local_outcome.rows_payload)?,
        frontier: local_outcome.frontier,
        resume: local_outcome.resume,
    });
    for res in remote_results {
        let (tagged, leg_log) = res?;
        out.extend(tagged);
        log.merge(leg_log);
    }
    Ok((out, log))
}

/// Enumerate the distinct non-local data-group leaders, each paired with one
/// vShard the group owns and every vShard it leads, and the vShards this node
/// leads. The metadata group (0) holds no vShards and is skipped. Resolution
/// uses LIVE Raft leadership where available so a stale routing hint cannot
/// misdirect the scatter.
fn round_zero_owners(state: &SharedState) -> crate::Result<RoundZeroOwners> {
    let mut owners = RoundZeroOwners {
        local_vshards: Vec::new(),
        remote: Vec::new(),
    };
    let Some(routing_lock) = state.cluster_routing.as_ref() else {
        return Ok(owners);
    };
    // Raft snapshot first, routing guard second: see `LiveLeaders`.
    let live = LiveLeaders::snapshot(state);
    let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());

    let mut remote_index: HashMap<u64, usize> = HashMap::new();
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
        let mut leader = live.leader_of(group_id);
        if leader == 0 {
            leader = routing.group_info(group_id).map(|g| g.leader).unwrap_or(0);
        }
        if leader == state.node_id {
            // This group is LOCAL — already covered by the local
            // `broadcast_match_to_all_cores`; skip from the remote-owner set.
            owners.local_vshards.extend(vshards);
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
                leader_term: 0,
            });
        }
        match remote_index.get(&leader) {
            Some(&index) => owners.remote[index].vshards.extend(vshards),
            None => {
                remote_index.insert(leader, owners.remote.len());
                owners.remote.push(RemoteOwner {
                    node_id: leader,
                    vshard_id: vshard_id as u64,
                    vshards,
                });
            }
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
