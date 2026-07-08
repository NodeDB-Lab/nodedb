// SPDX-License-Identifier: BUSL-1.1

//! Round-loop continuation dispatch: one round of pending continuations,
//! grouped by target shard, issued concurrently.

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::join_all;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::gateway::dispatcher::dispatch_route;
use crate::control::gateway::version_set::GatewayVersionSet;
use crate::control::gateway::{RouteDecision, TaskRoute};
use crate::control::server::graph_dispatch::match_broadcast::broadcast_match_to_all_cores;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_cluster::distributed_graph::PatternContinuation;
use nodedb_physical::physical_plan::GraphOp;

use crate::control::server::graph_dispatch::cluster_resolve::{gateway_shared, resolve_for_vshard};

use super::coord::{TaggedShardResult, decode_rows};
use super::resume_queue::PendingResume;
use super::round_zero::collect_remote_envelopes;

/// Boxed future type used to keep heterogeneous local/remote dispatch futures
/// in a single `Vec` for `join_all`.
type DispatchFut<'f> = std::pin::Pin<
    Box<dyn std::future::Future<Output = crate::Result<Vec<TaggedShardResult>>> + Send + 'f>,
>;

/// Shared per-round dispatch context, built once and reused for every plan
/// pushed in a round (continuations or resumes). Bundles the borrowed state and
/// the scalar request parameters so [`push_dispatch_fut`] stays single-purpose.
struct DispatchCtx<'f> {
    state: &'f SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    deadline_ms: u64,
    shared_arc: &'f Arc<SharedState>,
    version_set: GatewayVersionSet,
}

/// Push one dispatch future onto `futs` for an already-built `plan`.
///
/// `remote_coords == None` → local broadcast via `broadcast_match_to_all_cores`.
/// `Some((node_id, vshard_id))` → single remote dispatch via `dispatch_route`.
/// Both arms produce the same `TaggedShardResult` shape consumed by the
/// caller's `join_all` loop.
fn push_dispatch_fut<'f>(
    futs: &mut Vec<DispatchFut<'f>>,
    ctx: &DispatchCtx<'f>,
    plan: PhysicalPlan,
    remote_coords: Option<(u64, u64)>,
) {
    let (state, tenant_id, database_id, deadline_ms) =
        (ctx.state, ctx.tenant_id, ctx.database_id, ctx.deadline_ms);
    match remote_coords {
        None => {
            futs.push(Box::pin(async move {
                // Cross-shard MATCH continuation: the resumed pattern runs on
                // whichever owner shard emitted the frontier, not necessarily
                // the node holding the transaction's staged overlay, so no
                // txn_id is threaded here (committed-CSR-only). Cross-shard
                // read-your-own-writes for MATCH remains a separate unit.
                let outcome = broadcast_match_to_all_cores(
                    state,
                    tenant_id,
                    database_id,
                    plan,
                    TraceId::ZERO,
                    None,
                )
                .await?;
                Ok::<_, crate::Error>(vec![TaggedShardResult {
                    emitting_node: state.node_id,
                    rows: decode_rows(&outcome.rows_payload)?,
                    frontier: outcome.frontier,
                    resume: outcome.resume,
                }])
            }));
        }
        Some((node_id, vshard_id)) => {
            let route = TaskRoute {
                plan,
                decision: RouteDecision::Remote { node_id, vshard_id },
                vshard_id: (vshard_id % VShardId::COUNT as u64) as u32,
            };
            let shared_arc = ctx.shared_arc;
            let version_set = ctx.version_set.clone();
            futs.push(Box::pin(async move {
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
            }));
        }
    }
}

/// Dispatch one round of pending continuations, grouped by target shard.
pub(super) async fn dispatch_continuations(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    query_bytes: &[u8],
    deadline_ms: u64,
    pending: HashMap<u32, Vec<PatternContinuation>>,
) -> crate::Result<Vec<TaggedShardResult>> {
    let shared_arc = gateway_shared(state)?;
    let ctx = DispatchCtx {
        state,
        tenant_id,
        database_id,
        deadline_ms,
        shared_arc,
        version_set: GatewayVersionSet::from_pairs(Vec::new()),
    };

    let mut futs: Vec<DispatchFut<'_>> = Vec::new();
    for (target_shard, conts) in pending {
        // Resolve once per target shard, not once per continuation: every
        // continuation targeting the same vShard gets the same routing
        // decision, and `resolve_for_vshard` acquires a routing-table read
        // lock on each call.
        let decision = resolve_for_vshard(state, target_shard);

        // Extract the remote node coordinates (Copy-able u64 fields) so the
        // inner loop can reuse them without re-acquiring the routing lock.
        // `None` means Local.
        let remote_coords: Option<(u64, u64)> = match decision {
            RouteDecision::LeaderUnknown { vshard_id } => {
                return Err(crate::Error::NotLeader {
                    vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
                    leader_node: 0,
                    leader_addr: String::new(),
                });
            }
            RouteDecision::Broadcast { .. } => {
                return Err(crate::Error::Internal {
                    detail: "match scatter: resolve_decision returned Broadcast \
                             for a single vShard"
                        .into(),
                });
            }
            RouteDecision::Local => None,
            RouteDecision::Remote { node_id, vshard_id } => Some((node_id, vshard_id)),
        };

        for cont in conts {
            let partial_row = zerompk::to_msgpack_vec(&cont.bindings).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("continuation partial_row: {e}"),
                }
            })?;
            let plan = PhysicalPlan::Graph(GraphOp::MatchContinuation {
                query: query_bytes.to_vec(),
                resume_triple_idx: cont.next_triple_idx,
                partial_row,
                source_node: cont.start_node.clone(),
                source_binding: cont.start_binding.clone(),
            });
            push_dispatch_fut(&mut futs, &ctx, plan, remote_coords);
        }
    }

    let results = join_all(futs).await;
    let mut out = Vec::new();
    for res in results {
        out.extend(res?);
    }
    Ok(out)
}

/// Dispatch one round of pending variable-length resume cursors, each routed
/// back (in `resume_to_pending`) to the node owning its surviving frontier.
///
/// A LOCAL owner fans a `MatchVarLenResume` plan across all local cores; a
/// REMOTE owner gets a single gateway dispatch. Each produced `TaggedShardResult`
/// carries a FRESH frontier AND a FRESH resume cursor if the handler re-caps, so
/// it re-enters the coordinator's round loop and the capped expansion drains
/// across rounds.
pub(super) async fn dispatch_resumes(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    query_bytes: &[u8],
    deadline_ms: u64,
    pending_resumes: Vec<PendingResume>,
) -> crate::Result<Vec<TaggedShardResult>> {
    let shared_arc = gateway_shared(state)?;
    let ctx = DispatchCtx {
        state,
        tenant_id,
        database_id,
        deadline_ms,
        shared_arc,
        version_set: GatewayVersionSet::from_pairs(Vec::new()),
    };

    let mut futs: Vec<DispatchFut<'_>> = Vec::new();
    for pending in pending_resumes {
        let PendingResume {
            remote_coords,
            resume,
        } = pending;
        let resume_bytes =
            zerompk::to_msgpack_vec(&resume).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("varlen resume cursor: {e}"),
            })?;
        let plan = PhysicalPlan::Graph(GraphOp::MatchVarLenResume {
            query: query_bytes.to_vec(),
            resume: resume_bytes,
        });
        push_dispatch_fut(&mut futs, &ctx, plan, remote_coords);
    }

    let results = join_all(futs).await;
    let mut out = Vec::new();
    for res in results {
        out.extend(res?);
    }
    Ok(out)
}
