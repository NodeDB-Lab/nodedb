// SPDX-License-Identifier: BUSL-1.1

//! Shared per-core fan-out primitive for graph BSP/WCC superstep plans and
//! single-blob Meta ops (tenant snapshot, restore result). Used by every
//! single-blob merge path (`dispatch::single_blob_gather`, `snapshot`, `bsp`,
//! `wcc`). Every core must answer: see `exchange::core_outcome`.

use futures::future::join_all;

use crate::bridge::envelope::Response;
use crate::control::server::exchange::core_outcome::{
    CoreOutcome, classify_core_response, require_every_core,
};
use crate::control::server::exchange::gather::eager_dispatch_to_all_cores;
use crate::control::server::shared::session::statement_deadline;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};

/// Fan `plan` to every local core and require every core to answer.
///
/// The first core error fails the whole call with that core's typed error. A
/// `NotFound` refusal contributes nothing. An empty payload is kept as an
/// answer.
pub(super) async fn gather_every_core(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    label: &'static str,
) -> crate::Result<Vec<Response>> {
    let outcomes = dispatch_all_cores(state, tenant_id, database_id, plan, trace_id, label).await?;
    require_every_core(outcomes)
}

/// Dispatch `plan` to every local core and collect each core's outcome.
///
/// Must scope `owned_vshards` to `vshard % num_cores == core_id`, or every core
/// claims sibling-homed nodes in its local CSR, duplicating them in the merge.
async fn dispatch_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    label: &'static str,
) -> crate::Result<Vec<CoreOutcome<Response>>> {
    // Shared broadcast call counter (parity with gather_all_cores).
    crate::control::server::broadcast::broadcast_call_count_increment();

    // The running statement's deadline — the same instant the per-core
    // envelopes carry, so the Control-Plane wait and the Data-Plane execution
    // expire together.
    let deadline = statement_deadline(state.tuning.network.default_deadline_secs);
    let max_result_bytes = state.tuning.network.max_query_result_bytes as usize;

    let num_cores = state
        .dispatcher
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .num_cores();

    // Eager dispatch: register + dispatch to each core before awaiting any response.
    // Scope owned_vshards to `vshard % num_cores == core_id` — see doc above.
    let receivers =
        eager_dispatch_to_all_cores(state, tenant_id, database_id, trace_id, None, |core_id| {
            let mut core_plan = plan.clone();
            match &mut core_plan {
                PhysicalPlan::Graph(g) => match g {
                    // A core receives only the contributions to the nodes it
                    // owns: its handler refuses any other.
                    GraphOp::BspSuperstep(bsp) => {
                        bsp.owned_vshards
                            .retain(|v| (*v as usize) % num_cores == core_id);
                        bsp.incoming_contributions.retain(|(name, _)| {
                            (VShardId::from_key(name.as_bytes()).as_u32() as usize) % num_cores
                                == core_id
                        });
                    }
                    GraphOp::WccSuperstep(wcc) => {
                        wcc.owned_vshards
                            .retain(|v| (*v as usize) % num_cores == core_id);
                    }
                    // No per-core vShard set — fanned verbatim. Exhaustive (no `_ =>`) so a
                    // new superstep variant forces a scoping decision here.
                    GraphOp::Match { .. }
                    | GraphOp::MatchContinuation { .. }
                    | GraphOp::MatchVarLenResume { .. }
                    | GraphOp::EdgePut { .. }
                    | GraphOp::EdgePutBatch { .. }
                    | GraphOp::EdgeDelete { .. }
                    | GraphOp::EdgeDeleteBatch { .. }
                    | GraphOp::ResolveEdgeDelete(_)
                    | GraphOp::Hop { .. }
                    | GraphOp::Neighbors { .. }
                    | GraphOp::NeighborsMulti { .. }
                    | GraphOp::Path { .. }
                    | GraphOp::Subgraph { .. }
                    | GraphOp::RagFusion { .. }
                    | GraphOp::Algo { .. }
                    | GraphOp::SetNodeLabels { .. }
                    | GraphOp::RemoveNodeLabels { .. }
                    | GraphOp::TemporalNeighbors { .. }
                    | GraphOp::Stats { .. }
                    | GraphOp::NodeEdgeGuard { .. }
                    | GraphOp::NodePresenceGuard { .. }
                    | GraphOp::TruncateEdges { .. }
                    | GraphOp::NodePresenceRead { .. } => {}
                },
                // Non-graph plans fanned verbatim. Exhaustive (no `_ =>`) to force a decision.
                PhysicalPlan::Vector(_)
                | PhysicalPlan::Document(_)
                | PhysicalPlan::Kv(_)
                | PhysicalPlan::Text(_)
                | PhysicalPlan::Columnar(_)
                | PhysicalPlan::Timeseries(_)
                | PhysicalPlan::Spatial(_)
                | PhysicalPlan::Crdt(_)
                | PhysicalPlan::Query(_)
                | PhysicalPlan::Meta(_)
                | PhysicalPlan::Array(_)
                | PhysicalPlan::ClusterArray(_)
                | PhysicalPlan::ClusterEvent(_) => {}
            }
            core_plan
        })?;

    let response_futures = receivers
        .into_iter()
        .map(|(core_id, request_id, mut rx)| async move {
            let context = format!("{label} gather on core {core_id}");
            crate::control::local_dispatch::collect_under_deadline(
                &mut rx,
                crate::control::local_dispatch::DeadlineCollect {
                    request_id,
                    deadline,
                    max_result_bytes,
                    context: &context,
                },
            )
            .await
        });

    let results: Vec<crate::Result<Response>> = join_all(response_futures).await;

    Ok(results.into_iter().map(classify_core_response).collect())
}
