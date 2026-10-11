// SPDX-License-Identifier: BUSL-1.1

//! Leader-aware forwarding for in-transaction staging control ops.
//!
//! The per-transaction staging overlay lives on the Data-Plane core that owns
//! the target vShard — i.e. on that vShard's current Raft leader. When this
//! node is NOT the leader for a vShard a transaction stages a write to, the
//! `MetaOp::StageWrite` (and, at COMMIT / ROLLBACK, the `MetaOp::DropTxnOverlay`)
//! must execute on the leader, keyed purely by `txn_id` — the Data-Plane
//! handlers are session-less, so a wire-carried `txn_id` is all the leader needs
//! to stage into / drop the correct overlay.
//!
//! INVARIANT: for a given vShard, the stage, every read-your-own-writes read,
//! and the overlay drop all resolve to the SAME node (the vShard's leader at the
//! time each runs). A leader change mid-transaction strands the staged overlay
//! on the former leader — the same failure surface Calvin already tolerates for
//! staged cross-shard state. A stranded overlay is bounded (one transaction's
//! staging), invisible (its `txn_id` comes from a monotonic counter and is never
//! reused or committed), and observable (the `active_txn_overlays` gauge plus the
//! ERROR the commit/rollback teardown logs when a drop-forward fails); a former
//! leader that crashes clears it with its memory. There is no automatic
//! time-based reclaim of a still-running former leader's stranded overlay today —
//! it is cleared on that node's next restart.
//!
//! Both choke points (`staging_gate::stage_write`, `commit::drop_txn_overlay`)
//! resolve the leader themselves and keep their existing LOCAL dispatch
//! byte-identical; only the REMOTE arm routes through [`forward_to_leader`],
//! which fails CLOSED — a `LeaderUnknown` resolution surfaces as
//! `Error::NotLeader` via the gateway dispatcher. `commit::drop_txn_overlay`'s
//! remote arm wraps this in a bounded retry (`retry_not_leader`) so a
//! transient leader election does not strand the overlay after a single
//! attempt; `staging_gate::stage_write`'s remote arm surfaces the error
//! directly to the statement layer. Neither ever falls back to local staging
//! on a non-leader.

use crate::bridge::envelope::{Payload, PhysicalPlan, Response};
use crate::control::gateway::dispatcher::{
    DispatchRouteParams, dispatch_route, statement_deadline_ms,
};
use crate::control::gateway::live_leaders::resolve_live_decision;
use crate::control::gateway::{GatewayVersionSet, RouteDecision, TaskRoute};
use crate::control::server::graph_dispatch::cluster_resolve::gateway_shared;
use crate::control::server::shared::write_admission::bare_ok_response;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, RequestId, TenantId, TraceId};
use nodedb_physical::physical_task::PhysicalTask;

/// Resolve the current leader for `task`'s target vShard against live Raft
/// leadership (falling back to the routing-table hint), so a staging choke point
/// can branch between its existing local dispatch and a remote forward.
pub(crate) fn resolve_leader(task: &PhysicalTask, state: &SharedState) -> RouteDecision {
    resolve_live_decision(state, task.vshard_id.as_u32())
}

/// Forward an already-wrapped staging control op (`MetaOp::StageWrite` /
/// `MetaOp::DropTxnOverlay`) in `forward_task` to a REMOTE leader, adapting the
/// remote outcome into the same `crate::Result<Response>` a local dispatch of
/// the op yields.
///
/// `decision` is the non-`Local` resolution from [`resolve_leader`]; `Local` is
/// handled by the caller's own dispatch path and never reaches here. A
/// `LeaderUnknown` (or the unreachable `Broadcast`) resolution is passed
/// straight to the gateway dispatcher, which maps it to `Error::NotLeader` /
/// an internal error — fail-closed, never a local staging fallback.
///
/// `version_plan` is the plan used to compute the descriptor version set the
/// leader validates for OCC: for a `StageWrite` it is the INNER (un-wrapped)
/// write, so the forwarded stage carries the same descriptor versions a normal
/// remote write does and is not spuriously rejected; a `DropTxnOverlay` touches
/// no user collection, so its version set is empty.
pub(crate) async fn forward_to_leader(
    state: &SharedState,
    decision: RouteDecision,
    forward_task: PhysicalTask,
    version_plan: &PhysicalPlan,
) -> crate::Result<Response> {
    // The remote dispatch primitive needs an owned `Arc<SharedState>`; upgrade
    // the gateway's back-reference (loud typed error if the gateway is not wired
    // — a non-leader with no gateway cannot forward, which is a real fault, not a
    // reason to stage locally).
    let shared = gateway_shared(state)?;

    let version_set = version_set_for_plan(
        &shared,
        version_plan,
        forward_task.tenant_id,
        forward_task.database_id,
    );

    let route = TaskRoute {
        plan: forward_task.plan,
        decision,
        vshard_id: forward_task.vshard_id.as_u32(),
    };

    let outcome = dispatch_route(DispatchRouteParams {
        route,
        shared: &shared,
        tenant_id: forward_task.tenant_id,
        database_id: forward_task.database_id,
        trace_id: TraceId::ZERO,
        deadline_ms: statement_deadline_ms(&shared),
        version_set: &version_set,
        // The leader's session-less staging handler reads THIS to key the
        // per-transaction overlay; forwarding it is the whole point.
        txn_id: forward_task.txn_id,
        // A staged write or overlay drop, never a read.
        linearizable: false,
    })
    .await?;

    // Adapt the remote outcome into a local-shaped `Ok` response. A remote
    // staging rejection (constraint violation, missing txn_id, descriptor
    // mismatch) never reaches here as an `Ok` — the leader collapses it to a
    // dispatch `Err` that `dispatch_route` already returned above via `?`, so a
    // remote failure propagates exactly like a local dispatch `Err`. On success
    // the leader returns its staging handler's payload (an affected-count blob)
    // verbatim, so the caller's affected-count / tag extraction runs identically
    // to the local path.
    let payload = outcome.payloads.into_iter().next().unwrap_or_default();
    let mut resp = bare_ok_response(RequestId::new(0));
    resp.payload = Payload::from_vec(payload);
    resp.read_versions = outcome.read_versions;
    Ok(resp)
}

/// Descriptor version set for `plan`, looked up against this node's catalog —
/// the same `(collection, descriptor_version)` payload the gateway attaches to a
/// normal remote write so the leader's OCC descriptor check accepts it.
fn version_set_for_plan(
    state: &SharedState,
    plan: &PhysicalPlan,
    tenant_id: TenantId,
    database_id: DatabaseId,
) -> GatewayVersionSet {
    let catalog = state.credentials.catalog();
    GatewayVersionSet::from_plan(plan, |name| {
        catalog
            .get_collection(database_id, tenant_id.as_u64(), name)
            .ok()
            .flatten()
            .map(|col| col.descriptor_version.max(1))
            .unwrap_or(0)
    })
}
