// SPDX-License-Identifier: BUSL-1.1

//! Per-route dispatch: local SPSC or remote `ExecuteRequest` RPC.
//!
//! Executes a single [`TaskRoute`]: `Local` via the SPSC bridge, `Remote` via
//! an `ExecuteRequest` RPC, `Broadcast` never reached here (the router
//! splits it into concrete Local/Remote routes first). Returns raw Data
//! Plane response bytes for the fuser to merge.

use std::sync::Arc;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::result_stream::ResultStream;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, TxnId, VShardId};

use super::dispatch_local::{LocalContext, dispatch_local};
use super::dispatch_remote::{RemoteDispatchArgs, dispatch_remote, dispatch_remote_stream};
use super::read_leg::{confirm_local_read, linearizable_read_groups};
use super::route::{RouteDecision, TaskRoute};
use super::version_check::check_local_descriptor_versions;
use super::version_set::GatewayVersionSet;

/// Result of dispatching a single route: the raw payload bytes plus the
/// per-shard read watermarks observed while producing them.
///
/// `shard_watermarks` is one `(vshard, watermark_lsn)` per contributing shard
/// — local SPSC watermark, or remote `ExecuteResponse.watermark_lsn` keyed to
/// the owning vShard. A read-set entry takes its version from
/// `read_versions`, never from a watermark.
pub struct DispatchOutcome {
    pub payloads: Vec<Vec<u8>>,
    pub shard_watermarks: Vec<(VShardId, Lsn)>,
    /// The versions this route observed, one per vShard. Folded across
    /// routes for OCC read validation.
    pub read_versions: crate::types::ReadVersions,
    /// The owning core refused the task with `ErrorCode::NotFound`.
    ///
    /// A fan-out reads it as a shard that holds no slice. A single-route
    /// task reports it as the Data Plane's verdict on that task.
    pub not_found: bool,
}

/// Parameters for [`dispatch_route`]. `txn_id` is session-transaction
/// context for local overlay resolution and remote forwarding, `None` for
/// non-transactional dispatch (the common case).
pub struct DispatchRouteParams<'a> {
    pub route: TaskRoute,
    pub shared: &'a Arc<SharedState>,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub trace_id: TraceId,
    pub deadline_ms: u64,
    pub version_set: &'a GatewayVersionSet,
    pub txn_id: Option<TxnId>,
    /// The route is a leg of a linearizable read (see
    /// `QueryContext::linearizable`).
    pub linearizable: bool,
}

/// Dispatch a single route and return the raw payload bytes.
pub(crate) async fn dispatch_route(
    params: DispatchRouteParams<'_>,
) -> Result<DispatchOutcome, Error> {
    let DispatchRouteParams {
        route,
        shared,
        tenant_id,
        database_id,
        trace_id,
        deadline_ms,
        version_set,
        txn_id,
        linearizable,
    } = params;
    reject_unadmitted_crdt_apply(&route.plan)?;
    let read_groups = linearizable_read_groups(shared, &route, linearizable)?;
    match route.decision {
        RouteDecision::Local => {
            confirm_local_read(shared, database_id, &route.plan, &read_groups, deadline_ms).await?;
            dispatch_local(
                route,
                LocalContext {
                    shared,
                    tenant_id,
                    database_id,
                    trace_id,
                    txn_id,
                    version_set,
                },
            )
            .await
        }
        RouteDecision::Remote { node_id, vshard_id } => {
            dispatch_remote(RemoteDispatchArgs {
                plan: route.plan,
                shared,
                node_id,
                vshard_id,
                tenant_id,
                database_id,
                trace_id,
                deadline_ms,
                version_set,
                txn_id,
                linearizable,
                read_groups,
            })
            .await
        }
        RouteDecision::Broadcast { .. } => {
            // Split into individual Local/Remote routes by the router before
            // dispatch; this arm is unreachable.
            Err(Error::Internal {
                detail: "dispatcher: Broadcast route reached dispatch — should have been split"
                    .into(),
            })
        }
        RouteDecision::LeaderUnknown { vshard_id } => {
            // No known leader for this vShard: surface as NotLeader so the
            // retry loop re-resolves rather than serving stale local data.
            Err(Error::NotLeader {
                vshard_id: VShardId::new(vshard_id as u32),
                leader_node: 0,
                leader_addr: String::new(),
                leader_term: 0,
            })
        }
    }
}

/// Parameters for [`dispatch_route_stream`].
pub struct DispatchRouteStreamParams<'a> {
    pub route: TaskRoute,
    pub shared: &'a Arc<SharedState>,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub trace_id: TraceId,
    pub deadline_ms: u64,
    pub version_set: &'a GatewayVersionSet,
    /// The route is a leg of a linearizable read.
    pub linearizable: bool,
}

/// Refuse a CRDT apply or snapshot import that did not pass CRDT admission.
fn reject_unadmitted_crdt_apply(plan: &PhysicalPlan) -> Result<(), Error> {
    if matches!(
        plan,
        PhysicalPlan::Crdt(
            nodedb_physical::physical_plan::CrdtOp::Apply { .. }
                | nodedb_physical::physical_plan::CrdtOp::ApplyAuthenticated { .. }
                | nodedb_physical::physical_plan::CrdtOp::ImportSnapshot { .. }
        )
    ) {
        return Err(Error::CrdtApplyRequiresAdmission);
    }
    Ok(())
}

/// Streaming sibling of [`dispatch_route`]: `Local` fans to all local cores,
/// `Remote` uses eager-first-frame dispatch, `Broadcast` is unreachable
/// (pre-split by the router), `LeaderUnknown` returns `NotLeader`.
pub(crate) async fn dispatch_route_stream(
    args: DispatchRouteStreamParams<'_>,
) -> Result<ResultStream, Error> {
    let DispatchRouteStreamParams {
        route,
        shared,
        tenant_id,
        database_id,
        trace_id,
        deadline_ms,
        version_set,
        linearizable,
    } = args;
    reject_unadmitted_crdt_apply(&route.plan)?;
    let read_groups = linearizable_read_groups(shared, &route, linearizable)?;
    match route.decision {
        // Cluster gateway route dispatch: no session-transaction context
        // crosses this boundary yet, so `None`. TRACKED: cross-node
        // in-transaction reads are a known gap (see resolve/exchange.rs).
        RouteDecision::Local => {
            // Same fence as the one-shot local path: a streaming read planned
            // against a superseded descriptor must not reach the cores.
            check_local_descriptor_versions(shared, tenant_id, database_id, version_set)?;
            confirm_local_read(shared, database_id, &route.plan, &read_groups, deadline_ms).await?;
            crate::control::server::exchange::gather::gather_all_cores_stream(
                shared,
                tenant_id,
                database_id,
                route.plan,
                trace_id,
                None,
            )
        }
        RouteDecision::Remote { node_id, vshard_id } => {
            dispatch_remote_stream(RemoteDispatchArgs {
                plan: route.plan,
                shared,
                node_id,
                vshard_id,
                tenant_id,
                database_id,
                trace_id,
                deadline_ms,
                version_set,
                // No session-transaction context crosses the streaming gateway
                // boundary yet (see `resolve/exchange.rs`), so `None`.
                txn_id: None,
                linearizable,
                read_groups,
            })
            .await
        }
        RouteDecision::Broadcast { .. } => Err(Error::Internal {
            detail: "dispatcher: Broadcast route reached stream dispatch — should have been split"
                .into(),
        }),
        RouteDecision::LeaderUnknown { vshard_id } => Err(Error::NotLeader {
            vshard_id: VShardId::new(vshard_id as u32),
            leader_node: 0,
            leader_addr: String::new(),
            leader_term: 0,
        }),
    }
}

/// Milliseconds left on the running statement, for a remote hop's
/// `ExecuteRequest.deadline_remaining_ms`.
///
/// The session's `statement_timeout` when one is installed on this connection,
/// else the node's configured default. A forwarded route must stop when the
/// statement that spawned it does, not on a budget of its own.
pub fn statement_deadline_ms(shared: &SharedState) -> u64 {
    crate::control::server::shared::session::statement_deadline_ms(
        shared.tuning.network.default_deadline_secs,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_rejects_unadmitted_crdt_apply_before_route_selection() {
        let plan = PhysicalPlan::Crdt(nodedb_physical::physical_plan::CrdtOp::Apply {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "doc".into(),
            delta: vec![1],
            peer_id: 1,
            mutation_id: 1,
            surrogate: nodedb_types::Surrogate::new(1),
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });
        assert!(matches!(
            reject_unadmitted_crdt_apply(&plan),
            Err(Error::CrdtApplyRequiresAdmission)
        ));
    }
}
