// SPDX-License-Identifier: BUSL-1.1

//! Per-route dispatch: local SPSC or remote `ExecuteRequest` RPC.
//!
//! Executes a single [`TaskRoute`]: `Local` via the SPSC bridge, `Remote` via
//! an `ExecuteRequest` RPC, `Broadcast` never reached here (the router
//! splits it into concrete Local/Remote routes first). Returns raw Data
//! Plane response bytes for the fuser to merge.

use std::sync::Arc;

use nodedb_cluster::rpc_codec::TypedClusterError;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::dispatch_utils::{
    dispatch_to_data_plane_with_txn, reject_data_plane_error,
};
use crate::control::server::result_stream::ResultStream;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, TxnId, VShardId};

use super::dispatch_remote::{RemoteDispatchArgs, dispatch_remote, dispatch_remote_stream};
use super::route::{RouteDecision, TaskRoute};
use super::version_check::check_descriptor_versions;
use super::version_set::GatewayVersionSet;

/// Result of dispatching a single route: the raw payload bytes plus the
/// per-shard read watermarks observed while producing them.
///
/// `shard_watermarks` is one `(vshard, watermark_lsn)` per contributing shard
/// — local SPSC watermark, or remote `ExecuteResponse.watermark_lsn` keyed to
/// the owning vShard — accumulated so an in-transaction read gets one
/// read-set entry per shard at its true committed LSN.
pub struct DispatchOutcome {
    pub payloads: Vec<Vec<u8>>,
    pub shard_watermarks: Vec<(VShardId, Lsn)>,
    /// This route's scanned collection's read-version LSN (`coll_write_lsn`
    /// at read time), `Lsn::ZERO` for writes. Max-folded across routes — a
    /// read targets one collection, so one non-zero value survives — for
    /// cross-shard OCC read validation.
    pub read_version_lsn: Lsn,
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
    } = params;
    reject_unadmitted_crdt_apply(&route.plan)?;
    match route.decision {
        RouteDecision::Local => {
            dispatch_local(
                route,
                shared,
                tenant_id,
                database_id,
                trace_id,
                txn_id,
                version_set,
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
            })
            .await
        }
        RouteDecision::Broadcast { .. } => {
            // Split into individual Local/Remote routes by the router before
            // dispatch; this arm should not be reached.
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
}

/// Streaming sibling of [`dispatch_route`]: `Local` fans to all local cores,
/// `Remote` uses eager-first-frame dispatch, `Broadcast` is unreachable
/// (pre-split by the router), `LeaderUnknown` returns `NotLeader`.
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
    } = args;
    reject_unadmitted_crdt_apply(&route.plan)?;
    match route.decision {
        // Cluster gateway route dispatch: no session-transaction context
        // crosses this boundary yet, so `None`. TRACKED: cross-node
        // in-transaction reads are a known gap (see resolve/exchange.rs).
        RouteDecision::Local => {
            // Same fence as the one-shot local path: a streaming read planned
            // against a superseded descriptor must not reach the cores.
            check_local_descriptor_versions(shared, tenant_id, database_id, version_set)?;
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
        }),
    }
}

/// Re-compare a plan's stamped descriptor versions against this node's own
/// catalog. A mismatch surfaces as [`Error::RetryableSchemaChanged`], which the
/// gateway's cache-miss retry absorbs by re-planning against fresh state.
fn check_local_descriptor_versions(
    shared: &Arc<SharedState>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    version_set: &GatewayVersionSet,
) -> Result<(), Error> {
    check_descriptor_versions(
        shared.credentials.catalog(),
        database_id,
        tenant_id.as_u64(),
        version_set
            .iter()
            .map(|(collection, version)| (collection.as_str(), *version)),
    )?;
    Ok(())
}

/// Local dispatch via SPSC bridge.
///
/// Carries `txn_id` so the Data Plane can resolve this session transaction's
/// staging overlay (read-your-own-writes) for in-block SQL and direct ops.
async fn dispatch_local(
    route: TaskRoute,
    shared: &Arc<SharedState>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
    version_set: &GatewayVersionSet,
) -> Result<DispatchOutcome, Error> {
    // Staying on this node does not make the plan fresh: a DDL can bump a
    // descriptor between planning and dispatch, and a drain that times out is
    // force-ended. Fence the local plan against this node's catalog exactly as
    // the leaseholder fences a forwarded one.
    check_local_descriptor_versions(shared, tenant_id, database_id, version_set)?;

    let vshard_id = VShardId::new(route.vshard_id);

    if txn_id.is_some()
        && matches!(
            &route.plan,
            PhysicalPlan::Crdt(
                nodedb_physical::physical_plan::CrdtOp::Apply { .. }
                    | nodedb_physical::physical_plan::CrdtOp::ApplyAuthenticated { .. }
            )
        )
    {
        return Err(Error::CrdtApplyForbiddenInTransaction);
    }

    // In local mode, frontier-changing CRDT operations have no Raft ordering.
    // Serialize their complete Data Plane dispatch; replicated/transactional
    // paths rely on the fenced-apply retry rather than this local mutex.
    if txn_id.is_none()
        && shared.async_raft_proposer().is_none()
        && let PhysicalPlan::Crdt(op) = &route.plan
        && crate::control::crdt_admission::changes_crdt_frontier(op)
    {
        let resp = shared
            .vshard_admission_sequencer
            .run(vshard_id, || async {
                dispatch_to_data_plane_with_txn(
                    shared,
                    tenant_id,
                    database_id,
                    vshard_id,
                    route.plan,
                    trace_id,
                    None,
                )
                .await
            })
            .await?;
        reject_data_plane_error(&resp)?;
        return Ok(DispatchOutcome {
            payloads: vec![resp.payload.to_vec()],
            shard_watermarks: vec![(vshard_id, resp.watermark_lsn)],
            read_version_lsn: resp.read_version_lsn,
        });
    }

    if txn_id.is_none()
        && let Some(proposer) = shared.async_raft_proposer()
        && let Some(entry) = crate::control::wal_replication::to_replicated_entry(
            tenant_id,
            database_id,
            vshard_id,
            &crate::control::wal_replication::ReplicableWrite::decide_for_replication(&route.plan)?,
        )?
    {
        let (payload, write_version) =
            crate::control::wal_replication::propose_replicated_entry(shared, proposer, entry)
                .await?;
        return Ok(DispatchOutcome {
            payloads: vec![payload],
            // A write carries no read watermark (Lsn::ZERO); its post-write
            // `coll_write_lsn` is surfaced via `read_version_lsn` instead.
            shard_watermarks: vec![(vshard_id, Lsn::ZERO)],
            read_version_lsn: write_version,
        });
    }

    let resp = dispatch_to_data_plane_with_txn(
        shared,
        tenant_id,
        database_id,
        vshard_id,
        route.plan,
        trace_id,
        txn_id,
    )
    .await?;
    // The remote sibling turns `ExecuteResponse.error` into `Err`; the local
    // route must reject its own error status the same way. Keeping only the
    // payload would hand a post-scan operator's failed expression back as an
    // empty success — an error status is not an empty result set.
    reject_data_plane_error(&resp)?;
    Ok(DispatchOutcome {
        payloads: vec![resp.payload.to_vec()],
        shard_watermarks: vec![(vshard_id, resp.watermark_lsn)],
        read_version_lsn: resp.read_version_lsn,
    })
}

/// Map a [`TypedClusterError`] to an internal [`Error`].
///
/// `NotLeader` is mapped such that the gateway retry loop can extract the
/// hinted leader from `Error::NotLeader.leader_node` and update the routing
/// table before the next attempt.
pub(super) fn map_typed_cluster_error(err: TypedClusterError, vshard_id: u64) -> Error {
    match err {
        TypedClusterError::NotLeader {
            leader_node_id,
            leader_addr,
            ..
        } => Error::NotLeader {
            vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
            leader_node: leader_node_id.unwrap_or(0),
            leader_addr: leader_addr.unwrap_or_default(),
        },
        TypedClusterError::DescriptorMismatch {
            collection,
            expected_version,
            actual_version,
        } => {
            // A repeating mismatch means the planner and leaseholder disagree
            // persistently — a bug, not the transient race the retry assumes.
            tracing::debug!(
                %collection,
                expected_version,
                actual_version,
                "gateway: descriptor version mismatch at leaseholder"
            );
            Error::RetryableSchemaChanged {
                descriptor: collection,
            }
        }
        TypedClusterError::DeadlineExceeded { .. } => Error::DeadlineExceeded {
            request_id: crate::types::RequestId::new(0),
        },
        // Remote Data-Plane verdict: keep the code so the client sees the
        // SQLSTATE local execution renders, not a generic internal error.
        TypedClusterError::DataPlane { code } => Error::DataPlane(code.into()),
        // Remote constraint refusal: keep the kind so the client sees 23502
        // vs 23505, exactly as a local refusal on this node would render.
        TypedClusterError::RejectedConstraint {
            collection,
            constraint,
            detail,
        } => Error::RejectedConstraint {
            collection,
            constraint,
            detail,
        },
        TypedClusterError::Internal { message, .. } => Error::Internal { detail: message },
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
    use nodedb_cluster::rpc_codec::TypedClusterError;

    #[test]
    fn map_not_leader() {
        let err = TypedClusterError::NotLeader {
            group_id: 0,
            leader_node_id: Some(5),
            leader_addr: Some("10.0.0.5:9400".into()),
            term: 3,
        };
        match map_typed_cluster_error(err, 7) {
            Error::NotLeader { leader_node, .. } => assert_eq!(leader_node, 5),
            other => panic!("expected NotLeader, got {other:?}"),
        }
    }

    #[test]
    fn map_descriptor_mismatch() {
        let err = TypedClusterError::DescriptorMismatch {
            collection: "orders".into(),
            expected_version: 1,
            actual_version: 2,
        };
        match map_typed_cluster_error(err, 0) {
            Error::RetryableSchemaChanged { descriptor } => assert_eq!(descriptor, "orders"),
            other => panic!("expected RetryableSchemaChanged, got {other:?}"),
        }
    }

    #[test]
    fn map_deadline_exceeded() {
        let err = TypedClusterError::DeadlineExceeded { elapsed_ms: 100 };
        assert!(matches!(
            map_typed_cluster_error(err, 0),
            Error::DeadlineExceeded { .. }
        ));
    }

    #[test]
    fn gateway_rejects_unadmitted_crdt_apply_before_route_selection() {
        let plan = PhysicalPlan::Crdt(nodedb_physical::physical_plan::CrdtOp::Apply {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "doc".into(),
            delta: vec![1],
            peer_id: 1,
            mutation_id: 1,
            surrogate: nodedb_types::Surrogate::ZERO,
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
