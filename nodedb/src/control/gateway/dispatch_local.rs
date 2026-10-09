// SPDX-License-Identifier: BUSL-1.1

//! Local route dispatch: a plan this node applies on its own cores, over
//! the SPSC bridge or through a Raft proposal.

use std::sync::Arc;

use crate::Error;
use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::local_dispatch::reject_data_plane_error;
use crate::control::server::dispatch_utils::{
    AutocommitWrite, dispatch_autocommit_write, dispatch_routed_read_to_data_plane,
};
use crate::control::server::shared::write_admission::plan_is_write;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, TxnId, VShardId};

use super::dispatcher::DispatchOutcome;
use super::route::TaskRoute;
use super::router::is_task_vshard_scoped;
use super::version_check::check_local_descriptor_versions;
use super::version_set::GatewayVersionSet;

/// Session and trace context of one local dispatch.
pub(super) struct LocalContext<'a> {
    pub(super) shared: &'a Arc<SharedState>,
    pub(super) tenant_id: TenantId,
    pub(super) database_id: DatabaseId,
    pub(super) trace_id: TraceId,
    pub(super) txn_id: Option<TxnId>,
    pub(super) version_set: &'a GatewayVersionSet,
}

/// Local dispatch via SPSC bridge.
///
/// Carries `txn_id` so the Data Plane can resolve this session transaction's
/// staging overlay (read-your-own-writes) for in-block SQL and direct ops.
pub(super) async fn dispatch_local(
    route: TaskRoute,
    ctx: LocalContext<'_>,
) -> Result<DispatchOutcome, Error> {
    let LocalContext {
        shared,
        tenant_id,
        database_id,
        trace_id,
        txn_id,
        version_set,
    } = ctx;
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

    // A proposal carries resolved rows: the proposer resolves a timeseries
    // ingest before its entry exists.
    let resolved = if txn_id.is_none() {
        crate::control::write_resolve::resolve_for_log(
            shared,
            crate::control::write_resolve::WriteResolveContext {
                tenant_id,
                database_id,
            },
            vshard_id,
            &route.plan,
        )
        .await?
    } else {
        None
    };
    let plan = resolved.unwrap_or(route.plan);

    if txn_id.is_none()
        && let Some(entry) = crate::control::wal_replication::to_replicated_entry(
            tenant_id,
            database_id,
            vshard_id,
            &crate::control::wal_replication::ReplicableWrite::decide_for_replication(&plan)?,
        )?
    {
        let proposer = shared.async_raft_proposer()?;
        let (payload, write_version) = crate::control::wal_replication::propose_replicated_entry(
            shared,
            proposer,
            entry,
            crate::control::wal_replication::statement_propose_deadline(shared),
        )
        .await?;
        return Ok(DispatchOutcome {
            payloads: vec![payload],
            // A write carries no read watermark (Lsn::ZERO); its post-write
            // `coll_write_lsn` is surfaced via `read_version_lsn` instead.
            shard_watermarks: vec![(vshard_id, Lsn::ZERO)],
            read_version_lsn: write_version,
            not_found: false,
        });
    }

    let resp = dispatch_local_plan(LocalPlan {
        shared,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        txn_id,
    })
    .await?;
    // The remote sibling turns `ExecuteResponse.error` into `Err`; the local
    // route must reject its own error status the same way. Keeping only the
    // payload hands a post-scan operator's failed expression back as an
    // empty success — an error status is not an empty result set.
    reject_data_plane_error(&resp)?;
    Ok(DispatchOutcome {
        payloads: vec![resp.payload.to_vec()],
        shard_watermarks: vec![(vshard_id, resp.watermark_lsn)],
        read_version_lsn: resp.read_version_lsn,
        not_found: is_not_found(&resp),
    })
}

/// One plan this node applies on its own cores.
struct LocalPlan<'a> {
    shared: &'a Arc<SharedState>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
}

/// Dispatch a plan to this node's cores on the route its class needs.
///
/// A base-state write enters the funnel with `AppendHere`, which appends its
/// redo record under the write-admission guard. It reaches here when no Raft
/// proposal carries it: a plan with no replicated encoding, or a write a
/// transaction cannot buffer. The transaction meta-ops
/// own their durability, and a staged write is logged at COMMIT, so both take
/// the read route with everything else.
///
/// An autocommit CRDT op that moves the Loro frontier runs inside the vShard's
/// admission sequencer. A replicated CRDT write is serialized by the
/// sequenced proposer, so this covers the frontier ops with no replicated
/// form. Without it, such an op can interleave with an admission preview
/// and its fenced apply.
async fn dispatch_local_plan(local: LocalPlan<'_>) -> Result<Response, Error> {
    let LocalPlan {
        shared,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        txn_id,
    } = local;
    if plan_is_write(&plan) && !is_task_vshard_scoped(&plan) {
        let frontier_mutation = txn_id.is_none()
            && matches!(
                &plan,
                PhysicalPlan::Crdt(op) if crate::control::crdt_admission::changes_crdt_frontier(op)
            );
        let write = AutocommitWrite {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source: crate::event::EventSource::User,
            txn_id,
        };
        if frontier_mutation {
            return shared
                .vshard_admission_sequencer
                .run(vshard_id, || dispatch_autocommit_write(shared, write))
                .await;
        }
        return dispatch_autocommit_write(shared, write).await;
    }
    dispatch_routed_read_to_data_plane(
        shared,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        txn_id,
    )
    .await
}

/// Whether the core refused the task with `ErrorCode::NotFound`.
///
/// `reject_data_plane_error` passes this refusal as an empty success.
/// The flag keeps the verdict for a caller that needs it.
fn is_not_found(resp: &Response) -> bool {
    resp.status == Status::Error && resp.error_code.as_deref() == Some(&ErrorCode::NotFound)
}
