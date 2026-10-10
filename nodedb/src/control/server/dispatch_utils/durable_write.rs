// SPDX-License-Identifier: BUSL-1.1

//! The durable route for an autocommit write.
//!
//! A planned autocommit write reaches its engine one of two ways:
//!
//! - Replicable write: the write is proposed through Raft. Every replica
//!   applies the committed entry through the funnel, which appends the redo
//!   record. The proposing node publishes the change event. An edge write
//!   runs as a Calvin transaction at that seam instead
//!   (`planner::calvin::edge_sequencing`).
//! - A write inside a transaction block, or a plan with no replicated form:
//!   the write enters the funnel with `WalDurability::AppendHere`. The funnel
//!   appends the redo record under the write-admission guard, inside the
//!   write's outcome-floor window.
//!
//! A write dispatched any other way applies with no WAL record. A crash loses
//! it, and no replica sees it. Every caller that holds an autocommit write
//! dispatches it through this module.

use std::sync::atomic::Ordering;

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::server::shared::clone_write::CloneCheckedTask;
use crate::control::server::shared::write_admission::plan_is_write;
use crate::control::state::SharedState;
use crate::control::wal_replication::{
    ReplicableWrite, propose_replicated_entry, to_replicated_entry,
};
use crate::types::{DatabaseId, Lsn, RequestId, TenantId, TraceId, VShardId};

use super::dispatch::{
    dispatch_authorized_autocommit_write, dispatch_authorized_to_data_plane,
    dispatch_autocommit_write,
};
use super::types::AutocommitWrite;

/// Dispatch a trusted autocommit write on the durable route.
///
/// A write that carries a transaction id applies at the statement inside an
/// open block: a write the transaction cannot buffer. It is never proposed
/// on its own, so it takes the local `AppendHere` route.
pub(crate) async fn dispatch_durable_autocommit_write(
    shared: &SharedState,
    write: AutocommitWrite,
) -> crate::Result<Response> {
    if write.txn_id.is_none()
        && let Some(response) = propose_if_replicable(
            shared,
            WriteTarget {
                tenant_id: write.tenant_id,
                database_id: write.database_id,
                vshard_id: write.vshard_id,
            },
            &write.plan,
            write.event_source,
        )
        .await?
    {
        return Ok(response);
    }
    dispatch_autocommit_write(shared, write).await
}

/// Dispatch an authorized autocommit write on the durable route.
///
/// Same routing as [`dispatch_durable_autocommit_write`], for a task that
/// passed the clone-write gate and authorization.
///
/// A write whose RLS write policy is decided per row cannot be proposed
/// bare: a follower has no writing identity to decide the policy against. It
/// resolves to a concrete row set first, on this node, and the resolved write
/// is proposed (`control::write_resolve`), as the planned pgwire and native
/// writes do.
pub(crate) async fn dispatch_authorized_durable_write(
    shared: &SharedState,
    checked: CloneCheckedTask,
    trace_id: TraceId,
) -> crate::Result<Response> {
    if checked.txn_id().is_none()
        && let Some(resolver) = crate::control::write_resolve::resolver_for_plan(checked.plan())
    {
        let (authorized, _lease) = checked.into_parts();
        return crate::control::write_resolve::run_authorized_write_resolve(
            shared, authorized, resolver,
        )
        .await;
    }
    if checked.txn_id().is_none()
        && let Some(response) = propose_if_replicable(
            shared,
            WriteTarget {
                tenant_id: checked.tenant_id(),
                database_id: checked.database_id(),
                vshard_id: checked.vshard_id(),
            },
            checked.plan(),
            crate::event::EventSource::User,
        )
        .await?
    {
        return Ok(response);
    }
    dispatch_authorized_autocommit_write(shared, checked, trace_id).await
}

/// Dispatch an authorized autocommit write on the durable route, tagged with
/// `event_source`.
///
/// Same routing as [`dispatch_authorized_durable_write`], for a write that
/// runs under a source other than a client's, such as a Lite sync push. Every
/// replica stamps the source on the write's events, so a synced write does
/// not re-fire AFTER triggers.
///
/// A write whose RLS write policy must resolve against current rows before it
/// is proposed is refused: the resolved write carries none of the plan's sync
/// provenance, so the idempotency gate cannot run.
pub(crate) async fn dispatch_authorized_durable_write_with_source(
    shared: &SharedState,
    checked: CloneCheckedTask,
    trace_id: TraceId,
    event_source: crate::event::EventSource,
) -> crate::Result<Response> {
    if checked.txn_id().is_none()
        && crate::control::write_resolve::resolver_for_plan(checked.plan()).is_some()
    {
        return Err(crate::Error::PlanError {
            detail: format!(
                "a {event_source:?} write to '{}' needs its row-level-security policy resolved \
                 before it is proposed, and the resolved write cannot carry its sync provenance",
                checked.plan().collection().unwrap_or("<unknown>")
            ),
        });
    }
    if checked.txn_id().is_none()
        && let Some(response) = propose_if_replicable(
            shared,
            WriteTarget {
                tenant_id: checked.tenant_id(),
                database_id: checked.database_id(),
                vshard_id: checked.vshard_id(),
            },
            checked.plan(),
            event_source,
        )
        .await?
    {
        return Ok(response);
    }
    super::dispatch::dispatch_authorized_autocommit_write_with_source(
        shared,
        checked,
        trace_id,
        event_source,
    )
    .await
}

/// Dispatch one authorized task by its class: a write on the durable route,
/// anything else on the read route.
///
/// For a call site that carries both reads and writes on this node's cores:
/// the transaction meta-ops and the DML statement dispatch.
pub(crate) async fn dispatch_authorized_task_by_class(
    shared: &SharedState,
    checked: CloneCheckedTask,
    trace_id: TraceId,
) -> crate::Result<Response> {
    if plan_is_write(checked.plan()) {
        dispatch_authorized_durable_write(shared, checked, trace_id).await
    } else {
        dispatch_authorized_to_data_plane(shared, checked, trace_id).await
    }
}

/// The coordinates a write is proposed under.
struct WriteTarget {
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
}

/// Propose `plan` through Raft when the plan encodes to a replicated entry.
/// `None` means the plan has no replicated form and takes the local route.
///
/// A Data-Plane verdict comes back as `Err(Error::DataPlane(code))`, the shape
/// the pgwire replicated path returns.
async fn propose_if_replicable(
    shared: &SharedState,
    target: WriteTarget,
    plan: &PhysicalPlan,
    event_source: crate::event::EventSource,
) -> crate::Result<Option<Response>> {
    let proposer = shared.async_raft_proposer()?;
    let WriteTarget {
        tenant_id,
        database_id,
        vshard_id,
    } = target;
    // The entry carries resolved rows: a timeseries ingest resolves here,
    // on the proposer, before the entry exists.
    let resolved = crate::control::write_resolve::resolve_for_log(
        shared,
        crate::control::write_resolve::WriteResolveContext {
            tenant_id,
            database_id,
        },
        vshard_id,
        plan,
    )
    .await?;
    let plan = resolved.as_ref().unwrap_or(plan);
    let replicable = ReplicableWrite::decide_for_replication(plan)?;
    let Some(entry) = to_replicated_entry(tenant_id, database_id, vshard_id, &replicable)? else {
        return Ok(None);
    };
    let entry = entry.with_event_source(event_source);
    let deadline = crate::control::wal_replication::statement_propose_deadline(shared);
    let (payload, write_versions) =
        propose_replicated_entry(shared, proposer, entry, deadline).await?;
    Ok(Some(replicated_write_response(
        shared,
        payload,
        write_versions,
    )))
}

/// The response a committed and applied replicated write answers with.
///
/// `write_versions` are the versions the write stamped. A session floors its
/// later reads of the written vShards at them. A write carries no read
/// watermark.
fn replicated_write_response(
    shared: &SharedState,
    payload: Vec<u8>,
    write_versions: crate::types::ReadVersions,
) -> Response {
    Response {
        request_id: RequestId::new(shared.request_id_counter.fetch_add(1, Ordering::Relaxed)),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: payload.into(),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
        stage_vote: None,
        read_versions: write_versions,
        write_set: Vec::new(),
    }
}
