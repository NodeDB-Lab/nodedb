// SPDX-License-Identifier: BUSL-1.1

//! The durable route for an autocommit write.
//!
//! A planned autocommit write reaches its engine one of two ways:
//!
//! - Cluster mode, replicable write: the write is proposed through Raft. Every
//!   replica applies the committed entry through the funnel, which appends the
//!   redo record. The proposing node publishes the change event.
//! - Otherwise: the write enters the funnel with `WalDurability::AppendHere`.
//!   The funnel appends the redo record under the write-admission guard, inside
//!   the write's outcome-floor window.
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

use super::change_events::{extract_write_change_set, publish_change_set_with_lsn};
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
/// In cluster mode a write whose RLS write policy is decided per row cannot
/// be proposed bare: a follower has no writing identity to decide the policy
/// against. It resolves to a concrete row set first, on this node, and the
/// resolved write is proposed (`control::write_resolve`), as the planned
/// pgwire and native writes do.
pub(crate) async fn dispatch_authorized_durable_write(
    shared: &SharedState,
    checked: CloneCheckedTask,
    trace_id: TraceId,
) -> crate::Result<Response> {
    if checked.txn_id().is_none()
        && shared.async_raft_proposer().is_some()
        && let Some(resolver) = crate::control::write_resolve::resolver_for_plan(checked.plan())
    {
        return crate::control::write_resolve::run_authorized_write_resolve(
            shared,
            checked.into_authorized(),
            resolver,
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
/// A clustered write whose RLS write policy must resolve against current rows
/// before it is proposed is refused: the resolved write carries none of the
/// plan's sync provenance, so the idempotency gate could not run.
pub(crate) async fn dispatch_authorized_durable_write_with_source(
    shared: &SharedState,
    checked: CloneCheckedTask,
    trace_id: TraceId,
    event_source: crate::event::EventSource,
) -> crate::Result<Response> {
    if checked.txn_id().is_none()
        && shared.async_raft_proposer().is_some()
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
/// For a transport fallback that dispatches reads and writes through one call
/// site with no gateway installed.
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

/// Propose `plan` through Raft when this node runs a proposer and the plan
/// encodes to a replicated entry. `None` means the write takes the local route.
///
/// A Data-Plane verdict comes back as `Err(Error::DataPlane(code))`, the shape
/// the pgwire replicated path returns.
async fn propose_if_replicable(
    shared: &SharedState,
    target: WriteTarget,
    plan: &PhysicalPlan,
    event_source: crate::event::EventSource,
) -> crate::Result<Option<Response>> {
    let Some(proposer) = shared.async_raft_proposer() else {
        return Ok(None);
    };
    let WriteTarget {
        tenant_id,
        database_id,
        vshard_id,
    } = target;
    let replicable = ReplicableWrite::decide_for_replication(plan)?;
    let Some(entry) = to_replicated_entry(tenant_id, database_id, vshard_id, &replicable)? else {
        return Ok(None);
    };
    let entry = entry.with_event_source(event_source);
    let (payload, write_version) = propose_replicated_entry(shared, proposer, entry).await?;
    // Replicas apply with `ChangeFeedOwner::Unowned`. The proposing node
    // handled the write once, so it publishes the change event.
    publish_change_set_with_lsn(
        shared,
        tenant_id,
        database_id,
        extract_write_change_set(plan, tenant_id),
        write_version,
    );
    Ok(Some(replicated_write_response(
        shared,
        payload,
        write_version,
    )))
}

/// The response a committed and applied replicated write answers with.
///
/// `write_version` is the written collection's `coll_write_lsn` after the
/// write. It is the watermark and the read version, so a session can floor a
/// later read at it.
fn replicated_write_response(
    shared: &SharedState,
    payload: Vec<u8>,
    write_version: Lsn,
) -> Response {
    Response {
        request_id: RequestId::new(shared.request_id_counter.fetch_add(1, Ordering::Relaxed)),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: payload.into(),
        watermark_lsn: write_version,
        error_code: None,
        read_set_valid: None,
        read_version_lsn: write_version,
        write_set: Vec::new(),
    }
}
