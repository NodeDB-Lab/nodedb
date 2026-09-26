// SPDX-License-Identifier: BUSL-1.1

//! Sync dispatch that returns a full [`Response`].
//!
//! Used by the columnar, timeseries, FTS, spatial, and vector sync handlers,
//! which need the raw `Response` to extract the payload themselves.

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::server::dispatch_utils::{MintedRecords, RecordOwner};
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicableWrite, to_replicated_entry};
use crate::event::EventSource;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, VShardId};

use super::admission_guard::reject_unadmitted_crdt_apply;
use super::propose::propose_sync_write;

/// Parameters for [`dispatch_sync_response_inner`], bundled to keep the
/// argument list under clippy's `too_many_arguments` threshold.
struct SyncResponseDispatch {
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    event_source: EventSource,
    /// The redo records the caller already appended for this write, under
    /// their outcome-floor window, or `None` when the caller minted none. See
    /// [`dispatch_authorized_sync_response`] for the durability contract.
    minted: Option<MintedRecords>,
}

/// `minted` holds the caller's already-appended redo records, or `None`.
/// Threaded into the write funnel so the durable-at-ack barrier fsyncs them
/// before this returns — sync handlers ack their peer off this return value.
/// The funnel closes their outcome-floor window from the write's outcome.
pub(crate) async fn dispatch_authorized_sync_response(
    state: &SharedState,
    authorized: AuthorizedTask,
    trace_id: TraceId,
    event_source: EventSource,
    minted: Option<MintedRecords>,
) -> crate::Result<Response> {
    let task = authorized.into_physical_task();
    dispatch_sync_response_inner(
        state,
        SyncResponseDispatch {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            vshard_id: task.vshard_id,
            plan: task.plan,
            trace_id,
            event_source,
            minted,
        },
    )
    .await
}

/// Trusted-internal sync-shaped dispatch of a write whose redo records the
/// caller already appended under `minted`. Used by DDL paths that append
/// their own record before dispatch.
pub(crate) async fn dispatch_trusted_internal_minted_sync_response(
    state: &SharedState,
    owner: RecordOwner,
    plan: PhysicalPlan,
    event_source: EventSource,
    minted: MintedRecords,
) -> crate::Result<Response> {
    dispatch_sync_response_inner(
        state,
        SyncResponseDispatch {
            tenant_id: owner.tenant_id,
            database_id: owner.database_id,
            vshard_id: owner.vshard_id,
            plan,
            trace_id: TraceId::ZERO,
            event_source,
            minted: Some(minted),
        },
    )
    .await
}

/// Trusted-internal sync-shaped dispatch used by DDL index maintenance.
///
/// These callers mint no redo of their own since the driving DDL is already durable.
pub(crate) async fn dispatch_trusted_internal_sync_response(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    event_source: EventSource,
) -> crate::Result<Response> {
    dispatch_sync_response_inner(
        state,
        SyncResponseDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            minted: None,
        },
    )
    .await
}

/// Cluster path: proposes through Raft, wraps the payload in `Status::Ok`. Gate
/// verdict travels in the payload; non-`Ok` means a protocol error, not a gate
/// rejection. The Raft entry's apply appends its own records, so the caller's
/// records are cancelled. Single-node path carries the caller's records
/// through the write funnel, which closes them from the write's outcome.
async fn dispatch_sync_response_inner(
    state: &SharedState,
    params: SyncResponseDispatch,
) -> crate::Result<Response> {
    let SyncResponseDispatch {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
        minted,
    } = params;
    let owner = RecordOwner {
        tenant_id,
        database_id,
        vshard_id,
    };
    if let Err(error) = reject_unadmitted_crdt_apply(&plan) {
        // Nothing was dispatched.
        if let Some(minted) = minted {
            minted.cancel(&state.wal, owner, 0).await?;
        }
        return Err(error);
    }
    let replicated = match state.async_raft_proposer() {
        Some(proposer) => {
            let entry = ReplicableWrite::decide_for_replication(&plan).and_then(|replicable| {
                to_replicated_entry(tenant_id, database_id, vshard_id, &replicable)
                    .map(|entry| entry.map(|entry| entry.with_event_source(event_source)))
            });
            match entry {
                Ok(entry) => entry.map(|entry| (proposer, entry)),
                Err(error) => {
                    if let Some(minted) = minted {
                        minted.cancel(&state.wal, owner, 0).await?;
                    }
                    return Err(error);
                }
            }
        }
        None => None,
    };
    if let Some((proposer, entry)) = replicated {
        // The Raft entry's apply appends its own records.
        let superseded = minted.map(|minted| {
            minted.supersede(std::sync::Arc::clone(&state.wal), owner, "raft_proposal")
        });
        let proposed = propose_sync_write(state, entry, proposer).await;
        if let Some(superseded) = superseded {
            superseded.finish().await;
        }
        let payload = proposed?;
        let request_id = state.next_request_id();
        return Ok(Response {
            request_id,
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: payload.into(),
            watermark_lsn: Lsn::new(0),
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        });
    }

    crate::control::server::dispatch_utils::dispatch_trusted_internal_write_to_data_plane(
        state,
        crate::control::server::dispatch_utils::WriteDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source,
            txn_id: None,
            // Caller already appended this write's redo; funnel must not append a
            // second one — stamps this LSN and waits at the durable-at-ack barrier.
            wal_lsn: minted.as_ref().and_then(MintedRecords::highest),
            // Only a TTL-bearing KV write resolves a wall-clock instant, and KV has no sync handler.
            resolved_now_ms: None,
            minted,
        },
    )
    .await
}

/// Sync-path convenience: dispatches `plan` tagged [`EventSource::CrdtSync`],
/// returns the payload bytes. `minted` isn't optional in spirit — these
/// engines rebuild only by WAL replay, so acking without it loses a write on `kill -9`.
pub(crate) async fn dispatch_sync_payload(
    state: &SharedState,
    authorized: AuthorizedTask,
    minted: Option<MintedRecords>,
) -> crate::Result<Vec<u8>> {
    let response = dispatch_authorized_sync_response(
        state,
        authorized,
        TraceId::ZERO,
        EventSource::CrdtSync,
        minted,
    )
    .await?;
    Ok(response.payload.to_vec())
}

/// Build the loud error every `NoOp*Dispatcher` returns when a sync op reaches
/// a path lacking `SharedState` — such a path would ACK the client while
/// silently dropping the write. `op` names the operation for the diagnostic.
pub fn noop_dispatch_error(op: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "{op} routed through path lacking SharedState; \
             check listener wiring — {op} was ACKed but NOT applied"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::durability_test_support::{
        append_buffered_record, authorized_write, fixture, minted_buffered_record, respond_once,
    };
    use super::dispatch_sync_payload;

    /// Guards against acking a peer while the redo is still buffered: those
    /// engines rebuild only from WAL replay, so a `kill -9` would erase an
    /// acked write the peer never re-sends.
    #[tokio::test]
    async fn a_supplied_lsn_is_fsync_durable_before_the_payload_returns() {
        let (state, side, _directory) = fixture();
        let (minted, lsn) = minted_buffered_record(&state);
        assert!(
            state.wal.durable_through() < lsn.as_u64(),
            "the append must only buffer, or this test proves nothing"
        );
        let authorized = authorized_write(&state);

        let responder = tokio::spawn(respond_once(Arc::clone(&state), side));
        dispatch_sync_payload(&state, authorized, Some(minted))
            .await
            .expect("sync dispatch succeeds");
        responder.await.expect("responder completes");

        assert!(
            state.wal.durable_through() >= lsn.as_u64(),
            "the supplied redo must be fsync-durable before the peer is acked"
        );
    }

    /// The counterpart: a caller that appended nothing supplies no LSN and the
    /// funnel has nothing to wait on. This is what makes the assertion above a
    /// statement about the threaded LSN rather than about dispatch in general.
    #[tokio::test]
    async fn no_supplied_lsn_leaves_an_unrelated_buffered_record_alone() {
        let (state, side, _directory) = fixture();
        let lsn = append_buffered_record(&state);
        let authorized = authorized_write(&state);

        let responder = tokio::spawn(respond_once(Arc::clone(&state), side));
        dispatch_sync_payload(&state, authorized, None)
            .await
            .expect("sync dispatch succeeds");
        responder.await.expect("responder completes");

        assert!(
            state.wal.durable_through() < lsn.as_u64(),
            "nothing appended by this dispatch means nothing to fsync"
        );
    }
}
