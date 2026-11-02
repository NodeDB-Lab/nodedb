// SPDX-License-Identifier: BUSL-1.1

//! The funnel entries that append no WAL record of their own: in-transaction
//! tasks and routed reads.

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

use super::dispatch::dispatch_to_data_plane_inner;
use super::submit_write::WalDurability;
use super::types::{DataPlaneDispatch, ReadRoute};

/// Dispatch a physical plan to the Data Plane carrying an explicit transaction
/// id so the Data Plane can resolve this transaction's staging overlay
/// (read-your-own-writes) and route `StageWrite`. Used by the native endpoint,
/// whose in-transaction tasks flow through this shared path.
///
/// A read is strong and runs where the group that owns `vshard_id` serves it
/// (`owner_read`).
///
/// It appends no WAL record, so it refuses a write that only the funnel's
/// `AppendHere` route logs. A staged write is not such a write: COMMIT logs it.
pub(crate) async fn dispatch_to_data_plane_with_txn(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<crate::types::TxnId>,
) -> crate::Result<Response> {
    dispatch_unlogged(
        shared,
        UnloggedDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            txn_id,
        },
        ReadRoute::Owned,
    )
    .await
}

/// [`dispatch_to_data_plane_with_txn`] for a plan the caller already routed
/// to this node and confirmed as its session requires: the gateway's local
/// route, or a leg another node sent here. A read runs on this node as is.
pub(crate) async fn dispatch_routed_read_to_data_plane(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<crate::types::TxnId>,
) -> crate::Result<Response> {
    dispatch_unlogged(
        shared,
        UnloggedDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            txn_id,
        },
        ReadRoute::Routed,
    )
    .await
}

/// One dispatch that appends no WAL record of its own.
struct UnloggedDispatch {
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<crate::types::TxnId>,
}

async fn dispatch_unlogged(
    shared: &SharedState,
    dispatch: UnloggedDispatch,
    read_route: ReadRoute,
) -> crate::Result<Response> {
    let UnloggedDispatch {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        txn_id,
    } = dispatch;
    super::durability_barrier::refuse_unlogged_write(&plan)?;
    dispatch_to_data_plane_inner(
        shared,
        DataPlaneDispatch {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id,
            event_source: crate::event::EventSource::User,
            txn_id,
            // A staged write is judged at COMMIT, against the roles its redo
            // record carries.
            user_roles: Vec::new(),
            // Staged in-transaction writes are not yet durably committed; the
            // committed write is noted at COMMIT via the batch funnel, so
            // durability is not the funnel's to append here.
            durability: WalDurability::CallerSupplied {
                wal_lsn: None,
                resolved_now_ms: None,
                minted: None,
                system_ordinal: None,
            },
            read_route,
            change_feed: super::submit_write::ChangeFeedOwner::LocalApply,
        },
    )
    .await
}
