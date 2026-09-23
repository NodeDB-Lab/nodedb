// SPDX-License-Identifier: BUSL-1.1

//! Request-identity types passed into the dispatch core: the write's target
//! coordinates plus its WAL-durability handling.

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

/// Identity of a single autocommit write whose WAL append the core owns. Unlike
/// [`WriteDispatch`] it carries no `wal_lsn` / `resolved_now_ms`: those are
/// minted inside `dispatch_to_data_plane_inner`, under the write-admission
/// guard, so LSN-allocation order matches dispatcher-enqueue order per key.
pub(crate) struct AutocommitWrite {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
    pub plan: PhysicalPlan,
    pub trace_id: TraceId,
    pub event_source: crate::event::EventSource,
    pub txn_id: Option<crate::types::TxnId>,
}

/// Identity + WAL LSN of a single autocommit write dispatched to the Data
/// Plane. Bundles the fields so `dispatch_write_to_data_plane` avoids a long
/// positional argument list.
pub(crate) struct WriteDispatch {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
    pub plan: PhysicalPlan,
    pub trace_id: TraceId,
    pub event_source: crate::event::EventSource,
    pub txn_id: Option<crate::types::TxnId>,
    pub wal_lsn: Option<crate::types::Lsn>,
    /// Wall-clock instant (ms since epoch) the Control Plane resolved at
    /// WAL-append time: a TTL-bearing KV write's expiry base, or a timeseries
    /// ingest's default row timestamp. Stamped onto the `Request` (same as
    /// `wal_lsn`) so the Data Plane installs the SAME instant the durable WAL
    /// record carries instead of re-reading the clock at apply time. `None`
    /// for reads and other writes.
    pub resolved_now_ms: Option<u64>,
}

/// Inputs for `dispatch_to_data_plane_inner`: the Data Plane request identity
/// plus the write's event source and optional owning transaction.
pub(super) struct DataPlaneDispatch {
    pub(super) tenant_id: TenantId,
    pub(super) database_id: DatabaseId,
    pub(super) vshard_id: VShardId,
    pub(super) plan: PhysicalPlan,
    pub(super) trace_id: TraceId,
    pub(super) event_source: crate::event::EventSource,
    pub(super) txn_id: Option<crate::types::TxnId>,
    /// Who owns this write's durable redo record — the funnel appends it under
    /// the write-admission guard, or the caller already recorded durability
    /// elsewhere and supplies the LSN it minted.
    pub(super) durability: super::submit_write::WalDurability,
}
