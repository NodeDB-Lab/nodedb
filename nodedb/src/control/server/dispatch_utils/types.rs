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
    /// The roles of the identity that wrote the write. Empty for a write no
    /// user made.
    pub user_roles: Vec<String>,
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
    /// The records the caller appended for this write, under their
    /// outcome-floor window. The funnel closes the window from the write's
    /// outcome. `None` when the caller appended nothing for this dispatch.
    pub minted: Option<super::minted::MintedRecords>,
    /// The system-time ordinal the caller's records carry, `None` when they
    /// carry none or the caller appended nothing. The apply stamps it.
    pub system_ordinal: Option<i64>,
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
    /// The roles of the identity that wrote the write.
    pub(super) user_roles: Vec<String>,
    /// Who owns this write's durable redo record — the funnel appends it under
    /// the write-admission guard, or the caller already recorded durability
    /// elsewhere and supplies the LSN it minted.
    pub(super) durability: super::submit_write::WalDurability,
    /// Where a read in this dispatch runs. Writes ignore it.
    pub(super) read_route: ReadRoute,
    /// Whether the write publishes its change events on this node's feed.
    pub(super) change_feed: super::submit_write::ChangeFeedOwner,
}

/// Where the funnel runs a read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadRoute {
    /// The caller routed the read to this node and confirmed it as its
    /// session requires (the gateway's local route, a received leg). It runs
    /// here as is.
    Routed,
    /// No caller routed the read. It is a strong read, and the funnel serves
    /// it from the group that owns its vShard (`owner_read`).
    Owned,
}
