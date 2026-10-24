// SPDX-License-Identifier: BUSL-1.1

//! Input and output types for [`super::submit_write`].
//!
//! Pulled out of the funnel file itself so the ordering-sensitive logic in
//! `funnel.rs` stays under the file-size limit without touching a single
//! statement in the guard/append/enqueue/await/durability sequence — these
//! are pure data definitions with no behavior of their own.

use std::sync::Arc;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::server::dispatch_utils::minted::MintedRecords;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, TxnId, VShardId};

/// Who owns this write's durable redo record.
pub(crate) enum WalDurability {
    /// The funnel appends the redo itself — under the write-admission guard,
    /// immediately before the enqueue — and stamps the minted LSN onto the
    /// `Request`. Minting the LSN after admission and right before the enqueue
    /// is what makes WAL-LSN order equal dispatcher-enqueue order per key; the
    /// strict-FIFO per-database WFQ then makes apply order follow enqueue
    /// order, so restart replay (in LSN order) cannot diverge from live state.
    ///
    /// `apply_key` is the idempotency key of the replicated proposal this
    /// write applies, `0` for a write no proposal carries. Every record the
    /// funnel appends for the write carries it in its header, so the record
    /// names the proposal it applied in the same durable write.
    ///
    /// `commit_hlc` is the HLC wall time, in nanoseconds, at which the write
    /// committed upstream: the proposer's stamp on a replicated entry. `None`
    /// when this append is the commit, so the funnel stamps the instant of the
    /// append itself.
    ///
    /// `change_position` is the Raft log position of the data-group entry this
    /// write applies, `None` for a write no entry carries. The funnel makes it
    /// durable ahead of the redo and records it for the CDC router, so every
    /// replica positions the write's change events alike.
    ///
    /// `system_ordinal` is the system-time ordinal the leader of that entry
    /// assigned, `None` for a write no entry carries. Every replica stamps
    /// the write's bitemporal versions from it, so every replica writes the
    /// same version keys.
    AppendHere {
        now_override: Option<u64>,
        apply_key: u64,
        commit_hlc: Option<u64>,
        change_position: Option<crate::event::cdc::position::ReplicatedPosition>,
        system_ordinal: Option<i64>,
    },
    /// The caller already recorded this write's durability elsewhere — COMMIT's
    /// single `Transaction` record, the procedural batch flush, a trigger /
    /// sync path that owns its own funnel — and supplies the LSN it minted.
    /// The funnel appends nothing and stamps these values through unchanged;
    /// the supplied LSN names the record that replays this write.
    ///
    /// `minted` holds the records the caller appended for this write under
    /// their outcome-floor window. The funnel closes the window from the
    /// write's outcome: it cancels the records on a refusal that applied
    /// nothing, and on a Calvin route that applies the write from its own
    /// records.
    CallerSupplied {
        wal_lsn: Option<Lsn>,
        resolved_now_ms: Option<u64>,
        minted: Option<MintedRecords>,
    },
}

impl WalDurability {
    /// Whether the caller supplied records it appended for this write.
    pub(crate) fn has_minted(&self) -> bool {
        matches!(
            self,
            Self::CallerSupplied {
                minted: Some(_),
                ..
            }
        )
    }

    /// Take the caller's minted records out, leaving `None` in their place.
    pub(crate) fn take_minted(&mut self) -> Option<MintedRecords> {
        match self {
            Self::AppendHere { .. } => None,
            Self::CallerSupplied { minted, .. } => minted.take(),
        }
    }
}

/// Where this write's ordering was decided.
pub(crate) enum WriteOrdering {
    /// Run the write-admission gate: fast path, per-key order lock, or a route
    /// through the deterministic scheduler.
    Gate,
    /// Ordering was decided upstream and must not be re-decided. The Raft data
    /// group committed this entry at a fixed log index and every replica
    /// applies it in exactly that order; re-entering the gate can route it
    /// back through Calvin or block it behind a lock it does not need.
    AlreadyOrdered,
}

/// Who owns emitting this write's Control-Plane change event.
pub(crate) enum ChangeFeedOwner {
    /// The write applies on this node alone, outside any replicated entry:
    /// a staged write inside a transaction, or a plan with no replicated
    /// form. It has no position on any feed, so the funnel refuses one that
    /// yields change events.
    LocalApply,
    /// The write applies the committed data-group entry at `(group_id,
    /// log_index)`. Every replica applies it, and every replica stages its
    /// change events under that entry once the apply succeeds. The apply
    /// loop publishes them when it settles the entry in log order, so every
    /// replica emits the group's feed at the same positions.
    Replicated { group_id: u64, log_index: u64 },
    /// The funnel emits no change event for this write: its rows reach
    /// subscribers another way.
    Unowned,
}

/// What [`super::submit_write`] produced: the Data Plane's answer.
pub(crate) struct SubmitOutcome {
    /// The Data Plane's `Response` verbatim — including one whose `status` is
    /// `Error`. Callers that need an error status surfaced as a typed error
    /// check `status` themselves.
    pub response: Response,
}

/// Inputs for [`super::submit_write`].
pub(crate) struct SubmitWrite {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
    pub plan: PhysicalPlan,
    pub trace_id: TraceId,
    pub event_source: crate::event::EventSource,
    pub txn_id: Option<TxnId>,
    /// DML audit attribution. `None` for system-generated writes.
    pub user_id: Option<Arc<str>>,
    /// The roles of the identity that wrote the write. The Data Plane judges
    /// a role-guarded state transition against them. Empty for a write no
    /// user made.
    pub user_roles: Vec<String>,
    pub durability: WalDurability,
    pub ordering: WriteOrdering,
    pub change_feed: ChangeFeedOwner,
}
