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
    /// `Request`. Minting the LSN after admission and just before the enqueue
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
    AppendHere {
        now_override: Option<u64>,
        apply_key: u64,
        commit_hlc: Option<u64>,
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
    /// applies it in exactly that order; re-entering the gate could route it
    /// back through Calvin or block it behind a lock it does not need.
    AlreadyOrdered,
}

/// Who owns emitting this write's Control-Plane change event.
pub(crate) enum ChangeFeedOwner {
    /// The funnel extracts the write's change metadata from the plan and
    /// publishes it once the apply succeeds. This is the route for every write
    /// this node both handles and applies itself — the autocommit / internal
    /// funnel, the pgwire SQL path's local dispatch, and the array executor's
    /// single-node write — and it is what carries those writes to `/cdc` and
    /// WS-RPC subscribers.
    Funnel,
    /// The funnel emits no change event for this write, because the node that
    /// handled the write already emitted it.
    ///
    /// This is the route for a submit that applies a Raft-committed entry (the
    /// data-group apply loop and the array apply path). Those run on EVERY
    /// replica: publishing here would emit one event per replica, each with its
    /// own cluster-wide NOTIFY fan-out to every peer, and no dedup exists on
    /// either side — a subscriber would silently see the write once per
    /// replica, multiplied again by the fan-out. The proposing node handled the
    /// write exactly once and publishes there instead, after commit + apply
    /// (see `publish_origin_change_events`).
    Unowned,
}

/// What [`super::submit_write`] produced: the Data Plane's answer, and the LSN of the
/// record that reproduces this write on replay.
pub(crate) struct SubmitOutcome {
    /// The Data Plane's `Response` verbatim — including one whose `status` is
    /// `Error`. Callers that need an error status surfaced as a typed error
    /// check `status` themselves.
    pub response: Response,
    /// The forward write's redo LSN: minted here for `AppendHere`, echoed from
    /// the caller for `CallerSupplied`. `None` when this write mints no record
    /// of its own — a read / control op, a plan whose variant appends nothing
    /// (an array `Flush` reorganizes tiles already durable via their `Put`
    /// records), or a Calvin-routed write whose durability the scheduler owns.
    /// It is NOT a "no durability" signal, and no caller may substitute a
    /// fabricated LSN for it.
    pub wal_lsn: Option<Lsn>,
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
    pub durability: WalDurability,
    pub ordering: WriteOrdering,
    pub change_feed: ChangeFeedOwner,
}
