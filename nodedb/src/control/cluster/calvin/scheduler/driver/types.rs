// SPDX-License-Identifier: BUSL-1.1

//! In-flight transaction types for the Calvin scheduler driver.

use std::collections::BTreeSet;
use std::time::Instant;

use nodedb_cluster::calvin::types::SequencedTxn;

use super::super::lock_manager::{LockKey, TxnId};

/// An in-flight transaction that has been dispatched and is awaiting a
/// Data Plane response.
///
/// The executor response channel is held by a bridge task (see
/// `Scheduler::spawn_response_bridge`) that forwards completions to the
/// scheduler's fan-in `completion_rx`. This avoids polling and ensures the
/// main `select!` loop wakes the moment a response arrives.
pub(super) struct PendingTxn {
    /// Original sequenced transaction (for WAL record on completion).
    pub txn: SequencedTxn,
    /// The lock-table owner id for this txn (equals the apply-slot id unless a
    /// reservation owns the lock). Used by `on_txn_complete` to `release` the
    /// correct lock-manager identity.
    pub lock_owner: TxnId,
    /// Wall-clock time of the last stage dispatch attempt (for executor
    /// latency metrics). A re-send after a capacity refusal resets it.
    ///
    /// `Instant::now()` is used here for observability only; never
    /// influences WAL bytes.
    pub dispatch_time: Instant,
    /// Whether this vShard's slice carries a primary user data write (a non-edge
    /// Document/KV/Vector/Timeseries/Columnar/Array write). Only the primary-write
    /// participant deposits its applied `Response` (affected-count and any
    /// RETURNING rows) into `SharedState::calvin_apply_results`. The implicit-edge
    /// cleanup participants that dual-home alongside it carry no primary write and
    /// so never clobber the entry the coordinator drains.
    pub has_primary_write: bool,
    /// Whether this vShard's slice carries a RETURNING-bearing write (a plan
    /// whose applied response is DATA-ROWs, not a bare affected-count). Used at
    /// the commit tail to detect a genuine cross-shard RETURNING union: two
    /// returning-bearing participants for one txn are unsupported, whereas
    /// multiple plain-write participants (a multi-collection cross-shard COMMIT)
    /// coalesce without conflict.
    pub has_returning: bool,
    /// Participant-local Control-Plane change manifests. Consumed once after
    /// durable COMMIT apply; graph dual-home replicas are excluded at capture.
    pub change_sets: Vec<crate::control::server::dispatch_utils::WriteChangeSet>,
    /// Commit-resolution state for a static-set Calvin txn.
    ///
    /// `Some(CommitState::Staged)` for a txn dispatched, or parked for re-send,
    /// via the validate-and-stage path: its first executor response carries the local
    /// commit vote and drives a flush-or-drop before the commit tail runs.
    /// `None` for dependent/active txns, which apply directly.
    pub commit_state: Option<CommitState>,
    /// Stall deadline for a txn parked in [`CommitState::AwaitingVerdict`].
    ///
    /// `Some(instant)` only while parked: if the deadline passes with the
    /// durable global verdict still unknown, the scheduler emits a stall
    /// warning and re-arms this deadline — it NEVER releases locks and NEVER
    /// aborts (a unilateral abort while a peer may have already flushed a commit
    /// would tear the transaction). `None` in every other state.
    ///
    /// `Instant::now()` is used for this deadline (observability / liveness
    /// only; never influences WAL bytes).
    pub verdict_deadline: Option<Instant>,
    /// Error text of a stage response that was not `Ok` on this replica.
    ///
    /// `Some` when this replica never staged the txn. An abort verdict drops it
    /// as usual. A COMMIT verdict halts the scheduler, because the txn cannot
    /// apply here while its peers apply it.
    pub stage_error: Option<String>,
    /// The `TransactionRedo` record appended for a committed txn's flush,
    /// under its outcome-floor window. The window settles when the txn
    /// completes, and holds when the scheduler halts or stops with the txn
    /// still pending.
    pub redo_records: Option<crate::control::server::dispatch_utils::MintedRecords>,
    /// What a committed flush names beside its redo record, derived once
    /// from this vShard's slice when it stages.
    pub flush_scope: FlushScope,
}

/// What a vShard's committed flush carries: the collections its slice
/// writes, their materialized-sum targets, and the redo record. The Data
/// Plane installs the record the way every committed transaction installs.
///
/// The collections and sum targets are derived when the slice stages, from
/// the plans the stage sends. A flush that follows a COMMIT verdict
/// therefore never decodes or routes a plan, so it cannot fail after the
/// verdict is durable.
#[derive(Debug, Clone, Default)]
pub(super) struct FlushScope {
    pub collections: Vec<String>,
    pub sum_targets: Vec<nodedb_physical::physical_plan::RedoSumTargets>,
    /// The encoded redo record the resolve appended. Empty until the resolve
    /// answers, and empty when the slice wrote nothing on this vShard. A
    /// resent flush carries the same bytes.
    pub redo: Vec<u8>,
    /// Flushes sent for this txn. A refused install resends the flush until
    /// the count reaches its bound.
    pub sends: u32,
}

impl FlushScope {
    /// The flush scope of the local plans `plans`.
    pub(super) fn of_plans(plans: &[nodedb_physical::physical_plan::PhysicalPlan]) -> Self {
        Self {
            collections:
                crate::control::wal_replication::transaction_redo::collections::written_collections(
                    plans,
                ),
            sum_targets:
                crate::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets(
                    plans,
                ),
            // The resolve fills these once it answers.
            redo: Vec::new(),
            sends: 0,
        }
    }
}

/// Commit-resolution state of a staged static Calvin transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control::cluster::calvin::scheduler::driver) enum CommitState {
    /// Awaiting the validate-and-stage response, whose `read_set_valid` carries
    /// the local commit vote that drives the flush-or-drop decision.
    Staged,
    /// The staged txn has cast its local vote and PARKED, awaiting the durable
    /// authoritative GLOBAL verdict aggregated across all participant vShards.
    ///
    /// The scheduler holds this txn's locks and its staged Data-Plane buffer
    /// intact while parked; it resumes into a flush (verdict = commit) or a drop
    /// (verdict = abort) only once `registry.verdict(txn)` is known — via the
    /// verdict push, the probe on park, or the stall re-probe sweep. This is the
    /// cross-shard commit barrier: no participant self-decides on its local
    /// vote, so a torn commit (one shard flushes while a peer drops) is
    /// impossible.
    AwaitingVerdict,
    /// The txn committed and a `MetaOp::CalvinResolve` has been dispatched, or
    /// parked for re-send at dispatcher capacity, to resolve its staged
    /// post-images into a replayable `RedoRecord`; awaiting that response
    /// before the redo is WAL-appended and the flush dispatched.
    AwaitingRedoResolve,
    /// A flush (`committed = true`) or drop (`committed = false`) has been
    /// dispatched, or parked for re-send at dispatcher capacity; awaiting its
    /// response before the commit tail runs.
    ///
    /// `redo_lsn` is `Some(lsn)` when a `TransactionRedo` record was appended
    /// for this commit's non-empty write set — `commit_apply_tail` then only
    /// records write versions at it, since the redo record already IS the
    /// applied marker. `None` for a drop, or a committed txn whose resolved
    /// redo carried no ops (pure read / CRDT) — `commit_apply_tail` falls back
    /// to appending a `CalvinApplied` marker in that case.
    AwaitingResolve {
        committed: bool,
        redo_lsn: Option<crate::types::Lsn>,
    },
}

/// A transaction that is blocked on lock acquisition.
pub(super) struct BlockedTxn {
    pub txn: SequencedTxn,
    pub keys: BTreeSet<LockKey>,
    /// Wall-clock time at first block (for latency metrics).
    ///
    /// `Instant::now()` used for observability only.
    pub blocked_at: Instant,
}
