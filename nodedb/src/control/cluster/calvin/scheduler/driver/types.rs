// SPDX-License-Identifier: BUSL-1.1

//! In-flight transaction types for the Calvin scheduler driver.

use std::collections::BTreeMap;
use std::time::Instant;

use nodedb_cluster::calvin::types::SequencedTxn;

use super::super::lock_manager::{LockKey, LockMode, TxnId};

/// A transaction this vShard granted and has not finished.
///
/// On the data-group leader it stages, votes, resolves and proposes its
/// redo. On a follower it waits in [`CommitState::Following`] for its
/// verdict and its stamped redo.
///
/// The executor response channel is held by a bridge task (see
/// `Scheduler::spawn_response_bridge`) that forwards completions to the
/// scheduler's fan-in `completion_rx`. This avoids polling and ensures the
/// main `select!` loop wakes the moment a response arrives.
pub(super) struct PendingTxn {
    /// Original sequenced transaction.
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
    /// RETURNING rows) into `CalvinLocalState::apply_results`. The implicit-edge
    /// cleanup participants that dual-home alongside it carry no primary write and
    /// so never clobber the entry the coordinator drains.
    pub has_primary_write: bool,
    /// Whether the slice's install raises its tenant's write mark: a
    /// primary write that changes a tenant row. A schema install raises none.
    pub raises_write_mark: bool,
    /// Whether this vShard's slice carries a RETURNING-bearing write (a plan
    /// whose applied response is DATA-ROWs, not a bare affected-count). Two
    /// returning-bearing participants for one txn are an unsupported
    /// cross-shard RETURNING union.
    pub has_returning: bool,
    /// Commit-resolution state of the txn.
    pub commit_state: CommitState,
    /// The request whose response the txn waits for. A response to any
    /// other request of the txn is from a step it left, and is ignored.
    pub awaiting: Option<crate::types::RequestId>,
    /// Stall deadline for a txn parked in [`CommitState::AwaitingVerdict`].
    ///
    /// `Some(instant)` only while parked: if the deadline passes with the
    /// durable global verdict still unknown, the scheduler emits a stall
    /// warning and re-arms this deadline — it NEVER releases locks and NEVER
    /// aborts (a unilateral abort while a peer can have already committed
    /// will tear the transaction). `None` in every other state.
    ///
    /// `Instant::now()` is used for this deadline (observability / liveness
    /// only; never influences WAL bytes).
    pub verdict_deadline: Option<Instant>,
    /// Error text of a stage response that was not `Ok` on this leader.
    ///
    /// `Some` when this leader never staged the txn. An abort verdict drops
    /// it as usual. A COMMIT verdict halts the scheduler, because the txn
    /// cannot apply here while its peers apply it.
    pub stage_error: Option<String>,
    /// What this vShard's slice writes, derived once from its local plans.
    pub scope: SliceScope,
    /// The committed slice's redo, kept while it is proposed so a stall or
    /// a new term can propose it again.
    pub redo: Option<super::core::redo_propose::OwedRedo>,
    /// A collection the transaction names no longer held its planned
    /// incarnation when this leader staged it. The vote is an abort.
    pub superseded: bool,
    /// The gates of the collections the transaction names, held shared from
    /// the incarnation check until the txn leaves `pending`. A purge of one
    /// waits, so a staged slice never commits into a recreated collection.
    pub gates: Vec<crate::control::write_gate::SharedGate>,
    /// A halt released `gates` while the txn waited. Its proposal checks the
    /// incarnations again and takes the gates back first.
    pub ungated: bool,
}

/// What this vShard's slice of a transaction writes: derived once from its
/// local plans, on every replica alike.
#[derive(Debug, Clone)]
pub(super) struct SliceScope {
    /// Whether the slice holds any local write plan. A slice with none only
    /// validates reads: it commits with no log entry.
    pub writes: bool,
    pub collections: Vec<String>,
    pub sum_targets: Vec<nodedb_physical::physical_plan::RedoSumTargets>,
    /// The identities the local plans carry, bound in this node's catalog.
    /// Every replica binds them before the redo installs.
    pub identities: Vec<crate::control::surrogate::CarriedIdentity>,
    /// Which commit-boundary checks the install runs: a RESTORE's, or a
    /// commit's.
    pub origin: nodedb_physical::physical_plan::RedoOrigin,
    /// Every plan of this vShard's slice is one a trigger body buffered, and
    /// a client plan of the transaction homes on another vShard. The slice
    /// then commits under `Trigger`: a single overlay holding the whole
    /// transaction tags each of these rows `Trigger` beside the client's.
    pub body_only: bool,
    /// The slice reads a whole collection at resolve: a TRUNCATE of rows. A
    /// TRUNCATE's edge share reads none. A resolve otherwise runs as soon as
    /// its verdict arrives, beside lower txns that have not applied. A
    /// TRUNCATE's resolve then misses their rows, so it waits for its turn.
    pub resolve_at_turn: bool,
}

impl SliceScope {
    /// The scope of the local plans `plans`.
    pub(super) fn of_plans(plans: &[nodedb_physical::physical_plan::PhysicalPlan]) -> Self {
        Self {
            writes: !plans.is_empty(),
            collections:
                crate::control::wal_replication::transaction_redo::collections::written_collections(
                    plans,
                ),
            sum_targets:
                crate::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets(
                    plans,
                ),
            // The leader fills them once it binds the plans.
            identities: Vec::new(),
            origin: redo_origin(plans),
            body_only: false,
            resolve_at_turn: plans.iter().any(reads_whole_collection_at_resolve),
        }
    }
}

/// The origin of a Calvin slice's redo record: a RESTORE when its local
/// plans install a RESTORE batch, a commit otherwise. A RESTORE's rows
/// passed their collection's rules when they were first written, so the
/// install runs only the checks a RESTORE runs.
fn redo_origin(
    plans: &[nodedb_physical::physical_plan::PhysicalPlan],
) -> nodedb_physical::physical_plan::RedoOrigin {
    use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan, RedoOrigin};
    if plans
        .iter()
        .any(|plan| matches!(plan, PhysicalPlan::Meta(MetaOp::RestoreRedo(_))))
    {
        RedoOrigin::Restore
    } else {
        RedoOrigin::Commit
    }
}

/// Whether the resolve of `plan` reads a whole collection's stored state.
///
/// A TRUNCATE's edge share is not among them. Its resolve records a cut at
/// the transaction's ordinal and reads no stored edge, and the cut hides by
/// ordinal whatever this vShard installs before or after it. The rows'
/// truncate on the collection's vShard still reads every row at resolve.
fn reads_whole_collection_at_resolve(plan: &nodedb_physical::physical_plan::PhysicalPlan) -> bool {
    use nodedb_physical::physical_plan::{
        ColumnarOp, DocumentOp, KvOp, PhysicalPlan, TimeseriesOp, VectorOp,
    };
    matches!(
        plan,
        PhysicalPlan::Document(DocumentOp::Truncate { .. })
            | PhysicalPlan::Kv(KvOp::Truncate { .. })
            | PhysicalPlan::Columnar(ColumnarOp::Truncate { .. })
            | PhysicalPlan::Timeseries(TimeseriesOp::Truncate { .. })
            | PhysicalPlan::Vector(VectorOp::DirectTruncate { .. })
    )
}

/// Commit-resolution state of a granted Calvin transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control::cluster::calvin::scheduler::driver) enum CommitState {
    /// A follower holds the txn's locks and stages nothing. It completes on
    /// an abort verdict, on a COMMIT verdict for a slice with no write, or
    /// once the slice's stamped redo applies. A promotion stages it.
    Following,
    /// Awaiting the validate-and-stage response, whose `stage_vote` carries
    /// the local commit vote.
    Staged,
    /// The staged txn has cast its local vote and PARKED, awaiting the durable
    /// authoritative GLOBAL verdict aggregated across all participant vShards.
    ///
    /// The scheduler holds this txn's locks and its staged Data-Plane buffer
    /// intact while parked; it resumes into a resolve (verdict = commit) or a
    /// drop (verdict = abort) only once `registry.verdict(txn)` is known — via
    /// the verdict push, the probe on park, or the stall re-probe sweep. No
    /// participant self-decides on its local vote, so a torn commit is
    /// impossible.
    AwaitingVerdict,
    /// The txn committed, and its slice reads a whole collection at resolve
    /// (a TRUNCATE). Its resolve waits for its turn: every lower txn of this
    /// vShard finished, so the resolve reads every write sequenced before it
    /// and none after. See [`SliceScope::resolve_at_turn`].
    AwaitingResolveTurn,
    /// The txn committed and a `MetaOp::CalvinResolve` has been dispatched, or
    /// parked for re-send at dispatcher capacity, to resolve its staged
    /// post-images into a replayable `RedoRecord`.
    AwaitingRedoResolve,
    /// The txn committed and its stamped redo is proposed to the vShard's
    /// data group. `proposed` names the `(group_id, log_index)` of the entry
    /// that installs it, `None` while no proposal is in the log. The txn
    /// completes once its redo applies.
    AwaitingRedoApply { proposed: Option<(u64, u64)> },
    /// A `CalvinDrop` has been dispatched, or parked for re-send: the abort
    /// verdict, or a COMMIT verdict for a slice with no write. The txn
    /// completes once the drop answers.
    AwaitingDrop,
}

/// A transaction that is blocked on lock acquisition.
pub(super) struct BlockedTxn {
    pub txn: SequencedTxn,
    /// The moded lock request the txn waits on.
    pub keys: BTreeMap<LockKey, LockMode>,
    /// Wall-clock time at first block (for latency metrics).
    ///
    /// `Instant::now()` used for observability only.
    pub blocked_at: Instant,
}
