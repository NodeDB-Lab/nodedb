// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral Calvin multi-shard dispatch (static path).
//!
//! This is the session-UNAWARE core extracted from the pgwire
//! `dispatch_calvin_multishard` static (non-OLLP) branch: classify the task
//! set, reject a cross-shard span that is a mid-block single statement (it
//! cannot be buffered atomically), build the static `TxClass`, and route the
//! single submit-and-await to the sequencer-group leader via
//! `submit_calvin_routed`.
//!
//! It takes the session-derived inputs the core needs — the cross-shard
//! transaction mode, the dispatch's position in the transaction lifecycle
//! (autocommit / mid-block statement / COMMIT flush), and the session read-set —
//! as plain parameters, so both the pgwire and native protocol paths can supply
//! them and share one implementation. The function returns a raw
//! `crate::Result<()>`: Calvin's static path produces no Data-Plane payload —
//! its success is the durable, replicated commit acknowledged by
//! `submit_calvin_routed` — so the per-task command tags are synthesised by
//! each protocol from the original task list AFTER this returns `Ok`.
//!
//! The OLLP (dependent-predicate) variant is intentionally NOT handled here: it
//! is still tied to the local `OllpOrchestrator` and completion registry and is
//! not yet leader-routed (a declared follow-up). Callers that may carry a
//! dependent predicate must route that case through their own OLLP path; this
//! helper is the static cross-shard write path only.

use crate::bridge::envelope::Response;
use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, TxnDispatchPosition, build_static_tx_class,
    classify_dispatch, read_vshards_of, submit_calvin_routed,
};
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::state::SharedState;
use crate::types::TenantId;
use nodedb_physical::physical_task::PhysicalTask;

/// Drive the static Calvin multi-shard path for `tasks`.
///
/// - `cross_shard_mode`: the session's effective cross-shard transaction mode.
///   Only [`CrossShardTxnMode::Strict`] routes through Calvin here; callers are
///   expected to have already gated on this, but it is re-checked defensively.
/// - `position`: where this dispatch sits in the transaction lifecycle. Only a
///   [`TxnDispatchPosition::MidBlockStatement`] cross-shard span is rejected with
///   [`crate::Error::CrossShardInExplicitTransaction`] (it cannot be buffered
///   atomically); [`TxnDispatchPosition::Autocommit`] and the COMMIT
///   [`TxnDispatchPosition::CommitFlush`] both proceed.
/// - `reads`: the session read-set. It widens the dispatch classification and
///   the `TxClass` participants/OCC set in lockstep — a txn that writes shard A
///   but reads shard B enumerates B as a participant. Autocommit callers pass an
///   empty slice.
///
/// On success the Calvin transaction has been submitted and acknowledged by the
/// sequencer leader. Returns the applied Data-Plane [`Response`] when the write
/// carried a RETURNING clause (so the caller can emit its rows), or `None` for a
/// plain write — where the caller synthesises one command tag per task.
pub async fn dispatch_tasks_to_calvin(
    state: &SharedState,
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    cross_shard_mode: CrossShardTxnMode,
    position: TxnDispatchPosition,
    reads: &[ReadSetEntry],
) -> crate::Result<Option<Response>> {
    // The read-set widens classification exactly as it widens the TxClass
    // participants below: an empty slice (autocommit) preserves write-only
    // classification.
    let read_vshards = read_vshards_of(reads);
    match classify_dispatch(tasks, &read_vshards) {
        DispatchClass::MultiShard { .. } => {
            // A mid-block single statement cannot be buffered atomically, so a
            // cross-shard span there is rejected. The COMMIT flush of a buffered
            // block is NOT a mid-block statement — its whole batch commits
            // atomically — so it proceeds.
            if position == TxnDispatchPosition::MidBlockStatement {
                return Err(crate::Error::CrossShardInExplicitTransaction);
            }
            match cross_shard_mode {
                CrossShardTxnMode::Strict => {
                    // The sequencer inbox must be wired for the strict path.
                    // A non-leader local submit is silently discarded, so
                    // route the single submit-and-await to the leader.
                    if state.sequencer_inbox.get().is_none() {
                        return Err(crate::Error::SequencerUnavailable);
                    }
                    // Thread the session read-set into the TxClass so read shards
                    // are enumerated as Calvin participants and validated by OCC.
                    let tx_class = build_static_tx_class(tasks, tenant_id, reads)?;
                    submit_calvin_routed(state, tx_class).await
                }
                CrossShardTxnMode::BestEffortNonAtomic => {
                    // Best-effort never reaches this strict multi-shard entry
                    // point; surface a typed internal error rather than
                    // silently doing nothing.
                    Err(crate::Error::Internal {
                        detail: "unexpected non-Calvin dispatch outcome for strict \
                                 multi-shard query"
                            .to_owned(),
                    })
                }
            }
        }
        DispatchClass::SingleShard { .. } => Err(crate::Error::Internal {
            detail: "unexpected single-shard classification on the strict \
                     multi-shard Calvin path"
                .to_owned(),
        }),
    }
}
