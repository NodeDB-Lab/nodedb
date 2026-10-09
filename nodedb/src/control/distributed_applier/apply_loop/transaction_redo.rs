// SPDX-License-Identifier: BUSL-1.1

//! Apply path for a committed `ReplicatedWrite::TransactionRedo` entry.
//!
//! Every replica, the proposer included, applies the entry through
//! [`enqueue_transaction_redo`]: the redo record is appended to this node's WAL,
//! its header carrying the entry's idempotency key, and installed through the
//! WAL replay arms. The key makes the record the entry's applied-marker, so an
//! entry re-delivered after a restart is recognised by the proposal ledger and
//! skipped before it reaches here.
//!
//! A chunked body assembles from the `RedoChunk` entries of its stream
//! (see [`super::redo_chunk`]) and then applies like an inline one.
//!
//! A refusal the Data Plane proves applied nothing (a constraint verdict) is
//! final: every replica reaches it at the same log position against the same
//! state, and the funnel cancels the record in the WAL before it returns. The
//! cancelling marker carries the entry's key, so the ledger counts the
//! refusal as the entry's outcome. It advances the durable prefix like a
//! success, because replaying the entry can only refuse it again.

use crate::bridge::envelope::Status;
use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::distributed_applier::propose_tracker::{AppliedWrite, ProposeTracker};
use crate::control::server::dispatch_utils::{ChangeFeedOwner, SubmitOutcome, refusal_is_final};
use crate::control::wal_replication::decode::{DecodedTransactionRedo, decode_transaction_redo};
use crate::control::wal_replication::transaction_redo::chunks::OpenStream;
use crate::control::wal_replication::transaction_redo::{
    RedoTarget, TransactionRedoPayload, enqueue_transaction_redo, record_cross_shard_key,
};
use crate::control::wal_replication::{CollectionIncarnation, ReplicatedEntry};
use crate::types::{DatabaseId, TenantId, VShardId};

use super::context::{ApplyContext, EnqueueFuture, FinishedApply, Started, StartedEntry};
use super::helpers::committed_response_result;
use super::proposal_gate::{EntryOutcome, ledger_outcome};
use super::start::Prepared;

/// Prepare one committed `TransactionRedo` entry. Its enqueue appends the
/// record and hands it to its core. The apply that follows resolves the
/// propose waiter. Its outcome says whether the entry's effect is durable on
/// this node, which is what the group's applied prefix records. A chunked
/// body assembles from its stream first.
///
/// `incarnations` are the entry's collection incarnations, moved out of the
/// decoded entry.
pub(super) fn prepare_transaction_redo_entry<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    entry: &ReplicatedEntry,
    incarnations: Vec<CollectionIncarnation>,
) -> Prepared<'a> {
    let decoded = match decode_transaction_redo(&entry.write) {
        Ok(decoded) => decoded,
        Err(error) => return conclude_held(ctx, pos, error, None),
    };
    let target = RedoTarget {
        tenant_id: TenantId::new(entry.tenant_id),
        database_id: DatabaseId::new(entry.database_id),
        vshard_id: VShardId::new(entry.vshard_id),
    };
    match decoded {
        DecodedTransactionRedo::Inline(payload) => Prepared::Enqueue(enqueue_payload(
            ctx,
            pos,
            target,
            *payload,
            incarnations,
            None,
        )),
        DecodedTransactionRedo::Chunked(chunked) => {
            super::redo_chunk::prepare_chunked_final(ctx, pos, target, chunked, incarnations)
        }
    }
}

/// Conclude an entry this replica cannot apply as the other replicas do:
/// its own bytes are malformed, or its stream here is not the one the log
/// built. It holds the floor rather than skipping a committed transaction.
/// A taken stream stays on disk for it.
pub(super) fn conclude_held<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    error: crate::Error,
    stream: Option<OpenStream>,
) -> Prepared<'a> {
    ctx.tracker
        .complete(pos.group_id, pos.log_index, pos.applied_key, Err(error));
    let outcome = EntryOutcome::Applied {
        durable: false,
        result: None,
    };
    settle_stream(ctx, stream, &outcome);
    Prepared::Concluded(outcome)
}

/// Release a taken stream once its final entry concluded. A durable outcome
/// drops it. Any other keeps its records on disk, so the next boot rebuilds
/// the stream for the entry's re-apply.
fn settle_stream(ctx: ApplyContext<'_>, stream: Option<OpenStream>, outcome: &EntryOutcome) {
    let Some(stream) = stream else {
        return;
    };
    if matches!(outcome, EntryOutcome::Applied { durable: false, .. }) {
        ctx.state.redo_chunks.park(stream);
    }
}

/// Route `payload` to its collections, append it, and enqueue it on its
/// core. `stream` is the chunked stream the payload assembled from.
pub(super) fn enqueue_payload<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    target: RedoTarget,
    payload: TransactionRedoPayload,
    incarnations: Vec<CollectionIncarnation>,
    stream: Option<OpenStream>,
) -> EnqueueFuture<'a> {
    let ApplyContext { state, tracker, .. } = ctx;
    Box::pin(async move {
        let collection = payload.collections.first().cloned();
        // A redo for a collection incarnation this node no longer holds has
        // nothing to mutate. The gates stay held until the redo is enqueued.
        let routed = super::collection_route::route(
            state,
            target.tenant_id.as_u64(),
            target.database_id,
            &incarnations,
        )
        .await;
        let _gates = match routed {
            Ok(super::collection_route::CollectionRoute::Apply(gates)) => gates,
            Ok(super::collection_route::CollectionRoute::Superseded) => {
                tracker.complete(
                    pos.group_id,
                    pos.log_index,
                    pos.applied_key,
                    Err(crate::Error::DataPlane(
                        crate::bridge::envelope::ErrorCode::NotFound,
                    )),
                );
                let outcome = EntryOutcome::Applied {
                    durable: true,
                    result: None,
                };
                settle_stream(ctx, stream, &outcome);
                return StartedEntry::concluded(outcome);
            }
            Err(error) => {
                tracker.complete(pos.group_id, pos.log_index, pos.applied_key, Err(error));
                let outcome = EntryOutcome::Applied {
                    durable: false,
                    result: None,
                };
                settle_stream(ctx, stream, &outcome);
                return StartedEntry::concluded(outcome);
            }
        };
        let enqueued = enqueue_transaction_redo(
            state,
            target,
            &payload,
            pos.applied_key,
            pos.carried_commit_hlc(),
            Some(pos.change_position(state, target.vshard_id.as_u32())),
            // Every replica stages the redo's row changes under the entry and
            // publishes them at its log position once the entry settles.
            ChangeFeedOwner::Replicated {
                group_id: pos.group_id,
                log_index: pos.log_index,
            },
        )
        .await;
        let started = match enqueued {
            Ok(pending) => Started::Running(Box::pin(async move {
                let submitted = pending.finish(state).await;
                if let Ok(outcome) = &submitted {
                    record_cross_shard_key(state, &payload, outcome);
                }
                let outcome = conclude_transaction_redo(tracker, pos, submitted);
                settle_stream(ctx, stream, &outcome);
                FinishedApply {
                    group_id: pos.group_id,
                    log_index: pos.log_index,
                    outcome,
                }
            })),
            Err(error) => {
                let outcome = conclude_transaction_redo(tracker, pos, Err(error));
                settle_stream(ctx, stream, &outcome);
                Started::Concluded(outcome)
            }
        };
        // A committed transaction's redo carries the rows it wrote.
        StartedEntry {
            started,
            collection,
            user_write: true,
        }
    })
}

/// Resolve a transaction redo's waiter from what the funnel returned.
fn conclude_transaction_redo(
    tracker: &ProposeTracker,
    pos: AppliedPosition,
    submitted: crate::Result<SubmitOutcome>,
) -> EntryOutcome {
    let (result, durable) = match submitted {
        Ok(outcome) if outcome.response.status == Status::Ok => {
            (Ok(AppliedWrite::from_response(&outcome.response)), true)
        }
        Ok(outcome) => {
            let refused_finally = outcome
                .response
                .error_code
                .as_deref()
                .is_some_and(refusal_is_final);
            (
                committed_response_result(&outcome.response),
                refused_finally,
            )
        }
        Err(error) => {
            tracing::warn!(
                group_id = pos.group_id,
                index = pos.log_index,
                error = %error,
                "applying committed transaction redo failed"
            );
            (Err(error), false)
        }
    };
    let applied = ledger_outcome(&result);
    tracker.complete(pos.group_id, pos.log_index, pos.applied_key, result);
    EntryOutcome::Applied {
        durable,
        result: Some(applied),
    }
}
