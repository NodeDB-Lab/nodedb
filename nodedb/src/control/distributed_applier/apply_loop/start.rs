// SPDX-License-Identifier: BUSL-1.1

//! Prepare one committed entry, in log order: note it, skip a second copy of
//! an applied proposal, and route it to its apply path.
//!
//! Preparing never waits. A write leaves here as its enqueue, and the next
//! entry of its group waits for that enqueue to return, so every core
//! receives a group's writes in the order the log fixed. Other groups never
//! wait on it.

use crate::control::array_sync::ArrayOpTarget;
use crate::control::array_sync::raft_apply::{
    AppliedPosition, ArraySchemaPayload, apply_array_op, apply_array_schema,
};
use crate::control::wal_replication::ReplicatedWrite;
use crate::types::{DatabaseId, TenantId};

use super::calvin_read_result::{CalvinReadResultFields, forward_calvin_read_result};
use super::context::{ApplyContext, ApplyFuture, EnqueueFuture, FinishedApply};
use super::group_watch::GroupWatch;
use super::lane::QueuedEntry;
use super::proposal_gate::{EntryOutcome, ProposalGate};
use super::transaction_redo::prepare_transaction_redo_entry;
use super::write_dispatch::prepare_generic_entry;

/// How a prepared entry continues.
pub(super) enum Prepared<'a> {
    /// The entry concluded while it was prepared.
    Concluded(EntryOutcome),
    /// A backup's cut barrier: it completes once every earlier entry of its
    /// group settled.
    Barrier,
    /// The write's enqueue. The next entry of the group starts once it
    /// returns.
    Enqueue(EnqueueFuture<'a>),
    /// An apply that awaits its own write. It starts with nothing else of its
    /// group running, and the next entry starts once it finishes.
    Exclusive(ApplyFuture<'a>),
}

/// Prepare `queued`, the next entry of `group_id` in log order.
pub(super) fn prepare_entry<'a>(
    ctx: ApplyContext<'a>,
    watch: &mut GroupWatch,
    gate: &ProposalGate,
    group_id: u64,
    queued: QueuedEntry,
) -> Prepared<'a> {
    let QueuedEntry { entry, decoded } = queued;
    let log_index = entry.index;
    watch.note_apply(group_id, log_index);

    // A leader-change no-op committed where a proposer may wait. The
    // proposer's data is gone; firing an empty success would tell it the
    // write applied. `RetryableLeaderChange` makes the gateway re-propose.
    if entry.data.is_empty() {
        tracing::error!(
            group_id,
            log_index,
            "leader-change no-op committed at index where a proposer was waiting; \
             surfacing RetryableLeaderChange so the gateway re-proposes"
        );
        ctx.tracker.complete(
            group_id,
            log_index,
            0,
            Err(crate::Error::RetryableLeaderChange {
                group_id,
                log_index,
            }),
        );
        return Prepared::Concluded(EntryOutcome::Skipped);
    }

    // `0` for unparseable / pre-key entries; the tracker treats 0 as "no key"
    // (no mismatch detection).
    let applied_key = decoded.as_ref().map_or(0, |e| e.idempotency_key);
    // The proposer's commit stamp, raised above any backup cut the log placed
    // before this entry. The entry's mark carries it, not the instant this
    // replica applies, so a late apply never records a write as newer than a
    // backup taken after its ack.
    let commit_hlc = watch.commit_hlc(group_id, decoded.as_ref().map_or(0, |e| e.write_hlc));
    // Database scope for the entry, read from the wire. The generic decode
    // path returns no scope, so it is taken from the entry itself: a redo
    // appended under the wrong scope replays into the wrong namespace.
    let database_id = decoded
        .as_ref()
        .map_or(DatabaseId::DEFAULT, |e| DatabaseId::new(e.database_id));

    // A second committed copy of a proposal this node already applied (a
    // re-proposal after a leader change whose first copy also committed)
    // resolves its waiter with the first copy's result and applies nothing.
    if gate.skip_duplicate(ctx.tracker, group_id, log_index, applied_key) {
        return Prepared::Concluded(EntryOutcome::Repeat);
    }

    let pos = AppliedPosition {
        group_id,
        log_index,
        applied_key,
        commit_hlc,
    };
    let Some(replicated) = decoded else {
        return prepare_generic_entry(ctx, pos, entry, database_id, false);
    };
    let tenant_id = TenantId::new(replicated.tenant_id);
    let entry_database = DatabaseId::new(replicated.database_id);
    match replicated.write {
        ReplicatedWrite::ArrayOp {
            array,
            op_bytes,
            provenance,
            ..
        } => {
            // The op path submits through the write funnel, so its redo is
            // durable before it reports success. A failure breaks the
            // prefix: the entry must stay replayable.
            Prepared::Exclusive(Box::pin(async move {
                let applied_ok = apply_array_op(
                    ctx.state,
                    ctx.tracker,
                    pos,
                    ArrayOpTarget {
                        tenant_id,
                        database_id: entry_database,
                        array: &array,
                    },
                    &op_bytes,
                    provenance.as_deref(),
                )
                .await;
                FinishedApply {
                    group_id,
                    log_index,
                    outcome: EntryOutcome::Applied {
                        durable: applied_ok,
                        result: None,
                    },
                }
            }))
        }
        ReplicatedWrite::ArraySchema {
            ref array,
            ref snapshot_payload,
            schema_hlc_bytes,
        } => {
            // The one applied branch that mints no WAL redo record, and it
            // needs none: its whole effect is two fsync-committed redb
            // transactions, the schema registry's snapshot row and the array
            // catalog's entry, both written before it reports success.
            let applied_ok = apply_array_schema(
                ctx.state,
                ctx.tracker,
                pos,
                ArraySchemaPayload {
                    tenant_id,
                    database_id: entry_database,
                    array,
                    snapshot_payload,
                    schema_hlc_bytes,
                },
            );
            Prepared::Concluded(EntryOutcome::Applied {
                durable: applied_ok,
                result: None,
            })
        }
        ReplicatedWrite::ArrayCellPut { .. } | ReplicatedWrite::ArrayCellDelete { .. } => {
            prepare_generic_entry(ctx, pos, entry, database_id, true)
        }
        ReplicatedWrite::TransactionRedo { .. } => {
            prepare_transaction_redo_entry(ctx, pos, &replicated)
        }
        ReplicatedWrite::CutBarrier { hlc } => {
            // Every entry after the barrier records above the cut.
            watch.raise_cut(group_id, hlc);
            Prepared::Barrier
        }
        ReplicatedWrite::CalvinReadResult {
            epoch,
            position,
            passive_vshard,
            tenant_id,
            ref values,
        } => {
            forward_calvin_read_result(
                ctx.tracker,
                ctx.calvin_read_result_senders,
                pos,
                CalvinReadResultFields {
                    target_vshard: replicated.vshard_id,
                    epoch,
                    position,
                    passive_vshard,
                    tenant_id,
                    values,
                },
            );
            // A read result is forwarded to an in-memory Calvin scheduler and
            // writes nothing durable, so it neither advances the prefix nor
            // breaks it. The epoch it belongs to does not survive a restart,
            // so a re-delivery could not usefully replay it.
            Prepared::Concluded(EntryOutcome::Skipped)
        }
        _ => prepare_generic_entry(ctx, pos, entry, database_id, false),
    }
}
