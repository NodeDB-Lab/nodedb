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
use nodedb_raft::message::LogEntry;

use crate::control::backup::cut_order::OrderedCut;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::types::{DatabaseId, TenantId};

use super::calvin_read_result::{CalvinReadResultFields, forward_calvin_read_result};
use super::context::{ApplyContext, ApplyFuture, EnqueueFuture, FinishedApply};
use super::group_watch::GroupWatch;
use super::lane::QueuedEntry;
use super::proposal_gate::{EntryOutcome, ProposalGate};
use super::topic_publish::{TopicPublishEntry, prepare_topic_publish_entry};
use super::transaction_redo::prepare_transaction_redo_entry;
use super::write_dispatch::{EntryScope, prepare_generic_entry};

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
    let QueuedEntry { entry, mut decoded } = queued;
    let log_index = entry.index;
    let log_term = entry.term;
    watch.note_apply(group_id, log_index);

    if entry.data.is_empty() {
        return conclude_leader_change_noop(ctx, group_id, log_index);
    }

    // `0` for unparseable / pre-key entries; the tracker treats 0 as "no key"
    // (no mismatch detection).
    let applied_key = decoded.as_ref().map_or(0, |e| e.idempotency_key);
    // The proposer's commit stamp, raised above any backup cut the log placed
    // before this entry. The entry's mark carries it, not the instant this
    // replica applies, so a late apply never records a write as newer than a
    // backup taken after its ack.
    let commit_hlc = watch.commit_hlc(
        group_id,
        log_index,
        decoded.as_ref().map_or(0, |e| e.write_hlc),
    );
    let scope = entry_scope(&mut decoded);

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
        return prepare_generic_entry(ctx, pos, entry, scope, false);
    };
    prepare_replicated(ctx, watch, pos, log_term, entry, scope, replicated)
}

/// A leader-change no-op committed where a proposer can wait. The
/// proposer's data is gone; firing an empty success tells it the
/// write applied. `RetryableLeaderChange` makes the gateway re-propose.
fn conclude_leader_change_noop<'a>(
    ctx: ApplyContext<'a>,
    group_id: u64,
    log_index: u64,
) -> Prepared<'a> {
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
    Prepared::Concluded(EntryOutcome::Skipped)
}

/// The scope a decoded entry applies under. The entry's incarnations move
/// into the scope, so the entry no longer holds them.
fn entry_scope(decoded: &mut Option<ReplicatedEntry>) -> EntryScope {
    // Database scope for the entry, read from the wire. The generic decode
    // path returns no scope, so it is taken from the entry itself: a redo
    // appended under the wrong scope replays into the wrong namespace.
    let database_id = decoded
        .as_ref()
        .map_or(DatabaseId::DEFAULT, |e| DatabaseId::new(e.database_id));
    // The source the proposer stamped. An entry that does not decode applies
    // nothing, so its source is never read.
    let event_source = decoded
        .as_ref()
        .map_or(crate::event::EventSource::User, |e| e.event_source.into());
    EntryScope {
        database_id,
        event_source,
        incarnations: decoded
            .as_mut()
            .map(|e| std::mem::take(&mut e.incarnations))
            .unwrap_or_default(),
    }
}

/// Route a decoded entry to the apply path of its write.
fn prepare_replicated<'a>(
    ctx: ApplyContext<'a>,
    watch: &mut GroupWatch,
    pos: AppliedPosition,
    log_term: u64,
    entry: LogEntry,
    scope: EntryScope,
    replicated: ReplicatedEntry,
) -> Prepared<'a> {
    let tenant_id = TenantId::new(replicated.tenant_id);
    let entry_database = DatabaseId::new(replicated.database_id);
    match replicated.write {
        ReplicatedWrite::ArrayOp {
            array,
            op_bytes,
            cell_surrogate,
            provenance,
            incarnation,
            ..
        } => prepare_array_op(
            ctx,
            pos,
            entry_database,
            ArrayOpWrite {
                cell: super::array_cell_route::CellWrite {
                    tenant_id,
                    array,
                    incarnation,
                },
                op_bytes,
                cell_surrogate,
                provenance,
            },
        ),
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
        ReplicatedWrite::ArrayCellPut {
            array, incarnation, ..
        }
        | ReplicatedWrite::ArrayCellDelete {
            array, incarnation, ..
        } => super::array_cell_route::prepare_array_cell_entry(
            ctx,
            pos,
            entry,
            scope,
            super::array_cell_route::CellWrite {
                tenant_id,
                array,
                incarnation,
            },
        ),
        ReplicatedWrite::TransactionRedo { .. } => {
            prepare_transaction_redo_entry(ctx, pos, &replicated, scope.incarnations)
        }
        ReplicatedWrite::RedoChunk { .. } | ReplicatedWrite::RedoAbandon { .. } => {
            super::redo_chunk::prepare_stream_entry(ctx, pos, log_term, replicated)
        }
        ReplicatedWrite::SurrogateBind { ref identities } => {
            Prepared::Concluded(super::surrogate_bind::apply_surrogate_bind(
                ctx,
                pos,
                tenant_id,
                entry_database,
                identities,
            ))
        }
        ReplicatedWrite::TopicPublish {
            topic,
            payload,
            event_time,
            origin,
        } => prepare_topic_publish_entry(
            ctx,
            pos,
            TopicPublishEntry {
                database_id: entry_database,
                tenant_id,
                vshard_id: replicated.vshard_id,
                topic,
                payload,
                event_time,
                origin,
            },
        ),
        ReplicatedWrite::CutBarrier {
            hlc,
            restore_point,
            capture,
        } => {
            // Every entry after the barrier records above the cut, on this
            // life and on every later one.
            watch.raise_cut(pos.group_id, pos.log_index, hlc);
            super::cut_barrier::prepare_cut_barrier(
                ctx,
                pos,
                log_term,
                OrderedCut {
                    hlc,
                    restore_point,
                    capture,
                },
            )
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
            // so a re-delivery cannot usefully replay it.
            Prepared::Concluded(EntryOutcome::Skipped)
        }
        _ => prepare_generic_entry(ctx, pos, entry, scope, false),
    }
}

/// The parts of a `ReplicatedWrite::ArrayOp` its apply takes.
struct ArrayOpWrite {
    cell: super::array_cell_route::CellWrite,
    op_bytes: Vec<u8>,
    cell_surrogate: Option<u32>,
    provenance: Option<Vec<u8>>,
}

/// Prepare an array op. The op path submits through the write funnel, so
/// its redo is durable before it reports success. A failure breaks the
/// prefix: the entry must stay replayable.
fn prepare_array_op<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    entry_database: DatabaseId,
    write: ArrayOpWrite,
) -> Prepared<'a> {
    Prepared::Exclusive(Box::pin(async move {
        let ArrayOpWrite {
            cell,
            op_bytes,
            cell_surrogate,
            provenance,
        } = write;
        let (_gate, database_id) = match super::array_cell_route::route_cell_write(
            ctx,
            pos,
            entry_database,
            &cell,
        )
        .await
        {
            Ok(routed) => routed,
            Err(finished) => return *finished,
        };
        let applied_ok = apply_array_op(
            ctx.state,
            ctx.tracker,
            pos,
            ArrayOpTarget {
                tenant_id: cell.tenant_id,
                database_id,
                array: &cell.array,
            },
            &op_bytes,
            cell_surrogate,
            provenance.as_deref(),
        )
        .await;
        FinishedApply {
            group_id: pos.group_id,
            log_index: pos.log_index,
            outcome: EntryOutcome::Applied {
                durable: applied_ok,
                result: None,
            },
        }
    }))
}
