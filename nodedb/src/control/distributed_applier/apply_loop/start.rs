// SPDX-License-Identifier: BUSL-1.1

//! Prepare one committed entry, in log order: note it, skip a second copy of
//! an applied proposal, and route it to its apply path.
//!
//! Preparing never waits. A write leaves here as its enqueue, and the next
//! entry of its group waits for that enqueue to return, so every core
//! receives a group's writes in the order the log fixed. Other groups never
//! wait on it.

use crate::control::array_sync::raft_apply::{
    AppliedPosition, ArraySchemaPayload, apply_array_schema,
};
use nodedb_raft::message::LogEntry;

use crate::control::backup::cut_order::OrderedCut;
use crate::control::distributed_applier::propose_tracker::ProposeTracker;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::types::{DatabaseId, TenantId};

use super::array_op_prepare::{ArrayOpWrite, prepare_array_op};
use super::calvin_read_result::{
    CalvinReadResultFields, forward_calvin_read_result, forward_calvin_read_timeout,
    forward_calvin_sequence_effects,
};
use super::context::{ApplyContext, ApplyFuture, EnqueueFuture};
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
    // A second copy of an applied proposal never reaches the funnel, so its
    // plan is classified here.
    let plan_writes = queued.plan_writes_user_data();
    let QueuedEntry { entry, mut decoded } = queued;
    let log_index = entry.index;
    let log_term = entry.term;
    watch.note_apply(group_id, log_index);

    if entry.data.is_empty() {
        conclude_leader_change_noop(ctx.tracker, group_id, log_index);
        return Prepared::Concluded(EntryOutcome::Skipped);
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
    let wrote_rows = plan_writes && gate.prior_wrote_rows(applied_key);
    if gate.skip_duplicate(ctx.tracker, group_id, log_index, applied_key) {
        let repeat = EntryOutcome::Repeat { wrote_rows };
        // The same entry delivered again after a restart, whose change
        // events the journal does not hold: they stage again from what boot
        // read of its WAL records before it settles. A second committed copy
        // of a proposal finds nothing at its own index.
        if let Some(recovered) = ctx
            .state
            .change_stream
            .unjournaled()
            .take_recovered(group_id, log_index)
        {
            let pos = AppliedPosition {
                group_id,
                log_index,
                applied_key,
                commit_hlc,
                system_ordinal: scope.system_ordinal,
            };
            return super::marker_changes::restage_recovered(ctx, pos, recovered, repeat);
        }
        return Prepared::Concluded(repeat);
    }

    let pos = AppliedPosition {
        group_id,
        log_index,
        applied_key,
        commit_hlc,
        system_ordinal: scope.system_ordinal,
    };
    let Some(replicated) = decoded else {
        return prepare_generic_entry(ctx, pos, entry, scope, false);
    };
    prepare_replicated(ctx, watch, pos, log_term, entry, scope, replicated)
}

/// Conclude the leader-change no-op at `log_index`. Every new leader commits
/// one. Returns whether a proposer waited at the index.
///
/// A proposer whose entry held the index lost it to the leader change. It
/// gets `RetryableLeaderChange`, so the gateway re-proposes; an empty success
/// would tell it the write applied. A proposer that registers later finds
/// the same answer stored. Only an overwritten waiter is logged.
fn conclude_leader_change_noop(tracker: &ProposeTracker, group_id: u64, log_index: u64) -> bool {
    let overwritten = tracker.complete(
        group_id,
        log_index,
        0,
        Err(crate::Error::RetryableLeaderChange {
            group_id,
            log_index,
        }),
    );
    if overwritten {
        tracing::warn!(
            group_id,
            log_index,
            "a leader-change no-op committed where a proposer was waiting; surfacing \
             RetryableLeaderChange so the gateway re-proposes"
        );
    }
    overwritten
}

/// The scope a decoded entry applies under. The entry's incarnations, roles
/// and the config pins its write requires move into the scope, so the entry
/// no longer holds them.
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
        user_roles: decoded
            .as_mut()
            .map(|e| std::mem::take(&mut e.user_roles))
            .unwrap_or_default(),
        system_ordinal: decoded
            .as_ref()
            .and_then(ReplicatedEntry::carried_system_ordinal),
        // The pins move into the scope. An entry that does not decode
        // applies nothing, so it needs none.
        config_pins: decoded
            .as_mut()
            .map_or(Ok(None), ReplicatedEntry::take_applied_config_pins),
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
                vshard: crate::types::VShardId::new(replicated.vshard_id),
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
        ReplicatedWrite::SurrogateBind {
            ref collection,
            ref binds,
        } => Prepared::Concluded(super::surrogate_bind::apply_surrogate_bind(
            ctx,
            pos,
            super::surrogate_bind::BindKeys {
                tenant_id,
                database_id: entry_database,
                collection,
                binds,
            },
        )),
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
        ReplicatedWrite::TenantPurge {
            collections,
            vshards,
        } => super::tenant_purge::prepare_tenant_purge_entry(
            ctx,
            pos,
            super::tenant_purge::TenantPurgeEntry {
                database_id: entry_database,
                tenant_id,
                vshard_id: replicated.vshard_id,
                collections,
                vshards,
            },
        ),
        ReplicatedWrite::TimeseriesAggregateImage { collection, image } => {
            super::aggregate_image::prepare_aggregate_image_entry(
                ctx,
                pos,
                super::aggregate_image::AggregateImageEntry {
                    database_id: entry_database,
                    tenant_id,
                    vshard_id: replicated.vshard_id,
                    collection,
                    image,
                },
            )
        }
        ReplicatedWrite::RetentionDrop { op, .. } => {
            super::retention_drop::prepare_retention_drop_entry(
                ctx,
                pos,
                super::retention_drop::RetentionDropEntry {
                    database_id: entry_database,
                    tenant_id,
                    vshard_id: replicated.vshard_id,
                    op,
                },
            )
        }
        ReplicatedWrite::CollectionConvert {
            collection,
            target_type,
            schema_json,
            source_storage_mode,
            target_storage_mode,
            generation,
            incarnation,
        } => super::collection_convert::prepare_collection_convert_entry(
            ctx,
            pos,
            super::collection_convert::CollectionConvertEntry {
                database_id: entry_database,
                tenant_id,
                vshard_id: replicated.vshard_id,
                incarnations: scope.incarnations,
                collection,
                target_type,
                schema_json,
                source_storage_mode,
                target_storage_mode,
                generation,
                incarnation,
            },
        ),
        ReplicatedWrite::CutBarrier {
            hlc,
            restore_point,
            capture,
            mirror,
        } => {
            // Every entry after a restore cut's barrier records above the cut,
            // on this life and on every later one. A mirror cut names no
            // restore.
            if mirror.is_none() {
                watch.raise_cut(pos.group_id, pos.log_index, hlc);
            }
            super::cut_barrier::prepare_cut_barrier(
                ctx,
                pos,
                log_term,
                OrderedCut {
                    hlc,
                    restore_point,
                    capture,
                    mirror: mirror.map(|barrier| barrier.mark),
                },
            )
        }
        ReplicatedWrite::MirrorSeal {
            last: _,
            ref calvin_floors,
        } => Prepared::Concluded(super::mirror_hold::apply_mirror_seal(
            ctx,
            pos,
            replicated.database_id,
            replicated.mirror,
            calvin_floors,
        )),
        ReplicatedWrite::CalvinReadResult {
            epoch,
            position,
            passive_vshard,
            tenant_id: _,
            ref values,
            refused,
        } => {
            // The event's stored row is its durable effect: the entry
            // extends the applied prefix once the row holds it.
            Prepared::Concluded(forward_calvin_read_result(
                ctx,
                pos,
                CalvinReadResultFields {
                    target_vshard: replicated.vshard_id,
                    epoch,
                    position,
                    passive_vshard,
                    values,
                    refused,
                },
            ))
        }
        // The stored row is the entry's durable effect.
        ReplicatedWrite::CalvinFrozenReply { ref reply } => {
            Prepared::Concluded(super::calvin_frozen_reply::apply_calvin_frozen_reply(
                ctx,
                pos,
                replicated.vshard_id,
                reply,
            ))
        }
        ReplicatedWrite::CalvinSequenceEffects {
            epoch,
            position,
            source_vshard,
            ref plans,
            ..
        } => Prepared::Concluded(forward_calvin_sequence_effects(
            ctx,
            pos,
            replicated.vshard_id,
            (epoch, position),
            source_vshard,
            plans,
        )),
        ReplicatedWrite::CalvinReadTimeout {
            epoch,
            position,
            tenant_id: _,
        } => Prepared::Concluded(forward_calvin_read_timeout(
            ctx,
            pos,
            replicated.vshard_id,
            epoch,
            position,
        )),
        // A refresh cut flushes the collection's memtable at its log
        // position. It applies with nothing else of its group in flight, so
        // the group's applied floor never passes a cut whose apply has not
        // finished, and a re-delivery after a restart cuts at the same rows.
        ReplicatedWrite::TimeseriesAggregateRefresh { .. } => {
            prepare_generic_entry(ctx, pos, entry, scope, true)
        }
        _ => prepare_generic_entry(ctx, pos, entry, scope, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A no-op at an index a proposer waits on answers it with
    /// `RetryableLeaderChange`.
    #[tokio::test]
    async fn a_noop_over_a_waiter_surfaces_a_leader_change() {
        let tracker = ProposeTracker::new();
        let waiter = tracker.register(4, 9, 77);
        assert!(conclude_leader_change_noop(&tracker, 4, 9));
        let answer = waiter.await.expect("the waiter is answered");
        assert!(
            matches!(
                answer,
                Err(crate::Error::RetryableLeaderChange {
                    group_id: 4,
                    log_index: 9
                })
            ),
            "{answer:?}"
        );
    }

    /// A no-op at an index no proposer waits on, as after every election,
    /// finds no waiter and logs nothing.
    #[test]
    fn a_noop_with_no_waiter_finds_none() {
        let tracker = ProposeTracker::new();
        assert!(!conclude_leader_change_noop(&tracker, 4, 1));
    }

    fn put_entry() -> ReplicatedEntry {
        ReplicatedEntry::new(
            1,
            0,
            0,
            ReplicatedWrite::PointPut {
                collection: "entries".into(),
                document_id: "e1".into(),
                value: vec![0x80],
                surrogate: 1,
                resolved_sum_targets: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            },
        )
    }

    /// A document-body entry whose pins do not cover its write is refused
    /// with a final prevalidation refusal, which its waiter receives.
    #[tokio::test]
    async fn an_uncovered_document_entry_is_refused_before_dispatch() {
        let mut decoded = Some(put_entry());
        let scope = entry_scope(&mut decoded);
        let Err(uncovered) = scope.config_pins else {
            panic!("an unpinned document entry must not reach a core");
        };
        let tracker = ProposeTracker::new();
        let key = decoded.as_ref().map_or(0, |entry| entry.idempotency_key);
        let waiter = tracker.register(4, 9, key);
        let pos = AppliedPosition {
            group_id: 4,
            log_index: 9,
            applied_key: key,
            commit_hlc: 0,
            system_ordinal: None,
        };
        let outcome = super::super::write_dispatch::conclude_generic_entry(
            &tracker,
            pos,
            Err(uncovered.into()),
        );
        assert!(
            matches!(outcome, EntryOutcome::Applied { durable: true, .. }),
            "every replica refuses the entry alike, so the refusal is final"
        );
        let answer = waiter.await.expect("the waiter is answered");
        assert!(
            matches!(
                answer,
                Err(crate::Error::DataPlane(
                    crate::bridge::envelope::ErrorCode::RejectedPrevalidation { .. }
                ))
            ),
            "{answer:?}"
        );
    }

    /// An entry whose write requires no pin applies with none.
    #[test]
    fn an_entry_that_requires_no_pin_applies_with_none() {
        let mut decoded = Some(ReplicatedEntry::new(
            1,
            0,
            0,
            ReplicatedWrite::KvPersist {
                collection: "cache".into(),
                key: b"k".to_vec(),
            },
        ));
        assert!(matches!(entry_scope(&mut decoded).config_pins, Ok(None)));
        assert!(matches!(entry_scope(&mut None).config_pins, Ok(None)));
    }
}
