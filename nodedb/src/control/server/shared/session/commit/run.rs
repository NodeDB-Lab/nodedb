// SPDX-License-Identifier: BUSL-1.1

//! The neutral COMMIT sequence: classify the transaction's dispatch, replay its
//! durable batch, then run every post-commit side effect.

use nodedb_cluster::PendingDdlObject;
use nodedb_cluster::calvin::types::ReleaseReason;

use crate::control::gateway::RouteDecision;
use crate::control::planner::calvin::{DispatchClass, classify_dispatch, read_vshards_of};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::plan_util::extract_collection;
use crate::control::state::SharedState;

use super::super::commit_calvin;
use super::super::commit_fence;
use super::super::connection::SessionId;
use super::super::ddl_compensate;
use super::super::ddl_flush::{self, DdlCommitPlan};
use super::super::outcome::{AbortReason, CommitOutcome, TxnDataPlane};
use super::super::overlay_drop::drop_txn_overlay;
use super::super::reservation_release;
use super::super::store::SessionStore;
use super::metering::meter_committed_buffered_writes;
use super::read_split::SplitReads;
use super::read_validation::stale_read_abort;
use super::restart_identity::restart_truncated_identities;
use super::single_shard::{commit_publishes, dispatch_single_shard};

/// Reverse whatever `begin_commit` already finalized to the catalog before a
/// dispatch failure that follows it. Called on every abort path reachable
/// after `finalize_pending` — the caller still aborts either way, since the
/// durable batch never dispatched; a compensation failure is surfaced in the
/// trace alongside the original failure rather than swallowed.
async fn compensate_finalized_ddl(
    state: &SharedState,
    compensation: &Option<Vec<PendingDdlObject>>,
) {
    let Some(objects) = compensation else {
        return;
    };
    if let Err(error) = ddl_compensate::compensate_finalized(state, objects).await {
        tracing::error!(
            %error,
            "commit compensation failed: this transaction's finalized DDL may still be \
             cataloged despite the DML dispatch that used it having failed"
        );
    }
}

/// Reserve and finalize this connection's buffered DDL through the metadata
/// group. Called once no matter which dispatch shape follows, always after
/// the fence check appropriate to that shape and always before any buffered
/// DML dispatches — see `ddl_flush`'s module doc for why.
///
/// Returns the reserved objects to compensate if dispatch later fails, or
/// `None` when nothing was buffered. `Err` aborts the whole COMMIT before
/// anything durable happens.
async fn begin_ddl(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
) -> crate::Result<Option<Vec<PendingDdlObject>>> {
    match ddl_flush::begin_commit(state, sessions, session_id).await? {
        DdlCommitPlan::None => Ok(None),
        DdlCommitPlan::Pending(handle) => {
            let objects = handle.objects().to_vec();
            ddl_flush::finalize_pending(state, handle).await?;
            Ok(Some(objects))
        }
    }
}

/// Run the neutral COMMIT sequence for one collision-free session.
///
/// Returns [`CommitOutcome::Committed`] once every durable batch has flushed
/// and all post-commit side effects have fired, or [`CommitOutcome::Aborted`]
/// with the reason the transport maps to its wire error.
pub async fn run_commit(
    sessions: &SessionStore,
    session_id: SessionId,
    identity: &AuthenticatedIdentity,
    state: &SharedState,
    dp: &impl TxnDataPlane,
) -> CommitOutcome {
    // A cross-shard read made earlier in the same request as this COMMIT is
    // still pending in the connection scope. It joins the read-set before
    // validation.
    super::super::pending_shard_reads::record_pending(sessions, session_id);
    let read_set = sessions.take_read_set(session_id);
    // The engine side effects this transaction's index DDL deferred. They run
    // once its catalog entries landed, below. An abort before then drops them
    // with the buffer.
    let deferred_effects = super::super::ddl_buffer::drain_effects();
    // Collections this transaction wrote itself. A read of a collection the
    // same transaction has written is a read-your-own-write, not a
    // serialization conflict — reading uncommitted own state (served from the
    // staging overlay, which reports no watermark) must not abort the commit.
    // The read-set is collection-granular, so exclusion is too.
    let written_collections = sessions.buffered_collections(session_id, |plan| {
        extract_collection(plan).map(String::from)
    });
    // Peek the buffered write tasks WITHOUT draining them or leaving the block.
    // The session stays `InBlock` through classification and dispatch; the
    // buffered batch is flushed to Calvin as the COMMIT finalization (see
    // `run_commit_calvin`), then `sessions.commit` below drains the buffer.
    let buffered = sessions.buffered_tasks(session_id);
    // The lines the statements reported rejected, to compare with COMMIT's.
    let ts_preview = super::ts_rejections::preview_by_collection(
        &buffered,
        &sessions.ts_preview_rejected(session_id),
    );
    // The messages the transaction's trigger bodies published. They commit in
    // its redo record, and the Event Plane delivers them from there.
    let publishes = sessions.take_pending_publishes(session_id);
    let tenant_id = identity.tenant_id;
    // The interactive-COMMIT read-set widens dispatch classification: a txn that
    // writes shard X but read shard Y participates in {X, Y} and must route
    // through Calvin with Y as a participant. Autocommit has no session read-set.
    // A single node checks its homed cross-shard reads itself: they enlist no
    // Calvin participant (`read_split`).
    let split_reads = SplitReads::of(state, &read_set);
    let read_vshards = match read_vshards_of(split_reads.enlisted()) {
        Ok(vshards) => vshards,
        Err(error) => {
            reservation_release::release_and_rollback(state, sessions, session_id).await;
            return CommitOutcome::Aborted {
                reason: AbortReason::Dispatch(error),
            };
        }
    };

    // In-transaction `MERGE`, `UPDATE ... FROM <source>`, and `INSERT ... SELECT`
    // are resolved + staged into concrete, surrogate-carrying point writes
    // (`PointInsert` / `PointPut` / `PointDelete`) at STATEMENT time
    // (`session::expander_stage`), so by COMMIT the buffer already holds those
    // concrete point ops — no raw `Merge` / `UpdateFromJoin` / `InsertSelect`
    // plan remains to expand here, and COMMIT invokes no expander at all.

    // Buffered transactional DDL is finalized to the catalog before any
    // buffered DML dispatches — see `ddl_flush`'s module doc for why finalize
    // must precede dispatch. The reserved objects are handled inline in each
    // branch below, not hoisted here: only the buffered-DML branch dispatches
    // anything afterward that can fail in a way compensation will undo.
    if buffered.is_empty() {
        // Read-only interactive transaction: no writes to classify, but it can
        // still serialization-conflict against concurrent writers. Each read
        // is checked on its vShard's leader. Classifying an empty buffer will
        // misread a lone cross-shard READ as `MultiShard` and wrongly reject it.
        if let Some(outcome) =
            stale_read_abort(state, sessions, session_id, &read_set, &written_collections).await
        {
            return outcome;
        }
        // No buffered DML for this DDL to race, so finalize it here,
        // symmetrically with the buffered branch below.
        let ddl_compensation = match begin_ddl(state, sessions, session_id).await {
            Ok(result) => result,
            Err(error) => {
                reservation_release::release_and_rollback(state, sessions, session_id).await;
                return CommitOutcome::Aborted {
                    reason: AbortReason::DdlPropose(error),
                };
            }
        };
        // A transaction whose bodies published with no buffered write (an
        // INSTEAD OF body that replaced every write) commits its messages in
        // a redo record of their own. Nothing else dispatches in this path:
        // the remaining steps (metering, session drain, offset/GAP_FREE
        // finalize, field-inference merge) report no dispatch failure for a
        // finalize to compensate.
        if let Some(reason) = commit_publishes(state, tenant_id, publishes).await {
            compensate_finalized_ddl(state, &ddl_compensation).await;
            reservation_release::release_and_rollback(state, sessions, session_id).await;
            return CommitOutcome::Aborted { reason };
        }
    } else {
        // Every buffered write was planned at STATEMENT time and is dispatched
        // only now, so the catalog has had the whole open block to move on.
        // Re-compare the descriptor versions the statements were planned
        // against before anything durable is written — an abort here leaves no
        // side effect and the client retries the transaction.
        if let Some(reason) = commit_fence::check_buffered_descriptors(
            state.credentials.catalog(),
            sessions,
            session_id,
        ) {
            reservation_release::release_and_rollback(state, sessions, session_id).await;
            return CommitOutcome::Aborted { reason };
        }
        // Reserve and finalize the buffered DDL BEFORE dispatching any
        // buffered DML: a crash after this point leaves a real, cataloged,
        // empty, droppable collection rather than rows durable in a
        // collection with no catalog row anywhere.
        let ddl_compensation = match begin_ddl(state, sessions, session_id).await {
            Ok(result) => result,
            Err(error) => {
                reservation_release::release_and_rollback(state, sessions, session_id).await;
                return CommitOutcome::Aborted {
                    reason: AbortReason::DdlPropose(error),
                };
            }
        };
        match classify_dispatch(&buffered, &read_vshards) {
            DispatchClass::MultiShard { .. } => {
                // Flush the buffered cross-shard batch through Calvin's durable
                // Vote/Verdict barrier (`run_commit_calvin`), leader-routed.
                // Calvin's participants validate `versioned_reads` themselves
                // and return a serialization abort (SQLSTATE 40001) on an ABORT
                // verdict.
                if let Some(outcome) = split_reads
                    .stale_here(state, sessions, session_id, &written_collections)
                    .await
                {
                    compensate_finalized_ddl(state, &ddl_compensation).await;
                    return outcome;
                }
                if let Some(reason) = commit_calvin::run_commit_calvin(
                    sessions,
                    session_id,
                    state,
                    commit_calvin::CalvinCommit {
                        buffered: &buffered,
                        tenant_id,
                        reads: split_reads.enlisted(),
                        event_source: dp.event_source(),
                        publishes: &publishes,
                        applied_key: dp.applied_key().zip(dp.applied_key_vshard()),
                        ts_preview: &ts_preview,
                    },
                )
                .await
                {
                    compensate_finalized_ddl(state, &ddl_compensation).await;
                    reservation_release::release_and_rollback(state, sessions, session_id).await;
                    return CommitOutcome::Aborted { reason };
                }
            }
            DispatchClass::SingleShard { vshard: vshard_id } => {
                let leader = crate::control::gateway::live_leaders::resolve_live_decision(
                    state,
                    vshard_id.as_u32(),
                );
                if !matches!(leader, RouteDecision::Local)
                    || crate::control::planner::calvin::writes_edges(&buffered)
                {
                    // The interactive transaction WAL record belongs to this
                    // coordinator and cannot be forwarded as a bare remote
                    // Data-Plane LSN. Route a non-local single-shard commit
                    // through Calvin's replicated Vote/Verdict barrier instead;
                    // this gives it the same leader routing, OCC, durability,
                    // and apply ordering as any multi-participant commit.
                    // A commit that writes an edge takes it too: every edge
                    // version takes a Calvin ordinal, so it orders against a
                    // TRUNCATE's cut on every replica.
                    if let Some(outcome) = split_reads
                        .stale_here(state, sessions, session_id, &written_collections)
                        .await
                    {
                        compensate_finalized_ddl(state, &ddl_compensation).await;
                        return outcome;
                    }
                    if let Some(reason) = commit_calvin::run_commit_calvin(
                        sessions,
                        session_id,
                        state,
                        commit_calvin::CalvinCommit {
                            buffered: &buffered,
                            tenant_id,
                            reads: split_reads.enlisted(),
                            event_source: dp.event_source(),
                            publishes: &publishes,
                            applied_key: dp.applied_key().zip(dp.applied_key_vshard()),
                            ts_preview: &ts_preview,
                        },
                    )
                    .await
                    {
                        compensate_finalized_ddl(state, &ddl_compensation).await;
                        reservation_release::release_and_rollback(state, sessions, session_id)
                            .await;
                        return CommitOutcome::Aborted { reason };
                    }
                } else {
                    if let Some(outcome) = stale_read_abort(
                        state,
                        sessions,
                        session_id,
                        &read_set,
                        &written_collections,
                    )
                    .await
                    {
                        compensate_finalized_ddl(state, &ddl_compensation).await;
                        return outcome;
                    }
                    if let Some(reason) = dispatch_single_shard(
                        state,
                        dp,
                        &buffered,
                        tenant_id,
                        vshard_id,
                        publishes,
                        &ts_preview,
                    )
                    .await
                    {
                        compensate_finalized_ddl(state, &ddl_compensation).await;
                        reservation_release::release_and_rollback(state, sessions, session_id)
                            .await;
                        return CommitOutcome::Aborted { reason };
                    }
                }
            }
        }
    }

    // Every abort branch above has already returned, so every buffered write
    // durably committed. Meter the non-stageable ("Buffered") writes now
    // — this is the first point their dispatch has actually happened. A
    // stageable ("Staged") write was already metered at STATEMENT time
    // (`staging_gate::stage_write`, when it applied to the per-transaction
    // overlay), and is re-identified and skipped here by the exact same
    // `is_stageable_write` predicate `route_in_tx_write` uses to route it —
    // metering it again here will double-bill it, since it is buffered for
    // durable replay same as a non-stageable write. `buffered` still holds
    // the peeked (not yet drained) task list, so this reads the same tasks
    // `dispatch_single_shard` / `run_commit_calvin` replayed above.
    meter_committed_buffered_writes(state, identity, &buffered);

    // A staged `TRUNCATE ... RESTART IDENTITY` resets its sequences only
    // here, once the truncate is durable; a ROLLBACK never reaches this.
    restart_truncated_identities(state, tenant_id, &buffered);

    // Release this transaction's read reservations (belt-and-suspenders: the
    // Calvin batch's `on_txn_complete` already releases the owner for keys in the
    // batch — this covers reserved keys not in it) while the owner is still set,
    // before `sessions.commit` drains the session below.
    reservation_release::release_session_reservations(
        state,
        sessions,
        session_id,
        ReleaseReason::Commit,
    )
    .await;
    // Transition the session out of the block NOW — this drains the write buffer
    // and clears snapshot/txn state, moving the session to `Idle`. The aligned
    // descriptor-lease scope holders stay owned here until the drain-bearing
    // flushes below, which cannot wait on a hold this session still owns.
    let (_drained_tasks, lease_scopes) = match sessions.commit(session_id) {
        Ok(drained) => drained,
        Err(_msg) => {
            return CommitOutcome::Aborted {
                reason: AbortReason::NoTransaction,
            };
        }
    };

    // Release the per-transaction staging overlay on every vShard that hosted a
    // staged write, only after the durable batch(es) have flushed. Uses the
    // peeked buffer (identical contents to the drained one). Guarded on a staged
    // (txn_id-carrying) buffer.
    if let Some(txn_id) = buffered.first().and_then(|t| t.txn_id) {
        let mut dropped = std::collections::HashSet::new();
        for task in &buffered {
            if dropped.insert(task.vshard_id) {
                // The transaction is already durable at this point; a teardown
                // failure (e.g. the vShard's leader moved and the drop can no
                // longer reach the overlay) cannot un-commit it, so it is
                // surfaced at ERROR and the remaining vShards are still reaped
                // rather than aborting a committed transaction. `drop_txn_overlay`
                // already retries a transient remote failure a bounded number of
                // times internally (see `retry_not_leader`); a drop that still
                // fails after that budget strands a bounded, invisible (the
                // `txn_id` is never reused) overlay on the unreachable former
                // leader, cleared on that node's restart and visible meanwhile
                // via `active_txn_overlays`.
                if let Err(e) = drop_txn_overlay(state, dp, tenant_id, task.vshard_id, txn_id).await
                {
                    tracing::error!(
                        vshard = task.vshard_id.as_u32(),
                        error = %e,
                        "failed to release per-transaction staging overlay after commit"
                    );
                }
            }
        }
    }

    // Flush pending offset commits (deferred from COMMIT OFFSET inside
    // transaction) through the replicated catalog, so every node holds them.
    let pending_offsets = sessions.take_pending_offsets(session_id);
    for pending_offset in pending_offsets {
        if let Err(e) =
            crate::control::server::shared::ddl::neutral::consumer_group::commit::commit_group_offsets(
                state,
                pending_offset.database_id,
                pending_offset.tenant_id,
                &pending_offset.stream,
                &pending_offset.group,
                vec![crate::event::cdc::consumer_group::types::PartitionOffset::new(
                    pending_offset.partition_id,
                    pending_offset.offset,
                )],
            )
            .await
        {
            tracing::warn!(
                stream = %pending_offset.stream,
                group = %pending_offset.group,
                partition = pending_offset.partition_id,
                error = %e.message,
                "failed to commit deferred offset"
            );
        }
    }

    // Finalize GAP_FREE reservations (numbers become permanent).
    let reservations = sessions.take_pending_reservations(session_id);
    for handle in &reservations {
        state.sequence_registry.gap_free_manager().commit(handle);
        // Log to _system.sequence_log.
        {
            let catalog = state.credentials.catalog();
            crate::control::sequence::log::log_reservation(
                catalog,
                &crate::control::sequence::log::committed(
                    &handle.name,
                    handle.value,
                    &identity.username,
                    handle.database_id,
                    handle.tenant_id,
                ),
            );
        }
    }

    // The durable batch has flushed, so these holds have no remaining job.
    // Released BEFORE the field-inference merge below: it proposes a
    // descriptor version bump, which drains every lease at the prior version —
    // including one this committing session still owns, which will never
    // drain.
    drop(lease_scopes);

    // Index DDL's engine work follows its catalog entries: backfills, index
    // teardown, analyzer bindings and sorted-index trees. A failure after the
    // catalog landed reports as an abort.
    if let Err(error) =
        crate::control::server::shared::ddl::neutral::deferred_effects::run_deferred_effects(
            state,
            deferred_effects,
        )
        .await
    {
        return CommitOutcome::Aborted {
            reason: AbortReason::DdlPropose(error),
        };
    }

    // Record the schema fields this transaction's writes inferred, deferred
    // from statement time (`staging_gate`-buffered writes are planned against
    // the descriptor version a statement-time bump will invalidate). The
    // transaction is already durable, so a failure here is logged, not raised:
    // the projection is rebuildable and the next write re-supplies the fields.
    for pending in sessions.take_pending_field_inference(session_id) {
        if let Err(error) = crate::control::catalog_entry::merge_collection_fields_replicated(
            state,
            pending.database_id,
            pending.tenant_id,
            &pending.collection,
            None,
            &pending.fields,
        )
        .await
        {
            tracing::warn!(
                collection = %pending.collection,
                error = %error,
                "failed to record inferred schema fields after commit"
            );
        }
    }

    // Close non-WITH-HOLD cursors on transaction end.
    sessions.close_non_hold_cursors(session_id);
    // Flush NOTIFY messages buffered during this transaction.
    sessions.flush_pending_notifies(session_id, identity.tenant_id, &state.notify_bus);
    CommitOutcome::Committed
}
