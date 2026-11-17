// SPDX-License-Identifier: BUSL-1.1

//! The side effects a COMMIT runs once every buffered write is durable:
//! metering, identity restarts, reservation release, the session drain, the
//! overlay release, deferred offsets, GAP_FREE finalize, deferred index DDL
//! effects, inferred fields, cursors and NOTIFY.

use nodedb_cluster::calvin::types::ReleaseReason;
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::connection::SessionId;
use super::super::ddl_effect::DeferredDdlEffect;
use super::super::outcome::{AbortReason, CommitOutcome, TxnDataPlane};
use super::super::overlay_drop::drop_txn_overlay;
use super::super::reservation_release;
use super::super::store::SessionStore;
use super::metering::{BufferedWriteScope, meter_committed_buffered_writes};
use super::restart_identity::restart_truncated_identities;

/// A transaction whose every buffered write is durable, with what its side
/// effects read.
pub(super) struct CommittedTxn<'a, D: TxnDataPlane> {
    pub sessions: &'a SessionStore,
    pub session_id: SessionId,
    pub identity: &'a AuthenticatedIdentity,
    pub state: &'a SharedState,
    pub dp: &'a D,
    /// The peeked buffer: the tasks the commit replayed.
    pub buffered: &'a [PhysicalTask],
    pub metering_scopes: &'a [BufferedWriteScope<'a>],
    /// The engine side effects the transaction's index DDL deferred.
    pub deferred_effects: Vec<DeferredDdlEffect>,
}

/// Run every post-commit side effect of `txn`. Returns
/// [`CommitOutcome::Committed`], or an abort when the session holds no
/// transaction or a deferred index DDL effect fails.
pub(super) async fn finish_committed<D: TxnDataPlane>(txn: CommittedTxn<'_, D>) -> CommitOutcome {
    let CommittedTxn {
        sessions,
        session_id,
        identity,
        state,
        dp,
        buffered,
        metering_scopes,
        deferred_effects,
    } = txn;
    let tenant_id = identity.tenant_id;
    // Every buffered write durably committed. Meter the non-stageable
    // ("Buffered") writes now — the first point their dispatch happened. A
    // stageable ("Staged") write was already metered at STATEMENT time
    // (`staging_gate::stage_write`, when it applied to the per-transaction
    // overlay), and is re-identified and skipped here by the exact same
    // `is_stageable_write` predicate `route_in_tx_write` uses to route it —
    // metering it again here will double-bill it, since it is buffered for
    // durable replay same as a non-stageable write. `buffered` still holds
    // the peeked (not yet drained) task list, so this reads the same tasks
    // `dispatch_single_shard` / `run_commit_calvin` replayed.
    meter_committed_buffered_writes(state, buffered, metering_scopes);

    // A staged `TRUNCATE ... RESTART IDENTITY` resets its sequences only
    // here, once the truncate is durable; a ROLLBACK never reaches this.
    restart_truncated_identities(state, tenant_id, buffered);

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
        for task in buffered {
            if dropped.insert(task.vshard_id) {
                // The transaction is already durable at this point; a teardown
                // failure (e.g. the vShard's leader moved and the drop can no
                // longer reach the overlay) cannot un-commit it, so it is
                // surfaced at ERROR and the remaining vShards are still reaped
                // rather than aborting a committed transaction. `drop_txn_overlay`
                // already retries a transient remote failure until its cleanup
                // deadline (see `retry_not_leader`); a drop that still fails
                // after that budget strands a bounded, invisible (the
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
