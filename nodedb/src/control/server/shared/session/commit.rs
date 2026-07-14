// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral COMMIT orchestrator.
//!
//! Drives the full commit sequence off the neutral session state: snapshot
//! isolation conflict detection (with read-your-own-write exclusion), WAL
//! transaction batching, single-shard vs Calvin multi-shard dispatch, staging
//! overlay release, deferred offset flush, GAP_FREE sequence finalization,
//! buffered DDL propose, and cursor/notify flush. Every Data-Plane dispatch
//! goes through the injected [`TxnDataPlane`] seam so both pgwire and native
//! drive this one core; transports only shape the returned [`CommitOutcome`].

use std::net::SocketAddr;

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::planner::calvin::{DispatchClass, classify_dispatch, read_vshards_of};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::plan_util::extract_collection;
use crate::control::state::SharedState;
use nodedb_cluster::{MetadataEntry, encode_entry};
use nodedb_physical::physical_plan::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::ddl_buffer;
use super::outcome::{AbortReason, CommitOutcome, TxnDataPlane};
use super::read_set::ReadSetEntry;
use super::store::SessionStore;

/// Run the neutral COMMIT sequence for the connection at `addr`.
///
/// Returns [`CommitOutcome::Committed`] once every durable batch has flushed
/// and all post-commit side effects have fired, or [`CommitOutcome::Aborted`]
/// with the reason the transport maps to its wire error.
pub async fn run_commit(
    sessions: &SessionStore,
    addr: &SocketAddr,
    identity: &AuthenticatedIdentity,
    state: &SharedState,
    dp: &impl TxnDataPlane,
) -> CommitOutcome {
    let read_set = sessions.take_read_set(addr);
    // Collections this transaction wrote itself. A read of a collection the
    // same transaction has written is a read-your-own-write, not a
    // serialization conflict — reading uncommitted own state (served from the
    // staging overlay, which reports no watermark) must not abort the commit.
    // The read-set is collection-granular, so exclusion is too.
    let written_collections =
        sessions.buffered_collections(addr, |plan| extract_collection(plan).map(String::from));
    // Peek the buffered write tasks WITHOUT draining them or leaving the block.
    // The session stays `InBlock` through classification and dispatch; the
    // buffered batch is flushed to Calvin as the COMMIT finalization (see
    // `run_commit_calvin`), then `sessions.commit` below drains the buffer.
    let buffered = sessions.buffered_tasks(addr);
    let tenant_id = identity.tenant_id;
    // The interactive-COMMIT read-set widens dispatch classification: a txn that
    // writes shard X but read shard Y participates in {X, Y} and must route
    // through Calvin with Y as a participant. Autocommit has no session read-set.
    let read_vshards = read_vshards_of(&read_set);

    // In-transaction `MERGE`, `UPDATE ... FROM <source>`, and `INSERT ... SELECT`
    // are resolved + staged into concrete, surrogate-carrying point writes
    // (`PointInsert` / `PointPut` / `PointDelete`) at STATEMENT time
    // (`session::expander_stage`), so by COMMIT the buffer already holds those
    // concrete point ops — no raw `Merge` / `UpdateFromJoin` / `InsertSelect`
    // plan remains to expand here, and COMMIT invokes no expander at all.

    if buffered.is_empty() {
        // Read-only interactive transaction: no writes to classify, but it can
        // still serialization-conflict against concurrent writers. Run the
        // single-shard SI validation only — classifying an empty buffer would
        // misread a lone cross-shard READ as `MultiShard` and wrongly reject it.
        if let Some(outcome) =
            si_conflict_abort(sessions, addr, state, &read_set, &written_collections)
        {
            return outcome;
        }
    } else {
        match classify_dispatch(&buffered, &read_vshards) {
            DispatchClass::MultiShard { .. } => {
                // Flush the buffered cross-shard batch through Calvin's durable
                // Vote/Verdict barrier (`run_commit_calvin`), leader-routed. SI is
                // a single-shard validation and is intentionally NOT run here —
                // Calvin performs its own cross-shard OCC over `versioned_reads`
                // and returns a serialization abort (SQLSTATE 40001) on an ABORT
                // verdict.
                if let Some(reason) = super::commit_calvin::run_commit_calvin(
                    sessions, addr, state, &buffered, tenant_id, &read_set,
                )
                .await
                {
                    rollback_with_gap_free(sessions, addr, state);
                    return CommitOutcome::Aborted { reason };
                }
            }
            DispatchClass::SingleShard { vshard: vshard_id } => {
                if let Some(outcome) =
                    si_conflict_abort(sessions, addr, state, &read_set, &written_collections)
                {
                    return outcome;
                }
                if let Some(reason) =
                    dispatch_single_shard(state, dp, &buffered, tenant_id, vshard_id).await
                {
                    rollback_with_gap_free(sessions, addr, state);
                    return CommitOutcome::Aborted { reason };
                }
            }
        }
    }

    // Every abort branch above has already returned; the transaction is durable.
    // Transition the session out of the block NOW — this drains the write buffer
    // and clears snapshot/txn state, moving the session to `Idle`.
    match sessions.commit(addr) {
        Ok(_) => {}
        Err(_msg) => {
            return CommitOutcome::Aborted {
                reason: AbortReason::NoTransaction,
            };
        }
    }

    // Release the per-transaction staging overlay on every vShard that hosted a
    // staged write, now that the durable batch(es) have flushed. Uses the peeked
    // buffer (identical contents to the drained one). Guarded on a staged
    // (txn_id-carrying) buffer.
    if let Some(txn_id) = buffered.first().and_then(|t| t.txn_id) {
        let mut dropped = std::collections::HashSet::new();
        for task in &buffered {
            if dropped.insert(task.vshard_id) {
                drop_txn_overlay(dp, tenant_id, task.vshard_id, txn_id).await;
            }
        }
    }

    // Flush pending offset commits (deferred from COMMIT OFFSET inside transaction).
    let pending_offsets = sessions.take_pending_offsets(addr);
    for (tid, stream, group, partition_id, lsn) in pending_offsets {
        if let Err(e) = state
            .offset_store
            .commit_offset(tid, &stream, &group, partition_id, lsn)
        {
            tracing::warn!(
                stream = %stream,
                group = %group,
                partition = partition_id,
                error = %e,
                "failed to commit deferred offset"
            );
        }
    }

    // Finalize GAP_FREE reservations (numbers become permanent).
    let reservations = sessions.take_pending_reservations(addr);
    for handle in &reservations {
        state.sequence_registry.gap_free_manager().commit(handle);
        // Log to _system.sequence_log.
        {
            let catalog = state.credentials.catalog();
            crate::control::sequence::log::log_reservation(
                catalog,
                &crate::control::sequence::log::committed(
                    &handle.sequence_key,
                    handle.value,
                    &identity.username,
                    identity.tenant_id.as_u64(),
                ),
            );
        }
    }

    // Flush any buffered DDL entries as a single atomic batch.
    if let Some(reason) = flush_buffered_ddl(state) {
        return CommitOutcome::Aborted { reason };
    }

    // Close non-WITH-HOLD cursors on transaction end.
    sessions.close_non_hold_cursors(addr);
    // Flush NOTIFY messages buffered during this transaction.
    sessions.flush_pending_notifies(addr, identity.tenant_id, &state.notify_bus);
    CommitOutcome::Committed
}

/// Snapshot-isolation write-conflict check for a single-shard interactive
/// COMMIT. If any read key's collection advanced past both the read LSN and the
/// transaction snapshot LSN — and the transaction did not write that collection
/// itself (read-your-own-write is excluded) — the WAL moved under the reader:
/// roll the session back (releasing GAP_FREE reservations) and return a
/// serialization abort. Returns `None` when there is no conflict (or no
/// snapshot, i.e. not in a transaction).
///
/// This is a single-shard validation: it compares against the global WAL
/// `next_lsn`, so it is only sound for a transaction whose participants are one
/// shard, and is run exclusively on the `SingleShard` / read-only paths.
fn si_conflict_abort(
    sessions: &SessionStore,
    addr: &SocketAddr,
    state: &SharedState,
    read_set: &[ReadSetEntry],
    written_collections: &std::collections::HashSet<String>,
) -> Option<CommitOutcome> {
    let snapshot_lsn = sessions.snapshot_lsn(addr)?;
    let current_lsn = state.wal.next_lsn();
    let current = crate::types::Lsn::new(current_lsn.as_u64().saturating_sub(1));
    for entry in read_set {
        let collection = &entry.collection;
        let read_lsn = entry.read_lsn;
        if written_collections.contains(collection) {
            continue;
        }
        if current > read_lsn && current > snapshot_lsn {
            // WAL advanced past what we read — concurrent write detected.
            rollback_with_gap_free(sessions, addr, state);
            return Some(CommitOutcome::Aborted {
                reason: AbortReason::Serialization,
            });
        }
    }
    None
}

/// Roll the session back to `Idle` and release any pending GAP_FREE sequence
/// reservations. Used by every COMMIT abort branch that must leave the session
/// idle without persisting — the transport adapters map `Aborted` to a wire
/// error and never roll back afterward, so each abort branch owns its rollback.
fn rollback_with_gap_free(sessions: &SessionStore, addr: &SocketAddr, state: &SharedState) {
    if let Ok(reservations) = sessions.rollback(addr) {
        for handle in &reservations {
            let key = handle.sequence_key.clone();
            let registry = &state.sequence_registry;
            registry.gap_free_manager().rollback(handle, || {
                let map = registry.sequences_read();
                if let Some(h) = map.get(&key) {
                    h.rollback_one();
                }
            });
        }
    }
}

/// Single-shard commit: resolve the transaction's staged post-images into one
/// replayable `TransactionRedo` WAL record, then dispatch the buffered plans as
/// one atomic `TransactionBatch` stamped with that record's LSN. The redo
/// record restores restart durability for in-transaction writes into in-memory
/// secondary indexes (vector HNSW, FTS) that the base storage engine cannot
/// rebuild on its own. Returns `Some(reason)` on failure.
async fn dispatch_single_shard(
    state: &SharedState,
    dp: &impl TxnDataPlane,
    buffered: &[PhysicalTask],
    tenant_id: crate::types::TenantId,
    vshard_id: crate::types::VShardId,
) -> Option<AbortReason> {
    let plans: Vec<PhysicalPlan> = buffered.iter().map(|t| t.plan.clone()).collect();

    // txn_id is present for any staged commit (buffer_write stamps it).
    let Some(txn_id) = buffered.first().and_then(|t| t.txn_id) else {
        return Some(AbortReason::Dispatch(crate::Error::Internal {
            detail: "single-shard commit: buffered task carries no txn_id".into(),
        }));
    };

    // 1. Resolve the transaction's staged post-images into ONE replayable
    //    RedoRecord. Read-only: reads `txn_overlays[txn_id]` on the owning
    //    core, writes nothing.
    let resolve_task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: crate::types::DatabaseId::DEFAULT,
        plan: PhysicalPlan::Meta(MetaOp::ResolveTxn {
            txn_id,
            plans: plans.clone(),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let resolve_resp = match dp.dispatch_no_wal(resolve_task, None).await {
        Ok(r) if r.status == Status::Ok => r,
        Ok(r) => {
            return Some(AbortReason::BatchRejected {
                code: r.error_code.clone(),
            });
        }
        Err(e) => return Some(AbortReason::Dispatch(e)),
    };
    let redo = match crate::wal::RedoRecord::from_bytes(resolve_resp.payload.as_bytes()) {
        Ok(r) => r,
        Err(e) => {
            return Some(AbortReason::Dispatch(crate::Error::Internal {
                detail: format!("single-shard commit: resolve redo decode failed: {e}"),
            }));
        }
    };

    // 2. Write-ahead the transaction as ONE replayable `TransactionRedo` record
    //    (each sub-op keeps its real engine `record_type`). `None` when the txn
    //    has no durable writes (all reads / CRDT / text). Its LSN stamps the
    //    batch install so the Data Plane records the committed write version for
    //    every key in the batch.
    let wal_lsn = if redo.ops.is_empty() {
        None
    } else {
        match state.wal.append_transaction_redo(
            tenant_id,
            vshard_id,
            crate::types::DatabaseId::DEFAULT,
            &redo,
        ) {
            Ok(lsn) => Some(lsn),
            Err(e) => {
                return Some(AbortReason::Dispatch(crate::Error::Internal {
                    detail: format!("single-shard commit: transaction redo WAL append failed: {e}"),
                }));
            }
        }
    };
    let batch_task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: crate::types::DatabaseId::DEFAULT,
        plan: PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans,
            // Reuse the resolve-time bitemporal stamps recorded in this
            // transaction's staging overlay so a `bitemporal=true` document put
            // installs on the same version key the redo (WAL-appended just
            // above) carries — otherwise a normal restart writes a second
            // version of the row.
            txn_id: Some(txn_id),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    classify_batch_dispatch(dp.dispatch_no_wal(batch_task, wal_lsn).await)
}

/// Convert a transaction-batch dispatch result into a commit abort reason, if
/// any. `dispatch_no_wal` returns `Ok(Response { status: Error, .. })` for a
/// failed batch rather than a Rust `Err` — the status must be checked
/// explicitly or a failed sub-plan reports as COMMIT success.
pub(super) fn classify_batch_dispatch(result: crate::Result<Response>) -> Option<AbortReason> {
    match result {
        Err(e) => {
            tracing::warn!(error = %e, "transaction batch dispatch failed");
            Some(AbortReason::Dispatch(e))
        }
        Ok(resp) if resp.status != Status::Ok => Some(AbortReason::BatchRejected {
            code: resp.error_code.clone(),
        }),
        Ok(_) => None,
    }
}

/// Flush any DDL buffered during the transaction as a single atomic metadata
/// Raft batch. Returns `Some(reason)` on encode/propose failure.
fn flush_buffered_ddl(state: &SharedState) -> Option<AbortReason> {
    let payloads = ddl_buffer::take()?;
    if payloads.is_empty() {
        return None;
    }
    // Each buffered entry carries the audit context captured at its own
    // statement boundary (not COMMIT time). Map to `CatalogDdlAudited` when
    // present so every sub-DDL gets its own audit record on every replica.
    let sub_entries: Vec<MetadataEntry> = payloads
        .into_iter()
        .map(|e| match e.audit {
            Some(ctx) => MetadataEntry::CatalogDdlAudited {
                payload: e.payload,
                auth_user_id: ctx.auth_user_id,
                auth_user_name: ctx.auth_user_name,
                sql_text: ctx.sql_text,
            },
            None => MetadataEntry::CatalogDdl { payload: e.payload },
        })
        .collect();
    let batch = MetadataEntry::Batch {
        entries: sub_entries,
    };
    if let Some(handle) = state.metadata_raft.get() {
        let raw = match encode_entry(&batch) {
            Ok(raw) => raw,
            Err(e) => {
                return Some(AbortReason::DdlPropose(crate::Error::Internal {
                    detail: format!("DDL batch encode: {e}"),
                }));
            }
        };
        if let Err(e) = handle.propose(raw) {
            return Some(AbortReason::DdlPropose(crate::Error::Internal {
                detail: format!("DDL batch propose: {e}"),
            }));
        }
    }
    None
}

/// Best-effort release of a transaction's staging overlay on a vShard that
/// hosted staged writes. Dispatched AFTER the durable resolution (COMMIT batch
/// flush / ROLLBACK); a failure here leaks in-memory overlay state on that
/// core but does not affect the already-resolved transaction, so it is logged
/// rather than surfaced.
pub(super) async fn drop_txn_overlay(
    dp: &impl TxnDataPlane,
    tenant_id: crate::types::TenantId,
    vshard_id: crate::types::VShardId,
    txn_id: crate::types::TxnId,
) {
    let task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: crate::types::DatabaseId::DEFAULT,
        plan: PhysicalPlan::Meta(MetaOp::DropTxnOverlay { txn_id }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    // Overlay teardown is not a write — no WAL record, no write version.
    if let Err(e) = dp.dispatch_no_wal(task, None).await {
        tracing::warn!(error = %e, "failed to drop per-transaction staging overlay");
    }
}
