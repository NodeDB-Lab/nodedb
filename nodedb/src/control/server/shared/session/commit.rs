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
use crate::control::planner::calvin::{DispatchClass, classify_dispatch};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::plan_util::extract_collection;
use crate::control::state::SharedState;
use nodedb_cluster::{MetadataEntry, encode_entry};
use nodedb_physical::physical_plan::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::ddl_buffer;
use super::outcome::{AbortReason, CommitOutcome, TxnDataPlane};
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
    // Snapshot isolation: check for write conflicts before committing.
    let read_set = sessions.take_read_set(addr);
    // Collections this transaction wrote itself. A read of a collection the
    // same transaction has written is a read-your-own-write, not a
    // serialization conflict — reading uncommitted own state (served from the
    // staging overlay, which reports no watermark) must not abort the commit.
    // The read-set is collection-granular, so exclusion is too.
    let written_collections =
        sessions.buffered_collections(addr, |plan| extract_collection(plan).map(String::from));
    if let Some(snapshot_lsn) = sessions.snapshot_lsn(addr) {
        let current_lsn = state.wal.next_lsn();
        let current = crate::types::Lsn::new(current_lsn.as_u64().saturating_sub(1));
        for entry in &read_set {
            let collection = &entry.collection;
            let read_lsn = entry.read_lsn;
            if written_collections.contains(collection) {
                continue;
            }
            if current > read_lsn && current > snapshot_lsn {
                // WAL advanced past what we read — concurrent write detected.
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
                return CommitOutcome::Aborted {
                    reason: AbortReason::Serialization,
                };
            }
        }
    }

    let buffered = match sessions.commit(addr) {
        Ok(b) => b,
        Err(_msg) => {
            return CommitOutcome::Aborted {
                reason: AbortReason::NoTransaction,
            };
        }
    };

    if !buffered.is_empty() {
        let tenant_id = identity.tenant_id;

        match classify_dispatch(&buffered) {
            DispatchClass::SingleShard { vshard: vshard_id } => {
                if let Some(reason) =
                    dispatch_single_shard(state, dp, &buffered, tenant_id, vshard_id).await
                {
                    return CommitOutcome::Aborted { reason };
                }
            }
            DispatchClass::MultiShard { .. } => {
                if let Some(reason) = super::commit_calvin::run_commit_calvin(
                    sessions, addr, state, dp, &buffered, tenant_id,
                )
                .await
                {
                    return CommitOutcome::Aborted { reason };
                }
            }
        }

        // Release the per-transaction staging overlay on every vShard that
        // hosted a staged write, now that the durable batch(es) have flushed.
        // Guarded on a staged (txn_id-carrying) buffer.
        if let Some(txn_id) = buffered[0].txn_id {
            let mut dropped = std::collections::HashSet::new();
            for task in &buffered {
                if dropped.insert(task.vshard_id) {
                    drop_txn_overlay(dp, tenant_id, task.vshard_id, txn_id).await;
                }
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

/// Single-shard commit: write the transaction as one WAL record, then dispatch
/// the buffered plans as one atomic `TransactionBatch`. Returns `Some(reason)`
/// on failure.
async fn dispatch_single_shard(
    state: &SharedState,
    dp: &impl TxnDataPlane,
    buffered: &[PhysicalTask],
    tenant_id: crate::types::TenantId,
    vshard_id: crate::types::VShardId,
) -> Option<AbortReason> {
    let mut sub_records: Vec<(u16, Vec<u8>)> = Vec::with_capacity(buffered.len());
    for task in buffered {
        if let Some(entry) = crate::control::wal_replication::to_replicated_entry(
            task.tenant_id,
            task.database_id,
            task.vshard_id,
            &task.plan,
        ) {
            let bytes = entry.to_bytes();
            sub_records.push((nodedb_wal::record::RecordType::Put as u16, bytes));
        }
    }

    // The single transaction WAL record's LSN is stamped onto the batch
    // dispatch below so the Data Plane records the committed write version for
    // every key in the batch. `None` when the batch has no durable writes.
    let wal_lsn = if !sub_records.is_empty() {
        let tx_payload = match zerompk::to_msgpack_vec(&sub_records) {
            Ok(p) => p,
            Err(e) => {
                return Some(AbortReason::Dispatch(crate::Error::Internal {
                    detail: format!("transaction WAL serialization failed: {e}"),
                }));
            }
        };
        match state.wal.append_transaction(
            tenant_id,
            vshard_id,
            crate::types::DatabaseId::DEFAULT,
            &tx_payload,
        ) {
            Ok(lsn) => Some(lsn),
            Err(e) => {
                return Some(AbortReason::Dispatch(crate::Error::Internal {
                    detail: format!("transaction WAL append failed: {e}"),
                }));
            }
        }
    } else {
        None
    };

    let plans: Vec<PhysicalPlan> = buffered.iter().map(|t| t.plan.clone()).collect();
    let batch_task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: crate::types::DatabaseId::DEFAULT,
        plan: PhysicalPlan::Meta(MetaOp::TransactionBatch { plans }),
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
        // Envelope id doubles as the routing signal: the drop must reach the
        // vShard leader hosting this transaction's overlay, not a local replica.
        txn_id: Some(txn_id),
    };
    // Overlay teardown is not a write — no WAL record, no write version.
    if let Err(e) = dp.dispatch_no_wal(task, None).await {
        tracing::warn!(error = %e, "failed to drop per-transaction staging overlay");
    }
}
