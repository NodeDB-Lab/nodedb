//! Transaction control: BEGIN, COMMIT, ROLLBACK.

use nodedb_types::protocol::NativeResponse;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::physical::PhysicalTask;

use super::super::super::dispatch_utils;
use super::{DispatchCtx, error_to_native};

pub(crate) fn handle_begin(ctx: &DispatchCtx<'_>, seq: u64) -> NativeResponse {
    let snapshot_lsn = {
        let next = ctx.state.wal.next_lsn();
        crate::types::Lsn::new(next.as_u64().saturating_sub(1))
    };
    match ctx.sessions.begin(ctx.peer_addr, snapshot_lsn) {
        Ok(()) => NativeResponse::status_row(seq, "BEGIN"),
        Err(msg) => NativeResponse::error(seq, "25P02", msg),
    }
}

pub(crate) async fn handle_commit(ctx: &DispatchCtx<'_>, seq: u64) -> NativeResponse {
    // Snapshot isolation conflict check.
    let read_set = ctx.sessions.take_read_set(ctx.peer_addr);
    if let Some(snapshot_lsn) = ctx.sessions.snapshot_lsn(ctx.peer_addr) {
        let current_lsn = ctx.state.wal.next_lsn();
        let current = crate::types::Lsn::new(current_lsn.as_u64().saturating_sub(1));
        for (_collection, _doc_id, read_lsn) in &read_set {
            if current > *read_lsn && current > snapshot_lsn {
                let _ = ctx.sessions.rollback(ctx.peer_addr);
                return NativeResponse::error(
                    seq,
                    "40001",
                    "could not serialize access due to concurrent update",
                );
            }
        }
    }

    let buffered = match ctx.sessions.commit(ctx.peer_addr) {
        Ok(b) => b,
        Err(msg) => return NativeResponse::error(seq, "25000", msg),
    };

    if !buffered.is_empty() {
        let tenant_id = ctx.identity.tenant_id;
        let vshard_id = buffered[0].vshard_id;

        // WAL transaction record.
        let mut sub_records: Vec<(u16, Vec<u8>)> = Vec::with_capacity(buffered.len());
        for task in &buffered {
            if let Some(entry) = crate::control::wal_replication::to_replicated_entry(
                task.tenant_id,
                task.vshard_id,
                &task.plan,
            ) {
                let bytes = entry.to_bytes();
                sub_records.push((nodedb_wal::record::RecordType::Put as u16, bytes));
            }
        }

        if !sub_records.is_empty() {
            match rmp_serde::to_vec_named(&sub_records) {
                Ok(tx_payload) => {
                    if let Err(e) =
                        ctx.state
                            .wal
                            .append_transaction(tenant_id, vshard_id, &tx_payload)
                    {
                        return error_to_native(seq, &e);
                    }
                }
                Err(e) => {
                    return NativeResponse::error(
                        seq,
                        "XX000",
                        format!("transaction WAL serialization failed: {e}"),
                    );
                }
            }
        }

        // Dispatch as atomic TransactionBatch.
        let plans: Vec<PhysicalPlan> = buffered.iter().map(|t| t.plan.clone()).collect();
        let batch_task = PhysicalTask {
            tenant_id,
            vshard_id,
            plan: PhysicalPlan::TransactionBatch { plans },
        };
        if let Err(e) = dispatch_utils::dispatch_to_data_plane(
            ctx.state,
            batch_task.tenant_id,
            batch_task.vshard_id,
            batch_task.plan,
            0,
        )
        .await
        {
            return NativeResponse::error(seq, "40001", format!("transaction commit failed: {e}"));
        }
    }

    NativeResponse::status_row(seq, "COMMIT")
}

pub(crate) fn handle_rollback(ctx: &DispatchCtx<'_>, seq: u64) -> NativeResponse {
    let _ = ctx.sessions.rollback(ctx.peer_addr);
    NativeResponse::status_row(seq, "ROLLBACK")
}
