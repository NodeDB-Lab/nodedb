// SPDX-License-Identifier: BUSL-1.1

//! KV operation dispatch for transaction batches.

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::KvOp;

use super::undo::UndoEntry;

impl CoreLoop {
    /// Execute a KV operation in a transaction context. Write ops (including
    /// TTL and sorted-index DDL) capture prior state and push an
    /// `UndoEntry`; reads execute without undo tracking.
    pub(super) fn execute_tx_kv(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &KvOp,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let did = task.request.database_id.as_u64();
        match op {
            // ── Read-only KV ops — no undo needed ───────────────────────────
            KvOp::Get { .. }
            | KvOp::Scan { .. }
            | KvOp::MaterializeScan { .. }
            | KvOp::BatchGet { .. }
            | KvOp::GetTtl { .. }
            | KvOp::FieldGet { .. }
            | KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. } => {
                let resp = self.execute_kv(task, did, tid, op);
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv read failed".into(),
                    }));
                }
                Ok(resp)
            }

            // ── DDL — reject inside TransactionBatch ──
            // `plan_requires_txn_buffering` classifies these unbuffered, so a
            // client statement never replays through this arm at commit; it
            // guards a hypothetical direct-dispatch route.
            KvOp::RegisterIndex { .. } | KvOp::DropIndex { .. } => Err(ErrorCode::Internal {
                detail: "KV secondary-index DDL is not permitted inside a TransactionBatch".into(),
            }),

            // ── Truncate — replayed live, in statement order ──
            // Staged as an overlay marker at statement time; the live truncate
            // wipes every row replayed before it in this batch. Like the
            // Document truncate passthrough, it pushes no undo entry.
            KvOp::Truncate { collection } => {
                let resp = self.execute_kv_truncate(task, did, tid, collection.as_str());
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "kv truncate failed".into(),
                    }));
                }
                Ok(resp)
            }

            // ── TTL ops — capture prior expiry, execute, push undo ───────────
            KvOp::Expire {
                collection,
                key,
                ttl_ms,
                rls_write_check,
            } => self.execute_tx_kv_expire(
                task,
                crate::data::executor::handlers::kv::ttl::KvTtlTarget {
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    rls_write_check,
                },
                *ttl_ms,
                undo_log,
            ),

            KvOp::Persist {
                collection,
                key,
                rls_write_check,
            } => self.execute_tx_kv_persist(
                task,
                crate::data::executor::handlers::kv::ttl::KvTtlTarget {
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    rls_write_check,
                },
                undo_log,
            ),

            // ── Sorted-index DDL — capture prior def, execute, push undo ─────
            KvOp::RegisterSortedIndex {
                collection,
                index_name,
                sort_columns,
                key_column,
                window_type,
                window_timestamp_column,
                window_start_ms,
                window_end_ms,
            } => self.execute_tx_kv_register_sorted_index(
                task,
                super::sub_plan_kv_ttl_sorted::TxRegisterSortedIndexParams {
                    did,
                    tid,
                    collection: collection.as_str(),
                    index_name,
                    sort_columns,
                    key_column,
                    window_type,
                    window_timestamp_column,
                    window_start_ms: *window_start_ms,
                    window_end_ms: *window_end_ms,
                },
                undo_log,
            ),

            KvOp::DropSortedIndex { index_name } => {
                self.execute_tx_kv_drop_sorted_index(task, did, tid, index_name, undo_log)
            }

            // ── Write ops — delegated to `sub_plan_kv_writes::execute_tx_kv_write`.
            KvOp::Put { .. }
            | KvOp::Insert { .. }
            | KvOp::InsertIfAbsent { .. }
            | KvOp::InsertOnConflictUpdate { .. }
            | KvOp::Delete { .. }
            | KvOp::BatchPut { .. }
            | KvOp::FieldSet { .. }
            | KvOp::Incr { .. }
            | KvOp::IncrFloat { .. }
            | KvOp::Cas { .. }
            | KvOp::GetSet { .. }
            | KvOp::Transfer { .. }
            | KvOp::TransferItem { .. } => self.execute_tx_kv_write(task, did, tid, op, undo_log),

            // ── Resolve-before-propose — reject inside TransactionBatch ──
            // Both are autocommit-only: resolution is decided against
            // committed state and proposed straight through Raft.
            KvOp::ResolveWrite(_) | KvOp::ResolvedWrite { .. } => Err(ErrorCode::Internal {
                detail: "KV resolve-before-propose is not permitted inside a TransactionBatch"
                    .into(),
            }),

            // A predicate write resolves its row set from committed state at
            // apply time; the transaction redo record has no per-row shape
            // for that. Autocommit is the supported path.
            KvOp::PredicateUpdate { .. } | KvOp::PredicateDelete { .. } => {
                Err(ErrorCode::Internal {
                    detail: "KV predicate UPDATE/DELETE is not permitted inside a \
                             TransactionBatch; run it outside an explicit transaction"
                        .into(),
                })
            }
        }
    }
}
