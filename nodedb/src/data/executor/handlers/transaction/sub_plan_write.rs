// SPDX-License-Identifier: BUSL-1.1

//! Write-op execution helpers for transactional sub-plans.
//!
//! Each function here performs one engine's tracked write (recording an
//! `UndoEntry` for rollback) or the shared read-only / DDL passthrough
//! dispatch. Routing from `PhysicalPlan` variants to these helpers lives in
//! `sub_plan.rs`.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TraceId};

use super::undo::UndoEntry;

/// Fields for a transactional primary-vector insert (see `VectorOp::Insert`).
pub(super) struct TxVectorInsertParams<'a> {
    pub collection: &'a str,
    pub vector: &'a [f32],
    pub dim: usize,
    pub field_name: &'a str,
    pub surrogate: nodedb_types::Surrogate,
}

/// Fields for a transactional graph edge put (see `GraphOp::EdgePut`).
pub(super) struct TxEdgePutParams<'a> {
    pub collection: &'a str,
    pub src_id: &'a str,
    pub label: &'a str,
    pub dst_id: &'a str,
    pub properties: &'a [u8],
    pub src_surrogate: nodedb_types::Surrogate,
    pub dst_surrogate: nodedb_types::Surrogate,
}

/// Edge identity for a transaction-scoped edge delete.
pub(super) struct TxEdgeDeleteParams<'a> {
    pub collection: &'a str,
    pub src_id: &'a str,
    pub label: &'a str,
    pub dst_id: &'a str,
    /// Compiled RLS write-policy filters the staged plan carried. Decided
    /// against the edge's pre-image inside the batch, so a rejected delete
    /// fails the whole transaction instead of applying unchecked.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
}

impl CoreLoop {
    /// Insert into a primary-vector collection, recording an undo entry.
    pub(super) fn exec_tx_vector_insert(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        params: TxVectorInsertParams<'_>,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let TxVectorInsertParams {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
        } = params;

        let index_key = Self::vector_index_key(
            dummy_task.request.database_id.as_u64(),
            tid,
            collection,
            field_name,
        );
        let vp = self
            .vector_params
            .get(&index_key)
            .cloned()
            .unwrap_or_default();
        let index = self
            .vector_collections
            .entry(index_key.clone())
            .or_insert_with(|| crate::engine::vector::collection::VectorCollection::new(dim, vp));

        if vector.len() != index.dim() {
            return Err(ErrorCode::Internal {
                detail: format!(
                    "dimension mismatch: expected {}, got {}",
                    index.dim(),
                    vector.len()
                ),
            });
        }

        let vector_id = index.len() as u32;
        index.insert_with_surrogate(vector.to_vec(), surrogate);
        // Advance the checkpoint watermark with this transaction's WAL LSN so a
        // later vector checkpoint records the write as absorbed; the redo replay
        // (which carries the same enclosing record LSN) is then gated instead of
        // appending a duplicate node.
        if let Some(lsn) = dummy_task.wal_lsn() {
            index.note_checkpoint_lsn(lsn.as_u64());
        }
        // This is the direct primary-vector write path (VectorOp), not
        // the document auto-index cascade — it never populates
        // `vector_doc_map` (that reverse map is keyed by storage key,
        // which this path doesn't have). `None` `doc_id` tells
        // `apply_undo_vector` to skip the `vector_doc_map` mutation.
        undo_log.push(UndoEntry::InsertVector {
            index_key,
            vector_id,
            collection: collection.to_string(),
            field: field_name.to_string(),
            doc_id: None,
        });
        Ok(self.response_ok(dummy_task))
    }

    /// Delete from a primary-vector collection, recording an undo entry.
    pub(super) fn exec_tx_vector_delete(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        collection: &str,
        vector_id: u32,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Response {
        let index_key =
            Self::vector_index_key(dummy_task.request.database_id.as_u64(), tid, collection, "");
        if let Some(index) = self.vector_collections.get_mut(&index_key)
            && index.delete(vector_id)
        {
            // Same direct primary-vector path as `VectorOp::Insert`
            // above — no `vector_doc_map` entry to restore, so a
            // `None` `doc_id` skips that mutation in `apply_undo_vector`.
            undo_log.push(UndoEntry::DeleteVector {
                index_key,
                vector_id,
                collection: collection.to_string(),
                field: String::new(),
                doc_id: None,
            });
        }
        self.response_ok(dummy_task)
    }

    /// Upsert a graph edge, recording an undo entry with the prior properties.
    pub(super) fn exec_tx_edge_put(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        params: TxEdgePutParams<'_>,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let TxEdgePutParams {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate,
            dst_surrogate,
        } = params;

        // The compensation entry is recorded inside `execute_edge_put_with_undo`
        // at the only safe point — after the edge-store version is durably
        // written and before the fallible CSR mutation. Recording it here, up
        // front, would leave a phantom undo entry when dangling-endpoint
        // validation or the edge-store write itself rejects, corrupting
        // bitemporal history on rollback.
        let resp = self.execute_edge_put_with_undo(
            dummy_task,
            crate::data::executor::handlers::graph::EdgePutParams {
                tid,
                collection,
                src_id,
                label,
                dst_id,
                properties,
                src_surrogate,
                dst_surrogate,
            },
            Some(undo_log),
        );
        if resp.status == Status::Error {
            return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                detail: "edge put failed".into(),
            }));
        }
        Ok(resp)
    }

    /// Delete a graph edge, recording an undo entry with the prior properties.
    pub(super) fn exec_tx_edge_delete(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        params: TxEdgeDeleteParams<'_>,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let TxEdgeDeleteParams {
            collection,
            src_id,
            label,
            dst_id,
            rls_write_check,
        } = params;

        // Compensation is recorded inside `execute_edge_delete_with_undo` only
        // after the tombstone is durably written (and only when a live
        // pre-image existed), so a rejected/failed delete leaves no phantom
        // re-insert entry behind.
        let resp = self.execute_edge_delete_with_undo(
            dummy_task,
            crate::data::executor::handlers::graph::EdgeDeleteParams {
                tid,
                collection,
                src_id,
                label,
                dst_id,
                rls_write_check,
            },
            Some(undo_log),
        );
        if resp.status == Status::Error {
            return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                detail: "edge delete failed".into(),
            }));
        }
        Ok(resp)
    }

    /// Apply a columnar predicate `UPDATE`, recording undo for atomic rollback.
    pub(super) fn exec_tx_columnar_update(
        &mut self,
        dummy_task: &ExecutionTask,
        collection: &str,
        filters: &[u8],
        updates: &[(String, Vec<u8>)],
        rls_write_check: &nodedb_types::RlsWriteCheck,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let resp = self.execute_columnar_update(
            dummy_task,
            collection,
            filters,
            updates,
            rls_write_check,
            Some(undo_log),
        );
        if resp.status == Status::Error {
            return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                detail: "columnar update failed".into(),
            }));
        }
        Ok(resp)
    }

    /// Apply a columnar predicate `DELETE`, recording undo for atomic rollback.
    pub(super) fn exec_tx_columnar_delete(
        &mut self,
        dummy_task: &ExecutionTask,
        collection: &str,
        filters: &[u8],
        rls_write_check: &nodedb_types::RlsWriteCheck,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let resp = self.execute_columnar_delete(
            dummy_task,
            collection,
            filters,
            rls_write_check,
            Some(undo_log),
        );
        if resp.status == Status::Error {
            return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                detail: "columnar delete failed".into(),
            }));
        }
        Ok(resp)
    }

    /// Apply a columnar `ResolvedUpdate` (Control-Plane-resolved row set),
    /// recording undo for atomic rollback. Mirrors `exec_tx_columnar_update`
    /// exactly — the underlying handler decides `{"affected": N}` vs a
    /// drift-check `OllpRetryRequired` the same way regardless of caller.
    pub(super) fn exec_tx_columnar_resolved_update(
        &mut self,
        dummy_task: &ExecutionTask,
        collection: &str,
        rows: &[(nodedb_types::Value, Vec<nodedb_types::Value>)],
        rls_write_check: &nodedb_types::RlsWriteCheck,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let resp = self.execute_columnar_resolved_update(
            dummy_task,
            collection,
            rows,
            rls_write_check,
            Some(undo_log),
        );
        if resp.status == Status::Error {
            return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                detail: "columnar resolved update failed".into(),
            }));
        }
        Ok(resp)
    }

    /// Apply a columnar `ResolvedDelete` (Control-Plane-resolved row set),
    /// recording undo for atomic rollback.
    pub(super) fn exec_tx_columnar_resolved_delete(
        &mut self,
        dummy_task: &ExecutionTask,
        collection: &str,
        pks: &[nodedb_types::Value],
        rls_write_check: &nodedb_types::RlsWriteCheck,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        let resp = self.execute_columnar_resolved_delete(
            dummy_task,
            collection,
            pks,
            rls_write_check,
            Some(undo_log),
        );
        if resp.status == Status::Error {
            return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                detail: "columnar resolved delete failed".into(),
            }));
        }
        Ok(resp)
    }

    /// Execute a read-only / DDL sub-plan via the standard dispatch path.
    ///
    /// None of these variants mutate engine state, so no undo entry is needed.
    ///
    /// `deadline` is the enclosing statement's, copied from the parent task. A
    /// sub-plan is part of the statement that spawned it, so it inherits that
    /// statement's remaining budget; minting a fresh one here would let a
    /// transaction outlive the `statement_timeout` its client set by one
    /// sub-plan's worth of work per sub-plan.
    pub(super) fn exec_tx_passthrough(
        &mut self,
        tid: u64,
        plan: &PhysicalPlan,
        deadline: std::time::Instant,
    ) -> Result<Response, ErrorCode> {
        let resp = self.execute(&ExecutionTask::new(crate::bridge::envelope::Request {
            request_id: crate::types::RequestId::new(0),
            tenant_id: TenantId::new(tid),
            database_id: DatabaseId::DEFAULT,
            vshard_id: crate::types::VShardId::new(0),
            plan: plan.clone(),
            // no-determinism: sub-plan deadline is ephemeral, not written to WAL
            deadline,
            priority: crate::bridge::envelope::Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: crate::types::ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::AlreadyOrdered,
            ),
        }));
        if resp.status == Status::Error {
            return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                detail: "sub-plan execution failed".into(),
            }));
        }
        Ok(resp)
    }
}
