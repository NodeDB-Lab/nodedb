// SPDX-License-Identifier: BUSL-1.1

//! Per-sub-plan dispatch within a transaction batch.
//!
//! Write-op execution helpers (the pieces that actually mutate engine state
//! and record undo entries) live in `sub_plan_write.rs`; this file only
//! routes each `PhysicalPlan` variant to its engine-specific handler.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TraceId};
use nodedb_physical::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, GraphOp, MetaOp, TimeseriesOp, VectorOp,
};

use super::sub_plan_doc::{TxPointDelete, TxPointPut};
use super::sub_plan_write::{TxEdgeDeleteParams, TxEdgePutParams, TxVectorInsertParams};
use super::undo::UndoEntry;

impl CoreLoop {
    /// Execute a single sub-plan within a transaction, recording undo info.
    ///
    /// CRDT deltas are NOT applied immediately — they are buffered in
    /// `crdt_deltas` and only applied after all sub-plans succeed.
    ///
    /// Dispatches by outer `PhysicalPlan` variant to a per-engine helper.
    /// Each helper handles that engine's write sub-ops (pushing an
    /// `UndoEntry`) and routes every other sub-op through the standard
    /// read-only / DDL dispatch path.
    pub(super) fn execute_tx_sub_plan(
        &mut self,
        tid: u64,
        plan: &PhysicalPlan,
        undo_log: &mut Vec<UndoEntry>,
        crdt_deltas: &mut Vec<(Vec<u8>, u64, String)>,
        user_roles: &[String],
    ) -> Result<Response, ErrorCode> {
        let dummy_task = Self::build_dummy_task(tid);

        match plan {
            PhysicalPlan::Document(op) => {
                self.exec_tx_document(&dummy_task, tid, plan, op, user_roles, undo_log)
            }
            PhysicalPlan::Vector(op) => self.exec_tx_vector(&dummy_task, tid, plan, op, undo_log),
            PhysicalPlan::Graph(op) => self.exec_tx_graph(&dummy_task, tid, plan, op, undo_log),
            PhysicalPlan::Crdt(op) => self.exec_tx_crdt(&dummy_task, tid, plan, op, crdt_deltas),
            PhysicalPlan::Kv(kv_op) => self.execute_tx_kv(&dummy_task, tid, kv_op, undo_log),
            PhysicalPlan::Columnar(op) => {
                self.exec_tx_columnar(&dummy_task, tid, plan, op, undo_log)
            }
            PhysicalPlan::Timeseries(op) => {
                self.exec_tx_timeseries(&dummy_task, tid, plan, op, undo_log)
            }
            PhysicalPlan::Spatial(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_) => self.exec_tx_passthrough(tid, plan),
        }
    }

    /// Build the ephemeral task used for sub-plan response construction.
    ///
    /// no-determinism: the deadline is ephemeral, not written to WAL. The
    /// placeholder `plan` (a no-op `Meta::Cancel`) is never executed; it
    /// only carries request metadata for response building.
    pub(super) fn build_dummy_task(tid: u64) -> ExecutionTask {
        ExecutionTask::new(crate::bridge::envelope::Request {
            request_id: crate::types::RequestId::new(0),
            tenant_id: TenantId::new(tid),
            database_id: DatabaseId::DEFAULT,
            vshard_id: crate::types::VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Cancel {
                target_request_id: crate::types::RequestId::new(0),
            }),
            // no-determinism: ephemeral deadline is not written to Calvin state.
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
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
                crate::bridge::envelope::ExemptReason::Read,
            ),
        })
    }

    /// Document engine: point writes are undo-tracked; everything else
    /// (point reads, scans, DDL) passes through the standard dispatch path.
    fn exec_tx_document(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
        op: &DocumentOp,
        user_roles: &[String],
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        match op {
            DocumentOp::PointPut {
                collection,
                document_id,
                value,
                surrogate,
                ..
            } => self.tx_point_put(
                TxPointPut {
                    task: dummy_task,
                    tid,
                    collection,
                    document_id,
                    surrogate: *surrogate,
                    value,
                    user_roles,
                    insert_if_absent: None,
                },
                undo_log,
            ),

            DocumentOp::PointInsert {
                collection,
                document_id,
                value,
                if_absent,
                surrogate,
            } => self.tx_point_put(
                TxPointPut {
                    task: dummy_task,
                    tid,
                    collection,
                    document_id,
                    surrogate: *surrogate,
                    value,
                    user_roles,
                    insert_if_absent: Some(*if_absent),
                },
                undo_log,
            ),

            DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                ..
            } => self.tx_point_delete(
                TxPointDelete {
                    task: dummy_task,
                    tid,
                    collection,
                    document_id,
                    surrogate: *surrogate,
                    user_roles,
                },
                undo_log,
            ),

            _ => self.exec_tx_passthrough(tid, plan),
        }
    }

    /// Vector engine: primary-vector insert/delete are undo-tracked;
    /// everything else passes through the standard dispatch path.
    fn exec_tx_vector(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
        op: &VectorOp,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        match op {
            VectorOp::Insert {
                collection,
                vector,
                dim,
                field_name,
                surrogate,
                pk_bytes: _,
                provenance: _,
            } => self.exec_tx_vector_insert(
                dummy_task,
                tid,
                TxVectorInsertParams {
                    collection,
                    vector,
                    dim: *dim,
                    field_name,
                    surrogate: *surrogate,
                },
                undo_log,
            ),

            VectorOp::Delete {
                collection,
                vector_id,
            } => Ok(self.exec_tx_vector_delete(dummy_task, tid, collection, *vector_id, undo_log)),

            _ => self.exec_tx_passthrough(tid, plan),
        }
    }

    /// Graph engine: edge put/delete are undo-tracked; everything else
    /// passes through the standard dispatch path.
    fn exec_tx_graph(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
        op: &GraphOp,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        match op {
            GraphOp::EdgePut {
                collection,
                src_id,
                label,
                dst_id,
                properties,
                src_surrogate,
                dst_surrogate,
            } => self.exec_tx_edge_put(
                dummy_task,
                tid,
                TxEdgePutParams {
                    collection,
                    src_id,
                    label,
                    dst_id,
                    properties,
                    src_surrogate: *src_surrogate,
                    dst_surrogate: *dst_surrogate,
                },
                undo_log,
            ),

            GraphOp::EdgeDelete {
                collection,
                src_id,
                label,
                dst_id,
                ..
            } => self.exec_tx_edge_delete(
                dummy_task,
                tid,
                TxEdgeDeleteParams {
                    collection,
                    src_id,
                    label,
                    dst_id,
                },
                undo_log,
            ),

            _ => self.exec_tx_passthrough(tid, plan),
        }
    }

    /// CRDT engine: deltas are buffered (not applied) until commit; every
    /// other `CrdtOp` passes through the standard dispatch path.
    fn exec_tx_crdt(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
        op: &CrdtOp,
        crdt_deltas: &mut Vec<(Vec<u8>, u64, String)>,
    ) -> Result<Response, ErrorCode> {
        match op {
            CrdtOp::Apply {
                collection,
                delta,
                peer_id,
                ..
            } => {
                crdt_deltas.push((delta.clone(), *peer_id, collection.clone()));
                Ok(self.response_ok(dummy_task))
            }
            _ => self.exec_tx_passthrough(tid, plan),
        }
    }

    /// Columnar engine: insert / predicate update / predicate delete are
    /// undo-tracked; everything else passes through the standard dispatch
    /// path.
    ///
    /// Predicate update/delete are staged at statement time; this is the
    /// durable COMMIT replay. Undo is captured here so a sibling sub-plan
    /// failing later in the same COMMIT batch reverses this mutation —
    /// without it the columnar change would survive an atomic-rollback
    /// (partial commit).
    fn exec_tx_columnar(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
        op: &ColumnarOp,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        match op {
            ColumnarOp::Insert {
                collection,
                payload,
                format,
                intent,
                on_conflict_updates,
                surrogates,
                schema_bytes,
                provenance: _,
                wal_lsn: _,
            } => self.execute_tx_columnar_insert(
                dummy_task,
                super::sub_plan_kv::TxColumnarInsertParams {
                    collection,
                    payload,
                    format,
                    intent: *intent,
                    on_conflict_updates,
                    surrogates,
                    schema_bytes,
                },
                undo_log,
            ),

            ColumnarOp::Update {
                collection,
                filters,
                updates,
            } => self.exec_tx_columnar_update(dummy_task, collection, filters, updates, undo_log),

            ColumnarOp::Delete {
                collection,
                filters,
            } => self.exec_tx_columnar_delete(dummy_task, collection, filters, undo_log),

            _ => self.exec_tx_passthrough(tid, plan),
        }
    }

    /// Timeseries engine: ingest is undo-tracked; everything else passes
    /// through the standard dispatch path.
    fn exec_tx_timeseries(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
        op: &TimeseriesOp,
        undo_log: &mut Vec<UndoEntry>,
    ) -> Result<Response, ErrorCode> {
        match op {
            TimeseriesOp::Ingest {
                collection,
                payload,
                format,
                wal_lsn,
                ..
            } => self.execute_tx_timeseries_ingest(
                dummy_task,
                super::sub_plan_kv::TxTimeseriesIngestParams {
                    tid: TenantId::new(tid),
                    collection,
                    payload,
                    format,
                    wal_lsn: *wal_lsn,
                },
                undo_log,
            ),

            _ => self.exec_tx_passthrough(tid, plan),
        }
    }
}
