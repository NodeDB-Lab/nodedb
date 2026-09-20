// SPDX-License-Identifier: BUSL-1.1

//! Per-sub-plan dispatch within a transaction batch.
//!
//! Write-op execution helpers (the pieces that actually mutate engine state
//! and record undo entries) live in `sub_plan_write.rs`; this file only
//! routes each `PhysicalPlan` variant to its engine-specific handler.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TraceId};
use nodedb_physical::physical_plan::{CrdtOp, DocumentOp, GraphOp, MetaOp, TimeseriesOp, VectorOp};

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
    #[cfg(test)]
    pub(super) fn execute_tx_sub_plan(
        &mut self,
        tid: u64,
        plan: &PhysicalPlan,
        undo_log: &mut Vec<UndoEntry>,
        crdt_deltas: &mut Vec<(Vec<u8>, u64, String)>,
        user_roles: &[String],
    ) -> Result<Response, ErrorCode> {
        let task = Self::build_dummy_task(tid);
        self.execute_tx_sub_plan_with_task(&task, tid, plan, undo_log, crdt_deltas, user_roles)
    }

    /// Replay a sub-plan with the parent batch's database and vShard identity.
    /// Calvin executes the same logical batch on each participant, so erasing
    /// this routing identity makes every replica appear to be vShard zero and
    /// breaks canonical-owner side effects such as graph statistics.
    pub(super) fn execute_tx_sub_plan_from_batch(
        &mut self,
        parent: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
        undo_log: &mut Vec<UndoEntry>,
        crdt_deltas: &mut Vec<(Vec<u8>, u64, String)>,
        user_roles: &[String],
    ) -> Result<Response, ErrorCode> {
        // A deferred timeseries ingest must receive the enclosing transaction
        // record's WAL LSN. The synthetic task below intentionally has no LSN
        // (the batch records ordinary write versions only after commit), but
        // the timeseries partition stamp is its own replay floor. Losing the
        // enclosing LSN here would let a later flush stamp zero and replay the
        // committed transaction on top of its partition after restart.
        if let PhysicalPlan::Timeseries(op) = plan {
            return self.exec_tx_timeseries(parent, tid, plan, op, undo_log);
        }

        let task = Self::build_dummy_task_at(
            tid,
            parent.request.database_id,
            parent.request.vshard_id,
            // The sub-plan is part of the parent statement, so it runs on what
            // is left of the parent's budget rather than a fresh one.
            parent.request.deadline,
        );
        self.execute_tx_sub_plan_with_task(&task, tid, plan, undo_log, crdt_deltas, user_roles)
    }

    fn execute_tx_sub_plan_with_task(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
        undo_log: &mut Vec<UndoEntry>,
        crdt_deltas: &mut Vec<(Vec<u8>, u64, String)>,
        user_roles: &[String],
    ) -> Result<Response, ErrorCode> {
        match plan {
            PhysicalPlan::Document(op) => {
                self.exec_tx_document(dummy_task, tid, plan, op, user_roles, undo_log)
            }
            PhysicalPlan::Vector(op) => self.exec_tx_vector(dummy_task, tid, plan, op, undo_log),
            PhysicalPlan::Graph(op) => self.exec_tx_graph(dummy_task, tid, plan, op, undo_log),
            PhysicalPlan::Crdt(op) => self.exec_tx_crdt(dummy_task, tid, plan, op, crdt_deltas),
            PhysicalPlan::Kv(kv_op) => self.execute_tx_kv(dummy_task, tid, plan, kv_op, undo_log),
            PhysicalPlan::Columnar(op) => {
                self.exec_tx_columnar(dummy_task, tid, plan, op, undo_log)
            }
            PhysicalPlan::Timeseries(op) => {
                self.exec_tx_timeseries(dummy_task, tid, plan, op, undo_log)
            }
            PhysicalPlan::Spatial(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_) => {
                self.exec_tx_passthrough(tid, plan, dummy_task.request.deadline)
            }
        }
    }

    /// Build the ephemeral task used for sub-plan response construction.
    ///
    /// no-determinism: the deadline is ephemeral, not written to WAL. The
    /// placeholder `plan` (a no-op `Meta::Cancel`) is never executed; it
    /// only carries request metadata for response building.
    #[cfg(test)]
    pub(super) fn build_dummy_task(tid: u64) -> ExecutionTask {
        Self::build_dummy_task_at(
            tid,
            DatabaseId::DEFAULT,
            crate::types::VShardId::new(0),
            // no-determinism: test-only dummy deadline, never written to Calvin state
            std::time::Instant::now() + std::time::Duration::from_secs(60),
        )
    }

    /// `deadline` is the enclosing statement's, copied from the parent task, so
    /// every sub-plan this task carries stops when the statement does.
    fn build_dummy_task_at(
        tid: u64,
        database_id: DatabaseId,
        vshard_id: crate::types::VShardId,
        deadline: std::time::Instant,
    ) -> ExecutionTask {
        ExecutionTask::new(crate::bridge::envelope::Request {
            request_id: crate::types::RequestId::new(0),
            tenant_id: TenantId::new(tid),
            database_id,
            vshard_id,
            plan: PhysicalPlan::Meta(MetaOp::Cancel {
                target_request_id: crate::types::RequestId::new(0),
            }),
            // no-determinism: ephemeral deadline is not written to Calvin state.
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
                resolved_sum_targets,
                ..
            } => self.tx_point_put(
                TxPointPut {
                    task: dummy_task,
                    tid,
                    collection: collection.as_str(),
                    document_id,
                    surrogate: *surrogate,
                    value,
                    user_roles,
                    insert_if_absent: None,
                    resolved_sum_targets,
                    // A put carries no deferral list of its own: its balance is
                    // settled from row images and deferred by OMISSION from the
                    // resolution just above, which travels with it.
                    deferred_sum_targets: &[],
                },
                undo_log,
            ),

            DocumentOp::PointInsert {
                collection,
                document_id,
                value,
                if_absent,
                surrogate,
                resolved_sum_targets,
                // An insert's rows are new by construction, so its cross-shard
                // balance is settled at plan time and marked here rather than
                // omitted from the resolution. Forwarding it is not optional:
                // this arm is on the CALVIN apply path, which is the only path
                // a deferral-carrying write ever takes.
                deferred_sum_targets,
                ..
            } => self.tx_point_put(
                TxPointPut {
                    task: dummy_task,
                    tid,
                    collection: collection.as_str(),
                    document_id,
                    surrogate: *surrogate,
                    value,
                    user_roles,
                    insert_if_absent: Some(*if_absent),
                    resolved_sum_targets,
                    deferred_sum_targets,
                },
                undo_log,
            ),

            DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                resolved_sum_targets,
                ..
            } => self.tx_point_delete(
                TxPointDelete {
                    task: dummy_task,
                    tid,
                    collection: collection.as_str(),
                    document_id,
                    surrogate: *surrogate,
                    user_roles,
                    resolved_sum_targets,
                },
                undo_log,
            ),

            _ => self.exec_tx_passthrough(tid, plan, dummy_task.request.deadline),
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
                    collection: collection.as_str(),
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
            } => Ok(self.exec_tx_vector_delete(
                dummy_task,
                tid,
                collection.as_str(),
                *vector_id,
                undo_log,
            )),

            _ => self.exec_tx_passthrough(tid, plan, dummy_task.request.deadline),
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
                    collection: collection.as_str(),
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
                rls_write_check,
                ..
            } => self.exec_tx_edge_delete(
                dummy_task,
                tid,
                TxEdgeDeleteParams {
                    collection: collection.as_str(),
                    src_id,
                    label,
                    dst_id,
                    rls_write_check,
                },
                undo_log,
            ),

            GraphOp::EdgePutBatch { edges } => {
                let mut response = self.response_ok(dummy_task);
                for edge in edges {
                    response = self.exec_tx_edge_put(
                        dummy_task,
                        tid,
                        TxEdgePutParams {
                            collection: edge.collection.as_str(),
                            src_id: &edge.src_id,
                            label: &edge.label,
                            dst_id: &edge.dst_id,
                            properties: &[],
                            src_surrogate: edge.src_surrogate,
                            dst_surrogate: edge.dst_surrogate,
                        },
                        undo_log,
                    )?;
                }
                Ok(response)
            }

            GraphOp::EdgeDeleteBatch { edges } => {
                let mut response = self.response_ok(dummy_task);
                for edge in edges {
                    response = self.exec_tx_edge_delete(
                        dummy_task,
                        tid,
                        TxEdgeDeleteParams {
                            collection: edge.collection.as_str(),
                            src_id: &edge.src_id,
                            label: &edge.label,
                            dst_id: &edge.dst_id,
                            // A batched edge carries no property image, so the
                            // planner refuses the batch outright while a write
                            // policy applies — nothing reaches here to decide.
                            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                        },
                        undo_log,
                    )?;
                }
                Ok(response)
            }

            _ => self.exec_tx_passthrough(tid, plan, dummy_task.request.deadline),
        }
    }

    /// CRDT raw deltas cannot be part of a transaction batch: their exact
    /// post-merge authorization must be evaluated at the serialized admission
    /// boundary before any durable proposal. Document operations remain
    /// transaction-capable through their own staged handlers.
    fn exec_tx_crdt(
        &mut self,
        dummy_task: &ExecutionTask,
        tid: u64,
        plan: &PhysicalPlan,
        op: &CrdtOp,
        _crdt_deltas: &mut Vec<(Vec<u8>, u64, String)>,
    ) -> Result<Response, ErrorCode> {
        match op {
            CrdtOp::Apply { .. } | CrdtOp::ApplyAuthenticated { .. } => {
                Err(ErrorCode::Unsupported {
                    detail: "CRDT Apply is not supported inside transaction batches".into(),
                })
            }
            _ => self.exec_tx_passthrough(tid, plan, dummy_task.request.deadline),
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
                rls_write_check,
                ..
            } => self.execute_tx_timeseries_ingest(
                dummy_task,
                super::sub_plan_kv::TxTimeseriesIngestParams {
                    tid: TenantId::new(tid),
                    collection: collection.as_str(),
                    payload,
                    format,
                    rls_write_check,
                    // The enclosing transaction record, when present, is
                    // the durable identity of this ingest. A plan-local LSN is
                    // only a compatibility fallback for direct unit callers.
                    wal_lsn: dummy_task.wal_lsn().map(|lsn| lsn.as_u64()).or(*wal_lsn),
                },
                undo_log,
            ),

            // Staged as an overlay marker at statement time; the live truncate
            // records its whole pre-image (memory state plus the partition
            // directory renamed aside) for atomic rollback.
            TimeseriesOp::Truncate {
                collection,
                restart_identity: _,
            } => {
                let resp = self.execute_timeseries_truncate(
                    dummy_task,
                    collection.as_str(),
                    Some(undo_log),
                );
                if resp.status == Status::Error {
                    return Err(resp.error_code.map(|c| *c).unwrap_or(ErrorCode::Internal {
                        detail: "timeseries truncate failed".into(),
                    }));
                }
                Ok(resp)
            }

            TimeseriesOp::Scan { .. } | TimeseriesOp::ResolveIngest(_) => {
                self.exec_tx_passthrough(tid, plan, dummy_task.request.deadline)
            }
        }
    }
}
