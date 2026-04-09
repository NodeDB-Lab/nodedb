//! Transaction batch execution handler.
//!
//! Executes a `PhysicalPlan::TransactionBatch` atomically: all sub-plans
//! succeed or all are rolled back. Write operations (PointPut, PointDelete,
//! VectorInsert, EdgePut, EdgeDelete) are tracked for rollback on failure.
//! CRDT deltas are accumulated in a scratch buffer and only applied on success.

use tracing::{debug, warn};

use crate::bridge::envelope::{Response, Status};
use crate::bridge::physical_plan::PhysicalPlan;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::undo::UndoEntry;

impl CoreLoop {
    /// Execute a transaction batch atomically.
    ///
    /// All sub-plans are executed in order. If any sub-plan fails, all
    /// previous writes are rolled back. CRDT deltas are buffered and only
    /// applied to LoroDoc on full success.
    ///
    /// The Control Plane has already written a single `RecordType::Transaction`
    /// WAL record covering all operations before dispatching this batch.
    #[allow(clippy::too_many_lines)]
    pub(in crate::data::executor) fn execute_transaction_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        plans: &[PhysicalPlan],
    ) -> Response {
        debug!(
            core = self.core_id,
            plan_count = plans.len(),
            "transaction batch begin"
        );

        let mut undo_log: Vec<UndoEntry> = Vec::with_capacity(plans.len());
        let mut crdt_deltas: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut last_response = self.response_ok(task);

        for (i, plan) in plans.iter().enumerate() {
            let result = self.execute_tx_sub_plan(
                tid,
                plan,
                &mut undo_log,
                &mut crdt_deltas,
                &task.request.user_roles,
            );

            match result {
                Ok(resp) => {
                    last_response = resp;
                }
                Err(error_code) => {
                    warn!(
                        core = self.core_id,
                        plan_index = i,
                        "transaction sub-plan failed, rolling back {} operations",
                        undo_log.len()
                    );

                    // Roll back all previous writes in reverse order.
                    self.rollback_undo_log(tid, undo_log);

                    // Discard CRDT scratch buffer (never applied).
                    drop(crdt_deltas);

                    return Response {
                        request_id: task.request_id(),
                        status: Status::Error,
                        attempt: 1,
                        partial: false,
                        payload: crate::bridge::envelope::Payload::empty(),
                        watermark_lsn: self.watermark,
                        error_code: Some(error_code),
                    };
                }
            }
        }

        // Pre-commit: BALANCED constraint check across all inserts in this transaction.
        if let Err(error_code) = self.check_balanced_constraints(tid, &undo_log) {
            warn!(
                core = self.core_id,
                "BALANCED constraint violated, rolling back {} operations",
                undo_log.len()
            );
            self.rollback_undo_log(tid, undo_log);
            return Response {
                request_id: task.request_id(),
                status: Status::Error,
                attempt: 1,
                partial: false,
                payload: crate::bridge::envelope::Payload::empty(),
                watermark_lsn: self.watermark,
                error_code: Some(error_code),
            };
        }

        // All sub-plans succeeded. Apply buffered CRDT deltas.
        for (delta, peer_id) in crdt_deltas {
            let tenant_id = crate::types::TenantId::new(tid);
            if let Ok(engine) = self.get_crdt_engine(tenant_id) {
                let _ = peer_id; // peer_id used for dedup in future
                if let Err(e) = engine.apply_committed_delta(&delta) {
                    warn!(core = self.core_id, error = %e, "CRDT delta apply failed during tx commit");
                }
            }
        }

        debug!(
            core = self.core_id,
            committed = plans.len(),
            "transaction batch committed"
        );

        // Emit deferred trigger events for all writes in the committed transaction.
        use crate::data::executor::core_loop::deferred::DeferredWrite;
        let deferred_writes: Vec<DeferredWrite> = undo_log
            .into_iter()
            .filter_map(|entry| match entry {
                UndoEntry::PutDocument {
                    collection,
                    document_id,
                    old_value,
                } => Some(DeferredWrite {
                        collection,
                        op: if old_value.is_some() {
                            crate::event::WriteOp::Update
                        } else {
                            crate::event::WriteOp::Insert
                        },
                        row_id: document_id,
                        new_value: None,
                        old_value,
                    }),
                    UndoEntry::DeleteDocument {
                        collection,
                        document_id,
                        old_value,
                    } => Some(DeferredWrite {
                        collection,
                        op: crate::event::WriteOp::Delete,
                        row_id: document_id,
                        new_value: None,
                        old_value: Some(old_value),
                    }),
                    _ => None, // Vector and edge undo entries don't trigger deferred triggers.
                })
                .collect();

        if !deferred_writes.is_empty() {
            self.emit_deferred_events(
                deferred_writes,
                task.request.tenant_id,
                task.request.vshard_id,
            );
        }

        // Return the last sub-plan payload, but keyed to the outer transaction request.
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: last_response.payload,
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }
}
