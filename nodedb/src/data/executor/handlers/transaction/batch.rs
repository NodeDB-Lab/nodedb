// SPDX-License-Identifier: BUSL-1.1

//! Transaction batch execution handler.
//!
//! Executes a `PhysicalPlan::TransactionBatch` atomically: all sub-plans
//! succeed or all are rolled back. Write operations (PointPut, PointDelete,
//! VectorInsert, EdgePut, EdgeDelete) are tracked for rollback on failure.
//! CRDT deltas are accumulated in a scratch buffer and only applied on success.

use std::panic::{AssertUnwindSafe, catch_unwind};

use tracing::{debug, error, warn};

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_types::calvin::VersionedReadEntry;

use super::undo::UndoEntry;

/// A CRDT delta buffered during a transaction batch: `(delta_bytes, id,
/// collection)`. Deltas accumulate in a scratch buffer and are applied to
/// the LoroDoc only on full-batch success.
type CrdtDelta = (Vec<u8>, u64, String);

impl CoreLoop {
    /// Execute a transaction batch atomically.
    ///
    /// All sub-plans are executed in order. If any sub-plan fails, all
    /// previous writes are rolled back. CRDT deltas are buffered and only
    /// applied to LoroDoc on full success.
    ///
    /// The Control Plane has already written a single `RecordType::Transaction`
    /// WAL record covering all operations before dispatching this batch.
    pub(in crate::data::executor) fn execute_transaction_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
        versioned_reads: &[VersionedReadEntry],
        txn_id: Option<crate::types::TxnId>,
    ) -> Response {
        debug!(
            core = self.core_id,
            plan_count = plans.len(),
            "transaction batch begin"
        );

        // Check whether this participant's slice of the transaction's reads was
        // still current against the local write versions, observed BEFORE apply.
        // Non-gating: the outcome is reported on the response and the apply
        // proceeds regardless. Empty read-set (pure-write / autocommit / non-Calvin
        // fast path) is vacuously current.
        let read_set_current = self.read_set_still_current(task, tid, versioned_reads);

        let undo_log: Vec<UndoEntry> = Vec::with_capacity(plans.len());
        let crdt_deltas: Vec<CrdtDelta> = Vec::new();

        // Carry the resolve-time bitemporal stamps this transaction's overlay
        // recorded into per-core apply scratch, so `apply_point_put` installs
        // each bitemporal document put on the versioned store at the SAME stamp
        // the redo carries. Calvin threads its stamps in before the call
        // (`txn_id = None` here); the session single-shard commit passes its
        // `txn_id`. The scratch is consulted ONLY by the forward apply below, so
        // it is cleared the moment `run_sub_plans` returns (any path).
        if let Some(txn_id) = txn_id {
            self.load_bitemporal_stamps_for_txn(txn_id);
        }
        let sub = self.run_sub_plans(task, tid, plans, undo_log, crdt_deltas);
        self.active_bitemporal_stamps.clear();
        let (last_response, undo_log, crdt_deltas) = match sub {
            Ok(v) => v,
            Err(resp) => return resp,
        };

        let undo_log = match self.apply_balanced_constraint_check(task, tid, undo_log) {
            Ok(u) => u,
            Err(resp) => return resp,
        };

        if let Some(resp) = self.apply_crdt_deltas(task, tid, crdt_deltas) {
            return resp;
        }

        debug!(
            core = self.core_id,
            committed = plans.len(),
            "transaction batch committed"
        );

        // Record the per-key / per-collection write version for every sub-plan
        // in the committed batch. Covers the fast-path commit AND every Calvin
        // apply (both funnel through here); one batch WAL LSN for all keys.
        self.record_batch_write_versions(task, tid, plans);
        self.record_batch_index_write_values(task, tid, &undo_log);

        self.emit_deferred_writes(task, undo_log);

        // Return the last sub-plan payload, but keyed to the outer transaction request.
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: last_response.payload,
            watermark_lsn: self.watermark,
            error_code: None,
            read_set_valid: Some(read_set_current),
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    /// Copy every resolve-time bitemporal stamp recorded in `txn_id`'s staging
    /// overlay into the per-core `active_bitemporal_stamps` scratch. Surrogates
    /// are globally unique, so all collections' stamps flatten into one map.
    /// A no-op when no overlay exists (pure-read / non-bitemporal transaction).
    fn load_bitemporal_stamps_for_txn(&mut self, txn_id: crate::types::TxnId) {
        let stamps: Vec<_> = match self.txn_overlays.get(&txn_id) {
            Some(overlay) => overlay.all_bitemporal_stamps().collect(),
            None => return,
        };
        for (surrogate, stamp) in stamps {
            self.active_bitemporal_stamps.insert(surrogate, stamp);
        }
    }

    /// Run every sub-plan in order, tracking undo entries and buffered CRDT
    /// deltas as it goes.
    ///
    /// On success, returns the last sub-plan's response plus the accumulated
    /// undo log and CRDT delta buffer (for the caller's subsequent commit
    /// steps). On failure, rolls back all writes performed so far and
    /// returns the terminal error `Response` directly — the caller must
    /// return it unchanged.
    ///
    /// A panic (real or test-injected) during a sub-apply is caught so it
    /// routes through the same typed-rollback path instead of unwinding past
    /// `undo_log`, which would drop the log without running rollback and
    /// leave the shard half-committed.
    fn run_sub_plans(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
        mut undo_log: Vec<UndoEntry>,
        mut crdt_deltas: Vec<CrdtDelta>,
    ) -> Result<(Response, Vec<UndoEntry>, Vec<CrdtDelta>), Response> {
        let mut last_response = self.response_ok(task);

        for (i, plan) in plans.iter().enumerate() {
            let user_roles = &task.request.user_roles;
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                let r = self.execute_tx_sub_plan(
                    tid,
                    plan,
                    &mut undo_log,
                    &mut crdt_deltas,
                    user_roles,
                );
                crate::fail_point!("transaction_batch::between_subapply");
                r
            }));
            let result = match outcome {
                Ok(r) => r,
                Err(payload) => {
                    let detail = panic_payload_to_string(payload.as_ref());
                    error!(
                        core = self.core_id,
                        plan_index = i,
                        panic = %detail,
                        "transaction sub-apply panicked; routing through rollback path"
                    );
                    Err(ErrorCode::Internal {
                        detail: format!("panic in sub-apply at index {i}: {detail}"),
                    })
                }
            };

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
                    // If rollback itself fails, the shard state is unknown —
                    // return RollbackFailed (never warn-and-continue).
                    let rollback_error_code = match self.rollback_undo_log(
                        crate::types::DatabaseId::DEFAULT.as_u64(),
                        tid,
                        undo_log,
                    ) {
                        Ok(()) => error_code,
                        Err((entry_index, detail)) => {
                            error!(
                                core = self.core_id,
                                plan_index = i,
                                entry_index,
                                detail = %detail,
                                "transaction rollback failed; shard state unknown — \
                                 restart required for WAL replay"
                            );
                            crate::bridge::envelope::ErrorCode::RollbackFailed {
                                entry_index,
                                detail,
                            }
                        }
                    };

                    // Discard CRDT scratch buffer (never applied).
                    drop(crdt_deltas);

                    return Err(Response {
                        request_id: task.request_id(),
                        status: Status::Error,
                        attempt: 1,
                        partial: false,
                        payload: crate::bridge::envelope::Payload::empty(),
                        watermark_lsn: self.watermark,
                        error_code: Some(Box::new(rollback_error_code)),
                        read_set_valid: None,
                        read_version_lsn: crate::types::Lsn::ZERO,
                        write_set: Vec::new(),
                    });
                }
            }
        }

        Ok((last_response, undo_log, crdt_deltas))
    }

    /// Pre-commit: check the `BALANCED` constraint across all inserts in
    /// this transaction. On violation, rolls back and returns the terminal
    /// error `Response`; on success, hands the undo log back for the
    /// deferred-trigger step.
    fn apply_balanced_constraint_check(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        undo_log: Vec<UndoEntry>,
    ) -> Result<Vec<UndoEntry>, Response> {
        if let Err(error_code) =
            self.check_balanced_constraints(task.request.database_id.as_u64(), tid, &undo_log)
        {
            warn!(
                core = self.core_id,
                "BALANCED constraint violated, rolling back {} operations",
                undo_log.len()
            );
            let rollback_error_code = match self.rollback_undo_log(
                crate::types::DatabaseId::DEFAULT.as_u64(),
                tid,
                undo_log,
            ) {
                Ok(()) => error_code,
                Err((entry_index, detail)) => {
                    error!(
                        core = self.core_id,
                        entry_index,
                        detail = %detail,
                        "transaction rollback failed (BALANCED constraint path); \
                         shard state unknown — restart required for WAL replay"
                    );
                    crate::bridge::envelope::ErrorCode::RollbackFailed {
                        entry_index,
                        detail,
                    }
                }
            };
            return Err(Response {
                request_id: task.request_id(),
                status: Status::Error,
                attempt: 1,
                partial: false,
                payload: crate::bridge::envelope::Payload::empty(),
                watermark_lsn: self.watermark,
                error_code: Some(Box::new(rollback_error_code)),
                read_set_valid: None,
                read_version_lsn: crate::types::Lsn::ZERO,
                write_set: Vec::new(),
            });
        }
        Ok(undo_log)
    }

    /// Apply all buffered CRDT deltas now that every sub-plan and the
    /// `BALANCED` constraint check have succeeded.
    ///
    /// Failure here means the CRDT state is inconsistent with the
    /// already-committed forward writes — returns a `RollbackFailed`
    /// `Response` so the client knows the shard needs a restart to restore
    /// consistency via WAL replay. Never warn-and-continue.
    fn apply_crdt_deltas(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        crdt_deltas: Vec<CrdtDelta>,
    ) -> Option<Response> {
        for (crdt_idx, (delta, _peer_id, collection)) in crdt_deltas.into_iter().enumerate() {
            // Crash-injection point — between forward-write commit and CRDT
            // apply. WAL replay must roll the CRDT side forward (or roll
            // forward writes back) to restore consistency.
            crate::fail_point!("transaction_batch::between_crdt_delta");

            let tenant_id = crate::types::TenantId::new(tid);
            match self.get_crdt_engine(task.request.database_id, tenant_id) {
                Ok(engine) => {
                    // NOTE: applies committed CRDT deltas via a bare import, with NO
                    // constraint validation. If deterministic apply-time validation is
                    // ever added, it MUST also gate this path (and the WAL replay path) —
                    // otherwise a delta rejected on one path could persist here and
                    // diverge from peers.
                    if let Err(e) = engine.apply_committed_delta(&collection, &delta) {
                        error!(
                            core = self.core_id,
                            crdt_delta_index = crdt_idx,
                            error = %e,
                            "CRDT delta apply failed after forward writes committed; \
                             shard state unknown — restart required for WAL replay"
                        );
                        return Some(Response {
                            request_id: task.request_id(),
                            status: Status::Error,
                            attempt: 1,
                            partial: false,
                            payload: crate::bridge::envelope::Payload::empty(),
                            watermark_lsn: self.watermark,
                            error_code: Some(Box::new(
                                crate::bridge::envelope::ErrorCode::RollbackFailed {
                                    entry_index: crdt_idx,
                                    detail: format!("CRDT delta apply failed: {e}"),
                                },
                            )),
                            read_set_valid: None,
                            read_version_lsn: crate::types::Lsn::ZERO,
                            write_set: Vec::new(),
                        });
                    }
                }
                Err(e) => {
                    error!(
                        core = self.core_id,
                        crdt_delta_index = crdt_idx,
                        error = %e,
                        "CRDT engine not found after forward writes committed; \
                         shard state unknown — restart required for WAL replay"
                    );
                    return Some(Response {
                        request_id: task.request_id(),
                        status: Status::Error,
                        attempt: 1,
                        partial: false,
                        payload: crate::bridge::envelope::Payload::empty(),
                        watermark_lsn: self.watermark,
                        error_code: Some(Box::new(
                            crate::bridge::envelope::ErrorCode::RollbackFailed {
                                entry_index: crdt_idx,
                                detail: format!("CRDT engine not available: {e}"),
                            },
                        )),
                        read_set_valid: None,
                        read_version_lsn: crate::types::Lsn::ZERO,
                        write_set: Vec::new(),
                    });
                }
            }
        }
        None
    }

    /// Emit deferred trigger events for every write recorded in the
    /// committed transaction's undo log.
    fn emit_deferred_writes(&mut self, task: &ExecutionTask, undo_log: Vec<UndoEntry>) {
        use crate::data::executor::core_loop::deferred::DeferredWrite;
        let deferred_writes: Vec<DeferredWrite> = undo_log
            .into_iter()
            .filter_map(|entry| match entry {
                UndoEntry::PutDocument {
                    collection,
                    document_id,
                    old_value,
                    ..
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
                    ..
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
    }
}

/// Best-effort conversion of a panic payload to a human-readable string.
/// Tries the two common payload types (`&'static str` and `String`); falls
/// back to `"<non-string panic payload>"` for anything else.
fn panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}
