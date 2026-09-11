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
pub(super) type CrdtDelta = (Vec<u8>, u64, String);

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
            self.active_graph_system_from = self
                .graph_txn_overlays
                .get(&txn_id)
                .and_then(|overlay| overlay.resolved_system_from());
        }
        // Open the transaction's BALANCED accumulator for the duration of the
        // sub-plans. Every write handler they reach — including the ones
        // reached by passing an ordinary statement plan back through
        // `execute` — settles its entries onto this instead of checking them on
        // its own, so one leg of a journal per statement is allowed inside an
        // explicit transaction and only the whole batch has to balance.
        //
        // A batch reached from inside another batch (a `MetaOp::TransactionBatch`
        // sub-plan passes back through `execute`) does NOT open its own: the
        // outer boundary is the one that commits, so the inner batch's entries
        // belong to it and are left in place for it to judge.
        let owns_scope = self.balanced_txn_entries.is_none();
        if owns_scope {
            self.balanced_txn_entries = Some(Vec::new());
        }
        let sub = self.run_sub_plans(task, tid, plans, undo_log, crdt_deltas);
        // Taken on EVERY exit path, so a failed batch can never leave its
        // entries behind for the next transaction on this core to be judged by.
        let balanced_entries = if owns_scope {
            self.balanced_txn_entries.take().unwrap_or_default()
        } else {
            Vec::new()
        };
        self.active_bitemporal_stamps.clear();
        self.active_graph_system_from = None;
        let (last_response, undo_log, crdt_deltas) = match sub {
            Ok(v) => v,
            Err(resp) => return resp,
        };

        let constraint_check = catch_unwind(AssertUnwindSafe(|| {
            self.check_balanced_constraints(
                task.request.database_id.as_u64(),
                tid,
                balanced_entries,
            )
        }));
        match constraint_check {
            Ok(Ok(())) => {}
            Ok(Err(error_code)) => {
                warn!(
                    core = self.core_id,
                    "BALANCED constraint violated, rolling back {} operations",
                    undo_log.len()
                );
                return self.rollback_transaction_failure(
                    task,
                    tid,
                    undo_log,
                    self.response_error(task, error_code),
                );
            }
            Err(payload) => {
                let detail = panic_payload_to_string(payload.as_ref());
                error!(
                    core = self.core_id,
                    panic = %detail,
                    "BALANCED constraint check panicked; routing through rollback"
                );
                return self.rollback_transaction_failure(
                    task,
                    tid,
                    undo_log,
                    self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("panic in BALANCED constraint check: {detail}"),
                        },
                    ),
                );
            }
        }

        let crdt_delta_count = crdt_deltas.len();
        let undo_log = match self.apply_crdt_deltas_or_rollback(task, tid, undo_log, crdt_deltas) {
            Ok(undo_log) => undo_log,
            Err(response) => return response,
        };
        if crdt_delta_count > 0 {
            // Match the direct CRDT apply path: successful transaction-batch
            // imports must become checkpoint work, but never before the full
            // batch's rollback gates have succeeded.
            self.checkpoint_coordinator
                .mark_dirty("crdt", crdt_delta_count);
        }

        // All transactional gates have succeeded. Only now may timeseries
        // publish its write floor/timer/checkpoint/reservation state or start
        // best-effort post-commit maintenance.
        self.finalize_deferred_timeseries_ingests(task, &undo_log);

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
            // `partial` means another response frame will follow. A failed
            // best-effort maintenance flush leaves this committed transaction
            // durable in the memtable/WAL, but does not produce another frame.
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
                let r = self.execute_tx_sub_plan_from_batch(
                    task,
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

                    // Roll back all previous writes in reverse order. An undo
                    // panic is no safer than an `Err`: catch it and surface the
                    // same terminal `RollbackFailed` contract instead of
                    // unwinding past a half-restored transaction.
                    let undo_len = undo_log.len();
                    let rollback_error_code = match catch_unwind(AssertUnwindSafe(|| {
                        self.rollback_undo_log_at(
                            task.request.database_id.as_u64(),
                            tid,
                            task.request.vshard_id,
                            undo_log,
                        )
                    })) {
                        Ok(Ok(())) => error_code,
                        Ok(Err((entry_index, detail))) => {
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
                        Err(payload) => {
                            let detail = format!(
                                "panic during transaction rollback: {}",
                                panic_payload_to_string(payload.as_ref())
                            );
                            error!(
                                core = self.core_id,
                                plan_index = i,
                                entry_index = undo_len,
                                detail = %detail,
                                "transaction rollback panicked; shard state unknown — \
                                 restart required for WAL replay"
                            );
                            crate::bridge::envelope::ErrorCode::RollbackFailed {
                                entry_index: undo_len,
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

    /// Apply all buffered CRDT deltas only after every sub-plan and the
    /// `BALANCED` constraint check have succeeded.
    ///
    /// A failure is returned to the caller, which rolls every forward write
    /// back through the same undo log before responding. Never leave CRDT and
    /// engine state on different sides of a failed transaction boundary.
    pub(super) fn apply_crdt_deltas(
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
                    let outcome = engine.apply_committed_delta_validated(
                        &collection,
                        &delta,
                        nodedb_types::Surrogate::ZERO,
                        "",
                        0,
                    );
                    if !matches!(
                        outcome,
                        crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Clean { .. }
                    ) {
                        error!(
                            core = self.core_id,
                            crdt_delta_index = crdt_idx,
                            ?outcome,
                            "CRDT delta validation failed; transaction rollback required"
                        );
                        return Some(Response {
                            request_id: task.request_id(),
                            status: Status::Error,
                            attempt: 1,
                            partial: false,
                            payload: crate::bridge::envelope::Payload::empty(),
                            watermark_lsn: self.watermark,
                            error_code: Some(Box::new(
                                crate::bridge::envelope::ErrorCode::Internal {
                                    detail: format!("CRDT delta validation failed: {outcome:?}"),
                                },
                            )),
                            read_set_valid: None,
                            read_version_lsn: crate::types::Lsn::ZERO,
                            write_set: Vec::new(),
                        });
                    }
                    // This runs after the import so rollback coverage includes
                    // panics that occur once an earlier delta is already live.
                    crate::fail_point!("transaction_batch::after_crdt_delta");
                }
                Err(e) => {
                    error!(
                        core = self.core_id,
                        crdt_delta_index = crdt_idx,
                        error = %e,
                        "CRDT engine not found; transaction rollback required"
                    );
                    return Some(Response {
                        request_id: task.request_id(),
                        status: Status::Error,
                        attempt: 1,
                        partial: false,
                        payload: crate::bridge::envelope::Payload::empty(),
                        watermark_lsn: self.watermark,
                        error_code: Some(Box::new(crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!("CRDT engine not available: {e}"),
                        })),
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
    /// committed transaction's undo log. `UndoEntry::{PutDocument,
    /// DeleteDocument}.document_id` is the row's storage key, so a deferred
    /// trigger converts it to the client-visible identity here — the same
    /// conversion an immediate trigger sees via `emit_put_event` /
    /// `emit_document_delete_event`.
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
                    identity: crate::engine::document::store::identity_of(&document_id),
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
                    identity: crate::engine::document::store::identity_of(&document_id),
                    new_value: None,
                    old_value: Some(old_value),
                }),
                _ => None, // Vector and edge undo entries don't trigger deferred triggers.
            })
            .collect();

        if !deferred_writes.is_empty() {
            self.emit_deferred_events(
                deferred_writes,
                task.request.database_id,
                task.request.tenant_id,
                task.request.vshard_id,
            );
        }
    }
}

/// Best-effort conversion of a panic payload to a human-readable string.
/// Tries the two common payload types (`&'static str` and `String`); falls
/// back to `"<non-string panic payload>"` for anything else.
pub(super) fn panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::doc_format;
    use crate::data::executor::handlers::point::insert::PointInsertParams;
    use crate::engine::document::store::CollectionConfig;
    use crate::types::{DatabaseId, TenantId};
    use nodedb_physical::physical_plan::{DocumentOp, ResolvedSumTarget};
    use nodedb_types::{QualifiedCollection, Surrogate};

    const DB: u64 = 0;
    const TID: u64 = 1;
    const SOURCE: &str = "point_txns";
    const TARGET: &str = "point_holders";

    /// The premise every test below rests on.
    #[test]
    fn the_fixture_is_co_resident() {
        assert!(
            crate::query::sum_target_is_co_resident(DatabaseId::DEFAULT, SOURCE, TARGET),
            "'{SOURCE}' and '{TARGET}' must share a vShard: a cross-shard binding's balance \
             travels on its own task and is never folded into the source write's transaction"
        );
    }
    const A1: &str = "a1";
    const T1: Surrogate = Surrogate(4001);

    fn binding() -> nodedb_physical::physical_plan::MaterializedSumBinding {
        nodedb_physical::physical_plan::MaterializedSumBinding {
            target_collection: TARGET.to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        }
    }

    fn resolved() -> Vec<ResolvedSumTarget> {
        vec![ResolvedSumTarget::new(TARGET, A1, T1)]
    }

    fn config_key(collection: &str) -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            collection.to_string(),
        )
    }

    /// A source collection bound to the sum, and a target row starting at zero.
    fn seeded_core(dir: &std::path::Path) -> CoreLoop {
        let (mut core, _req, _resp) = make_core_with_dir(dir);

        let mut source = CollectionConfig::new(SOURCE);
        source.enforcement.materialized_sum_sources = vec![binding()];
        core.doc_configs.insert(config_key(SOURCE), source);
        core.doc_configs
            .insert(config_key(TARGET), CollectionConfig::new(TARGET));

        let seed = serde_json::json!({"id": A1, "balance": "0"});
        core.sparse
            .put(
                DB,
                TID,
                TARGET,
                &nodedb_types::StorageKey::for_surrogate(T1),
                &doc_format::encode_to_msgpack(&seed),
            )
            .expect("seed target row");
        core
    }

    /// A source row body, in the MessagePack every handler receives.
    fn entry(account: &str, amount: i64) -> Vec<u8> {
        doc_format::encode_to_msgpack(&serde_json::json!({
            "account_id": account,
            "amount": amount,
        }))
    }

    /// The balance the target row currently holds.
    fn balance(core: &CoreLoop, surrogate: Surrogate) -> String {
        let stored = core
            .sparse
            .get(
                DB,
                TID,
                TARGET,
                &nodedb_types::StorageKey::for_surrogate(surrogate),
            )
            .expect("read target")
            .expect("target row must exist");
        doc_format::decode_document(&stored)
            .expect("target row must decode")
            .get("balance")
            .and_then(|v| v.as_str())
            .expect("target row must carry a balance")
            .to_string()
    }

    fn insert(
        core: &mut CoreLoop,
        task: &ExecutionTask,
        surrogate: Surrogate,
        body: &[u8],
    ) -> Status {
        let targets = resolved();
        let document_id = format!("e{}", surrogate.as_u32());
        core.execute_point_insert(PointInsertParams {
            task,
            tid: TID,
            collection: SOURCE,
            document_id: &document_id,
            surrogate,
            value: body,
            if_absent: false,
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &targets,
            deferred_sum_targets: &[],
        })
        .status
    }

    #[test]
    fn a_transactional_delete_takes_the_row_back_off_the_total() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = seeded_core(dir.path());
        let task = make_default_task();

        assert_eq!(
            insert(&mut core, &task, Surrogate(81), &entry(A1, 90)),
            Status::Ok
        );
        assert_eq!(balance(&core, T1), "90");

        let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, SOURCE),
            document_id: "e81".to_string(),
            surrogate: Surrogate(81),
            pk_bytes: b"e81".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: resolved(),
        });
        let resp = core.execute_transaction_batch(&task, TID, &[plan], &[], None);
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            balance(&core, T1),
            "0",
            "a delete inside a transaction must debit the target too"
        );
    }
}
