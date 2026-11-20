// SPDX-License-Identifier: BUSL-1.1

//! Actual per-request policy and UNIQUE proofs inside shared-IO transactions.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::unique::{PostImage, UniqueJudge};
use crate::data::executor::handlers::point::apply_put::{PointPutOutcome, PointPutParams};
use crate::data::executor::handlers::transaction::redo_apply::check_task_pinned;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{DocumentOp, RedoPins};
use redb::WriteTransaction;

pub(super) struct BatchApplyResult {
    pub outcomes: Vec<Result<PointPutOutcome, Response>>,
    pub apply_failed: bool,
}

impl CoreLoop {
    /// A preparation refusal changes no row; an application error requires whole-IO rollback.
    pub(super) fn apply_batched_puts(
        &mut self,
        txn: &WriteTransaction,
        batch: &[ExecutionTask],
    ) -> BatchApplyResult {
        // Every task of the batch carries one pin set (`same_pin_set`), so the
        // batch derives and judges under one view, opened once. A view that
        // does not open refuses every task, and nothing is stored.
        let pins = batch
            .first()
            .and_then(|task| task.request.config_pins.clone());
        let held = match pins.as_deref().map(|pins| self.begin_pinned_apply(pins)) {
            None => None,
            Some(Ok(held)) => Some(held),
            Some(Err(refusal)) => {
                let code = ErrorCode::from(refusal);
                return BatchApplyResult {
                    outcomes: batch
                        .iter()
                        .map(|task| Err(self.response_error(task, code.clone())))
                        .collect(),
                    apply_failed: false,
                };
            }
        };
        let applied = self.apply_batched_puts_under(txn, batch, pins.as_deref());
        if let Some(held) = held {
            self.restore_judging(held);
        }
        applied
    }

    /// [`Self::apply_batched_puts`] under the open view of `pins`. A task
    /// that leaves a collection it writes unpinned is refused alone and
    /// stores nothing.
    fn apply_batched_puts_under(
        &mut self,
        txn: &WriteTransaction,
        batch: &[ExecutionTask],
        pins: Option<&RedoPins>,
    ) -> BatchApplyResult {
        let mut results: Vec<Result<PointPutOutcome, Response>> = Vec::with_capacity(batch.len());
        let mut apply_failed = false;
        let mut accepted_documents = Vec::with_capacity(batch.len());
        let previous_schemas = std::mem::take(self.apply_scope.storage_schemas.get_mut());
        for (index, task) in batch.iter().enumerate() {
            if let Some(pins) = pins
                && let Err(refusal) = check_task_pinned(task, pins)
            {
                accepted_documents.push(None);
                results.push(Err(self.response_error(task, refusal)));
                continue;
            }
            let PhysicalPlan::Document(DocumentOp::PointPut {
                collection,
                document_id,
                value,
                rls_write_check,
                surrogate,
                resolved_sum_targets,
                ..
            }) = task.plan()
            else {
                apply_failed = true;
                accepted_documents.push(None);
                results.push(Err(self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "coalesced write contains a non-PointPut request".into(),
                    },
                )));
                continue;
            };
            let tid = task.request.tenant_id.as_u64();
            let db_id = task.request.database_id.as_u64();
            let storage_key = crate::engine::document::store::StorageKey::for_surrogate(*surrogate);
            let carried = task.request.system_ordinal;
            if let Some(ordinal) = carried {
                self.observe_bitemporal_ordinal(ordinal);
            }
            let previous_ordinal =
                std::mem::replace(&mut self.apply_scope.replicated_ordinal, carried);
            let prepared = if let Some(error) =
                crate::data::executor::handlers::unbound_surrogate::refuse_unbound(
                    "document",
                    collection.as_str(),
                    *surrogate,
                ) {
                Err(crate::Error::DataPlane(error))
            } else {
                self.prepare_point_put(
                    txn,
                    PointPutParams {
                        database_id: db_id,
                        tid,
                        collection: collection.as_str(),
                        storage_key,
                        surrogate: *surrogate,
                        value,
                        index_text: true,
                        user_roles: &task.request.user_roles,
                        enforce: true,
                        unique: UniqueJudge::Unit,
                        wal_lsn: task.wal_lsn(),
                        resolved_targets: resolved_sum_targets.as_slice(),
                        guards: crate::data::executor::enforcement::typeguard::GuardScope::Document,
                    },
                )
            };
            let prepared = prepared.and_then(|prepared| {
                crate::data::executor::handlers::rls_write_gate::admit_installed_row(
                    rls_write_check,
                    prepared.installed(),
                    &crate::engine::document::store::RowIdentity::from_user_key(
                        document_id.as_str(),
                    ),
                    &self.identity_column(db_id, tid, collection.as_str()),
                    tid,
                    collection.as_str(),
                    &self.crs_context(),
                )?;
                let document = self.check_batched_put_unique(
                    &batch[..=index],
                    &accepted_documents,
                    prepared.installed(),
                )?;
                Ok((prepared, document))
            });
            let applied = match prepared {
                Ok((prepared, document)) => match self.apply_prepared_point_put(txn, prepared) {
                    Ok(outcome) => {
                        accepted_documents.push(document);
                        Ok(outcome)
                    }
                    Err(error) => {
                        apply_failed = true;
                        accepted_documents.push(None);
                        Err(error)
                    }
                },
                Err(error) => {
                    accepted_documents.push(None);
                    Err(error)
                }
            };
            self.apply_scope.replicated_ordinal = previous_ordinal;
            // The error keeps its own code: a UNIQUE refusal answers 23505.
            results.push(applied.map_err(|e| self.response_error(task, e)));
        }

        *self.apply_scope.storage_schemas.get_mut() = previous_schemas;
        BatchApplyResult {
            outcomes: results,
            apply_failed,
        }
    }

    /// Judge one request against earlier accepted images and the committed index.
    fn check_batched_put_unique(
        &self,
        tasks: &[ExecutionTask],
        earlier_documents: &[Option<serde_json::Value>],
        candidate: &crate::types::SharedInstalledDocument,
    ) -> crate::Result<Option<serde_json::Value>> {
        let Some((last, earlier)) = tasks.split_last() else {
            return Ok(None);
        };
        let PhysicalPlan::Document(DocumentOp::PointPut {
            collection,
            surrogate,
            ..
        }) = last.plan()
        else {
            return Ok(None);
        };
        let database_id = last.request.database_id;
        let tenant_id = last.request.tenant_id;
        let Some(config) = self.unique_config(
            database_id.as_u64(),
            tenant_id.as_u64(),
            collection.as_str(),
        ) else {
            return Ok(None);
        };
        let mut rows = Vec::new();
        for (task, document) in earlier.iter().zip(earlier_documents) {
            let Some(document) = document else {
                continue;
            };
            let PhysicalPlan::Document(DocumentOp::PointPut {
                collection: written,
                surrogate,
                ..
            }) = task.plan()
            else {
                continue;
            };
            if task.request.database_id == database_id
                && task.request.tenant_id == tenant_id
                && written.as_str() == collection.as_str()
            {
                rows.push(PostImage {
                    surrogate: surrogate.as_u32(),
                    doc: Some(document),
                    judged: false,
                });
            }
        }
        let document = self.decode_stored_document(config, &candidate.stored)?;
        rows.push(PostImage {
            surrogate: surrogate.as_u32(),
            doc: Some(&document),
            judged: true,
        });
        self.check_unit_unique(
            database_id.as_u64(),
            tenant_id.as_u64(),
            collection.as_str(),
            &rows,
        )?;
        Ok(Some(document))
    }
}
