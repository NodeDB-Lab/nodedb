// SPDX-License-Identifier: BUSL-1.1

//! Atomic, fully-indexed multi-row upsert (`DocumentOp::BatchUpsert`).
//!
//! Each candidate materializes once under one redb write transaction.
//! The write policy judges its exact stored image before application.
//! UNIQUE judges actual final source and target rows before commit. A key the page names twice merges into
//! the body its earlier row produced, and is stored once with the final
//! body. Any error drops the transaction and reverses the page's in-memory
//! side effects, so the page lands whole or not at all.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{DocumentOp, ResolvedSumTarget, ReturningSpec, UpdateValue};
use nodedb_types::Surrogate;
use tracing::debug;

use super::batch_plan::{PageRows, PageScope};
use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::WriteSetRow;
use crate::data::executor::enforcement::chain_guard::{
    AbandonedWrite, ChainGuard, abort_after_apply,
};
use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_put::PointPutParams;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{RowIdentity, StorageKey};

/// Parameters for [`CoreLoop::execute_upsert_batch`].
pub(in crate::data::executor) struct UpsertBatchParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub documents: &'a [(String, Vec<u8>)],
    pub surrogates: &'a [Surrogate],
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    /// Decided against each key's body that persists.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// When `Some`, return one row per row of the page, each its key's STORED
    /// post-image, in `documents` order.
    pub returning: Option<&'a ReturningSpec>,
    pub rls_filters: &'a [u8],
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    /// Run a `DocumentOp::BatchUpsert`. Any other op is refused.
    pub(in crate::data::executor) fn dispatch_upsert_batch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &DocumentOp,
    ) -> Response {
        let DocumentOp::BatchUpsert {
            collection,
            documents,
            surrogates,
            on_conflict_updates,
            rls_write_check,
            returning,
            rls_filters,
            resolved_sum_targets,
        } = op
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "batch upsert dispatch received another document op".into(),
                },
            );
        };
        self.execute_upsert_batch(
            task,
            UpsertBatchParams {
                tid,
                collection: collection.as_str(),
                documents,
                surrogates,
                on_conflict_updates,
                rls_write_check,
                returning: returning.as_ref(),
                rls_filters,
                resolved_sum_targets,
            },
        )
    }

    /// Upsert a page of documents. See the module doc for the contract.
    pub(in crate::data::executor) fn execute_upsert_batch(
        &mut self,
        task: &ExecutionTask,
        params: UpsertBatchParams<'_>,
    ) -> Response {
        let UpsertBatchParams {
            tid,
            collection,
            documents,
            surrogates,
            on_conflict_updates,
            rls_write_check,
            returning,
            rls_filters,
            resolved_sum_targets,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            count = documents.len(),
            "document batch upsert"
        );
        if surrogates.len() != documents.len() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "document batch upsert for '{collection}' carries {} documents but {} \
                         surrogates; every row must carry its own surrogate",
                        documents.len(),
                        surrogates.len(),
                    ),
                },
            );
        }
        // One unbound row refuses the whole page before any row is written.
        for surrogate in surrogates {
            if let Some(refusal) =
                crate::data::executor::handlers::unbound_surrogate::refuse_unbound(
                    "document", collection, *surrogate,
                )
            {
                return self.response_error(task, refusal);
            }
        }

        let database_id = task.request.database_id.as_u64();
        let strict_schema = self.strict_schema_for(
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection,
        );
        let identity_column = self.identity_column(database_id, tid, collection);
        let scope = PageScope {
            database_id,
            tid,
            collection,
            strict_schema: strict_schema.as_ref(),
        };
        let rows = PageRows {
            documents,
            surrogates,
            on_conflict_updates,
        };
        let keys = match self.plan_upsert_page(&scope, rows) {
            Ok(keys) => keys,
            Err(e) => return self.response_error(task, e),
        };
        let hook_ctx = HookCtx {
            target_unique: crate::data::executor::enforcement::unique::UniqueJudge::Unit,
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets: &[],
            wal_lsn: task.wal_lsn(),
        };
        let mut prepared_notes = Vec::with_capacity(keys.len());
        for key in &keys {
            match self.prepare_surrogate_write(task, tid, collection, key.surrogate.as_u32()) {
                Ok(note) => prepared_notes.push(note),
                Err(error) => return self.response_error(task, error),
            }
        }
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };
        // One guard for the whole page: each inserted key advances the head,
        // and a failure anywhere rolls the page back to where it started.
        let mut chain = ChainGuard::begin(self, database_id, tid, collection);

        let mut event_images = Vec::with_capacity(keys.len());
        let mut write_set: Vec<WriteSetEntry> = Vec::with_capacity(keys.len());
        let mut target_write_set: Vec<WriteSetEntry> = Vec::new();
        let mut balanced_entries = Vec::new();
        let mut page_undo: Vec<UndoEntry> = Vec::new();
        let mut page_targets: Vec<TargetWrite> = Vec::new();
        let mut unit_writes = Vec::new();
        // The stored body of each key, parallel to `keys`.
        let mut stored: Vec<Vec<u8>> = Vec::with_capacity(keys.len());
        let mut ordinals = Vec::with_capacity(keys.len());
        let mut written_keys: Vec<StorageKey> = Vec::with_capacity(keys.len());
        let mut failure: Option<crate::Error> = None;
        for key in &keys {
            let storage_key = StorageKey::for_surrogate(key.surrogate);
            written_keys.push(storage_key);
            // An absent key is INSERT-shaped, so it is a chain link. On an
            // overwrite the put ends the row's prior vector nodes at the new
            // version's ordinal, and its undo joins the page's.
            if key.committed.is_none()
                && let Err(e) = chain.chain_insert(self, key.surrogate, &key.post)
            {
                failure = Some(e);
                break;
            }
            let guards = match &key.assigned {
                Some(assigned) => {
                    crate::data::executor::enforcement::typeguard::GuardScope::Assigned(assigned)
                }
                None => crate::data::executor::enforcement::typeguard::GuardScope::Document,
            };
            let prepared = match self.prepare_point_put(
                &txn,
                PointPutParams {
                    database_id,
                    tid,
                    collection,
                    storage_key,
                    surrogate: key.surrogate,
                    value: &key.post,
                    index_text: true,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    unique: crate::data::executor::enforcement::unique::UniqueJudge::Unit,
                    wal_lsn: task.wal_lsn(),
                    resolved_targets: resolved_sum_targets,
                    guards,
                },
            ) {
                Ok(o) => o,
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            };
            if let Err(error) = crate::data::executor::handlers::rls_write_gate::admit_installed_row(
                rls_write_check,
                prepared.installed(),
                &key.identity,
                &identity_column,
                tid,
                collection,
                &self.crs_context(),
            ) {
                failure = Some(error);
                break;
            }
            let mut outcome = match self.apply_prepared_point_put(&txn, prepared) {
                Ok(outcome) => outcome,
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            };
            page_undo.append(&mut outcome.memory_undo);
            unit_writes.push((collection.to_owned(), storage_key));
            // The head persists with each row, so the next row's link reads
            // the head this transaction already holds.
            if key.committed.is_none()
                && let Err(e) = chain
                    .settle_installed(self, key.surrogate, &mut outcome.installed)
                    .and_then(|()| chain.persist_head(self, &txn))
            {
                failure = Some(e);
                break;
            }
            // An overwrite is an UPDATE: the pre-image takes the row's old
            // contribution off a total before the new one goes on.
            let images = match outcome.prior_value.as_deref() {
                Some(old) => WriteImages::Update {
                    old: ImageBody::Stored(old),
                    new: ImageBody::Stored(&outcome.installed.stored),
                },
                None => WriteImages::Insert {
                    new: ImageBody::Stored(&outcome.installed.stored),
                },
            };
            let enforcement = match write_hook::run(self, &txn, &hook_ctx, images) {
                Ok(enforcement) => enforcement,
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            };
            unit_writes.extend(enforcement.target_writes.iter().map(|target| {
                (
                    target.collection.clone(),
                    nodedb_types::StorageKey::for_surrogate(target.surrogate),
                )
            }));
            target_write_set.extend(write_hook::target_write_set(&enforcement.target_writes));
            page_targets.extend(enforcement.target_writes);
            balanced_entries.extend(enforcement.balanced_entries);
            let event = match self.prepare_installed_put_event(
                &WriteSetRow {
                    database_id,
                    tid,
                    collection,
                    identity: key.identity.clone(),
                },
                &outcome.installed,
                outcome.prior_value.as_deref(),
            ) {
                Ok(event) => event,
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            };
            write_set.push(crate::bridge::envelope::WriteSetEntry::installed_put(
                key.surrogate.as_u32(),
                key.identity.clone(),
                outcome.installed.clone(),
                event.clone(),
            ));
            event_images.push(event);
            ordinals.push(outcome.installed.version.sys_from);
            stored.push(outcome.installed.stored);
        }

        if let Some(error) = failure {
            let error = abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::rows(database_id, tid, collection, &written_keys)
                    .undo(page_undo)
                    .targets(page_targets),
                error,
            );
            return self.response_error(task, error);
        }
        if let Err(error) = self.check_written_unit_unique(&txn, database_id, tid, &unit_writes) {
            drop(txn);
            let error = abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::rows(database_id, tid, collection, &written_keys)
                    .undo(page_undo)
                    .targets(page_targets),
                error,
            );
            return self.response_error(task, error);
        }

        let row_count: usize = keys.iter().map(|key| key.rows.len()).sum();
        let prepared_response = match returning {
            // One row per row of the page, in `documents` order, each its
            // key's stored body.
            Some(spec) => {
                let mut by_row: HashMap<&str, usize> = HashMap::new();
                for (slot, key) in keys.iter().enumerate() {
                    for row in &key.rows {
                        by_row.insert(row, slot);
                    }
                }
                let rows: Vec<(&RowIdentity, &[u8])> = documents
                    .iter()
                    .filter_map(|(document_id, _)| by_row.get(document_id.as_str()).copied())
                    .filter_map(|slot| {
                        Some((&keys.get(slot)?.identity, stored.get(slot)?.as_slice()))
                    })
                    .collect();
                self.prepare_stored_returning_response(
                    task,
                    spec,
                    rls_filters,
                    strict_schema.as_ref(),
                    &identity_column,
                    &rows,
                )
            }
            // An upsert always writes its row: one row affected per row.
            None => Ok(self.response_affected(task, row_count as u64)),
        };
        let mut response = match prepared_response {
            Ok(response) => response,
            Err(error) => {
                let error = abort_after_apply(
                    self,
                    &mut chain,
                    AbandonedWrite::rows(database_id, tid, collection, &written_keys)
                        .undo(page_undo)
                        .targets(page_targets),
                    error,
                );
                return self.response_error(task, error);
            }
        };
        // `wal_append_document_op` mints no pre-dispatch record for an
        // upsert, so every key is journalled after apply, from the body
        // `apply_point_put` took.
        response.write_set = write_set;
        response.write_set.extend(target_write_set);

        // The whole page is one boundary, so a page that leaves any journal
        // group unbalanced writes no rows at all.
        let settled = self
            .settle_balanced_entries(database_id, tid, collection, balanced_entries)
            .and_then(|()| chain.persist_head(self, &txn));
        let committed = settled.and_then(|()| {
            txn.commit().map_err(|e| {
                self.seal_journalled();
                self.fail_stop_core(
                    crate::data::executor::core_loop::fail_stop::FailStopCause::WriteSetUnpersisted,
                    &format!("batch upsert commit outcome is unknown: {e}"),
                );
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("batch upsert commit: {e}"),
                }
            })
        });
        if let Err(error) = committed {
            let error = abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::rows(database_id, tid, collection, &written_keys)
                    .undo(page_undo)
                    .targets(page_targets),
                error,
            );
            return self.response_error(task, error);
        }

        for (note, ordinal) in prepared_notes.into_iter().zip(ordinals) {
            self.publish_surrogate_write(note, ordinal);
        }
        self.checkpoint_coordinator.mark_dirty("sparse", keys.len());
        for (key, event) in keys.iter().zip(&event_images) {
            self.emit_prepared_put_event(task, collection, key.identity.clone(), event);
        }

        response.watermark_lsn = self.watermark;
        response
    }
}
