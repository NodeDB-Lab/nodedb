// SPDX-License-Identifier: BUSL-1.1

//! Atomic, fully-indexed document batch insert (`DocumentOp::BatchInsert`).

use tracing::{debug, warn};

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
use nodedb_physical::physical_plan::{ResolvedSumTarget, ReturningSpec};

/// Parameters for [`CoreLoop::execute_document_batch_insert`].
pub(in crate::data::executor) struct DocumentBatchInsertParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub documents: &'a [(String, Vec<u8>)],
    pub surrogates: &'a [nodedb_types::Surrogate],
    /// `ON CONFLICT DO NOTHING`: a row whose key is already stored, or named
    /// by an earlier row of the page, is skipped instead of refused.
    pub if_absent: bool,
    /// When `Some`, return one row per inserted document — the STORED
    /// post-image of each, in `documents` order.
    pub returning: Option<&'a ReturningSpec>,
    /// Compiled read policy bounding which of those rows may be shown back.
    pub rls_filters: &'a [u8],
    /// Compiled write policy judged on each exact prepared row.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// this page may credit — one entry per DISTINCT join value across the
    /// batch. Resolved on the Control Plane at plan time.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
    /// Materialized-sum TARGET collections whose delta the Control Plane
    /// settled at plan time and appended as its own `ApplyBalanceDelta` task,
    /// homed on the target's vShard. This page must not apply them as well.
    pub deferred_sum_targets: &'a [String],
}

impl CoreLoop {
    /// Insert a page of documents.
    ///
    /// Every index in the system — FTS, vector, spatial, and the secondary
    /// btree — is keyed by a row's global surrogate, so a batch is only
    /// insertable when it carries one surrogate per document. A batch that does
    /// not is refused here rather than stored: writing those rows would put
    /// documents in the collection that no index can ever return, while
    /// reporting the insert as successful. There is no partial answer to give —
    /// the surrogate list IS the rows' identity, and it is the part that is
    /// missing.
    pub(in crate::data::executor) fn execute_document_batch_insert(
        &mut self,
        task: &ExecutionTask,
        params: DocumentBatchInsertParams<'_>,
    ) -> Response {
        debug!(
            core = self.core_id,
            collection = %params.collection,
            count = params.documents.len(),
            "document batch insert"
        );

        if params.surrogates.len() != params.documents.len() {
            // Recorded here, at the detection site, and never re-emitted as the
            // rejection propagates: the malformation is upstream (a plan
            // builder, or a replicated write record that lost rows), and the
            // error the caller receives names only the symptom.
            crate::diag::batch_insert_without_surrogates(
                params.collection,
                params.documents.len(),
                params.surrogates.len(),
            );
            warn!(
                core = self.core_id,
                collection = %params.collection,
                doc_count = params.documents.len(),
                surrogate_count = params.surrogates.len(),
                "document batch insert without a surrogate per row; rejecting the batch"
            );
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "document batch insert for '{}' carries {} documents but {} \
                         surrogates; every cross-engine index is surrogate-keyed, so these \
                         rows cannot be indexed and are not written",
                        params.collection,
                        params.documents.len(),
                        params.surrogates.len(),
                    ),
                },
            );
        }
        // One unbound row refuses the whole batch before any row is written.
        for surrogate in params.surrogates {
            if let Some(refusal) =
                crate::data::executor::handlers::unbound_surrogate::refuse_unbound(
                    "document",
                    params.collection,
                    *surrogate,
                )
            {
                return self.response_error(task, refusal);
            }
        }

        self.execute_document_batch_insert_indexed(task, params)
    }

    /// Atomic, fully-indexed batch insert (surrogates parallel to documents).
    ///
    /// Applies every row through [`CoreLoop::apply_point_put`] under ONE redb
    /// write transaction so the document store, FTS inverted index, HNSW vector
    /// index, spatial R-tree, and secondary indexes are all maintained and keyed
    /// by each row's stable surrogate. UNIQUE uses the actual final source
    /// and target rows before commit. Any per-row error drops the
    /// transaction, leaving the whole page unchanged. On success the transaction commits once and one Insert write
    /// event is emitted per row.
    fn execute_document_batch_insert_indexed(
        &mut self,
        task: &ExecutionTask,
        params: DocumentBatchInsertParams<'_>,
    ) -> Response {
        let DocumentBatchInsertParams {
            tid,
            collection,
            documents,
            surrogates,
            if_absent,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
            deferred_sum_targets,
        } = params;
        let database_id = task.request.database_id.as_u64();
        let identity_column = self.identity_column(database_id, tid, collection);
        let hook_ctx = HookCtx {
            target_unique: crate::data::executor::enforcement::unique::UniqueJudge::Unit,
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets,
            wal_lsn: task.wal_lsn(),
        };
        let mut prepared_notes = Vec::with_capacity(surrogates.len());
        for surrogate in surrogates {
            match self.prepare_surrogate_write(task, tid, collection, surrogate.as_u32()) {
                Ok(note) => prepared_notes.push(note),
                Err(error) => return self.response_error(task, error),
            }
        }
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => return self.response_error(task, e),
        };
        // Each row is judged as `PointInsert` judges it, inside the page's
        // write transaction: a key already stored, or one an earlier row of
        // the page names, is a `unique` violation, or a skipped row under
        // `if_absent`. A refusal drops the transaction and writes nothing.
        let landing = match self.batch_insert_landing_rows(
            &txn,
            super::batch_insert_judge::BatchPage {
                database_id,
                tid,
                collection,
                documents,
                surrogates,
                if_absent,
            },
        ) {
            Ok(landing) => landing,
            Err(e) => return self.response_error(task, e),
        };
        // One guard for the whole page: each row advances the head, and a
        // failure anywhere rolls the page back to the head it started from.
        let mut chain = ChainGuard::begin(self, database_id, tid, collection);

        // Retain each accepted row's identity, version, and prepared event image.
        // Dropping the transaction reverses every source row on an error.
        let mut applied: Vec<(
            usize,
            crate::engine::document::store::RowIdentity,
            nodedb_types::StorageKey,
            i64,
            crate::types::DocumentPutEvent,
        )> = Vec::with_capacity(landing.len());
        // One post-apply `Put` redo entry per stored row, in insert order:
        // `wal_append_document_op` mints no pre-dispatch record for
        // `BatchInsert`, so these entries are the rows' only WAL record.
        let mut write_set: Vec<WriteSetEntry> = Vec::with_capacity(documents.len());
        // The exact bytes each row landed as, parallel to `documents`, kept only
        // when a `RETURNING` projection will read them.
        let mut stored_bodies: Vec<Vec<u8>> = Vec::new();
        // Redo entries for the target rows this page credited, accumulated
        // across rows and attached to the response below.
        let mut target_write_set: Vec<WriteSetEntry> = Vec::new();
        // Signed BALANCED contributions, accumulated across the whole page: a
        // multi-row INSERT is one boundary and one genuine set, so its rows are
        // judged together and a journal written as several rows of one
        // statement balances.
        let mut balanced_entries = Vec::new();
        // The page's in-memory side effects, in write order: every applied
        // row's R-tree, vector and sparse entries, and the target rows its
        // enforcement wrote. Dropping `txn` reverses the durable writes and
        // none of these.
        let mut page_undo: Vec<UndoEntry> = Vec::new();
        let mut page_targets: Vec<TargetWrite> = Vec::new();
        let mut unit_writes = Vec::new();
        // The row that failed, plus why. Collected rather than returned inline
        // so the page's in-memory side effects — these, the advanced chain
        // head and the document-cache entries `apply_point_put` populated —
        // are reversed in one place.
        let mut failure: Option<(nodedb_types::StorageKey, crate::Error)> = None;
        for &i in &landing {
            let (document_id, value) = &documents[i];
            let surrogate = surrogates[i];
            let key = nodedb_types::StorageKey::for_surrogate(surrogate);
            // The plan's `document_id` is the identity INSERT minted for this
            // row: the declared primary key value, else the decimal surrogate.
            let row_identity =
                crate::engine::document::store::RowIdentity::from_user_key(document_id.as_str());
            // Every row of a batch insert is INSERT-shaped, so every row is a
            // chain link. The row is marked before its write, `build_stored_body`
            // writes the link, and settling it makes it the head the next row
            // links after.
            if let Err(e) = chain.chain_insert(self, surrogate, value) {
                // `key` is `Copy`, so this costs nothing.
                failure = Some((key, e));
                break;
            }
            let prepared = match self.prepare_point_put(
                &txn,
                PointPutParams {
                    database_id,
                    tid,
                    collection,
                    storage_key: key,
                    surrogate,
                    value,
                    index_text: true,
                    user_roles: &task.request.user_roles,
                    enforce: true,
                    unique: crate::data::executor::enforcement::unique::UniqueJudge::Unit,
                    wal_lsn: task.wal_lsn(),
                    resolved_targets: resolved_sum_targets,
                    guards: crate::data::executor::enforcement::typeguard::GuardScope::Document,
                },
            ) {
                Ok(o) => o,
                Err(e) => {
                    // `key` is `Copy`, so this costs nothing and `row_key`
                    // stays borrowed by the parameters of the call this arm
                    // is handling.
                    failure = Some((key, e));
                    break;
                }
            };
            if let Err(error) = crate::data::executor::handlers::rls_write_gate::admit_installed_row(
                rls_write_check,
                prepared.installed(),
                &row_identity,
                &identity_column,
                tid,
                collection,
                &self.crs_context(),
            ) {
                failure = Some((key, error));
                break;
            }
            let mut outcome = match self.apply_prepared_point_put(&txn, prepared) {
                Ok(outcome) => outcome,
                Err(error) => {
                    failure = Some((key, error));
                    break;
                }
            };
            page_undo.append(&mut outcome.memory_undo);
            unit_writes.push((collection.to_owned(), key));
            // The head persists with each row, so the next row's link reads
            // the head this transaction already holds.
            if let Err(e) = chain
                .settle_installed(self, surrogate, &mut outcome.installed)
                .and_then(|()| chain.persist_head(self, &txn))
            {
                failure = Some((key, e));
                break;
            }
            // Image-folding enforcement per row, in the SAME transaction the
            // page is being applied in, so a derived total lands or rolls back
            // with every actual stored mutation, including repeated-row updates.
            let enforcement = match write_hook::run(
                self,
                &txn,
                &hook_ctx,
                match outcome.prior_value.as_deref() {
                    Some(old) => WriteImages::Update {
                        old: ImageBody::Stored(old),
                        new: ImageBody::Stored(&outcome.installed.stored),
                    },
                    None => WriteImages::Insert {
                        new: ImageBody::Stored(&outcome.installed.stored),
                    },
                },
            ) {
                Ok(enforcement) => enforcement,
                Err(e) => {
                    // `key` is `Copy`, so this costs nothing.
                    failure = Some((key, e));
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
                    identity: row_identity.clone(),
                },
                &outcome.installed,
                outcome.prior_value.as_deref(),
            ) {
                Ok(event) => event,
                Err(error) => {
                    failure = Some((key, error));
                    break;
                }
            };
            write_set.push(crate::bridge::envelope::WriteSetEntry::installed_put(
                surrogate.as_u32(),
                row_identity.clone(),
                outcome.installed.clone(),
                event.clone(),
            ));
            if returning.is_some() {
                stored_bodies.push(outcome.installed.stored);
            }

            applied.push((
                i,
                row_identity,
                key,
                outcome.installed.version.sys_from,
                event,
            ));
        }

        // Every row the page called `apply_point_put` for. An abort below
        // drops their cache entries: a cached body for a row that never
        // committed is served to readers as though it had.
        let mut written_keys: Vec<nodedb_types::StorageKey> =
            applied.iter().map(|(_, _, key, _, _)| *key).collect();

        if let Some((failed_key, error)) = failure {
            // The whole page rolls back: the chain head, the cache entries
            // and every in-memory index entry of the page go back.
            written_keys.push(failed_key);
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

        // The whole page is one boundary, so it is judged once here — before
        // the commit, so a page that leaves any journal group unbalanced writes
        // no rows at all.
        if let Err(e) = self.settle_balanced_entries(database_id, tid, collection, balanced_entries)
        {
            let e = abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::rows(database_id, tid, collection, &written_keys)
                    .undo(page_undo)
                    .targets(page_targets),
                e,
            );
            return self.response_error(task, e);
        }

        let prepared_response = if let Some(spec) = returning {
            // One row per inserted document, in `documents` order — the order
            // the rows were applied in, which is the order PostgreSQL returns
            // them in for a multi-row INSERT. A skipped row returns nothing.
            let strict_schema = self.strict_schema_for(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
            );
            let identities: Vec<crate::engine::document::store::RowIdentity> = landing
                .iter()
                .map(|&i| {
                    crate::engine::document::store::RowIdentity::from_user_key(
                        documents[i].0.as_str(),
                    )
                })
                .collect();
            let rows: Vec<(&crate::engine::document::store::RowIdentity, &[u8])> = identities
                .iter()
                .zip(stored_bodies.iter())
                .map(|(identity, stored)| (identity, stored.as_slice()))
                .collect();
            let identity_column = self.identity_column(database_id, tid, collection);
            self.prepare_stored_returning_response(
                task,
                spec,
                rls_filters,
                strict_schema.as_ref(),
                &identity_column,
                &rows,
            )
        } else {
            crate::data::executor::response_codec::encode_count("inserted", landing.len())
                .map(|bytes| self.response_with_payload(task, bytes))
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
        response.write_set = write_set;
        response.write_set.extend(target_write_set);

        // The advanced head lands in the SAME transaction as the rows whose
        // hashes it covers.
        if let Err(e) = chain.persist_head(self, &txn) {
            let e = abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::rows(database_id, tid, collection, &written_keys)
                    .undo(page_undo)
                    .targets(page_targets),
                e,
            );
            return self.response_error(task, e);
        }

        if let Err(e) = txn.commit() {
            self.seal_journalled();
            self.fail_stop_core(
                crate::data::executor::core_loop::fail_stop::FailStopCause::WriteSetUnpersisted,
                &format!("document commit outcome is unknown: {e}"),
            );
            let e = abort_after_apply(
                self,
                &mut chain,
                AbandonedWrite::rows(database_id, tid, collection, &written_keys)
                    .undo(page_undo)
                    .targets(page_targets),
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("batch insert commit: {e}"),
                },
            );
            return self.response_error(task, e);
        }

        for (i, _, _, ordinal, _) in &applied {
            self.publish_surrogate_write(prepared_notes[*i].take(), *ordinal);
        }

        self.checkpoint_coordinator
            .mark_dirty("sparse", landing.len());
        if let Some(ref m) = self.metrics {
            m.record_document_insert();
        }

        for (_, identity, _, _, event) in applied {
            self.emit_prepared_put_event(task, collection, identity, &event);
        }

        response.watermark_lsn = self.watermark;
        response
    }
}

#[cfg(test)]
#[path = "batch_insert_tests.rs"]
mod tests;
