// SPDX-License-Identifier: BUSL-1.1

//! Columnar UPDATE and DELETE handlers for plain/spatial collections.
//!
//! Uses `nodedb-columnar`'s `MutationEngine` for full mutation support
//! (PK index, delete bitmaps, WAL records). The per-row apply, including
//! the R-tree cascade for spatial collections, is shared with the
//! resolved-row-set handlers through `columnar_mutation_apply.rs`.

use nodedb_physical::physical_plan::UpdateValue;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::decode_scan_filters;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_resolve::{
    ResolveDeleteRowsParams, ResolveUpdateRowsParams, require_pk_column_index,
};
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Handle columnar UPDATE: match the current rows, flushed and in the
    /// memtable, and apply the field updates to each match.
    /// Returns `{"affected": N}` as JSON payload.
    ///
    /// When `undo_log` is `Some` (the durable COMMIT-replay path inside a
    /// transaction batch), the pre-image of every mutated row is captured into
    /// a [`UndoEntry::ColumnarUpdate`] so a sibling sub-plan failing later in
    /// the same COMMIT can reverse this update. On the autocommit path it is
    /// `None` (no batch to roll back).
    pub(in crate::data::executor) fn execute_columnar_update(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        filter_bytes: &[u8],
        updates: &[(String, UpdateValue)],
        rls_write_check: &nodedb_types::RlsWriteCheck,
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        debug!(core = self.core_id, %collection, "columnar update");

        let key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let engine = match self.columnar_engines.get(&key) {
            Some(e) => e,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar engine not found for collection '{collection}'"),
                    },
                );
            }
        };

        // Match the current rows against the filter predicates, then apply
        // the updates through the PK-based MutationEngine (delete + re-insert).
        let schema = engine.schema().clone();
        let row_count_before = engine.memtable().row_count();
        let pk_col_idx = match require_pk_column_index(&schema, "UPDATE") {
            Ok(idx) => idx,
            Err(e) => return self.response_error(task, e),
        };

        // A filter that does not decode refuses the statement. Read as no
        // filter, it would update every row.
        let filter_predicates = match decode_scan_filters(filter_bytes, "columnar UPDATE filter") {
            Ok(filters) => filters,
            Err(e) => return self.response_error(task, e),
        };

        // Resolve every matching row's post-image, and let the write policy
        // decide all of them, BEFORE any row is mutated. The post-image is what
        // the policy governs and it exists only once the assignments have been
        // applied, so the check cannot happen earlier — and it must happen for
        // the whole statement before the first `engine.update`, or a rejection
        // partway through would leave the rows ahead of it already changed with
        // no way for the caller to see or undo that. Shared with
        // `execute_columnar_resolve_dml`, which reports this same selection
        // instead of applying it.
        //
        // An expression assignment evaluates against each row's pre-image, and
        // each post-image meets the declared column rule inside the resolve. A
        // failed evaluation or a value past a declared width refuses the
        // statement whole.
        let pending = match self.resolve_columnar_update_rows(ResolveUpdateRowsParams {
            key: &key,
            schema: &schema,
            pk_col_idx,
            filter_predicates: &filter_predicates,
            updates,
            rls_write_check,
            tid: task.request.tenant_id.as_u64(),
            collection,
        }) {
            Ok(rows) => rows,
            Err(e) => return self.response_error(task, e),
        };
        // Undo capture (only on the durable COMMIT-replay path). `row_count_before`
        // is the memtable size before any replacement row is appended, so the
        // undo can truncate back to it; `inserted_pks`/`displaced` reverse the
        // insert half, `restored` re-materializes each tombstoned original.
        let mut undo_log = undo_log;
        let outcome = match self.apply_columnar_update_rows(
            task,
            &key,
            &schema,
            &pending,
            undo_log.as_deref_mut(),
        ) {
            Ok(outcome) => outcome,
            Err(e) => return self.response_error(task, e),
        };
        let affected = outcome.affected;

        if let Some(log) = undo_log {
            log.push(UndoEntry::ColumnarUpdate {
                collection_key: key,
                row_count_before,
                inserted_pks: outcome.inserted_pks,
                displaced: outcome.displaced,
                restored: outcome.restored,
            });
        }

        // Advance the collection floor for this committed columnar write, exactly
        // as `execute_columnar_insert` does.
        //
        // The columnar checkpoint stamps its generation with the core watermark
        // and that stamp becomes the replay floor, so the watermark must mean
        // "every columnar record at or below this is folded into the engines".
        // An UPDATE that mutated rows without raising it would sit ABOVE the
        // stamp of a checkpoint that already contains it, and replay would
        // re-execute it — appending a duplicate row, since the update is
        // delete-old-PK + insert-new-row rather than an overwrite.
        //
        // Gated on `affected` for the same reason as the insert path: a
        // predicate that matched nothing wrote nothing, so it owes no floor, and
        // re-executing it against the identical restored state matches nothing
        // again.
        if affected > 0 {
            self.note_collection_write(task, collection);
        }

        debug!(core = self.core_id, %collection, affected, "columnar update complete");
        let result = serde_json::json!({ "affected": affected });
        match super::super::response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Handle columnar DELETE: match the current rows, flushed and in the
    /// memtable, and delete each match.
    /// Returns `{"affected": N}` as JSON payload.
    ///
    /// When `undo_log` is `Some` (the durable COMMIT-replay path inside a
    /// transaction batch), the `(pk_bytes, RowLocation)` of every deleted row
    /// is captured into a [`UndoEntry::ColumnarDelete`] so a sibling sub-plan
    /// failing later in the same COMMIT can restore the rows. On the autocommit
    /// path it is `None`.
    pub(in crate::data::executor) fn execute_columnar_delete(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        filter_bytes: &[u8],
        rls_write_check: &nodedb_types::RlsWriteCheck,
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        debug!(core = self.core_id, %collection, "columnar delete");

        let key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let engine = match self.columnar_engines.get(&key) {
            Some(e) => e,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar engine not found for collection '{collection}'"),
                    },
                );
            }
        };

        let schema = engine.schema().clone();
        let pk_col_idx = match require_pk_column_index(&schema, "DELETE") {
            Ok(idx) => idx,
            Err(e) => return self.response_error(task, e),
        };

        // A filter that does not decode refuses the statement. Read as no
        // filter, it would delete every row.
        let filter_predicates = match decode_scan_filters(filter_bytes, "columnar DELETE filter") {
            Ok(filters) => filters,
            Err(e) => return self.response_error(task, e),
        };

        // The image a delete is governed by is the row it removes. Every
        // matched row is decided before the first `engine.delete`, so a
        // rejection removes nothing at all rather than leaving the rows ahead
        // of it already tombstoned. Shared with `execute_columnar_resolve_dml`,
        // which reports this same selection instead of applying it.
        let pk_values = match self.resolve_columnar_delete_rows(ResolveDeleteRowsParams {
            key: &key,
            schema: &schema,
            pk_col_idx,
            filter_predicates: &filter_predicates,
            rls_write_check,
            tid: task.request.tenant_id.as_u64(),
            collection,
        }) {
            Ok(pks) => pks,
            Err(e) => return self.response_error(task, e),
        };

        // Undo capture (only on the durable COMMIT-replay path): the location
        // and PK bytes of each tombstoned row, so the undo can clear its
        // delete-bitmap bit and re-bind the PK index.
        let mut undo_log = undo_log;
        let outcome = match self.apply_columnar_delete_pks(
            &key,
            &schema,
            &pk_values,
            undo_log.as_deref_mut(),
        ) {
            Ok(outcome) => outcome,
            Err(e) => return self.response_error(task, e),
        };
        let affected = outcome.affected;

        if let Some(log) = undo_log {
            log.push(UndoEntry::ColumnarDelete {
                collection_key: key,
                restored: outcome.restored,
            });
        }

        // Advance the collection floor for this committed columnar write. DELETE
        // replays idempotently, so unlike UPDATE it is not the record whose
        // double-application corrupts — but the watermark is a single claim
        // across all columnar records, and a delete left unstamped understates
        // it, needlessly holding WAL segments and denying a concurrent
        // transaction the conflict it should see against the rows this removed.
        if affected > 0 {
            self.note_collection_write(task, collection);
        }

        debug!(core = self.core_id, %collection, affected, "columnar delete complete");
        let result = serde_json::json!({ "affected": affected });
        match super::super::response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
