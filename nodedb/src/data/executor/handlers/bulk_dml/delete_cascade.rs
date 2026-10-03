// SPDX-License-Identifier: BUSL-1.1

//! Post-commit cascade for one bulk-deleted row, step by step:
//!
//! - Secondary indexes: removes the row's entries. An error fails the
//!   statement after the row's other steps run (see below).
//! - Deleted-node bookkeeping, vector index, doc cache, write-version and
//!   index write-value tracking: in-memory updates that cannot fail.
//! - Journal: the row's delete entry joins the statement's write set.
//! - Event Plane: one delete event per row.
//! - Inverted index: the delete loop collects removed surrogates and removes
//!   their text in one write transaction per batch. An error fails the
//!   statement.
//!
//! Runs AFTER the row's own transaction committed, so none of this reverses
//! on failure. A failed step fails the statement with every removed row
//! journalled, its event emitted, and its pending text removed first.

use nodedb_types::Surrogate;
use nodedb_types::columnar::StrictSchema;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::partial_refusal::refusal_after_rows;
use crate::data::executor::handlers::transaction::stage_write::stored_row_identity;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{IndexPath, StorageKey};

/// Removed rows whose text leaves the inverted index in one write
/// transaction.
const BULK_DELETE_TEXT_BATCH: usize = 512;

/// When a bulk delete flushes its pending text, and the statement it
/// belongs to.
pub(in crate::data::executor) struct TextFlush<'a> {
    tid: u64,
    collection: &'a str,
    /// Flush once the pending rows reach this count.
    at_least: usize,
    /// Rows the statement removed so far.
    affected: u64,
}

impl<'a> TextFlush<'a> {
    /// Flush once a full batch is pending.
    pub(in crate::data::executor) fn full_batch(tid: u64, collection: &'a str, affected: u64) -> Self {
        Self {
            tid,
            collection,
            at_least: BULK_DELETE_TEXT_BATCH,
            affected,
        }
    }

    /// Flush whatever is pending: the statement's last batch.
    pub(in crate::data::executor) fn remainder(tid: u64, collection: &'a str, affected: u64) -> Self {
        Self {
            tid,
            collection,
            at_least: 1,
            affected,
        }
    }
}

/// Borrowed + owned inputs for [`CoreLoop::bulk_delete_row_cascade`], grouped
/// so the call stays within the argument-count budget.
pub(in crate::data::executor) struct BulkDeleteRowCascade<'a> {
    pub task: &'a ExecutionTask,
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub storage_key: StorageKey,
    /// The row's pre-deletion bytes, as `sparse.delete` returned them —
    /// never re-read.
    pub deleted_bytes: &'a [u8],
    /// The collection's strict schema, when it stores Binary Tuples. Decodes
    /// `deleted_bytes` so the row's identity column is readable.
    pub strict_schema: Option<&'a StrictSchema>,
    /// The collection's declared `PRIMARY KEY` column, when it has one.
    pub declared_primary_key: Option<&'a str>,
    pub has_vectors: bool,
    pub index_paths: &'a [IndexPath],
    /// The pre-deletion document, when the caller captured one (`RETURNING`
    /// or an indexed collection). `None` costs this cascade nothing beyond
    /// skipping the write-value note and the `RETURNING` row.
    pub pre_delete_doc: Option<serde_json::Value>,
    pub returning: bool,
}

impl CoreLoop {
    /// Cascade one committed row removal into every secondary structure a
    /// bulk delete must also clean up, and account it into `write_set` /
    /// `returned_docs`.
    ///
    /// Every step runs for the committed row, so its journal entry and event
    /// are never lost. `Err` is the secondary-index removal error: the
    /// statement fails with this row counted as removed.
    pub(in crate::data::executor) fn bulk_delete_row_cascade(
        &mut self,
        cascade: BulkDeleteRowCascade<'_>,
        write_set: &mut Vec<WriteSetEntry>,
        returned_docs: &mut Vec<nodedb_types::Value>,
    ) -> crate::Result<()> {
        let BulkDeleteRowCascade {
            task,
            database_id,
            tid,
            collection,
            storage_key,
            deleted_bytes,
            strict_schema,
            declared_primary_key,
            has_vectors,
            index_paths,
            pre_delete_doc,
            returning,
        } = cascade;

        // The identity INSERT minted for this row: its declared primary key
        // when the collection declares one, else its decimal surrogate. The
        // redo entry and the delete event both name the row by it.
        let row_identity = stored_row_identity(
            deleted_bytes,
            strict_schema,
            declared_primary_key,
            storage_key,
        );

        let row_surrogate = storage_key.surrogate();
        // Cascade: secondary indexes. An error is recorded at its detection
        // site and returned once the row's remaining steps ran.
        let secondary = self.sparse.delete_indexes_for_document(
            task.request.database_id.as_u64(),
            tid,
            collection,
            &storage_key,
        );
        if let Err(e) = &secondary {
            crate::diag::orphaned_index_entry_after_delete(e, collection, "secondary");
        }
        // The row's graph node keeps its edges here: the delete's own
        // transaction tombstones them with `EdgeDelete` tasks. The node is
        // recorded deleted for edge referential integrity.
        self.mark_node_deleted(database_id, tid, collection, row_identity.as_str());
        // Cascade: secondary HNSW vector index. The put path indexed
        // this row's vectors under its surrogate; the delete must
        // soft-delete those nodes and drop the reverse-map entry, or the
        // leaked vector keeps scoring in KNN search in the same process.
        if has_vectors {
            self.remove_document_vector_indexes(database_id, tid, collection, storage_key);
        }
        self.doc_cache.invalidate(
            task.request.database_id.as_u64(),
            tid,
            collection,
            &storage_key,
        );
        // Record the committed delete's write version against its
        // surrogate + collection.
        self.note_surrogate_write_lsn(task, tid, collection, row_surrogate.as_u32());
        // Record the removed secondary-index tuples into the
        // per-index write-value substrate, recomputed from the
        // pre-delete document (see `index_paths` comment above).
        if let (Some(lsn), Some(doc)) = (task.wal_lsn(), pre_delete_doc.as_ref()) {
            let tuples = self.index_tuples_for_doc(doc, index_paths);
            self.note_index_write_values(
                task.request.database_id,
                crate::types::TenantId::new(tid),
                collection,
                &tuples,
                lsn,
            );
        }
        // The removal, journalled after apply: the plan carries no
        // pre-dispatch record of it. A delete carries no post-image body.
        write_set.push(WriteSetEntry::delete(
            row_surrogate.as_u32(),
            row_identity.clone(),
        ));
        // Emit a delete event per affected row to the Event Plane, so
        // AFTER-DELETE triggers and CDC/change-stream consumers see
        // each row a bulk DELETE removed — mirroring
        // `execute_point_delete`'s single-row emit. `deleted_bytes` is
        // the prior stored bytes `sparse.delete` returned above (no
        // second read needed); the emit converts a strict row to
        // MessagePack for triggers. Emitted per row
        // (not a `WriteOp::BulkDelete` summary) — the Event Plane's
        // WAL-replay bulk variant is aggregate metadata reconstructed
        // only when the live per-row events were lost.
        self.emit_document_delete_event(task, tid, collection, row_identity, Some(deleted_bytes));
        if returning && let Some(doc) = pre_delete_doc {
            returned_docs.push(nodedb_types::Value::from(doc));
        }
        secondary
    }

    /// Remove the text of the rows in `pending` from the inverted index in
    /// one write transaction, then clear `pending`.
    pub(in crate::data::executor) fn flush_deleted_text(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        pending: &mut Vec<Surrogate>,
    ) -> crate::Result<()> {
        if let Err(e) = self.inverted.remove_documents(
            database_id,
            crate::types::TenantId::new(tid),
            collection,
            pending,
        ) {
            // Recorded at the detection site: the rows' own transactions have
            // committed, so the statement fails with their entries journalled.
            crate::diag::orphaned_index_entry_after_delete(&e, collection, "inverted");
            return Err(e);
        }
        pending.clear();
        Ok(())
    }

    /// Flush `pending` per `flush`. The refusal code when the removal fails.
    pub(in crate::data::executor) fn flush_deleted_text_at(
        &self,
        task: &ExecutionTask,
        flush: TextFlush<'_>,
        pending: &mut Vec<Surrogate>,
    ) -> Result<(), ErrorCode> {
        if pending.len() < flush.at_least {
            return Ok(());
        }
        self.flush_deleted_text(
            task.request.database_id.as_u64(),
            flush.tid,
            flush.collection,
            pending,
        )
        .map_err(|e| {
            refusal_after_rows(
                flush.affected,
                ErrorCode::Internal {
                    detail: format!("bulk delete: removing deleted rows' text failed: {e}"),
                },
            )
        })
    }

    /// The refusal of a bulk delete that stopped part-way. The rows removed
    /// so far stay removed, so their pending text leaves the inverted index
    /// first. A failure of that removal is the refusal.
    pub(in crate::data::executor) fn bulk_delete_refusal(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        code: ErrorCode,
        pending: &mut Vec<Surrogate>,
        write_set: Vec<WriteSetEntry>,
    ) -> Response {
        let removed = pending.len() as u64;
        let code = match self.flush_deleted_text(
            task.request.database_id.as_u64(),
            tid,
            collection,
            pending,
        ) {
            Ok(()) => code,
            Err(e) => refusal_after_rows(
                removed,
                ErrorCode::Internal {
                    detail: format!(
                        "{code:?}; removing the deleted rows' text from the inverted index \
                         failed: {e}"
                    ),
                },
            ),
        };
        self.refusal_with_landed_rows(task, code, write_set)
    }
}
