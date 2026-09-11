// SPDX-License-Identifier: BUSL-1.1

//! Post-commit cascade for one bulk-deleted row: inverted index, secondary
//! indexes, graph edges, vector index, doc cache, write-version tracking,
//! and the Event Plane emit.
//!
//! Runs AFTER the row's own transaction committed, so none of this reverses
//! on failure — each step logs and continues rather than aborting a
//! statement it cannot undo.

use tracing::warn;

use crate::bridge::envelope::WriteSetEntry;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{IndexPath, StorageKey};

/// Borrowed + owned inputs for [`CoreLoop::bulk_delete_row_cascade`], grouped
/// so the call stays within the argument-count budget.
pub(in crate::data::executor) struct BulkDeleteRowCascade<'a> {
    pub task: &'a ExecutionTask,
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub doc_id: &'a str,
    pub storage_key: StorageKey,
    /// The row's pre-deletion bytes, as `sparse.delete` returned them —
    /// never re-read.
    pub deleted_bytes: &'a [u8],
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
    pub(in crate::data::executor) fn bulk_delete_row_cascade(
        &mut self,
        cascade: BulkDeleteRowCascade<'_>,
        write_set: &mut Vec<WriteSetEntry>,
        returned_docs: &mut Vec<serde_json::Value>,
    ) {
        let BulkDeleteRowCascade {
            task,
            database_id,
            tid,
            collection,
            doc_id,
            storage_key,
            deleted_bytes,
            has_vectors,
            index_paths,
            pre_delete_doc,
            returning,
        } = cascade;

        // Cascade: inverted index.
        let row_surrogate = storage_key.surrogate();
        if let Err(e) = self.inverted.remove_document(
            task.request.database_id.as_u64(),
            crate::types::TenantId::new(tid),
            collection,
            row_surrogate,
        ) {
            // Recorded here, at the detection site: the row's own
            // transaction has already committed, so this cleanup
            // failure cannot roll it back.
            crate::diag::orphaned_index_entry_after_delete(&e, collection, "inverted");
            warn!(core = self.core_id, %collection, %doc_id, error = %e, "bulk delete: inverted index removal failed");
        }
        // Cascade: secondary indexes.
        if let Err(e) = self.sparse.delete_indexes_for_document(
            task.request.database_id.as_u64(),
            tid,
            collection,
            &storage_key,
        ) {
            crate::diag::orphaned_index_entry_after_delete(&e, collection, "secondary");
            warn!(core = self.core_id, %collection, %doc_id, error = %e, "bulk delete: secondary index cascade failed");
        }
        // Cascade: graph edges.
        let edges_removed = self
            .csr_partition_mut(database_id, tid)
            .remove_node_edges(doc_id);
        let cascade_ord = self.hlc.next_ordinal();
        if edges_removed > 0
            && let Err(e) = self.edge_store.delete_edges_for_node(
                database_id,
                nodedb_types::TenantId::new(tid),
                doc_id,
                cascade_ord,
            )
        {
            crate::diag::orphaned_index_entry_after_delete(&e, collection, "graph_edge");
            warn!(core = self.core_id, %doc_id, error = %e, "bulk delete: edge cascade failed");
        }
        self.mark_node_deleted(database_id, tid, doc_id);
        // Cascade: secondary HNSW vector index. The put path indexed
        // this row's vectors under its surrogate; the delete must
        // soft-delete those nodes and drop the reverse-map entry, or the
        // leaked vector keeps scoring in KNN search in the same process.
        if has_vectors {
            self.remove_document_vector_indexes(database_id, tid, collection, doc_id);
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
        // Carry the surrogate back for a post-apply `Delete` redo so
        // the removed vector node does not resurrect on a WAL-only
        // restart. Gated on `has_vectors` — a non-vector collection
        // pays nothing. A delete carries no post-image body.
        if has_vectors {
            write_set.push(WriteSetEntry {
                surrogate: row_surrogate.as_u32(),
                is_delete: true,
                value: Vec::new(),
                collection: None,
            });
        }
        // Emit a delete event per affected row to the Event Plane, so
        // AFTER-DELETE triggers and CDC/change-stream consumers see
        // each row a bulk DELETE removed — mirroring
        // `execute_point_delete`'s single-row emit. `deleted_bytes` is
        // the prior stored bytes `sparse.delete` returned above (no
        // second read needed); `resolve_event_payload` handles the
        // strict->msgpack conversion for triggers. Emitted per row
        // (not a `WriteOp::BulkDelete` summary) — the Event Plane's
        // WAL-replay bulk variant is aggregate metadata reconstructed
        // only when the live per-row events were lost.
        let old_converted = self.resolve_event_payload(
            task.request.database_id.as_u64(),
            tid,
            collection,
            deleted_bytes,
        );
        let event_identity = storage_key.to_identity();
        self.emit_document_delete_event(
            task,
            collection,
            event_identity,
            Some(old_converted.as_deref().unwrap_or(deleted_bytes)),
        );
        if returning && let Some(doc) = pre_delete_doc {
            returned_docs.push(doc);
        }
    }
}
