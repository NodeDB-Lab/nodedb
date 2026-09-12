// SPDX-License-Identifier: BUSL-1.1

//! Soft-delete a document's prior vector nodes, per field or whole-document.

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::document::store::StorageKey;

use super::types::VectorIndexDelta;

impl CoreLoop {
    /// Soft-delete the single HNSW vector node a document produced for one
    /// `field`, keyed by its storage key, and drop the paired
    /// `vector_doc_map` reverse entry. Returns the removed delta, or `None`
    /// when the `(db, tid, collection, field, storage_key)` key had no prior
    /// node (a genuine first insert). This is the per-field unit the whole-doc
    /// `remove_document_vector_indexes` loops over, and the put path calls it
    /// for the current field only so a sibling field's just-inserted node is
    /// never clobbered.
    pub(in crate::data::executor) fn remove_document_vector_index_field(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        field: &str,
        storage_key: StorageKey,
    ) -> Option<VectorIndexDelta> {
        let db_id = nodedb_types::DatabaseId::new(database_id);
        let tid_id = crate::types::TenantId::new(tid);
        let doc_key = (
            db_id,
            tid_id,
            collection.to_string(),
            field.to_string(),
            storage_key,
        );
        let vector_id = self.vector_doc_map.remove(&doc_key)?;
        let index_key = Self::vector_index_key(database_id, tid, collection, field);
        if let Some(coll) = self.vector_collections.get_mut(&index_key) {
            coll.delete(vector_id);
        }
        Some(VectorIndexDelta {
            index_key,
            vector_id,
            collection: collection.to_string(),
            field: field.to_string(),
            doc_id: storage_key,
        })
    }

    /// Soft-delete every HNSW vector entry a document produced, keyed by its
    /// storage key, and drop the paired `vector_doc_map` reverse entries.
    /// Shared by the PointDelete cascade (which orphans the
    /// vectors of a removed row) and the PointUpdate re-index (which must clear
    /// the surrogate's old embedding before inserting the new one, since
    /// `insert_with_surrogate` appends rather than replaces).
    ///
    /// Candidate fields come from the same strict-schema / `vector_params`
    /// enumeration the put path uses, so each `vector_doc_map` entry is looked
    /// up by its exact key (via `remove_document_vector_index_field`) instead
    /// of scanning the whole map. Returns the removed `(index_key, vector_id)`
    /// deltas so a transactional caller can push `UndoEntry::DeleteVector`
    /// reversals.
    pub(in crate::data::executor) fn remove_document_vector_indexes(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        storage_key: StorageKey,
    ) -> Vec<VectorIndexDelta> {
        let strict_fields = self.strict_vector_fields(database_id, tid, collection);
        let candidate_fields: Vec<String> = if !strict_fields.is_empty() {
            strict_fields.into_iter().map(|(name, _dim)| name).collect()
        } else {
            self.schemaless_vector_field_names(database_id, tid, collection)
        };
        let mut vector_deletes = Vec::with_capacity(candidate_fields.len());
        if candidate_fields.is_empty() {
            return vector_deletes;
        }
        for field in candidate_fields {
            if let Some(delta) = self.remove_document_vector_index_field(
                database_id,
                tid,
                collection,
                &field,
                storage_key,
            ) {
                vector_deletes.push(delta);
            }
        }
        vector_deletes
    }
}
