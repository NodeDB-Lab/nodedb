// SPDX-License-Identifier: BUSL-1.1

//! Live sparse-index maintenance for `PointUpdate`.
//!
//! `execute_point_update` rewrites a document's stored body directly (via
//! `sparse.put` / `bitemporal_update_reindex`) and reconciles the secondary
//! btree, FTS, graph, and dense-vector overlays — but not the sparse inverted
//! index. An UPDATE that changes (or clears) a `SparseVector` field must
//! therefore re-index the row here, or sparse search keeps returning the stale
//! pre-update literal in the same process (no restart required).
//!
//! The row's `row_key` is stable across an update, so this is a remove-then-
//! insert keyed by it: `remove_document_sparse_indexes` drops the old literal
//! (covering the field-cleared case, where the re-insert never re-adds it) and
//! `apply_point_put_sparse_indexes` upserts the new one. This reuses the exact
//! put-time and delete-time maintenance so the update path stays in lockstep
//! with insert and delete.

use crate::data::executor::core_loop::CoreLoop;

/// Inputs for [`CoreLoop::update_reindex_sparse_indexes`].
pub(in crate::data::executor) struct UpdateSparseReindex<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    /// Hex-surrogate storage key (matches the sparse-index doc-id keying used
    /// by the put and delete paths).
    pub row_key: &'a str,
    /// The freshly-written stored body (Binary Tuple for strict, MessagePack
    /// for schemaless). Decoded storage-mode-aware to extract the new literal.
    pub new_body: &'a [u8],
    /// Whether `collection` is a strict-schema collection, i.e. whether
    /// `new_body` is a Binary Tuple that needs the decode→re-encode-to-
    /// MessagePack round trip below.
    pub is_strict: bool,
    /// Whether `collection` declares a `SparseVector` column, computed ONCE by
    /// the caller via `collection_has_sparse`. `false` short-circuits this call
    /// to a no-op so a non-sparse UPDATE is byte-identical to today.
    pub has_sparse: bool,
}

impl CoreLoop {
    /// Re-index a just-updated document's sparse vectors into their inverted
    /// indexes.
    ///
    /// No-op when the collection declares no sparse fields. Otherwise drops the
    /// row's prior sparse entries (so a cleared field stops matching) and
    /// upserts the new literal under the same row key.
    pub(in crate::data::executor) fn update_reindex_sparse_indexes(
        &mut self,
        p: UpdateSparseReindex<'_>,
    ) {
        if !p.has_sparse {
            return;
        }

        // Drop the row's old sparse entries before re-inserting: an UPDATE that
        // clears the `SparseVector` field must not leave the stale literal
        // searchable, and the re-insert below only re-adds fields present in
        // the new body.
        self.remove_document_sparse_indexes(p.database_id, p.tid, p.collection, p.row_key);

        // Re-extract from the new body via the exact put-time path. Sparse
        // extraction reads MessagePack; strict bodies are stored as Binary
        // Tuples, so decode storage-mode-aware, then re-encode to MessagePack —
        // matching the `value` `apply_point_put` feeds the indexer. Schemaless
        // `new_body` is already standard MessagePack, so that case reuses the
        // bytes as-is.
        let owned_mp;
        let mp: &[u8] = if p.is_strict {
            let config_key = (
                crate::types::DatabaseId::new(p.database_id),
                crate::types::TenantId::new(p.tid),
                p.collection.to_string(),
            );
            let Some(config) = self.doc_configs.get(&config_key) else {
                return;
            };
            let Some(doc) = self.decode_stored_document(config, p.new_body) else {
                return;
            };
            let Ok(mp) = nodedb_types::json_to_msgpack(&doc) else {
                return;
            };
            owned_mp = mp;
            &owned_mp
        } else {
            p.new_body
        };

        self.apply_point_put_sparse_indexes(p.database_id, p.tid, p.collection, p.row_key, mp);
    }
}
