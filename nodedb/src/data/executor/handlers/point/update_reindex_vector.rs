// SPDX-License-Identifier: BUSL-1.1

//! Live HNSW maintenance for `PointUpdate`.
//!
//! `execute_point_update` rewrites a document's stored body directly (via
//! `sparse.put` / `bitemporal_update_reindex`) and reconciles the secondary
//! btree, FTS, and graph overlays — but not the secondary VECTOR index. An
//! UPDATE that changes an embedding field must therefore re-index the row's
//! vectors here, or KNN search keeps returning the stale pre-update embedding
//! in the same process (no restart required).
//!
//! The row's surrogate is stable across an update, so re-indexing is a
//! remove-then-insert keyed by that surrogate: `insert_with_surrogate` appends
//! a fresh HNSW node rather than replacing the old one, so the prior node must
//! be soft-deleted first or the index would carry both embeddings. This reuses
//! the exact put-time (`apply_point_put_vector_indexes`) and delete-time
//! (`remove_document_vector_indexes`) maintenance so the update path stays in
//! lockstep with insert and delete.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::VectorIndexPutParams;
use nodedb_types::Surrogate;

/// Inputs for [`CoreLoop::update_reindex_vector_indexes`].
pub(in crate::data::executor) struct UpdateVectorReindex<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    /// Hex-surrogate storage key (matches the `vector_doc_map` keying used by
    /// the put and delete paths).
    pub row_key: &'a str,
    pub surrogate: Surrogate,
    /// The freshly-written stored body (Binary Tuple for strict, MessagePack
    /// for schemaless). Decoded storage-mode-aware to extract the new vectors.
    pub new_body: &'a [u8],
    /// Whether `collection` is a strict-schema collection, i.e. whether
    /// `new_body` is a Binary Tuple that needs the decode→re-encode-to-
    /// MessagePack round trip below. Schemaless `new_body` is already the
    /// standard MessagePack `apply_point_put_vector_indexes` expects, so that
    /// case skips the round trip and reuses the bytes directly.
    pub is_strict: bool,
    /// Whether `collection` has a vector index, computed ONCE by the caller via
    /// `collection_has_vectors`. Recomputing per row would make a statement cost
    /// `rows * vector_params` rather than `rows + vector_params`, because the
    /// schemaless half scans `vector_params` unindexed. `false` short-circuits
    /// this call to a no-op.
    pub has_vectors: bool,
}

impl CoreLoop {
    /// Re-index a just-updated document's vectors into their HNSW collections.
    ///
    /// No-op when the collection declares no vector fields. Otherwise soft-
    /// deletes the surrogate's prior vector nodes (so the old embedding stops
    /// scoring in KNN) and inserts the new embedding under the same surrogate.
    pub(in crate::data::executor) fn update_reindex_vector_indexes(
        &mut self,
        p: UpdateVectorReindex<'_>,
    ) {
        // Gate: skip all vector work unless this collection has a vector index.
        // Trusts the caller's precomputed `has_vectors` instead of recomputing
        // it here, so a caller looping over N rows pays for the (unindexed,
        // `vector_params`-scanning) schemaless check once, not once per row.
        if !p.has_vectors {
            return;
        }

        // Remove the surrogate's old vector nodes (and their reverse-map
        // entries) before re-inserting: `insert_with_surrogate` appends a new
        // node rather than replacing, so skipping this would leave the stale
        // embedding searchable alongside the new one.
        self.remove_document_vector_indexes(p.database_id, p.tid, p.collection, p.row_key);

        // Re-extract vectors from the new body via the exact put-time path.
        // Vector extraction reads MessagePack; strict bodies are stored as
        // Binary Tuples, so decode storage-mode-aware, then re-encode to
        // MessagePack — matching the `value` `apply_point_put` feeds the
        // indexer (the injected-rowid MessagePack, never the Binary Tuple).
        // Schemaless `new_body` is already standard MessagePack (produced by
        // the same `doc_format` paths `apply_point_put` uses), so that case
        // skips the decode/re-encode round trip and reuses the bytes as-is.
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

        // Live in-memory maintenance only: `wal_lsn = 0` disables the
        // checkpoint-straddle guard (the WAL record for this update is appended
        // in the Control Plane, not here).
        self.apply_point_put_vector_indexes(VectorIndexPutParams {
            database_id: p.database_id,
            tid: p.tid,
            collection: p.collection,
            document_id: p.row_key,
            surrogate: p.surrogate,
            value: mp,
            wal_lsn: 0,
        });
    }
}
