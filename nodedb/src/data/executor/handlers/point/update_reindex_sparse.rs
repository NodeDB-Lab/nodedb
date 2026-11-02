// SPDX-License-Identifier: BUSL-1.1

//! Live sparse-index maintenance for `PointUpdate`.
//!
//! `execute_point_update` rewrites a document's stored body directly (via
//! `update_reindex`) and reconciles the secondary
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
use crate::engine::document::store::StorageKey;

/// Inputs for [`CoreLoop::update_reindex_sparse_indexes`].
pub(in crate::data::executor) struct UpdateSparseReindex<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    /// Storage key (matches the sparse-index doc-id keying used by the put
    /// and delete paths).
    pub storage_key: StorageKey,
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
    /// The system-time ordinal of the row version the update wrote. The
    /// row's prior sparse versions end there and its new ones start there.
    pub sys_from: i64,
}

impl CoreLoop {
    /// Re-index a just-updated document's sparse vectors into their inverted
    /// indexes.
    ///
    /// No-op when the collection declares no sparse fields. Otherwise ends the
    /// row's prior sparse versions at `sys_from` (so a cleared field stops
    /// matching a current read) and inserts the new literal there under the
    /// same row key. A read at an earlier cut still scores the old literal.
    ///
    /// Fails rather than returning early when the new body will not decode: the
    /// prior entries have already been dropped by then, so returning would leave
    /// the row unsearchable with the update reported as successful. Mirrors
    /// [`Self::update_reindex_vector_indexes`], which is fallible for the same
    /// reason.
    pub(in crate::data::executor) fn update_reindex_sparse_indexes(
        &mut self,
        p: UpdateSparseReindex<'_>,
    ) -> crate::Result<()> {
        if !p.has_sparse {
            return Ok(());
        }

        // End the row's old sparse versions before re-inserting: an UPDATE that
        // clears the `SparseVector` field must not leave the stale literal
        // searchable, and the re-insert below only re-adds fields present in
        // the new body.
        // The row this re-indexes is already committed, so the undo entries
        // go unused.
        self.remove_document_sparse_indexes(
            p.database_id,
            p.tid,
            p.collection,
            p.storage_key,
            p.sys_from,
            &mut Vec::new(),
        );

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
                return Ok(());
            };
            let doc = self.decode_stored_document(config, p.new_body)?;
            owned_mp =
                nodedb_types::json_to_msgpack(&doc).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".to_string(),
                    detail: format!("re-encode decoded strict body for sparse re-index: {e}"),
                })?;
            &owned_mp
        } else {
            p.new_body
        };

        // The row this re-indexes is already committed, so the undo entries
        // go unused.
        self.apply_point_put_sparse_indexes(
            super::apply_put::SparseIndexPut {
                database_id: p.database_id,
                tid: p.tid,
                collection: p.collection,
                storage_key: p.storage_key,
                value: mp,
                sys_from: p.sys_from,
            },
            &mut Vec::new(),
        );
        Ok(())
    }
}
