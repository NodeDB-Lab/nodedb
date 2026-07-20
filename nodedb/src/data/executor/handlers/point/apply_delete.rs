// SPDX-License-Identifier: BUSL-1.1

//! Shared "apply a PointDelete" helper — manages its own doc-store write
//! transaction internally (see doc comment on `apply_point_delete`).
//!
//! Reused by both the autocommit PointDelete path and the transactional
//! `tx_point_delete` path. Every side-effect it performs (including the EXTRA
//! spatial-removal + node-tombstone cascade) is captured in the returned
//! [`PointDeleteOutcome`] so a transactional caller can build a fully
//! reversible undo log.

use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::{append_only, period_lock, retention};
use nodedb_types::Surrogate;

use crate::data::executor::handlers::point::apply_put::VectorIndexDelta;
use crate::data::executor::handlers::point::apply_put::map_enforcement_error;
use crate::data::executor::spatial_key::SpatialIndexKey;

/// Parameters for [`CoreLoop::apply_point_delete`].
pub(in crate::data::executor) struct PointDeleteParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    /// Roles held by the authenticated user. Currently unused by DELETE
    /// enforcement (no role-gated delete checks exist yet), but threaded
    /// through for symmetry with `PointPutParams` and future-proofing.
    pub user_roles: &'a [String],
    /// Whether to run stateless DELETE enforcement (append-only, period
    /// lock, retention/legal-hold).
    ///
    /// `true` for user-DML callers (autocommit PointDelete, and the
    /// transactional path in a later unit). `false` for system-sourced
    /// deletes (e.g. CRDT-sync materialization) whose admission already
    /// happened on their origin replica.
    pub enforce: bool,
}

/// Capture of the mutations an [`CoreLoop::apply_point_delete`] performed, so
/// a transactional caller can build an undo entry that fully reverses it.
pub(in crate::data::executor) struct PointDeleteOutcome {
    /// Prior stored bytes when a row was actually removed, else `None`.
    pub prior_value: Option<Vec<u8>>,
    /// System-time key the bitemporal tombstone row (and its versioned index
    /// tombstones) were appended at. `Some(t)` on the bitemporal branch,
    /// `None` on the plain delete branch.
    pub bitemporal_sys_from_ms: Option<i64>,
    /// `(field, value)` pairs whose versioned index tombstones this op wrote
    /// at `bitemporal_sys_from_ms`. Empty when not bitemporal / none written.
    pub bitemporal_index_tuples: Vec<(String, String)>,
    /// `(field, value)` pairs the plain (non-bitemporal) secondary-index
    /// cascade removed for this document. Captured unconditionally (the cascade
    /// runs for both autocommit and transactional callers) so a transactional
    /// caller can re-insert them on rollback — closing the pre-existing hole
    /// where a rolled-back DELETE never restored its secondary-index entries.
    /// Empty on the bitemporal path (which has no plain INDEXES entries).
    pub secondary_index_tuples: Vec<(String, String)>,
    /// Vector index mutations this delete soft-deleted from HNSW vector
    /// indexes. Populated unconditionally (autocommit and transactional) so the
    /// owning document's vectors never orphan; a transactional caller pushes an
    /// `UndoEntry::DeleteVector` per entry so a rolled-back delete restores
    /// them, including the paired `vector_doc_map` entry this cascade removed.
    pub vector_deletes: Vec<VectorIndexDelta>,
    /// `(spatial_index_key, entry_id, bbox, document_id)` tuples this delete
    /// removed from per-field spatial R-trees (and the reverse
    /// `spatial_doc_map`). The bbox is captured BEFORE the R-tree `delete`
    /// (which does not return it) so a transactional caller can push
    /// `UndoEntry::SpatialDelete` re-insert reversals. Empty when the document
    /// had no spatial fields. Autocommit callers ignore it (an aborted redb txn
    /// does not reverse in-memory spatial writes).
    pub spatial_deletes: Vec<(SpatialIndexKey, u64, nodedb_types::BoundingBox, String)>,
    /// Graph edges the unconditional graph-edge cascade removed from BOTH the
    /// in-memory CSR partition AND the persistent edge store — each captured as
    /// `(collection, src, label, dst, old_properties)`. Populated regardless of
    /// caller (the cascade is unconditional), so a transactional caller pushes
    /// one `UndoEntry::DeleteEdge` per entry and a rolled-back delete restores
    /// every cascaded edge into both stores. Autocommit callers ignore it.
    pub edge_deletes: Vec<crate::engine::graph::edge_store::EdgeRestore>,
    /// The node id this delete NEWLY marked deleted in the in-memory
    /// `deleted_nodes` edge referential-integrity tracker, if any. `Some(id)`
    /// only when `mark_node_deleted` newly inserted the node (it was not already
    /// tombstoned by a prior committed op). A transactional caller pushes an
    /// `UndoEntry::MarkNodeDeleted` so a rolled-back delete un-marks exactly the
    /// node it added — never resurrecting a pre-existing tombstone. `None` when
    /// the cascade didn't run or the node was already marked. Autocommit callers
    /// ignore it.
    pub mark_node_deleted: Option<String>,
}

impl CoreLoop {
    /// Apply a PointDelete, managing its own doc-store write transaction.
    ///
    /// Handles the bitemporal-aware tombstone/versioned-index-tombstone
    /// branch, the non-bitemporal overwrite-delete branch, and all cascades
    /// (inverted index, secondary indexes, graph edges, spatial R-tree,
    /// node-deleted bookkeeping, doc cache invalidation).
    ///
    /// The doc-store write transaction (bitemporal: an explicit
    /// `begin_write`/`commit` pair; non-bitemporal: the self-committing
    /// `SparseEngine::delete`) is committed BEFORE any cascade runs. Several
    /// cascades (`delete_indexes_for_document`, the inverted index removal)
    /// open their own internal write transactions, and redb allows only one
    /// write transaction at a time — keeping the doc-store txn open across
    /// those calls would deadlock every delete.
    ///
    /// Does NOT emit WriteEvents, mark checkpoints dirty, or build
    /// RETURNING payloads — those stay with the caller.
    ///
    /// Returns a [`PointDeleteOutcome`] capturing the prior stored bytes
    /// (present when a row was actually removed) plus the bitemporal system
    /// time and versioned index tombstone tuples written, so a transactional
    /// caller can build a fully-reversible undo entry. Autocommit callers
    /// read only `prior_value`.
    pub(in crate::data::executor) fn apply_point_delete(
        &mut self,
        params: PointDeleteParams<'_>,
    ) -> crate::Result<PointDeleteOutcome> {
        let PointDeleteParams {
            database_id,
            tid,
            collection,
            document_id,
            surrogate,
            user_roles,
            enforce,
        } = params;
        let _ = user_roles;

        let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        let bitemporal = self.is_bitemporal(database_id, tid, collection);
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );

        // On bitemporal collections: append a doc tombstone + versioned
        // index tombstones for every current field value. `prior` is the
        // pre-delete body so the Event Plane sees `old_value` correctly.
        // Current-state-only indexes (text, graph, spatial, vector) are
        // still cascaded below — they track "what exists now" regardless
        // of bitemporal history.
        let mut bitemporal_sys_from_ms: Option<i64> = None;
        let mut bitemporal_index_tuples: Vec<(String, String)> = Vec::new();
        let prior = if bitemporal {
            let prior = self
                .sparse
                .versioned_get_current(database_id, tid, collection, row_key)?;
            if let Some(ref body) = prior {
                if enforce && let Some(config) = self.doc_configs.get(&config_key) {
                    run_delete_enforcement(
                        &self.sparse,
                        database_id,
                        tid,
                        collection,
                        config,
                        Some(body),
                    )?;
                }
                let sys_from = self.bitemporal_now_ms();
                bitemporal_sys_from_ms = Some(sys_from);
                let txn = self.sparse.begin_write()?;
                self.sparse.versioned_tombstone_in_txn(
                    &txn,
                    database_id,
                    tid,
                    collection,
                    row_key,
                    sys_from,
                )?;
                // Index tombstones: reflect every current value so
                // `index_lookup_as_of` at or after `sys_from` skips this
                // doc_id. `body` is the STORED bytes (MessagePack for
                // schemaless, Binary Tuple for strict) — use the
                // storage-mode-aware decoder so strict bitemporal deletes
                // also tombstone their secondary-index entries instead of
                // silently skipping this loop.
                if let Some(config) = self.doc_configs.get(&config_key)
                    && let Some(doc) = self.decode_stored_document(config, body)
                {
                    for path in config.index_paths.clone() {
                        for v in crate::engine::document::store::extract_index_values(
                            &doc,
                            &path.path,
                            path.is_array,
                        ) {
                            let value = if path.case_insensitive {
                                v.to_lowercase()
                            } else {
                                v
                            };
                            self.sparse.versioned_index_tombstone_in_txn(
                                &txn,
                                crate::engine::sparse::btree_versioned::VersionedIndexEntry {
                                    database_id,
                                    tenant: tid,
                                    coll: collection,
                                    field: &path.path,
                                    value: &value,
                                    doc_id: row_key,
                                    sys_from_ms: sys_from,
                                },
                            )?;
                            bitemporal_index_tuples.push((path.path.clone(), value));
                        }
                    }
                }
                txn.commit().map_err(|e| crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("commit: {e}"),
                })?;
            }
            prior
        } else {
            if enforce && let Some(config) = self.doc_configs.get(&config_key) {
                let old_value = self.sparse.get(database_id, tid, collection, row_key)?;
                run_delete_enforcement(
                    &self.sparse,
                    database_id,
                    tid,
                    collection,
                    config,
                    old_value.as_deref(),
                )?;
            }
            self.sparse.delete(database_id, tid, collection, row_key)?
        };

        // Capture the plain secondary-index `(field, value)` tuples this
        // document contributed, BEFORE cascade 2 wipes them, so a transactional
        // caller can restore them on rollback. Only the non-bitemporal path
        // writes plain INDEXES entries (bitemporal uses versioned tombstones
        // above), so gate on `!bitemporal`. Applies the same predicate +
        // case-insensitive folding the forward secondary-index write uses.
        let mut secondary_index_tuples: Vec<(String, String)> = Vec::new();
        // `prior` is the STORED blob of the just-deleted row: MessagePack for
        // schemaless collections, Binary Tuple for strict ones. Decode through
        // the storage-mode-aware helper so strict tx-DELETEs also capture the
        // real removed index tuples for rollback restore.
        if !bitemporal
            && let Some(ref body) = prior
            && let Some(config) = self.doc_configs.get(&config_key)
            && let Some(doc) = self.decode_stored_document(config, body)
        {
            for path in config.index_paths.clone() {
                if let Some(ref pred) = path.predicate
                    && !pred.evaluate_json(&doc)
                {
                    continue;
                }
                for v in crate::engine::document::store::extract_index_values(
                    &doc,
                    &path.path,
                    path.is_array,
                ) {
                    let value = if path.case_insensitive {
                        v.to_lowercase()
                    } else {
                        v
                    };
                    secondary_index_tuples.push((path.path.clone(), value));
                }
            }
        }

        // Cascade 1: Remove from full-text inverted index. The inverted
        // index was populated by `apply_point_put` with the substrate row
        // key (hex surrogate), not the user-visible PK — keep the cascade
        // keyed the same way so a delete actually wipes the term postings.
        if let Err(e) = self.inverted.remove_document(
            database_id,
            crate::types::TenantId::new(tid),
            collection,
            surrogate,
        ) {
            warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index removal failed");
        }

        // Cascade 2: Remove secondary index entries for this document.
        // Secondary indexes use key format "{tenant}:{collection}:{field}:{value}:{doc_id}".
        // We scan and delete all entries ending with this doc_id.
        if let Err(e) =
            self.sparse
                .delete_indexes_for_document(database_id, tid, collection, row_key)
        {
            warn!(core = self.core_id, %collection, %document_id, error = %e, "secondary index cascade failed");
        }

        // Cascade 3: Remove graph edges where this document is src or dst.
        // Captured unconditionally (the cascade runs for both autocommit and
        // transactional callers) so a transactional caller can restore every
        // removed edge on rollback via `UndoEntry::DeleteEdge`, which re-inserts
        // into BOTH the CSR partition and the persistent edge store — matching
        // the two stores this cascade removes from.
        let mut edge_deletes: Vec<crate::engine::graph::edge_store::EdgeRestore> = Vec::new();
        let edges_removed = self
            .csr_partition_mut(database_id, tid)
            .remove_node_edges(document_id);
        if edges_removed > 0 {
            // Also tombstone in persistent edge store, capturing each removed
            // edge (with its pre-delete properties) for rollback restore.
            let cascade_ord = self.hlc.next_ordinal();
            match self.edge_store.delete_edges_for_node(
                database_id,
                nodedb_types::TenantId::new(tid),
                document_id,
                cascade_ord,
            ) {
                Ok(removed) => edge_deletes = removed,
                Err(e) => {
                    warn!(core = self.core_id, %document_id, error = %e, "edge cascade failed");
                }
            }
            tracing::trace!(core = self.core_id, %document_id, edges_removed, "EDGE_CASCADE_DELETE");
        }

        // Cascade 4: Remove from spatial R-tree indexes + reverse map, and
        // record the node deletion for edge referential integrity. Both are
        // fully captured (`spatial_deletes` + `mark_node_deleted` in the
        // outcome) and reversed on rollback, so they run unconditionally for
        // both the autocommit and transactional delete paths.
        //
        // `apply_point_put` hashes the substrate row key as the R-tree entry
        // id, so delete must hash the same key to find the entry. Hashing the
        // user PK would leak ghost bbox entries that survive the row's removal.
        // `(spatial_index_key, entry_id, bbox, document_id)` tuples removed by
        // the spatial cascade below are captured so a transactional caller can
        // reverse them.
        let mut mark_node_deleted_capture: Option<String> = None;
        // The put path hashes the hex-surrogate storage key (== `row_key`) as
        // the R-tree entry id, so the shared removal hashes the same key to
        // find and drop every per-field entry + reverse-map pair for this
        // document. Captures each removed `(skey, entry_id, bbox, doc)` for
        // reversible undo.
        let spatial_deletes =
            self.remove_document_spatial_indexes(database_id, tid, collection, row_key);

        // Record deletion for edge referential integrity. Capture the id
        // for undo ONLY when this call newly marked it — un-marking a node
        // a prior committed op already tombstoned would wrongly resurrect
        // it as a valid edge target.
        if self.mark_node_deleted(database_id, tid, document_id) {
            mark_node_deleted_capture = Some(document_id.to_string());
        }

        // Cascade 5 (CORE, UNCONDITIONAL): soft-delete any HNSW vector entries
        // this document produced. Runs for BOTH autocommit and transactional
        // callers — leaving them behind orphans the vector index forever (a
        // deleted doc keeps scoring in KNN). The reverse map `vector_doc_map`
        // was populated by `apply_point_put_vector_indexes` under the same hex
        // surrogate row key used here. Soft-delete (not hard) so a rolled-back
        // transactional delete can `undelete` the exact vector id.
        //
        // The candidate fields are known from the same schema/vector_params
        // enumeration the put path uses, so each `vector_doc_map` entry is
        // looked up by its exact key rather than scanning the whole map on
        // every delete. Shared with the PointUpdate re-index path.
        let vector_deletes =
            self.remove_document_vector_indexes(database_id, tid, collection, row_key);

        // Sparse inverted-index cleanup, mirroring the dense-vector cascade
        // above: drop this document's sparse posting entries under the same hex
        // surrogate row key the put path indexed them by. A no-op unless the
        // strict schema declares a `SparseVector` column.
        self.remove_document_sparse_indexes(database_id, tid, collection, row_key);

        // Invalidate document cache.
        self.doc_cache
            .invalidate(database_id, tid, collection, row_key);

        // Invalidate aggregate cache — a delete changes count(*) for this
        // collection. Only needed when a row was actually removed.
        if prior.is_some() {
            self.invalidate_aggregate_cache_for_collection(database_id, tid, collection);
        }

        Ok(PointDeleteOutcome {
            prior_value: prior,
            bitemporal_sys_from_ms,
            bitemporal_index_tuples,
            secondary_index_tuples,
            vector_deletes,
            spatial_deletes,
            edge_deletes,
            mark_node_deleted: mark_node_deleted_capture,
        })
    }
}

/// Stateless DELETE enforcement, unified across the autocommit
/// (`apply_point_delete`) and transactional (`tx_point_delete`) paths.
/// These checks have no persistent side effect, so a violation here
/// simply aborts before the write.
fn run_delete_enforcement(
    sparse: &crate::engine::sparse::btree::SparseEngine,
    database_id: u64,
    tid: u64,
    collection: &str,
    config: &crate::engine::document::store::CollectionConfig,
    old_value: Option<&[u8]>,
) -> crate::Result<()> {
    append_only::check_point_delete(collection, &config.enforcement)
        .map_err(map_enforcement_error)?;
    if let Some(ref pl) = config.enforcement.period_lock
        && let Some(old_bytes) = old_value
    {
        period_lock::check_period_lock(sparse, database_id, tid, collection, old_bytes, pl)
            .map_err(map_enforcement_error)?;
    }
    let created_at = old_value.and_then(retention::extract_created_at_secs);
    retention::check_delete_allowed(collection, &config.enforcement, created_at)
        .map_err(map_enforcement_error)?;
    Ok(())
}
