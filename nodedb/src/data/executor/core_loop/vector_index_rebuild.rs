// SPDX-License-Identifier: BUSL-1.1

//! Crash-recovery backstop: rebuild every HNSW vector index by re-indexing
//! all documents from the durable redb `sparse` store.
//!
//! The only other rebuild source is WAL replay, and the WAL is not
//! crash-durable — a `kill -9` before the group-commit flush loses the
//! `VectorParams` and `Put` records, so on reopen the HNSW would be empty
//! even though the documents themselves survived in redb. This pass scans
//! the durable store and re-runs the live vector-indexing side-effect for
//! every document, so vector search survives a hard crash.
//!
//! It is idempotent — `apply_point_put_vector_indexes` removes-then-inserts
//! per surrogate — so it safely overlays whatever the vector checkpoint plus
//! WAL replay already restored, never double-indexing a surrogate.

use crate::data::executor::handlers::point::apply_put::VectorIndexPutParams;

use super::state::CoreLoop;

impl CoreLoop {
    /// Re-index every document of each seeded vector-index collection from
    /// the durable `sparse` store into the HNSW. Run after WAL replay so it
    /// overlays (rather than races) the replayed state.
    pub fn rebuild_vector_indexes_from_store(
        &mut self,
        entries: &[nodedb_types::StoredVectorIndexParams],
    ) {
        use std::collections::HashSet;

        // One scan per (db, tenant, collection): `apply_point_put_vector_indexes`
        // re-indexes ALL of the document's vector fields, so multiple field
        // indexes on one collection need only a single scan. `database_id`
        // comes from the durable entry (0 = DEFAULT for pre-existing entries,
        // matching the historical behavior of this path).
        let mut seen: HashSet<(u64, u64, String)> = HashSet::new();
        let mut targets: Vec<(u64, u64, String)> = Vec::new();
        for e in entries {
            let key = (e.database_id, e.tenant_id, e.collection.clone());
            if seen.insert(key.clone()) {
                targets.push(key);
            }
        }

        for (db, tenant_id, collection) in targets {
            // `entries` comes from the `CREATE VECTOR INDEX` param seed, so
            // every target here is a classic collection with a vector index
            // over a document field, and `apply_point_put_vector_indexes`
            // extracts that field out of the document body itself. A
            // vector-primary collection has no vector field in its sidecar at
            // all — the vector lives only in the HNSW graph — and its
            // durability is served by `replay_direct_upsert` in
            // `wal_replay_vector_extended` instead. Normalizing a sidecar here
            // would rebuild nothing extra and would corrupt the classic rebuild
            // this serves, so that encoding is skipped rather than converted.
            //
            // Every other encoding IS normalized, from the collection's
            // registered kind: the forward write path hands
            // `apply_point_put_vector_indexes` the MessagePack input, so
            // feeding it a strict collection's stored Binary Tuple instead
            // finds no vector field and silently leaves those embeddings out of
            // the rebuilt index.
            let body_format = self.sparse_body_format(
                crate::types::DatabaseId::new(db),
                crate::types::TenantId::new(tenant_id),
                &collection,
            );
            if matches!(
                body_format,
                crate::data::executor::sparse_body_format::SparseBodyFormat::VectorSidecar
            ) {
                continue;
            }

            // Collect first (the scan borrows `&self.sparse`); re-index after
            // the borrow ends so `&mut self` is free for the HNSW insert.
            let mut docs: Vec<(nodedb_types::Surrogate, Vec<u8>)> = Vec::new();
            let scan = self.sparse.scan_documents_for_each(
                db,
                tenant_id,
                &collection,
                usize::MAX,
                |doc_id, value| {
                    if let Some(surrogate) =
                        crate::engine::document::store::doc_id_to_surrogate(doc_id)
                    {
                        let normalized: std::borrow::Cow<[u8]> = match &body_format {
                            crate::data::executor::sparse_body_format::SparseBodyFormat::Strict(schema) => {
                                match crate::data::executor::strict_format::binary_tuple_to_msgpack(value, schema) {
                                    Some(mp) => std::borrow::Cow::Owned(mp),
                                    None => {
                                        tracing::warn!(
                                            core = self.core_id,
                                            %collection,
                                            doc_id = %doc_id,
                                            "strict BT decode failed in vector rebuild — doc will be unsearchable"
                                        );
                                        crate::data::executor::scan_normalize::sparse_body_to_msgpack(
                                            value,
                                            body_format.as_format_ref(),
                                        )
                                    }
                                }
                            }
                            _ => crate::data::executor::scan_normalize::sparse_body_to_msgpack(
                                value,
                                body_format.as_format_ref(),
                            ),
                        };
                        docs.push((surrogate, normalized.into_owned()));
                    }
                    Ok(())
                },
            );
            if let Err(e) = scan {
                tracing::warn!(
                    core = self.core_id,
                    %collection,
                    error = %e,
                    "vector-index rebuild scan failed"
                );
                continue;
            }

            let mut rebuilt = 0usize;
            for (surrogate, value) in docs {
                let doc_id = crate::engine::document::store::surrogate_to_doc_id(surrogate);
                // Same as WAL replay: the document is already durable, so a
                // width mismatch from before the forward-path check existed is
                // reported and skipped rather than aborting the rebuild.
                let deltas = match self.apply_point_put_vector_indexes(VectorIndexPutParams {
                    database_id: db,
                    tid: tenant_id,
                    collection: &collection,
                    document_id: &doc_id,
                    surrogate,
                    value: &value,
                    wal_lsn: 0,
                }) {
                    Ok(deltas) => deltas,
                    Err(e) => {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            error = %e,
                            "vector-index rebuild rejected this document; \
                             its embeddings will not be searchable"
                        );
                        continue;
                    }
                };
                if !deltas.is_empty() {
                    rebuilt += 1;
                }
            }
            if rebuilt > 0 {
                tracing::info!(
                    core = self.core_id,
                    %collection,
                    rebuilt,
                    "rebuilt vector index from durable store"
                );
            }
        }
    }
}
