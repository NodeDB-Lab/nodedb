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
    ///
    /// `Err` when the store does not read or an insert fails for a reason
    /// other than the document's data. The index then misses rows the store
    /// holds, so the core must not serve it as complete. A document whose
    /// vector does not fit its index is reported and skipped: no rebuild can
    /// index it.
    pub fn rebuild_vector_indexes_from_store(
        &mut self,
        entries: &[nodedb_types::StoredVectorIndexParams],
    ) -> crate::Result<()> {
        use std::collections::HashSet;
        // One scan per (database, tenant, collection):
        // `apply_point_put_vector_indexes` re-indexes ALL of the document's
        // vector fields, so multiple field indexes on one collection need only
        // a single scan. The database comes from the stored entry — an index
        // created outside the default database rebuilds under its own.
        let mut seen: HashSet<(u64, u64, String)> = HashSet::new();
        let mut targets: Vec<(u64, u64, String)> = Vec::new();
        for e in entries {
            if seen.insert((e.database_id, e.tenant_id, e.collection.clone())) {
                targets.push((e.database_id, e.tenant_id, e.collection.clone()));
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
            // Each row at its current version, so a bitemporal collection's
            // vectors are rebuilt too.
            let scan = self.for_each_current_row(db, tenant_id, &collection, |doc_id, value| {
                let normalized = crate::data::executor::scan_normalize::sparse_body_to_msgpack(
                    value,
                    body_format.as_format_ref(),
                );
                docs.push((doc_id.surrogate(), normalized.into_owned()));
                Ok(std::ops::ControlFlow::Continue(()))
            });
            scan.map_err(|e| rebuild_error(self.core_id, db, tenant_id, &collection, e))?;

            let mut rebuilt = 0usize;
            for (surrogate, value) in docs {
                let storage_key =
                    crate::engine::document::store::StorageKey::for_surrogate(surrogate);
                // The rebuilt node starts where the row's current version
                // starts, so a read at an earlier cut does not see it.
                let sys_from = match self.sparse.versioned_get_current_with_sys_from(
                    db,
                    tenant_id,
                    &collection,
                    &storage_key,
                ) {
                    Ok(Some((sys_from, _))) => sys_from,
                    Ok(None) => continue,
                    Err(e) => {
                        return Err(rebuild_error(self.core_id, db, tenant_id, &collection, e));
                    }
                };
                // Same as WAL replay: the document is already durable, so a
                // width mismatch from before the forward-path check existed is
                // reported and skipped rather than aborting the rebuild. The
                // rebuild re-derives durable rows and has no write to abandon,
                // so the undo entries go unused.
                let inserted = match self.apply_point_put_vector_indexes(
                    VectorIndexPutParams {
                        database_id: db,
                        tid: tenant_id,
                        collection: &collection,
                        storage_key,
                        value: &value,
                        wal_lsn: 0,
                        sys_from,
                        // The rebuild indexes each stored row once, into an
                        // index that holds none of its nodes.
                        replaces_row: false,
                    },
                    &mut Vec::new(),
                ) {
                    Ok(inserted) => inserted,
                    Err(e @ crate::Error::DataException { .. }) => {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            error = %e,
                            "vector-index rebuild rejected this document; \
                             its embeddings will not be searchable"
                        );
                        continue;
                    }
                    // Every other error says nothing about the document, so
                    // skipping it publishes an index that misses a good row.
                    Err(e) => {
                        return Err(rebuild_error(self.core_id, db, tenant_id, &collection, e));
                    }
                };
                if inserted > 0 {
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
        Ok(())
    }
}

/// The boot error for a vector-index rebuild that cannot finish.
fn rebuild_error(
    core_id: usize,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    cause: crate::Error,
) -> crate::Error {
    crate::Error::Storage {
        engine: "vector".into(),
        detail: format!(
            "core {core_id}: vector index rebuild of '{collection}' \
             (database {database_id}, tenant {tenant_id}) did not finish: {cause}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::document::store::StorageKey;

    const COLLECTION: &str = "rebuild_vec";

    fn entry() -> nodedb_types::StoredVectorIndexParams {
        nodedb_types::StoredVectorIndexParams {
            database_id: crate::types::DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            collection: COLLECTION.to_string(),
            field_name: "embedding".to_string(),
            dim: 3,
            metric: "cosine".to_string(),
            m: 0,
            ef_construction: 0,
            index_type: String::new(),
            pq_m: 0,
            ivf_cells: 0,
            ivf_nprobe: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    /// A store row that does not read fails the rebuild. A rebuild that
    /// continues publishes an index without that row, and boot reports success.
    #[test]
    fn a_row_that_does_not_read_fails_the_rebuild() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        let db = crate::types::DatabaseId::DEFAULT.as_u64();
        let key = StorageKey::for_surrogate(nodedb_types::Surrogate::new(9));
        core.sparse
            .corrupt_current_version_for_test(db, 1, COLLECTION, &key);

        let error = core
            .rebuild_vector_indexes_from_store(&[entry()])
            .expect_err("an unreadable row fails the rebuild");

        assert!(
            matches!(error, crate::Error::Storage { ref detail, .. } if detail.contains(COLLECTION)),
            "{error}"
        );
    }

    #[test]
    fn an_empty_store_rebuilds_without_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        core.rebuild_vector_indexes_from_store(&[entry()])
            .expect("nothing to read");
    }
}
