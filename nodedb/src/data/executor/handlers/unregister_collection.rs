// SPDX-License-Identifier: BUSL-1.1

//! Collection-scoped purge handler.
//!
//! Reclaims storage for a single `(tenant_id, collection)` pair
//! across every engine on this Data Plane core. Dispatched by the
//! Control Plane via `MetaOp::UnregisterCollection` after the
//! metadata-raft commit of `CatalogEntry::PurgeCollection`.
//!
//! Runs on **every node** (leader and followers) — each node's Data
//! Plane reclaims its own local storage symmetrically with the
//! metadata row removal.
//!
//! Idempotent: safe to re-run after partial completion. Missing
//! in-memory state is a no-op; missing files are a no-op.
//!
//! # Current coverage
//!
//! In-memory, tuple-keyed state is reclaimed here (retain filters on
//! maps keyed by `(TenantId, collection_name)` or
//! `(TenantId, collection_name, ...)`). This covers the vector, KV,
//! timeseries, spatial, columnar, CRDT, cache, doc-config, chain-hash,
//! and sparse-vector-index maps.
//!
//! Persistent, redb-backed engines (sparse current documents, sparse
//! bitemporal versioned document/index history, inverted index, graph
//! edges) are reclaimed here via collection-scoped purge methods on each
//! store. The versioned tables must be cleared unconditionally: a
//! `bitemporal=true` collection re-CREATEd under the same name reuses the
//! same key prefix, so surviving versioned rows would resurrect its
//! dropped history.

use nodedb_types::DatabaseId;
use tracing::info;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::reclaim;
use crate::data::executor::handlers::reclaim_retry::retry_reclaim;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

/// Per-engine reclaim counts produced by clearing one collection.
///
/// Returned by [`CoreLoop::clear_collection_all_engines`] so callers that
/// surface a purge audit trail (the `UnregisterCollection` handler) can build
/// their response; callers that only need the side effect (snapshot
/// clear-then-install) ignore it.
#[derive(Default)]
pub(in crate::data::executor) struct ClearCollectionStats {
    pub docs_removed: usize,
    pub idxs_removed: usize,
    pub inv_removed: usize,
    pub edges_removed: usize,
    pub vec_removed: usize,
    pub ts_removed: u64,
    pub spatial_removed: usize,
    pub kv_removed: usize,
    pub crdt_rows_removed: usize,
    pub l1: reclaim::ReclaimStats,
}

impl CoreLoop {
    /// Purge collection-scoped data on this core.
    pub(in crate::data::executor) fn execute_unregister_collection(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        collection: &str,
        purge_lsn: u64,
        reclaim_l1_files: bool,
    ) -> Response {
        info!(
            core = self.core_id,
            tenant_id, collection, purge_lsn, "starting collection purge"
        );

        let ClearCollectionStats {
            docs_removed,
            idxs_removed,
            inv_removed,
            edges_removed,
            vec_removed,
            ts_removed,
            spatial_removed,
            kv_removed,
            crdt_rows_removed,
            l1: l1_stats,
        } = match self.clear_collection_all_engines(
            task.request.database_id,
            TenantId::new(tenant_id),
            collection,
            false,
            reclaim_l1_files,
        ) {
            Ok(stats) => stats,
            // Fail-closed: a failed engine purge must surface as an error so
            // the DROP does not finalize the catalog-row removal over storage
            // rows that survive — the resurrection hole on re-CREATE.
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Surface the L1 byte reclaim to the purge metrics. Stays in the
        // caller: the reusable clear is side-effect-only and metrics are an
        // audit concern of the unregister path.
        if let Some(metrics) = self.metrics.as_ref()
            && l1_stats.bytes_freed > 0
        {
            metrics
                .purge
                .add_bytes_reclaimed(tenant_id, "l1-mixed", "l1", l1_stats.bytes_freed);
        }

        info!(
            core = self.core_id,
            tenant_id,
            collection,
            purge_lsn,
            docs_removed,
            idxs_removed,
            inv_removed,
            kv_removed,
            crdt_rows_removed,
            vec_removed,
            ts_removed,
            spatial_removed,
            edges_removed,
            "collection purge reclaim complete"
        );

        let summary = serde_json::json!({
            "tenant_id": tenant_id,
            "collection": collection,
            "purge_lsn": purge_lsn,
            "documents_removed": docs_removed,
            "indexes_removed": idxs_removed,
            "inverted_entries_removed": inv_removed,
            "kv_tables_removed": kv_removed,
            "crdt_rows_removed": crdt_rows_removed,
            "vector_indexes_removed": vec_removed,
            "timeseries_removed": ts_removed,
            "spatial_removed": spatial_removed,
            "edges_removed": edges_removed,
            "l1_files_unlinked": l1_stats.files_unlinked,
            "l1_bytes_freed": l1_stats.bytes_freed,
        });

        match crate::data::executor::response_codec::encode_json_as_msgpack(&summary) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(_) => self.response_ok(task),
        }
    }

    /// Reclaim all per-collection state on this core across every engine.
    ///
    /// Drops the in-memory tuple-keyed maps, range-deletes the redb-backed
    /// engines, and unlinks the on-disk L1 checkpoints/partitions for one
    /// `(database, tenant, collection)`. Returns the per-engine reclaim
    /// counts; callers that don't need them (snapshot clear-then-install)
    /// can discard the result.
    ///
    /// When `preserve_collection_metadata` is true the collection's schema and
    /// derived metadata (`CollectionConfig`, bitemporal chain hashes, aggregate
    /// cache) are kept — used by clear-then-install, where the row data is
    /// cleared and reinstalled from the snapshot but the collection definition
    /// (which the data snapshot does not carry) must remain so the reinstalled
    /// rows stay readable. A full DROP passes false to remove everything.
    ///
    /// Idempotent: missing in-memory state is a no-op; missing files are a
    /// no-op. Metrics emission and audit/response building are the caller's
    /// concern.
    ///
    /// Fail-closed: any persistent-engine reclaim that fails after its bounded
    /// retries propagates as `Err`; the caller must abort the purge rather than
    /// let a partially-cleared collection have its catalog row removed.
    ///
    /// `reclaim_l1_files` gates the shared on-disk L1 unlink pass. Those paths
    /// are keyed by `(database, tenant, collection)` — not by core — so in the
    /// all-cores `UnregisterCollection` fan-out only the homing core passes
    /// `true`; other cores still evict their own per-core in-memory state but
    /// must not race `remove_dir_all`/`unlink` on the same tree. Single-core
    /// callers (snapshot restore) pass `true`.
    pub(in crate::data::executor) fn clear_collection_all_engines(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        preserve_collection_metadata: bool,
        reclaim_l1_files: bool,
    ) -> crate::Result<ClearCollectionStats> {
        let db = database_id;
        let tid = tenant_id;
        let db_raw = database_id.as_u64();
        let tid_raw = tenant_id.as_u64();
        let coll = collection.to_string();

        // ── Persistent engines (redb-backed, collection-scoped range drop) ──

        // Sparse engine: current documents + secondary indexes.
        let (mut docs_removed, mut idxs_removed) = retry_reclaim(
            "sparse.delete_all_for_collection",
            tid_raw,
            collection,
            || {
                self.sparse
                    .delete_all_for_collection(db_raw, tid_raw, collection)
            },
        )?;

        // Sparse engine: bitemporal versioned document + index history. Cleared
        // unconditionally — a `bitemporal=true` collection reuses the same key
        // prefix on re-CREATE, so surviving versioned rows would resurrect the
        // dropped collection's history (and yield a corrupt current row).
        let (v_docs, v_idxs) = retry_reclaim(
            "sparse.delete_all_versioned_for_collection",
            tid_raw,
            collection,
            || {
                self.sparse
                    .delete_all_versioned_for_collection(db_raw, tid_raw, collection)
            },
        )?;
        docs_removed += v_docs;
        idxs_removed += v_idxs;

        // Inverted index: postings + doc_lengths + stats + segments.
        let inv_removed = retry_reclaim("inverted.purge_collection", tid_raw, collection, || {
            self.inverted.purge_collection(db_raw, tid, collection)
        })?;

        // Graph edge store: remove all edges scoped to this (database, collection).
        let edges_removed =
            retry_reclaim("edge_store.purge_collection", tid_raw, collection, || {
                self.edge_store.purge_collection(db_raw, tid, collection)
            })?;
        // The CSR in-memory index is collection-agnostic. Stale edges will
        // be absent from the next CSR rebuild (which reads from EdgeStore).
        self.csr.drop_collection(db, tid, collection);

        // ── In-memory, tuple-keyed state (reclaimable today) ─────────────────

        // Vector engine. Field-vector indexes are keyed "{coll}:{field}" in the
        // String component, so eviction must drop the plain collection key AND every
        // field variant — not just the bare "{coll}" key (which would leak the
        // field-keyed entries in memory on drop).
        let vec_removed = {
            let coll_prefix = format!("{coll}:");
            let keep = |c: &String| !(c == &coll || c.starts_with(&coll_prefix));
            let before = self.vector_collections.len();
            self.vector_collections
                .retain(|(d, t, c), _| !(*d == db && *t == tid) || keep(c));
            let removed = before - self.vector_collections.len();
            self.vector_params
                .retain(|(d, t, c), _| !(*d == db && *t == tid) || keep(c));
            self.index_configs
                .retain(|(d, t, c), _| !(*d == db && *t == tid) || keep(c));
            self.ivf_indexes
                .retain(|(d, t, c), _| !(*d == db && *t == tid) || keep(c));
            removed
        };

        // Timeseries engine.
        let ts_removed = {
            let key = (db, tid, coll.clone());
            let mut r = 0u64;
            if self.columnar_memtables.remove(&key).is_some() {
                r += 1;
            }
            self.columnar_memtable_mem.remove(&key);
            self.ts_registries.remove(&key);
            self.ts_max_ingested_lsn.remove(&key);
            self.ts_last_value_caches.remove(&key);
            self.ts_series_catalogs.remove(&key);
            self.ts_truncate_floors.remove(&key);
            r
        };

        // Spatial indexes.
        let spatial_removed = {
            let before = self.spatial_indexes.len();
            self.spatial_indexes
                .retain(|(d, t, c, _), _| !(*d == db && *t == tid && c == &coll));
            self.spatial_doc_map
                .retain(|(d, t, c, _, _), _| !(*d == db && *t == tid && c == &coll));
            before - self.spatial_indexes.len()
        };

        // Columnar engine state (per-core, tuple-keyed).
        self.columnar_engines
            .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &coll));
        self.columnar_flushed_segments
            .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &coll));
        // Lockstep: drop the surrogate sidecar for the same keys.
        self.columnar_flushed_surrogates
            .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &coll));

        // Sparse vector indexes (tuple key: database, tenant, collection, field).
        self.sparse_vector_indexes
            .retain(|(d, t, c, _), _| !(*d == db && *t == tid && c == &coll));

        // KV engine: drop this collection's hash table + indexes.
        let kv_removed = self.kv_engine.purge_collection(db_raw, tid_raw, collection);

        // CRDT engine: clear rows for this collection in the tenant state.
        let crdt_rows_removed = match self.crdt_engines.get_mut(&(db, tid)) {
            Some(engine) => retry_reclaim("crdt.purge_collection", tid_raw, collection, || {
                engine.purge_collection(collection)
            })?,
            None => 0,
        };
        // Stored dead-letter entries go with it, or the next engine created
        // for this tenant restores them.
        retry_reclaim("crdt.dead_letters", tid_raw, collection, || {
            self.sparse
                .delete_crdt_dead_letters_for_collection(db_raw, tid_raw, collection)
        })?;

        // Doc cache: evict entries for this collection.
        self.doc_cache.evict_collection(db_raw, tid_raw, collection);

        // ── Persistent on-disk unlinks (per-engine reclaim) ──────────────────
        //
        // Engines whose state is in shared redb (document, document-
        // strict, FTS, graph edges) already reclaimed above; the KV
        // hash index has no per-collection persistent file at all.
        // The CRDT checkpoint DOES write one file per collection, so
        // it is reclaimed here like the others — clearing the tenant's
        // rows above does not remove the file, and a file left in the
        // live generation re-imports the dropped collection's Loro
        // state at every boot.
        // Shared on-disk L1 files are keyed by (database, tenant, collection),
        // not by core. Only the homing core reclaims them so concurrent cores
        // in the all-cores fan-out cannot race `remove_dir_all`/`unlink` on the
        // same tree and turn a benign concurrent-removal errno into a fatal
        // barrier failure.
        let mut l1 = reclaim::ReclaimStats::default();
        if reclaim_l1_files {
            l1.merge(retry_reclaim(
                "vector checkpoints",
                tid_raw,
                collection,
                || {
                    reclaim::vector::reclaim_vector_checkpoints(
                        &self.data_dir,
                        db_raw,
                        tid_raw,
                        collection,
                    )
                },
            )?);
            l1.merge(retry_reclaim(
                "spatial checkpoints",
                tid_raw,
                collection,
                || {
                    reclaim::spatial::reclaim_spatial_checkpoints(
                        &self.data_dir,
                        db_raw,
                        tid_raw,
                        collection,
                    )
                },
            )?);
            l1.merge(retry_reclaim(
                "sparse-vector checkpoints",
                tid_raw,
                collection,
                || {
                    reclaim::sparse_vector::reclaim_sparse_vector_checkpoints(
                        &self.data_dir,
                        db_raw,
                        tid_raw,
                        collection,
                    )
                },
            )?);
            l1.merge(retry_reclaim(
                "CRDT checkpoints",
                tid_raw,
                collection,
                || {
                    reclaim::crdt::reclaim_crdt_checkpoints(
                        &self.data_dir,
                        db_raw,
                        tid_raw,
                        collection,
                    )
                },
            )?);
            l1.merge(retry_reclaim(
                "timeseries partitions",
                tid_raw,
                collection,
                || {
                    reclaim::timeseries::reclaim_timeseries_partitions(
                        &self.data_dir,
                        db_raw,
                        tid_raw,
                        collection,
                    )
                },
            )?);
        }

        // Doc configs + chain hashes + derived-result cache: the collection's
        // schema and metadata. Preserved for clear-then-install (the snapshot
        // carries row data, not the collection definition); removed on a full
        // DROP. Cache entries encode query shape after a NUL-terminated
        // collection prefix, so lifecycle eviction must use the canonical
        // invalidation API rather than compare the encoded key to `coll`.
        if !preserve_collection_metadata {
            self.doc_configs
                .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &coll));
            // In memory AND on disk, in lockstep: a persisted head left behind
            // by a DROP is rehydrated at the next restart, so a collection
            // recreated under the same name would resume the dropped
            // collection's chain instead of starting at genesis. The
            // clear-then-install path (`preserve_collection_metadata`) keeps
            // both, so the map and the table never disagree.
            self.chain_hashes
                .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &coll));
            self.sparse.delete_chain_head(db_raw, tid_raw, collection)?;
            self.invalidate_aggregate_cache_for_collection(db_raw, tid_raw, collection);
        }

        Ok(ClearCollectionStats {
            docs_removed,
            idxs_removed,
            inv_removed,
            edges_removed,
            vec_removed,
            ts_removed,
            spatial_removed,
            kv_removed,
            crdt_rows_removed,
            l1,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_bridge::buffer::RingBuffer;

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};

    fn open_core() -> (CoreLoop, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_request_tx, request_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (response_tx, _response_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            request_rx,
            response_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core loop");
        (core, dir)
    }

    #[test]
    fn full_clear_removes_encoded_derived_cache_entries() {
        let (mut core, _dir) = open_core();
        let database = DatabaseId::DEFAULT;
        let tenant = TenantId::new(1);

        core.aggregate_cache.insert(
            (database, tenant, "products\0count(*)".to_string()),
            crate::data::executor::handlers::aggregate::AggregateCacheEntry {
                kv_epoch: 0,
                payload: vec![1],
            },
        );
        core.aggregate_cache.insert(
            (database, tenant, "products\0facet:brand".to_string()),
            crate::data::executor::handlers::aggregate::AggregateCacheEntry {
                kv_epoch: 0,
                payload: vec![2],
            },
        );
        core.aggregate_cache.insert(
            (database, tenant, "other\0count(*)".to_string()),
            crate::data::executor::handlers::aggregate::AggregateCacheEntry {
                kv_epoch: 0,
                payload: vec![3],
            },
        );

        core.clear_collection_all_engines(database, tenant, "products", false, true)
            .expect("full collection clear");

        assert_eq!(core.aggregate_cache.len(), 1);
        assert!(
            core.aggregate_cache
                .contains_key(&(database, tenant, "other\0count(*)".to_string())),
            "collection-scoped clear must preserve another collection's cache"
        );
    }
}
