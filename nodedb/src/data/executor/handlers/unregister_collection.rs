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

/// Bounded retry wrapper for collection-purge reclaim ops.
///
/// Runs the op up to `MAX_ATTEMPTS` times; returns the first `Ok(T)`
/// it sees. No sleep between attempts — the Data Plane is single-threaded
/// per core and a `sleep` here would stall every other request on the
/// shard; an immediate retry still recovers the vast majority of
/// transient fs-level errors (momentary lock, inflight fsync race).
///
/// **Fail-closed:** after exhausting all attempts this returns the last
/// error rather than swallowing it. The purge path must not warn-and-
/// continue: a partially-purged collection whose catalog row is then
/// removed leaves addressable storage rows that a re-CREATE of the same
/// name would resurrect. The caller propagates the error so the DROP
/// fails and the collection remains fully intact for the next attempt.
const L1_RECLAIM_MAX_ATTEMPTS: u32 = 3;

fn retry_reclaim<T, E, F>(
    op_name: &str,
    tenant_id: u64,
    collection: &str,
    mut op: F,
) -> crate::Result<T>
where
    F: FnMut() -> Result<T, E>,
    E: std::fmt::Display,
{
    let mut last_err: Option<String> = None;
    for attempt in 1..=L1_RECLAIM_MAX_ATTEMPTS {
        match op() {
            Ok(v) => {
                if attempt > 1 {
                    info!(
                        tenant_id,
                        collection,
                        op = op_name,
                        attempt,
                        "collection-purge reclaim recovered after transient failure"
                    );
                }
                return Ok(v);
            }
            Err(e) => {
                last_err = Some(e.to_string());
            }
        }
    }
    Err(crate::Error::Storage {
        engine: "collection-purge".into(),
        detail: format!(
            "reclaim op '{op_name}' for tenant {tenant_id} collection '{collection}' \
             failed after {L1_RECLAIM_MAX_ATTEMPTS} attempts: {}",
            last_err.as_deref().unwrap_or("(no detail)")
        ),
    })
}

impl CoreLoop {
    /// Purge collection-scoped data on this core.
    pub(in crate::data::executor) fn execute_unregister_collection(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        collection: &str,
        purge_lsn: u64,
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

        match crate::data::executor::response_codec::encode_json(&summary) {
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
    pub(in crate::data::executor) fn clear_collection_all_engines(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        preserve_collection_metadata: bool,
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

        // Doc cache: evict entries for this collection.
        self.doc_cache.evict_collection(db_raw, tid_raw, collection);

        // ── Persistent on-disk unlinks (per-engine reclaim) ──────────────────
        //
        // Engines whose state is in shared redb (document, document-
        // strict, FTS, graph edges) already reclaimed above; engines
        // with no per-collection persistent file (KV hash index,
        // CRDT — per-tenant checkpoint) are N/A.
        let mut l1 = reclaim::ReclaimStats::default();
        l1.merge(reclaim::vector::reclaim_vector_checkpoints(
            &self.data_dir,
            db_raw,
            tid_raw,
            collection,
        ));
        l1.merge(reclaim::spatial::reclaim_spatial_checkpoints(
            &self.data_dir,
            db_raw,
            tid_raw,
            collection,
        ));
        l1.merge(reclaim::sparse_vector::reclaim_sparse_vector_checkpoints(
            &self.data_dir,
            db_raw,
            tid_raw,
            collection,
        ));
        l1.merge(reclaim::timeseries::reclaim_timeseries_partitions(
            &self.data_dir,
            db_raw,
            tid_raw,
            collection,
        ));

        // Doc configs + chain hashes + aggregate cache: the collection's schema
        // and derived metadata. Preserved for clear-then-install (the snapshot
        // carries row data, not the collection definition); removed on a full DROP.
        if !preserve_collection_metadata {
            self.doc_configs
                .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &coll));
            self.chain_hashes
                .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &coll));
            self.aggregate_cache
                .retain(|(d, t, c), _| !(*d == db && *t == tid && c == &coll));
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
