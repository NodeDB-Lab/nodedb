// SPDX-License-Identifier: BUSL-1.1

//! Tenant data purge handler.
//!
//! Deletes ALL data for a tenant across every engine and cache on this
//! Data Plane core. Called via `MetaOp::PurgeTenant`.
//!
//! Purge order: persistent storage first (sparse, edges, inverted index),
//! then in-memory state (vectors, timeseries, KV, CRDT, caches).
//! Idempotent: safe to re-run after a crash (missing data is a no-op).

use tracing::{info, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

impl CoreLoop {
    /// Purge all data for a tenant across every engine and cache.
    ///
    /// Dispatched by the Control Plane via `MetaOp::PurgeTenant` through the
    /// SPSC bridge. Deletes are atomic per-engine and idempotent (safe to retry).
    pub(in crate::data::executor) fn execute_purge_tenant(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
    ) -> Response {
        info!(core = self.core_id, tenant_id, "starting tenant purge");

        // 1. Sparse engine: documents + secondary indexes (persistent, redb).
        // A tenant lives in exactly one database, so the purge is scoped by
        // (database_id, tenant_id).
        let database_id = task.request.database_id.as_u64();
        let (mut docs, mut idxs) = match self.sparse.delete_all_for_tenant(database_id, tenant_id) {
            Ok(counts) => counts,
            Err(e) => {
                warn!(tenant_id, error = %e, "sparse purge failed");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("sparse purge: {e}"),
                    },
                );
            }
        };

        // 1b. Sparse engine: bitemporal versioned document + index history for
        // every collection of this tenant. Cleared unconditionally so a tenant
        // re-created under the same id cannot resurrect dropped versioned rows.
        match self
            .sparse
            .delete_all_versioned_for_tenant(database_id, tenant_id)
        {
            Ok((v_docs, v_idxs)) => {
                docs += v_docs;
                idxs += v_idxs;
            }
            Err(e) => {
                warn!(tenant_id, error = %e, "sparse versioned purge failed");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("sparse versioned purge: {e}"),
                    },
                );
            }
        }

        // 2. Graph engine: edges in redb. DB-scoped — a tenant's graph in
        // database A must not be purged by a purge in database B.
        let edges = match self
            .edge_store
            .purge_tenant(database_id, crate::types::TenantId::new(tenant_id))
        {
            Ok(n) => n,
            Err(e) => {
                warn!(tenant_id, error = %e, "edge store purge failed");
                0
            }
        };
        // CSR in-memory index: drop the (database, tenant) partition outright.
        // O(1) structural deletion — no key-prefix scan needed.
        self.csr.drop_partition(
            nodedb_types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tenant_id),
        );
        // Deleted-nodes tracker: drop the whole (database, tenant) bucket.
        self.deleted_nodes.remove(&(
            nodedb_types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tenant_id),
        ));

        // 3. Inverted index (fulltext): postings + doc_lengths (persistent, redb).
        let inv = match self
            .inverted
            .purge_tenant(database_id, crate::types::TenantId::new(tenant_id))
        {
            Ok(n) => n,
            Err(e) => {
                warn!(tenant_id, error = %e, "inverted index purge failed");
                0
            }
        };

        // 4. Vector engine: remove all collections for this tenant. O(1) structural
        // deletion per entry — no key-prefix scan needed with tuple keys.
        let vec_removed = {
            let tid_key = TenantId::new(tenant_id);
            let before = self.vector_collections.len();
            self.vector_collections.retain(|(_, t, _), _| *t != tid_key);
            self.vector_params.retain(|(_, t, _), _| *t != tid_key);
            self.index_configs.retain(|(_, t, _), _| *t != tid_key);
            self.ivf_indexes.retain(|(_, t, _), _| *t != tid_key);
            before - self.vector_collections.len()
        };

        // 5. Timeseries: memtables + partition registries.
        let ts_removed = {
            let tid_key = TenantId::new(tenant_id);
            let before = self.columnar_memtables.len();
            // Tenant-wide purge: a tenant lives in exactly one database, so the
            // database component of the key is ignored — match on tenant only.
            self.columnar_memtables.retain(|(_, t, _), _| *t != tid_key);
            self.columnar_memtable_mem
                .retain(|(_, t, _), _| *t != tid_key);
            self.ts_registries.retain(|(_, t, _), _| *t != tid_key);
            self.ts_max_ingested_lsn
                .retain(|(_, t, _), _| *t != tid_key);
            self.ts_last_value_caches
                .retain(|(_, t, _), _| *t != tid_key);
            before - self.columnar_memtables.len()
        };

        // 6. KV engine: remove all tenant hash tables.
        let kv_removed = self.kv_engine.purge_tenant(tenant_id);

        // 7. CRDT engine: remove tenant state.
        let crdt_before = self.crdt_engines.len();
        self.crdt_engines
            .retain(|(_, tenant), _| *tenant != TenantId::new(tenant_id));
        let crdt_removed = (crdt_before - self.crdt_engines.len()) as u32;

        // 8. Spatial indexes: remove tenant-scoped entries.
        let tid_key = TenantId::new(tenant_id);
        let spatial_removed = {
            let before = self.spatial_indexes.len();
            self.spatial_indexes.retain(|(_, t, _, _), _| *t != tid_key);
            self.spatial_doc_map
                .retain(|(_, t, _, _, _), _| *t != tid_key);
            before - self.spatial_indexes.len()
        };

        // 9. Caches: evict all tenant data.
        self.doc_cache
            .evict_tenant(task.request.database_id.as_u64(), tenant_id);
        self.aggregate_cache.retain(|(_, t, _), _| *t != tid_key);

        // 10. Doc configs: remove collection configs for this tenant.
        self.doc_configs.retain(|(_, t, _), _| *t != tid_key);

        // Chain hashes: remove for this tenant.
        self.chain_hashes.retain(|(_, t, _), _| *t != tid_key);

        // Sparse vector indexes: remove for this tenant (all databases).
        self.sparse_vector_indexes
            .retain(|(_, t, _, _), _| *t != tid_key);

        // Columnar engines + flushed segments: remove for this tenant.
        self.columnar_engines.retain(|(_, t, _), _| *t != tid_key);
        self.columnar_flushed_segments
            .retain(|(_, t, _), _| *t != tid_key);
        // Lockstep: drop the surrogate sidecar for the same keys.
        self.columnar_flushed_surrogates
            .retain(|(_, t, _), _| *t != tid_key);

        info!(
            core = self.core_id,
            tenant_id,
            docs,
            idxs,
            edges,
            inv,
            vec_removed,
            ts_removed,
            kv_removed,
            crdt_removed,
            spatial_removed,
            "tenant purge complete"
        );

        let summary = serde_json::json!({
            "tenant_id": tenant_id,
            "documents_removed": docs,
            "indexes_removed": idxs,
            "edges_removed": edges,
            "inverted_entries_removed": inv,
            "vector_collections_removed": vec_removed,
            "timeseries_collections_removed": ts_removed,
            "kv_tables_removed": kv_removed,
            "crdt_engines_removed": crdt_removed,
            "spatial_indexes_removed": spatial_removed,
        });

        match crate::data::executor::response_codec::encode_json(&summary) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(_) => self.response_ok(task),
        }
    }
}
