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
        tenant_id: u32,
    ) -> Response {
        info!(core = self.core_id, tenant_id, "starting tenant purge");
        let prefix = format!("{tenant_id}:");

        // 1. Sparse engine: documents + secondary indexes (persistent, redb).
        let (docs, idxs) = match self.sparse.delete_all_for_tenant(tenant_id) {
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

        // 2. Graph engine: edges in redb.
        let edges = match self
            .edge_store
            .purge_tenant(crate::types::TenantId::new(tenant_id))
        {
            Ok(n) => n,
            Err(e) => {
                warn!(tenant_id, error = %e, "edge store purge failed");
                0
            }
        };
        // CSR in-memory index: drop the tenant's partition outright. O(1)
        // structural deletion — no key-prefix scan needed.
        self.csr
            .drop_partition(crate::types::TenantId::new(tenant_id));
        // Deleted-nodes tracker: drop the whole tenant bucket.
        self.deleted_nodes
            .remove(&crate::types::TenantId::new(tenant_id));

        // 3. Inverted index (fulltext): postings + doc_lengths (persistent, redb).
        let inv = match self.inverted.purge_tenant(tenant_id) {
            Ok(n) => n,
            Err(e) => {
                warn!(tenant_id, error = %e, "inverted index purge failed");
                0
            }
        };

        // 4. Vector engine: remove all collections keyed by "{tid}:*".
        let vec_removed = {
            let keys: Vec<String> = self
                .vector_collections
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();
            let count = keys.len();
            for key in keys {
                self.vector_collections.remove(&key);
            }
            count
        };

        // 5. Timeseries: memtables + partition registries.
        let ts_removed = {
            let mt_keys: Vec<String> = self
                .columnar_memtables
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();
            let count = mt_keys.len();
            for key in mt_keys {
                self.columnar_memtables.remove(&key);
            }
            let reg_keys: Vec<String> = self
                .ts_registries
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();
            for key in reg_keys {
                self.ts_registries.remove(&key);
            }
            count
        };

        // 6. KV engine: remove all tenant hash tables.
        let kv_removed = self.kv_engine.purge_tenant(tenant_id);

        // 7. CRDT engine: remove tenant state.
        let crdt_removed = u32::from(
            self.crdt_engines
                .remove(&TenantId::new(tenant_id))
                .is_some(),
        );

        // 8. Spatial indexes: remove tenant-scoped entries.
        let spatial_removed = {
            let keys: Vec<String> = self
                .spatial_indexes
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();
            let count = keys.len();
            for key in keys {
                self.spatial_indexes.remove(&key);
            }
            self.spatial_doc_map
                .retain(|(coll, _), _| !coll.starts_with(&prefix));
            count
        };

        // 9. Caches: evict all tenant data.
        self.doc_cache.evict_tenant(tenant_id);
        self.aggregate_cache.retain(|k, _| !k.starts_with(&prefix));

        // 10. Doc configs: remove collection configs for this tenant.
        self.doc_configs.retain(|k, _| !k.starts_with(&prefix));

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
