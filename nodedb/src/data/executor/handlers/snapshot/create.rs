//! Tenant snapshot creation: export Data Plane state for all engines.

use tracing::{info, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantDataSnapshot;

impl CoreLoop {
    /// Create a snapshot of a tenant's data across ALL engines.
    ///
    /// Returns MessagePack-serialized `TenantDataSnapshot`.
    pub(in crate::data::executor) fn execute_create_tenant_snapshot(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u32,
    ) -> Response {
        info!(
            core = self.core_id,
            tenant_id, "creating full tenant snapshot"
        );
        let mut snapshot = TenantDataSnapshot::default();
        // Used by out-of-scope maps (timeseries) that still use string-prefix keys.
        let _prefix = format!("{tenant_id}:");

        // 1. Sparse engine: documents + indexes.
        match self.sparse.scan_all_for_tenant(tenant_id) {
            Ok(docs) => snapshot.documents = docs,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("snapshot: sparse doc scan failed: {e}"),
                    },
                );
            }
        }
        match self.sparse.scan_indexes_for_tenant(tenant_id) {
            Ok(idx) => snapshot.indexes = idx,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("snapshot: sparse index scan failed: {e}"),
                    },
                );
            }
        }

        // 2. Graph edges: scan edge_store by tenant prefix.
        match self
            .edge_store
            .scan_edges_for_tenant(crate::types::TenantId::new(tenant_id))
        {
            Ok(edges) => snapshot.edges = edges,
            Err(e) => warn!(tenant_id, error = %e, "snapshot: edge scan failed, skipping"),
        }

        // 3. Vector collections: export raw vectors + doc_id_map.
        // The snapshot format stores keys as `"{tid}:{coll_key}"` strings for
        // disk/wire compatibility — convert the tuple key at the boundary.
        let tid_obj = crate::types::TenantId::new(tenant_id);
        for (key, collection) in &self.vector_collections {
            if key.0 != tid_obj {
                continue;
            }
            let vectors = collection.export_snapshot();
            let key_str = format!("{tenant_id}:{}", key.1);
            match zerompk::to_msgpack_vec(&vectors) {
                Ok(bytes) => snapshot.vectors.push((key_str, bytes)),
                Err(e) => warn!(key = &key.1, error = %e, "snapshot: vector serialization failed"),
            }
        }

        // 4. KV tables: export all entries per tenant table.
        for (&hash, table) in &self.kv_engine.tables {
            let Some(&tid) = self.kv_engine.hash_to_tenant.get(&hash) else {
                continue;
            };
            if tid != tenant_id {
                continue;
            }
            let collection_name = self
                .kv_engine
                .hash_to_collection
                .get(&hash)
                .cloned()
                .unwrap_or_else(|| hash.to_string());
            let entries = table.export_entries();
            match zerompk::to_msgpack_vec(&entries) {
                Ok(bytes) => snapshot.kv_tables.push((collection_name, bytes)),
                Err(e) => warn!(hash, error = %e, "snapshot: kv serialization failed"),
            }
        }

        // 5. CRDT state: Loro export.
        if let Some(crdt) = self
            .crdt_engines
            .get(&crate::types::TenantId::new(tenant_id))
        {
            match crdt.export_snapshot_bytes() {
                Ok(bytes) => snapshot.crdt_state.push((tenant_id.to_string(), bytes)),
                Err(e) => warn!(tenant_id, error = %e, "snapshot: crdt export failed"),
            }
        }

        // 6. Timeseries memtables: serialize column data.
        // Snapshot format preserves "{tenant_id}:{collection}" string keys.
        let tid_id = crate::types::TenantId::new(tenant_id);
        for ((t, coll), mt) in &self.columnar_memtables {
            if *t != tid_id {
                continue;
            }
            let key_str = format!("{tenant_id}:{coll}");
            match zerompk::to_msgpack_vec(&mt.export_snapshot()) {
                Ok(bytes) => snapshot.timeseries.push((key_str, bytes)),
                Err(e) => {
                    let key = &key_str;
                    warn!(key, error = %e, "snapshot: timeseries serialization failed");
                }
            }
        }

        info!(
            tenant_id,
            documents = snapshot.documents.len(),
            indexes = snapshot.indexes.len(),
            edges = snapshot.edges.len(),
            vectors = snapshot.vectors.len(),
            kv_tables = snapshot.kv_tables.len(),
            crdt = snapshot.crdt_state.len(),
            timeseries = snapshot.timeseries.len(),
            "full tenant snapshot created"
        );

        match zerompk::to_msgpack_vec(&snapshot) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("snapshot serialization failed: {e}"),
                },
            ),
        }
    }
}
