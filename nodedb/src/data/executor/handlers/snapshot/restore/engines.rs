// SPDX-License-Identifier: BUSL-1.1

//! Per-engine snapshot install helpers called by
//! `tenant_snapshot::execute_restore_tenant_snapshot` for the sparse/document,
//! vector, KV, CRDT, and timeseries engines.

use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;

use super::keys::database_id_from_qualified;

impl CoreLoop {
    pub(super) fn restore_sparse(
        &self,
        _tenant_id: u64,
        documents: &[(String, Vec<u8>)],
        indexes: &[(String, Vec<u8>)],
    ) -> (u64, u64) {
        let mut docs_written = 0u64;
        for (key, value) in documents {
            if let Err(e) = self.sparse.put_raw(key, value) {
                warn!(key, error = %e, "failed to restore document");
                continue;
            }
            docs_written += 1;
        }
        let mut indexes_written = 0u64;
        for (key, value) in indexes {
            if let Err(e) = self.sparse.put_index_raw(key, value) {
                warn!(key, error = %e, "failed to restore index");
                continue;
            }
            indexes_written += 1;
        }
        (docs_written, indexes_written)
    }

    pub(super) fn restore_vector_collection(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        coll_key: &str,
        vectors: Vec<(u32, Vec<f32>, Option<nodedb_types::Surrogate>)>,
        replace_mode: bool,
    ) {
        if vectors.is_empty() {
            return;
        }
        let dim = vectors[0].1.len();
        let map_key = (
            nodedb_types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tenant_id),
            coll_key.to_string(),
        );
        let params = self
            .vector_params
            .get(&map_key)
            .cloned()
            .unwrap_or_default();
        // Raft InstallSnapshot apply (`replace_mode`) must REPLACE the local
        // collection so the snapshot's vectors are not appended on top of stale
        // entries. User RESTORE (`!replace_mode`) keeps the prior insert-into-
        // existing-or-create behavior.
        if replace_mode {
            self.vector_collections.insert(
                map_key.clone(),
                crate::engine::vector::collection::VectorCollection::new(dim, params.clone()),
            );
        }
        let coll = self.vector_collections.entry(map_key).or_insert_with(|| {
            crate::engine::vector::collection::VectorCollection::new(dim, params)
        });
        for (_, data, surrogate) in vectors {
            coll.insert_with_surrogate(data, surrogate.unwrap_or(nodedb_types::Surrogate::ZERO));
        }
    }

    pub(super) fn restore_kv_table(
        &mut self,
        tenant_id: u64,
        collection: &str,
        entries: Vec<(Vec<u8>, Vec<u8>, u64)>,
    ) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // The snapshot stores the db-qualified collection name (e.g. "2/orders"
        // for database 2; bare name for the default database). Recover the
        // database id from that prefix so the restored hash key matches the one
        // live reads compute from the same (database_id, qualified collection).
        let database_id = database_id_from_qualified(collection);
        for (key, value, expire_at) in entries {
            let ttl_ms = if expire_at > now_ms {
                expire_at - now_ms
            } else if expire_at == 0 {
                0
            } else {
                continue; // Already expired.
            };
            self.kv_engine.put(crate::engine::kv::KvPutParams {
                database_id,
                tenant_id,
                collection,
                key: &key,
                value: &value,
                ttl_ms,
                now_ms,
                surrogate: nodedb_types::Surrogate::ZERO,
            });
        }
    }

    pub(super) fn restore_crdt_state(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        bytes: &[u8],
    ) -> crate::Result<()> {
        let tid = crate::types::TenantId::new(tenant_id);
        // Lazily create the tenant engine if absent, then import into the
        // target collection's per-collection LoroDoc.
        let engine = self.get_crdt_engine(crate::types::DatabaseId::new(database_id), tid)?;
        engine.import_snapshot_bytes(collection, bytes)
    }

    /// Reconstructs a collection's installed constraint set + version from a
    /// snapshot entry. Version-fenced via `set_collection_constraints`
    /// (`>=`), so this is idempotent against later replay/reconcile.
    pub(super) fn restore_crdt_constraints(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        constraint_version: u64,
        encoded: &[Vec<u8>],
    ) -> crate::Result<()> {
        let tid = crate::types::TenantId::new(tenant_id);
        let engine = self.get_crdt_engine(crate::types::DatabaseId::new(database_id), tid)?;
        let mut constraints = Vec::with_capacity(encoded.len());
        for blob in encoded {
            let c: nodedb_crdt::Constraint =
                zerompk::from_msgpack(blob).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: e.to_string(),
                })?;
            constraints.push(c);
        }
        engine.set_collection_constraints(collection, constraint_version, constraints);
        Ok(())
    }

    pub(super) fn restore_timeseries(&mut self, key: &str, bytes: &[u8]) -> crate::Result<()> {
        use crate::engine::timeseries::columnar_memtable::{
            ColumnarMemtable, ColumnarMemtableConfig, MemtableSnapshot,
        };

        let snap: MemtableSnapshot =
            zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: e.to_string(),
            })?;

        // Parse key: "{database_id}:{tenant_id}:{collection}" (canonical).
        // Legacy 2-part key ("{tenant_id}:{collection}") and bare keys are
        // handled by `parse_timeseries_snapshot_key`.
        let (database_id, tenant_id, collection) = super::keys::parse_timeseries_snapshot_key(key);

        // Restore under this core's operator tuning, not the compiled defaults:
        // a memtable keeps the limits it was built with for its whole life, so
        // a restored collection would otherwise run budgets the operator never
        // configured until it happened to flush.
        let mt = ColumnarMemtable::from_snapshot(
            snap,
            ColumnarMemtableConfig::from_tuning(&self.ts_tuning),
        )?;

        let tid = crate::types::TenantId::new(tenant_id);
        let db_id = nodedb_types::DatabaseId::new(database_id);
        let map_key = (db_id, tid, collection.clone());
        self.columnar_memtables.insert(map_key, mt);

        // Persist the restored memtable to an on-disk segment immediately so
        // timeseries data is durable across restart. Uses a wall-clock timestamp
        // (same source as the idle-flush path in maintenance.rs) because there
        // is no Calvin epoch in a restore context.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        // Propagate the flush error directly — flush_ts_collection already
        // wraps the underlying I/O error in crate::Error::Storage with the
        // collection name included.
        self.flush_ts_collection(tid, db_id, &collection, now_ms)?;

        Ok(())
    }
}
