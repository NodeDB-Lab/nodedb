// SPDX-License-Identifier: BUSL-1.1

//! Per-engine snapshot install helpers called by
//! `tenant_snapshot::execute_restore_tenant_snapshot` for the sparse/document,
//! vector, KV, CRDT, and timeseries engines.

use crate::data::executor::core_loop::CoreLoop;

use super::keys::database_id_from_qualified;

impl CoreLoop {
    /// Install the snapshot's document rows, document versions and index
    /// entries under their exported keys. Returns `(documents, indexes)`
    /// written. The first entry that fails to install fails the restore, so
    /// no follower serves a partial collection. Each installed
    /// version's system-time ordinal folds into the shared clock, so a later
    /// local write stamps above every installed version.
    pub(super) fn restore_sparse(
        &self,
        snap: &crate::types::TenantDataSnapshot,
    ) -> crate::Result<(u64, u64)> {
        for (key, value) in &snap.documents {
            self.sparse.put_raw(key, value)?;
        }
        for (key, value) in &snap.documents_versioned {
            self.sparse.put_versioned_document_raw(key, value)?;
            if let Some(sys_from) = crate::engine::sparse::btree_versioned::parse_sys_from(key) {
                self.observe_bitemporal_ordinal(sys_from);
            }
        }
        for (key, value) in &snap.indexes {
            self.sparse.put_index_raw(key, value)?;
        }
        for (key, value) in &snap.indexes_versioned {
            self.sparse.put_versioned_index_raw(key, value)?;
        }
        Ok((
            (snap.documents.len() + snap.documents_versioned.len()) as u64,
            (snap.indexes.len() + snap.indexes_versioned.len()) as u64,
        ))
    }

    /// Install the snapshot's vectors into their collection. A vector whose
    /// width differs from the collection's fails the restore before any of
    /// the collection's vectors lands.
    pub(super) fn restore_vector_collection(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        coll_key: &str,
        vectors: Vec<(u32, Vec<f32>, Option<nodedb_types::Surrogate>)>,
        multi_documents: &std::collections::HashSet<nodedb_types::Surrogate>,
        replace_mode: bool,
    ) -> crate::Result<()> {
        let Some(dim) = vectors.first().map(|(_, data, _)| data.len()) else {
            return Ok(());
        };
        let map_key = (
            nodedb_types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tenant_id),
            coll_key.to_string(),
        );
        // A multi-vector document installs as one multi-vector insert, so its
        // membership survives even with one vector. Every other row installs
        // as a single-vector row.
        let mut documents: std::collections::HashMap<nodedb_types::Surrogate, Vec<Vec<f32>>> =
            std::collections::HashMap::new();
        let mut data = Vec::new();
        let mut surrogates = Vec::new();
        for (vector_id, vector, surrogate) in vectors {
            // Every stored vector is bound: a vector insert refuses an unbound
            // row. A snapshot vector without a surrogate fails the restore
            // before the local collection changes.
            let surrogate = surrogate.ok_or(crate::Error::Internal {
                detail: format!(
                    "restore: vector {vector_id} of '{coll_key}' carries no surrogate; every \
                     stored vector is bound"
                ),
            })?;
            if multi_documents.contains(&surrogate) {
                documents.entry(surrogate).or_default().push(vector);
            } else {
                data.push(vector);
                surrogates.push(surrogate);
            }
        }
        // Raft InstallSnapshot apply (`replace_mode`) must REPLACE the local
        // collection so the snapshot's vectors are not appended on top of stale
        // entries. User RESTORE (`!replace_mode`) keeps the prior insert-into-
        // existing-or-create behavior.
        if replace_mode {
            self.vector_collections.remove(&map_key);
        }
        let coll = self.ensure_vector_collection(&map_key, &map_key, dim)?;
        coll.insert_batch_with_surrogates(&data, &surrogates)?;
        for (document, group) in documents {
            // Replaces the document's earlier vectors on a restore into an
            // existing collection.
            coll.delete_multi_vector(document);
            let slices: Vec<&[f32]> = group.iter().map(Vec::as_slice).collect();
            coll.insert_multi_vector(&slices, document)?;
        }
        self.train_ivf_if_ready(&map_key);
        Ok(())
    }

    /// Install one KV table's rows. `snapshot_key` is the section key
    /// `"{db}:{tid}:{collection}"`, where `collection` is the db-qualified name
    /// the KV engine keys the table by (e.g. "2/orders" for database 2; the
    /// bare name for the default database). Each table installs under the
    /// database and tenant its key names, so a merged multi-tenant snapshot
    /// keeps every table with its owner. A key whose database disagrees with
    /// its qualified collection name fails the restore.
    ///
    /// Each row installs under the surrogate the snapshot carries. A Raft
    /// snapshot install restores the same cluster, so that surrogate is the
    /// row's bound identity. A row carrying `0` fails the restore.
    pub(super) fn restore_kv_table(
        &mut self,
        snapshot_key: &str,
        entries: Vec<crate::engine::kv::hash_table::KvSnapshotRow>,
    ) -> crate::Result<()> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let (database_id, tenant_id, collection) =
            super::keys::parse_scoped_snapshot_key(snapshot_key);
        let collection = collection.as_str();
        if database_id_from_qualified(collection) != database_id {
            return Err(crate::Error::Internal {
                detail: format!(
                    "restore: KV table key '{snapshot_key}' names database {database_id}, \
                     but its collection is qualified for another database"
                ),
            });
        }
        for (key, value, expire_at, surrogate) in entries {
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
                surrogate: nodedb_types::Surrogate::new(surrogate),
            })?;
        }
        Ok(())
    }

    pub(super) fn restore_crdt_state(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        bytes: &[u8],
    ) -> crate::Result<()> {
        let crs = self.crs_context();
        let tid = crate::types::TenantId::new(tenant_id);
        // Lazily create the tenant engine if absent, then import into the
        // target collection's per-collection LoroDoc.
        let engine = self.get_crdt_engine(crate::types::DatabaseId::new(database_id), tid)?;
        engine.import_snapshot_bytes(collection, bytes, &crs)
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
        // handled by `parse_scoped_snapshot_key`.
        let (database_id, tenant_id, collection) = super::keys::parse_scoped_snapshot_key(key);

        // Restore under this core's operator tuning, not the compiled defaults.
        // A memtable keeps the limits it was built with until it flushes, so
        // the restored one runs the budgets the operator configured.
        let mt = ColumnarMemtable::from_snapshot(
            snap,
            ColumnarMemtableConfig::from_tuning(&self.ts_tuning),
        )?;

        let tid = crate::types::TenantId::new(tenant_id);
        let db_id = nodedb_types::DatabaseId::new(database_id);
        let map_key = (db_id, tid, collection.clone());
        self.columnar_memtables.insert(map_key.clone(), mt);
        // The leader's schema at the snapshot's position, as its next log
        // entry meets it. The flush below skips an empty memtable, so the
        // schema is written here and survives a restart either way.
        self.persist_ts_schema(&map_key)?;

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
