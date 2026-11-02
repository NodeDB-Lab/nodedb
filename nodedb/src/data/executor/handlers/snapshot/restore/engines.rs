// SPDX-License-Identifier: BUSL-1.1

//! Per-engine snapshot install helpers called by
//! `tenant_snapshot::execute_restore_tenant_snapshot` for the sparse/document,
//! vector, KV, CRDT, and timeseries engines.

use crate::data::executor::core_loop::CoreLoop;

use super::keys::database_id_from_qualified;

/// Inputs to [`CoreLoop::restore_vector_collection`].
pub(super) struct VectorInstall<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub coll_key: &'a str,
    /// `(exported node id, vector, surrogate)` of every live row.
    pub vectors: Vec<(u32, Vec<f32>, Option<nodedb_types::Surrogate>)>,
    /// The multi-vector documents of the index.
    pub multi_documents: &'a std::collections::HashSet<nodedb_types::Surrogate>,
    /// The index's live starts and ended nodes.
    pub history: crate::types::VectorIndexHistory,
    /// Whether the install replaces the local index.
    pub replace_mode: bool,
}

impl CoreLoop {
    /// Install the snapshot's document versions and index entry versions
    /// under their exported keys. Returns `(documents, indexes)` written.
    /// The first entry that fails to install fails the restore, so no
    /// follower serves a partial collection. Each installed version's
    /// system-time ordinal folds into the shared clock, so a later local
    /// write stamps above every installed version.
    pub(super) fn restore_sparse(
        &self,
        snap: &crate::types::TenantDataSnapshot,
    ) -> crate::Result<(u64, u64)> {
        for (key, value) in &snap.documents_versioned {
            self.sparse.put_versioned_document_raw(key, value)?;
            if let Some(sys_from) = crate::engine::sparse::btree_versioned::parse_sys_from(key) {
                self.observe_bitemporal_ordinal(sys_from);
            }
        }
        for (key, value) in &snap.indexes_versioned {
            self.sparse.put_versioned_index_raw(key, value)?;
        }
        Ok((
            snap.documents_versioned.len() as u64,
            snap.indexes_versioned.len() as u64,
        ))
    }

    /// Install the snapshot's vectors into their collection with their
    /// history: each live row starts at the ordinal it started at on the
    /// source, and each ended node installs with its span, so a read at any
    /// retained cut answers as on the source. A vector whose width differs
    /// from the collection's fails the restore before any of the
    /// collection's vectors lands.
    pub(super) fn restore_vector_collection(
        &mut self,
        install: VectorInstall<'_>,
    ) -> crate::Result<()> {
        let VectorInstall {
            database_id,
            tenant_id,
            coll_key,
            vectors,
            multi_documents,
            history,
            replace_mode,
        } = install;
        let dim = vectors
            .first()
            .map(|(_, data, _)| data.len())
            .or_else(|| history.ended.first().map(|node| node.vector.len()));
        let Some(dim) = dim else {
            return Ok(());
        };
        let map_key = (
            nodedb_types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tenant_id),
            coll_key.to_string(),
        );
        let starts: std::collections::HashMap<u32, i64> =
            history.live_starts.iter().copied().collect();
        // A multi-vector document installs as one multi-vector insert, so its
        // membership survives even with one vector. Every other row installs
        // as a single-vector row.
        let mut documents: std::collections::HashMap<
            nodedb_types::Surrogate,
            (i64, Vec<Vec<f32>>),
        > = std::collections::HashMap::new();
        let mut rows = Vec::new();
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
            let sys_from = starts.get(&vector_id).copied().unwrap_or(i64::MIN);
            if multi_documents.contains(&surrogate) {
                let entry = documents.entry(surrogate).or_insert((sys_from, Vec::new()));
                entry.1.push(vector);
            } else {
                rows.push((vector, surrogate, sys_from));
            }
        }
        // The installed ordinals fold into the shared clock, so a later local
        // write stamps above every installed version.
        let live_froms = rows
            .iter()
            .map(|(_, _, sys_from)| *sys_from)
            .chain(documents.values().map(|(sys_from, _)| *sys_from));
        for sys_from in live_froms {
            self.observe_bitemporal_ordinal(sys_from);
        }
        for node in &history.ended {
            self.observe_bitemporal_ordinal(node.sys_from);
            self.observe_bitemporal_ordinal(node.sys_to);
        }
        // Raft InstallSnapshot apply (`replace_mode`) must REPLACE the local
        // collection so the snapshot's vectors are not appended on top of stale
        // entries. User RESTORE (`!replace_mode`) keeps the prior insert-into-
        // existing-or-create behavior.
        if replace_mode {
            self.vector_collections.remove(&map_key);
        }
        let coll = self.ensure_vector_collection(&map_key, &map_key, dim)?;
        // Ended nodes bind no row, so they install first and never displace
        // a live one.
        for node in history.ended {
            coll.install_ended_node(node)?;
        }
        for (vector, surrogate, sys_from) in rows {
            coll.insert_at(vector, surrogate, sys_from)?;
        }
        for (document, (sys_from, group)) in documents {
            // Ends the document's earlier vectors on a restore into an
            // existing collection.
            let slices: Vec<&[f32]> = group.iter().map(Vec::as_slice).collect();
            coll.insert_multi_vector_at(&slices, document, sys_from)?;
        }
        self.train_ivf_if_ready(&map_key);
        Ok(())
    }

    /// Install one KV table's rows and their history. `snapshot_key` is the
    /// section key `"{db}:{tid}:{collection}"`, where `collection` is the
    /// db-qualified name the KV engine keys the table by (e.g. "2/orders" for
    /// database 2; the bare name for the default database). Each table
    /// installs under the database and tenant its key names, so a merged
    /// multi-tenant snapshot keeps every table with its owner. A key whose
    /// database disagrees with its qualified collection name fails the
    /// restore.
    ///
    /// Each row installs under the surrogate the snapshot carries. A Raft
    /// snapshot install restores the same cluster, so that surrogate is the
    /// row's bound identity. A row carrying `0` fails the restore.
    ///
    /// Each live row installs at the ordinal its version started at on the
    /// source, with its absolute expiry, and each ended version installs with
    /// its own interval. A read at any cut then answers on this node as it
    /// answered on the source. A row whose TTL elapsed installs too: every
    /// read hides it from its expiry on, and its reap ends it at the expiry
    /// instant, as on the source.
    pub(super) fn restore_kv_table(
        &mut self,
        snapshot_key: &str,
        table: crate::engine::kv::hash_table::KvSnapshotTable,
    ) -> crate::Result<()> {
        let now_ms = crate::engine::kv::current_ms();

        let (database_id, tenant_id, collection) =
            super::keys::parse_scoped_snapshot_key(snapshot_key)?;
        let collection = collection.as_str();
        if database_id_from_qualified(collection)? != database_id {
            return Err(crate::Error::Internal {
                detail: format!(
                    "restore: KV table key '{snapshot_key}' names database {database_id}, \
                     but its collection is qualified for another database"
                ),
            });
        }
        let crate::engine::kv::hash_table::KvSnapshotTable { rows, history } = table;
        // Rows first: a key's history installs beside its live version.
        for row in rows {
            self.kv_engine.restore_live_row(
                crate::engine::kv::KvPutParams {
                    database_id,
                    tenant_id,
                    collection,
                    key: &row.key,
                    value: &row.value,
                    // Unused by the absolute-expiry path.
                    ttl_ms: 0,
                    now_ms,
                    surrogate: nodedb_types::Surrogate::new(row.surrogate),
                },
                row.expire_at_ms,
                row.sys_from,
            )?;
        }
        for key_history in history {
            let versions = key_history
                .versions
                .into_iter()
                .map(crate::engine::kv::hash_table::KvSnapshotVersion::into_history)
                .collect();
            self.kv_engine.restore_key_history(
                database_id,
                tenant_id,
                collection,
                &key_history.key,
                versions,
            )?;
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

        // Key: "{database_id}:{tenant_id}:{collection}". Any other shape fails.
        let (database_id, tenant_id, collection) = super::keys::parse_scoped_snapshot_key(key)?;

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
        // The catalog the snapshot carried is in memory already
        // (`restore_ts_series_catalog`). The flush below writes it again
        // only when the memtable holds rows.
        self.persist_ts_series_catalog(&map_key)?;

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

    /// Put the series catalog a snapshot carried in place, before any row
    /// of its collection is restored, so every key resolves to the id the
    /// snapshot's rows carry. A Raft install (`replace`) replaces the held
    /// catalog. A restore installs into it, and refuses an id it already
    /// gives another key. The catalog is written to the collection
    /// directory.
    pub(super) fn restore_ts_series_catalog(
        &mut self,
        key: &str,
        bytes: &[u8],
        replace: bool,
    ) -> crate::Result<()> {
        let (database_id, tenant_id, collection) = super::keys::parse_scoped_snapshot_key(key)?;
        let map_key = (
            nodedb_types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tenant_id),
            collection,
        );
        let catalog =
            crate::data::executor::timeseries_checkpoint::series_catalog::decode_ts_series_catalog(
                bytes,
                std::path::Path::new(key),
            )?;
        if replace {
            self.ts_series_catalogs.insert(map_key.clone(), catalog);
        } else {
            let held = self.ts_series_catalogs.entry(map_key.clone()).or_default();
            for (id, series_key, attempt) in catalog.entries() {
                if !held.install(id, series_key.clone(), attempt) {
                    return Err(crate::Error::BadRequest {
                        detail: format!(
                            "restore: series id {id} of '{}' names another key here",
                            map_key.2
                        ),
                    });
                }
            }
        }
        let dir = crate::data::executor::handlers::timeseries::paths::ts_collection_dir(
            &self.data_dir,
            database_id,
            tenant_id,
            &map_key.2,
        );
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!("create collection dir {}: {e}", dir.display()),
        })?;
        self.persist_ts_series_catalog(&map_key)
    }
}
