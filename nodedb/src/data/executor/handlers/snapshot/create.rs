// SPDX-License-Identifier: BUSL-1.1

//! Tenant snapshot creation: export Data Plane state for all engines.

use tracing::{info, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantDataSnapshot, TsFlushedCollectionBlob, TsFlushedPartitionBlob};

impl CoreLoop {
    /// Create a snapshot of a tenant's data across ALL engines.
    ///
    /// Returns MessagePack-serialized `TenantDataSnapshot`.
    pub(in crate::data::executor) fn execute_create_tenant_snapshot(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
    ) -> Response {
        info!(
            core = self.core_id,
            tenant_id, "creating full tenant snapshot"
        );
        let mut snapshot = TenantDataSnapshot::default();

        // 1. Sparse engine: documents + indexes. Keys carry the leading
        // `{database_id}:` component; restore re-inserts them verbatim.
        let database_id = task.request.database_id.as_u64();
        match self.sparse.scan_all_for_tenant(database_id, tenant_id) {
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
        match self.sparse.scan_indexes_for_tenant(database_id, tenant_id) {
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
            .scan_edges_for_tenant(database_id, crate::types::TenantId::new(tenant_id))
        {
            Ok(edges) => snapshot.edges = edges,
            Err(e) => warn!(tenant_id, error = %e, "snapshot: edge scan failed, skipping"),
        }

        // 3. Vector collections: export raw vectors + doc_id_map.
        // The snapshot format stores keys as `"{db}:{tid}:{coll_key}"` strings
        // for disk/wire compatibility — convert the tuple key at the boundary.
        let tid_obj = crate::types::TenantId::new(tenant_id);
        for (key, collection) in &self.vector_collections {
            if key.1 != tid_obj {
                continue;
            }
            let vectors = collection.export_snapshot();
            let key_str = format!("{}:{}:{}", key.0.as_u64(), key.1.as_u64(), key.2);
            match zerompk::to_msgpack_vec(&vectors) {
                Ok(bytes) => snapshot.vectors.push((key_str, bytes)),
                Err(e) => warn!(key = &key.2, error = %e, "snapshot: vector serialization failed"),
            }
        }

        // 3b. Vector params: export HnswParams per collection.
        for (key, params) in &self.vector_params {
            if key.1 != tid_obj {
                continue;
            }
            let key_str = format!("{}:{}:{}", key.0.as_u64(), key.1.as_u64(), key.2);
            match zerompk::to_msgpack_vec(params) {
                Ok(bytes) => snapshot.vector_params.push((key_str, bytes)),
                Err(e) => {
                    warn!(key = &key.2, error = %e, "snapshot: vector_params serialization failed")
                }
            }
        }

        // 3c. Index configs: export IndexConfig per collection.
        for (key, cfg) in &self.index_configs {
            if key.1 != tid_obj {
                continue;
            }
            let key_str = format!("{}:{}:{}", key.0.as_u64(), key.1.as_u64(), key.2);
            match zerompk::to_msgpack_vec(cfg) {
                Ok(bytes) => snapshot.index_configs.push((key_str, bytes)),
                Err(e) => {
                    warn!(key = &key.2, error = %e, "snapshot: index_configs serialization failed")
                }
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

        // 5. CRDT state: one Loro export per collection. Each (tenant,
        // collection) owns its own doc; entries are carried tenant-explicit and
        // collection-tagged so the per-group Raft snapshot builder routes each
        // by its single collection's vshard.
        if let Some(crdt) = self.crdt_engines.get(&(task.request.database_id, tid_obj)) {
            match crdt.export_all_snapshots() {
                Ok(per_collection) => {
                    for (collection, bytes) in per_collection {
                        snapshot.crdt_state.push((
                            task.request.database_id.as_u64(),
                            tenant_id,
                            collection,
                            bytes,
                        ));
                    }
                }
                Err(e) => warn!(tenant_id, error = %e, "snapshot: crdt export failed"),
            }

            // 5b. CRDT constraint state: capture the installed constraint set
            // + version per collection so a snapshot-installed follower
            // reconstructs its validator instead of coming up empty and
            // retry-fencing every peer delta on constrained collections.
            for collection in crdt.collections_with_constraints() {
                let version = crdt.installed_constraint_version(&collection);
                if version == 0 {
                    continue;
                }
                let constraints = crdt.constraints_for_collection(&collection);
                let mut encoded = Vec::with_capacity(constraints.len());
                let mut failed = false;
                for constraint in &constraints {
                    match zerompk::to_msgpack_vec(constraint) {
                        Ok(bytes) => encoded.push(bytes),
                        Err(e) => {
                            warn!(
                                tenant_id,
                                collection,
                                error = %e,
                                "snapshot: crdt constraint serialization failed"
                            );
                            failed = true;
                            break;
                        }
                    }
                }
                if failed || encoded.is_empty() {
                    continue;
                }
                snapshot.crdt_constraints.push((
                    task.request.database_id.as_u64(),
                    tenant_id,
                    collection,
                    version,
                    encoded,
                ));
            }
        }

        // 6. Timeseries memtables: serialize column data.
        // Snapshot format encodes "{database_id}:{tenant_id}:{collection}" keys.
        let tid_id = crate::types::TenantId::new(tenant_id);
        for ((d, t, coll), mt) in &self.columnar_memtables {
            if *t != tid_id {
                continue;
            }
            let key_str = format!("{}:{}:{}", d.as_u64(), t.as_u64(), coll);
            match zerompk::to_msgpack_vec(&mt.export_snapshot()) {
                Ok(bytes) => snapshot.timeseries.push((key_str, bytes)),
                Err(e) => {
                    let key = &key_str;
                    warn!(key, error = %e, "snapshot: timeseries serialization failed");
                }
            }
        }

        // 7. Flushed timeseries segments: capture all on-disk partition
        // directories for this tenant from `ts_registries`.
        match self.capture_flushed_ts_segments(database_id, tid_id) {
            Ok(blobs) => snapshot.flushed_ts_segments = blobs,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("snapshot: flushed ts segment capture failed: {e}"),
                    },
                );
            }
        }

        // 8. Plain-columnar engines: export MutationEngine state for this tenant.
        match self.capture_columnar_engines(database_id, tid_id) {
            Ok(entries) => snapshot.columnar_engines = entries,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("snapshot: columnar engine capture failed: {e}"),
                    },
                );
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
            crdt_constraints = snapshot.crdt_constraints.len(),
            timeseries = snapshot.timeseries.len(),
            flushed_ts_collections = snapshot.flushed_ts_segments.len(),
            columnar_engines = snapshot.columnar_engines.len(),
            vector_params = snapshot.vector_params.len(),
            index_configs = snapshot.index_configs.len(),
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

    /// Capture all flushed on-disk timeseries segments for one tenant.
    ///
    /// Iterates `ts_registries` entries belonging to `tid`, reads every file
    /// in each partition directory from disk, and returns the packed blobs.
    /// Returns `Err` on any I/O failure so the snapshot aborts cleanly rather
    /// than silently omitting data.
    fn capture_flushed_ts_segments(
        &self,
        database_id: u64,
        tid: crate::types::TenantId,
    ) -> crate::Result<Vec<TsFlushedCollectionBlob>> {
        let mut result = Vec::new();

        for ((reg_db, reg_tid, collection), registry) in &self.ts_registries {
            if *reg_tid != tid || reg_db.as_u64() != database_id {
                continue;
            }

            let segment_dir = super::super::timeseries::paths::ts_collection_dir(
                &self.data_dir,
                reg_db.as_u64(),
                reg_tid.as_u64(),
                collection,
            );

            let mut partition_blobs = Vec::new();

            for (_start_ts, entry) in registry.iter() {
                let partition_dir = segment_dir.join(&entry.dir_name);

                // Serialize PartitionMeta to msgpack bytes for the wire blob.
                let meta_bytes = zerompk::to_msgpack_vec(&entry.meta).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!(
                            "serialize PartitionMeta for {}/{}: {e}",
                            collection, entry.dir_name
                        ),
                    }
                })?;

                // Read all files in the partition directory.
                let mut files: Vec<(String, Vec<u8>)> = Vec::new();
                let read_dir = std::fs::read_dir(&partition_dir)?;
                for dir_entry in read_dir {
                    let dir_entry = dir_entry?;
                    let file_name = dir_entry.file_name();
                    let Some(name_str) = file_name.to_str() else {
                        warn!(
                            partition = &entry.dir_name,
                            "skipping non-UTF8 filename in partition dir during snapshot"
                        );
                        continue;
                    };
                    if !dir_entry.file_type()?.is_file() {
                        continue;
                    }
                    let bytes = std::fs::read(dir_entry.path())?;
                    files.push((name_str.to_string(), bytes));
                }

                // Sort files for deterministic snapshot output.
                files.sort_unstable_by(|a, b| a.0.cmp(&b.0));

                partition_blobs.push(TsFlushedPartitionBlob {
                    dir_name: entry.dir_name.clone(),
                    meta_bytes,
                    files,
                });
            }

            if !partition_blobs.is_empty() {
                let collection_key = format!("{}:{}:{}", database_id, tid.as_u64(), collection);
                result.push(TsFlushedCollectionBlob {
                    collection_key,
                    partitions: partition_blobs,
                });
            }
        }

        // Sort collections for deterministic snapshot output (ts_registries is
        // a HashMap so its iteration order is unspecified).
        result.sort_unstable_by(|a, b| a.collection_key.cmp(&b.collection_key));

        Ok(result)
    }

    /// Capture all plain-columnar (and spatial) engine state for one tenant.
    ///
    /// Iterates `columnar_engines` entries belonging to `tid`, exports each
    /// `MutationEngine` via `export_snapshot` (supplying any flushed segment
    /// blobs from `columnar_flushed_segments`), and serialises the result to
    /// msgpack. Returns `Err` on any export or serialization failure so the
    /// snapshot aborts cleanly rather than silently omitting data.
    fn capture_columnar_engines(
        &self,
        database_id: u64,
        tid: crate::types::TenantId,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let mut result = Vec::new();

        for ((eng_db, eng_tid, collection), engine) in &self.columnar_engines {
            if *eng_tid != tid || eng_db.as_u64() != database_id {
                continue;
            }

            let flushed: &[Vec<u8>] = self
                .columnar_flushed_segments
                .get(&(*eng_db, *eng_tid, collection.clone()))
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            // Cross-engine surrogate sidecar, keyed identically to the segment
            // bytes. Absent for collections that never flushed surrogate-bearing
            // rows; defaults to empty so the snapshot still round-trips.
            let flushed_surrogates: &[Vec<Option<nodedb_types::Surrogate>>] = self
                .columnar_flushed_surrogates
                .get(&(*eng_db, *eng_tid, collection.clone()))
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            let snap = engine
                .export_snapshot(flushed, flushed_surrogates)
                .map_err(|e| crate::Error::Storage {
                    engine: "columnar".into(),
                    detail: format!("export_snapshot for collection '{collection}': {e}"),
                })?;

            let bytes =
                zerompk::to_msgpack_vec(&snap).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("serialize ColumnarEngineSnapshot for '{collection}': {e}"),
                })?;

            let key_str = format!("{}:{}:{}", database_id, tid.as_u64(), collection);
            result.push((key_str, bytes));
        }

        // Sort for deterministic output (columnar_engines is a HashMap).
        result.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        Ok(result)
    }
}
