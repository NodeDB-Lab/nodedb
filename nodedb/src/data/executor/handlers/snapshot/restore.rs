//! Tenant snapshot restoration: import Data Plane state for all engines.

use sonic_rs;
use tracing::{info, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Restore a tenant's data across ALL engines from a snapshot.
    ///
    /// Receives MessagePack-serialized documents and indexes (legacy format)
    /// and writes them to the sparse engine. Extended fields (edges, vectors,
    /// KV, CRDT, timeseries) are restored from the full `TenantDataSnapshot`
    /// when present.
    pub(in crate::data::executor) fn execute_restore_tenant_snapshot(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u32,
        documents_bytes: &[u8],
        indexes_bytes: &[u8],
    ) -> Response {
        info!(core = self.core_id, tenant_id, "restoring tenant snapshot");

        // Try to deserialize as full TenantDataSnapshot first (new format).
        // Fall back to legacy (documents + indexes only) if that fails.
        let full_snapshot: Option<crate::types::TenantDataSnapshot> =
            zerompk::from_msgpack(documents_bytes).ok();

        let (docs_written, indexes_written) = if let Some(ref snap) = full_snapshot {
            self.restore_sparse(tenant_id, &snap.documents, &snap.indexes)
        } else {
            // Legacy format: separate documents and indexes bytes.
            let documents: Vec<(String, Vec<u8>)> =
                zerompk::from_msgpack(documents_bytes).unwrap_or_default();
            let indexes: Vec<(String, Vec<u8>)> =
                zerompk::from_msgpack(indexes_bytes).unwrap_or_default();
            self.restore_sparse(tenant_id, &documents, &indexes)
        };

        let mut edges_written = 0u64;
        let mut vectors_written = 0u64;
        let mut kv_written = 0u64;
        let mut crdt_written = 0u64;
        let mut ts_written = 0u64;

        if let Some(ref snap) = full_snapshot {
            // Restore graph edges.
            for (key, props) in &snap.edges {
                if let Err(e) = self.edge_store.put_edge_raw(key, props) {
                    warn!(key, error = %e, "failed to restore edge");
                    continue;
                }
                edges_written += 1;
            }
            // Rebuild CSR from restored edges.
            if edges_written > 0
                && let Ok(rebuilt) =
                    crate::engine::graph::csr::rebuild::rebuild_from_store(&self.edge_store)
            {
                self.csr = rebuilt;
            }

            // Restore vector collections.
            for (key, bytes) in &snap.vectors {
                let vectors: Vec<(u32, Vec<f32>, Option<String>)> =
                    match zerompk::from_msgpack(bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(key, error = %e, "failed to decode vector snapshot");
                            continue;
                        }
                    };
                let count = vectors.len() as u64;
                self.restore_vector_collection(tenant_id, key, vectors);
                vectors_written += count;
            }

            // Restore KV tables.
            for (collection_name, bytes) in &snap.kv_tables {
                let entries: Vec<(Vec<u8>, Vec<u8>, u64)> = match zerompk::from_msgpack(bytes) {
                    Ok(e) => e,
                    Err(e) => {
                        warn!(collection_name, error = %e, "failed to decode kv snapshot");
                        continue;
                    }
                };
                let count = entries.len() as u64;
                self.restore_kv_table(tenant_id, collection_name, entries);
                kv_written += count;
            }

            // Restore CRDT state.
            for (_key, bytes) in &snap.crdt_state {
                if let Err(e) = self.restore_crdt_state(tenant_id, bytes) {
                    warn!(tenant_id, error = %e, "failed to restore crdt state");
                } else {
                    crdt_written += 1;
                }
            }

            // Restore timeseries memtables.
            for (key, bytes) in &snap.timeseries {
                if let Err(e) = self.restore_timeseries(key, bytes) {
                    warn!(key, error = %e, "failed to restore timeseries");
                } else {
                    ts_written += 1;
                }
            }
        }

        info!(
            tenant_id,
            docs_written,
            indexes_written,
            edges_written,
            vectors_written,
            kv_written,
            crdt_written,
            ts_written,
            "full tenant snapshot restored"
        );

        let result = serde_json::json!({
            "tenant_id": tenant_id,
            "documents_restored": docs_written,
            "indexes_restored": indexes_written,
            "edges_restored": edges_written,
            "vectors_restored": vectors_written,
            "kv_entries_restored": kv_written,
            "crdt_restored": crdt_written,
            "timeseries_restored": ts_written,
        });
        match crate::data::executor::response_codec::encode_json(&result) {
            Ok(p) => self.response_with_payload(task, p),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("result serialization failed: {e}"),
                },
            ),
        }
    }

    fn restore_sparse(
        &self,
        _tenant_id: u32,
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

    fn restore_vector_collection(
        &mut self,
        _tenant_id: u32,
        index_key: &str,
        vectors: Vec<(u32, Vec<f32>, Option<String>)>,
    ) {
        if vectors.is_empty() {
            return;
        }
        let dim = vectors[0].1.len();
        let params = self
            .vector_params
            .get(index_key)
            .cloned()
            .unwrap_or_default();
        let coll = self
            .vector_collections
            .entry(index_key.to_string())
            .or_insert_with(|| {
                crate::engine::vector::collection::VectorCollection::new(dim, params)
            });
        for (_, data, doc_id) in vectors {
            match doc_id {
                Some(did) => {
                    coll.insert_with_doc_id(data, did);
                }
                None => {
                    coll.insert(data);
                }
            }
        }
    }

    fn restore_kv_table(
        &mut self,
        tenant_id: u32,
        collection: &str,
        entries: Vec<(Vec<u8>, Vec<u8>, u64)>,
    ) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        for (key, value, expire_at) in entries {
            let ttl_ms = if expire_at > now_ms {
                expire_at - now_ms
            } else if expire_at == 0 {
                0
            } else {
                continue; // Already expired.
            };
            self.kv_engine
                .put(tenant_id, collection, &key, &value, ttl_ms, now_ms);
        }
    }

    fn restore_crdt_state(&mut self, tenant_id: u32, bytes: &[u8]) -> crate::Result<()> {
        let tid = crate::types::TenantId::new(tenant_id);
        // If an engine already exists, import into it. Otherwise create a fresh one.
        if let Some(engine) = self.crdt_engines.get(&tid) {
            engine.import_snapshot_bytes(bytes)
        } else {
            let engine = crate::engine::crdt::TenantCrdtEngine::new(
                tid,
                0, // Default peer_id for restore.
                Default::default(),
            )?;
            engine.import_snapshot_bytes(bytes)?;
            self.crdt_engines.insert(tid, engine);
            Ok(())
        }
    }

    fn restore_timeseries(&mut self, key: &str, bytes: &[u8]) -> crate::Result<()> {
        let columns: Vec<(String, Vec<u8>)> =
            zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: e.to_string(),
            })?;
        // Store column data in sparse engine keyed by scoped collection.
        // Timeseries engine will rebuild memtable from these on access.
        for (col_name, col_data) in columns {
            let restore_key = format!("{key}:{col_name}");
            if let Err(e) = self.sparse.put_raw(&restore_key, &col_data) {
                warn!(restore_key, error = %e, "failed to restore timeseries column");
            }
        }
        Ok(())
    }
}
