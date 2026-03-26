//! `NodeDbLite` struct definition, open/flush, and utility methods.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::lock_ext::LockExt;
use crate::engine::crdt::CrdtEngine;
use crate::engine::graph::index::CsrIndex;
use crate::engine::vector::graph::{HnswIndex, HnswParams};
use crate::memory::{EngineId, MemoryGovernor};
use crate::storage::engine::{StorageEngine, WriteOp};

/// Storage key constants.
pub(crate) const META_HNSW_COLLECTIONS: &[u8] = b"meta:hnsw_collections";
pub(crate) const META_CSR: &[u8] = b"meta:csr_checkpoint";
pub(crate) const META_SPATIAL_INDEXES: &[u8] = b"meta:spatial_indexes";
pub(crate) const META_CRDT_SNAPSHOT: &[u8] = b"crdt:snapshot";
pub(crate) const META_CRDT_DELTAS: &[u8] = b"crdt:pending_deltas";
/// Last flushed mutation_id — used for partial flush safety.
/// On cold start, if pending deltas have mutation_ids that don't align
/// with this watermark, we know the previous flush was interrupted.
pub(crate) const META_LAST_FLUSHED_MID: &[u8] = b"meta:last_flushed_mid";

/// NodeDB-Lite — the embedded edge database.
///
/// Fully capable of vector search, graph traversal, and document CRUD
/// entirely offline. Optional sync to Origin via WebSocket.
pub struct NodeDbLite<S: StorageEngine> {
    pub(crate) storage: Arc<S>,
    /// Per-collection HNSW indices.
    pub(crate) hnsw_indices: Mutex<HashMap<String, HnswIndex>>,
    /// Single CSR graph index (covers all collections).
    pub(crate) csr: Mutex<CsrIndex>,
    /// CRDT engine for delta generation and sync.
    /// Arc-wrapped for sharing with the query engine's TableProvider.
    pub(crate) crdt: Arc<Mutex<CrdtEngine>>,
    /// Memory budget governor.
    pub(crate) governor: MemoryGovernor,
    /// HNSW search ef parameter (configurable).
    pub(crate) search_ef: usize,
    /// Vector ID to collection+doc_id mapping (for CRDT integration).
    pub(crate) vector_id_map: Mutex<HashMap<String, (String, u32)>>,
    /// SQL query engine (DataFusion over Loro documents).
    pub(crate) query_engine: crate::query::LiteQueryEngine,
    /// Per-collection in-memory inverted index for full-text search.
    /// Updated incrementally on `document_put` and `document_delete`.
    pub(crate) text_indices: Mutex<HashMap<String, nodedb_query::text_search::InvertedIndex>>,
    /// Spatial R-tree indexes for geometry fields.
    pub(crate) spatial: Mutex<crate::engine::spatial::SpatialIndexManager>,
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Open or create a Lite database backed by the given storage engine.
    pub async fn open(storage: S, peer_id: u64) -> NodeDbResult<Self> {
        Self::open_with_budget(storage, peer_id, 100 * 1024 * 1024).await
    }

    /// Open with a custom memory budget.
    pub async fn open_with_budget(
        storage: S,
        peer_id: u64,
        memory_budget: usize,
    ) -> NodeDbResult<Self> {
        let storage = Arc::new(storage);

        // ── Restore CRDT state (with CRC32C validation) ──
        let mut crdt = match storage
            .get(Namespace::LoroState, META_CRDT_SNAPSHOT)
            .await?
        {
            Some(envelope) => {
                match crate::storage::checksum::unwrap(&envelope) {
                    Some(snapshot) => CrdtEngine::from_snapshot(peer_id, &snapshot)
                        .map_err(|e| NodeDbError::storage(format!("CRDT restore failed: {e}")))?,
                    None => {
                        tracing::error!(
                            "CRDT snapshot CRC32C mismatch — discarding corrupted snapshot. \
                             Will start with empty state. A full re-sync from Origin is needed."
                        );
                        // Delete the corrupted snapshot so we don't re-read it.
                        let _ = storage
                            .delete(Namespace::LoroState, META_CRDT_SNAPSHOT)
                            .await;
                        CrdtEngine::new(peer_id)
                            .map_err(|e| NodeDbError::storage(format!("CRDT init failed: {e}")))?
                    }
                }
            }
            None => CrdtEngine::new(peer_id)
                .map_err(|e| NodeDbError::storage(format!("CRDT init failed: {e}")))?,
        };

        // Restore pending deltas — prefer incremental entries over legacy bulk blob.
        let incremental_entries = storage.scan_prefix(Namespace::Crdt, b"delta:").await?;

        if !incremental_entries.is_empty() {
            // Use incremental entries (append-only format).
            crdt.restore_pending_deltas_incremental(&incremental_entries);
        } else if let Some(delta_bytes) = storage.get(Namespace::Crdt, META_CRDT_DELTAS).await? {
            // Fall back to legacy bulk blob.
            crdt.restore_pending_deltas(&delta_bytes);
        }

        // Partial flush safety: check if the last-flushed mutation_id matches.
        if crdt.pending_count() > 0
            && let Some(last_flushed_bytes) =
                storage.get(Namespace::Meta, META_LAST_FLUSHED_MID).await?
            && last_flushed_bytes.len() == 8
        {
            let last_flushed = u64::from_le_bytes(last_flushed_bytes.try_into().unwrap_or([0; 8]));
            let max_pending = crdt
                .pending_deltas()
                .iter()
                .map(|d| d.mutation_id)
                .max()
                .unwrap_or(0);

            if max_pending > 0 && last_flushed > 0 && max_pending != last_flushed {
                tracing::warn!(
                    last_flushed,
                    max_pending,
                    "partial flush detected — pending deltas may be inconsistent. \
                     Clearing pending queue; CRDT state is authoritative."
                );
                crdt.clear_pending_deltas();
            }
        }

        // ── Restore CSR (with CRC32C validation) ──
        let csr = match storage.get(Namespace::Graph, META_CSR).await? {
            Some(envelope) => match crate::storage::checksum::unwrap(&envelope) {
                Some(bytes) => CsrIndex::from_checkpoint(&bytes).unwrap_or_else(|| {
                    tracing::warn!(
                        "CSR checkpoint deserialization failed, rebuilding from CRDT edges"
                    );
                    CsrIndex::new()
                }),
                None => {
                    tracing::error!(
                        "CSR checkpoint CRC32C mismatch — discarding corrupted checkpoint. \
                         Graph index will be rebuilt from CRDT edge documents."
                    );
                    let _ = storage.delete(Namespace::Graph, META_CSR).await;
                    CsrIndex::new()
                }
            },
            None => CsrIndex::new(),
        };

        // ── Restore HNSW indices ──
        let hnsw_indices = Self::restore_hnsw_indices(&storage).await?;

        // ── Restore spatial indices ──
        let spatial = Self::restore_spatial_indices(&storage).await;

        let governor = MemoryGovernor::new(memory_budget);

        let crdt = Arc::new(Mutex::new(crdt));
        let query_engine = crate::query::LiteQueryEngine::new(Arc::clone(&crdt));

        let db = Self {
            storage,
            hnsw_indices: Mutex::new(hnsw_indices),
            csr: Mutex::new(csr),
            crdt,
            governor,
            search_ef: 128,
            vector_id_map: Mutex::new(HashMap::new()),
            query_engine,
            text_indices: Mutex::new(HashMap::new()),
            spatial: Mutex::new(spatial),
        };

        // Rebuild text indices from CRDT state (cold start).
        db.rebuild_text_indices();

        // Rebuild spatial indices if restore produced empty trees.
        // The R-tree checkpoint only stores bounding boxes, not doc IDs.
        // A full rebuild from CRDT documents ensures doc_to_entry is correct.
        {
            let spatial = db.spatial.lock_or_recover();
            if spatial.is_empty() {
                drop(spatial);
                db.rebuild_spatial_indices();
            }
        }

        Ok(db)
    }

    /// Restore HNSW indices from storage.
    async fn restore_hnsw_indices(storage: &Arc<S>) -> NodeDbResult<HashMap<String, HnswIndex>> {
        let mut hnsw_indices = HashMap::new();
        let Some(collections_bytes) = storage.get(Namespace::Meta, META_HNSW_COLLECTIONS).await?
        else {
            return Ok(hnsw_indices);
        };
        let Ok(names) = rmp_serde::from_slice::<Vec<String>>(&collections_bytes) else {
            return Ok(hnsw_indices);
        };
        for name in &names {
            let key = format!("hnsw:{name}");
            if let Some(envelope) = storage.get(Namespace::Vector, key.as_bytes()).await? {
                match crate::storage::checksum::unwrap(&envelope) {
                    Some(checkpoint) => {
                        if let Some(index) = HnswIndex::from_checkpoint(&checkpoint) {
                            hnsw_indices.insert(name.clone(), index);
                        } else {
                            tracing::warn!(
                                collection = %name,
                                "HNSW checkpoint deserialization failed, will rebuild from CRDT"
                            );
                        }
                    }
                    None => {
                        tracing::error!(
                            collection = %name,
                            "HNSW checkpoint CRC32C mismatch — discarding. \
                             Will rebuild from CRDT document vectors on next vector insert."
                        );
                        let _ = storage.delete(Namespace::Vector, key.as_bytes()).await;
                    }
                }
            }
        }
        Ok(hnsw_indices)
    }

    /// Restore spatial indices from storage.
    async fn restore_spatial_indices(
        storage: &Arc<S>,
    ) -> crate::engine::spatial::SpatialIndexManager {
        let Some(index_list_bytes) = storage
            .get(Namespace::Meta, META_SPATIAL_INDEXES)
            .await
            .ok()
            .flatten()
        else {
            return crate::engine::spatial::SpatialIndexManager::new();
        };

        let Ok(index_keys) = rmp_serde::from_slice::<Vec<(String, String)>>(&index_list_bytes)
        else {
            return crate::engine::spatial::SpatialIndexManager::new();
        };

        let mut checkpoints = Vec::new();
        for (collection, field) in &index_keys {
            let key = format!("spatial:{collection}:{field}");
            if let Ok(Some(envelope)) = storage.get(Namespace::Spatial, key.as_bytes()).await {
                match crate::storage::checksum::unwrap(&envelope) {
                    Some(bytes) => checkpoints.push((collection.clone(), field.clone(), bytes)),
                    None => {
                        tracing::error!(
                            collection = %collection,
                            field = %field,
                            "spatial index CRC32C mismatch — discarding"
                        );
                        let _ = storage.delete(Namespace::Spatial, key.as_bytes()).await;
                    }
                }
            }
        }

        crate::engine::spatial::SpatialIndexManager::restore_all(&checkpoints)
    }

    /// Persist all in-memory state to storage (call before shutdown).
    pub async fn flush(&self) -> NodeDbResult<()> {
        let mut ops = Vec::new();

        // ── Persist CRDT snapshot (CRC32C wrapped) ──
        {
            let crdt = self.crdt.lock_or_recover();
            let snapshot = crdt.export_snapshot().map_err(NodeDbError::storage)?;
            ops.push(WriteOp::Put {
                ns: Namespace::LoroState,
                key: META_CRDT_SNAPSHOT.to_vec(),
                value: crate::storage::checksum::wrap(&snapshot),
            });

            // Write pending deltas individually (append-only persistence).
            // Each delta is stored under `crdt:delta:{mutation_id:016x}`.
            // Also write the legacy bulk blob for backward compatibility.
            let pending = crdt.pending_deltas();
            let max_mid = pending.iter().map(|d| d.mutation_id).max().unwrap_or(0);

            for delta in pending {
                let key = CrdtEngine::delta_storage_key(delta.mutation_id);
                let value = CrdtEngine::serialize_delta(delta).map_err(NodeDbError::storage)?;
                ops.push(WriteOp::Put {
                    ns: Namespace::Crdt,
                    key,
                    value,
                });
            }

            // Legacy bulk blob (for clients that haven't upgraded to incremental restore).
            let deltas_bulk = crdt
                .serialize_pending_deltas()
                .map_err(NodeDbError::storage)?;
            ops.push(WriteOp::Put {
                ns: Namespace::Crdt,
                key: META_CRDT_DELTAS.to_vec(),
                value: deltas_bulk,
            });

            // Write the last-flushed mutation_id for partial flush safety.
            ops.push(WriteOp::Put {
                ns: Namespace::Meta,
                key: META_LAST_FLUSHED_MID.to_vec(),
                value: max_mid.to_le_bytes().to_vec(),
            });
        }

        // ── Persist CSR (CRC32C wrapped) ──
        {
            let csr = self.csr.lock_or_recover();
            let checkpoint = csr.checkpoint_to_bytes();
            ops.push(WriteOp::Put {
                ns: Namespace::Graph,
                key: META_CSR.to_vec(),
                value: crate::storage::checksum::wrap(&checkpoint),
            });
        }

        // ── Persist HNSW indices ──
        {
            let indices = self.hnsw_indices.lock_or_recover();
            let names: Vec<String> = indices.keys().cloned().collect();
            let names_bytes = rmp_serde::to_vec_named(&names)
                .map_err(|e| NodeDbError::serialization("msgpack", e))?;
            ops.push(WriteOp::Put {
                ns: Namespace::Meta,
                key: META_HNSW_COLLECTIONS.to_vec(),
                value: names_bytes,
            });

            for (name, index) in indices.iter() {
                let key = format!("hnsw:{name}");
                let checkpoint = index.checkpoint_to_bytes();
                ops.push(WriteOp::Put {
                    ns: Namespace::Vector,
                    key: key.into_bytes(),
                    value: crate::storage::checksum::wrap(&checkpoint),
                });
            }
        }

        // ── Persist spatial indices ──
        {
            let spatial = self.spatial.lock_or_recover();
            let checkpoints = spatial.checkpoint_all();
            let index_keys: Vec<(String, String)> = checkpoints
                .iter()
                .map(|(c, f, _)| (c.clone(), f.clone()))
                .collect();
            let keys_bytes = rmp_serde::to_vec_named(&index_keys)
                .map_err(|e| NodeDbError::serialization("msgpack", e))?;
            ops.push(WriteOp::Put {
                ns: Namespace::Meta,
                key: META_SPATIAL_INDEXES.to_vec(),
                value: keys_bytes,
            });

            for (collection, field, bytes) in &checkpoints {
                let key = format!("spatial:{collection}:{field}");
                ops.push(WriteOp::Put {
                    ns: Namespace::Spatial,
                    key: key.into_bytes(),
                    value: crate::storage::checksum::wrap(bytes),
                });
            }
        }

        self.storage
            .batch_write(&ops)
            .await
            .map_err(NodeDbError::storage)?;

        Ok(())
    }

    /// Get or create an HNSW index for a collection.
    /// Rebuild all text indices from CRDT state.
    ///
    /// Called once on cold start after CRDT snapshot restore.
    /// Scans all collections and indexes all string fields.
    fn rebuild_text_indices(&self) {
        let crdt = self.crdt.lock_or_recover();
        let collections = crdt.collection_names();

        let mut indices = self.text_indices.lock_or_recover();

        for collection in &collections {
            if collection.starts_with("__") {
                continue;
            }
            let ids = crdt.list_ids(collection);
            if ids.is_empty() {
                continue;
            }

            let idx = indices.entry(collection.clone()).or_default();

            for id in &ids {
                if let Some(loro_val) = crdt.read(collection, id) {
                    let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                    let text: String = doc
                        .fields
                        .values()
                        .filter_map(|v| match v {
                            nodedb_types::Value::String(s) => Some(s.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    if !text.is_empty() {
                        idx.index_document(id, &text);
                    }
                }
            }
        }
    }

    /// Rebuild spatial indices from CRDT state (cold start fallback).
    ///
    /// Scans all collections for geometry-valued fields and indexes them.
    /// Called when checkpoint restore produces empty spatial indices.
    fn rebuild_spatial_indices(&self) {
        let crdt = self.crdt.lock_or_recover();
        let collections = crdt.collection_names();
        let mut spatial = self.spatial.lock_or_recover();

        for collection in &collections {
            if collection.starts_with("__") {
                continue;
            }
            let ids = crdt.list_ids(collection);
            for id in &ids {
                if let Some(loro_val) = crdt.read(collection, id) {
                    let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                    for (field, value) in &doc.fields {
                        // Geometry fields are stored as GeoJSON strings.
                        if let nodedb_types::Value::String(s) = value
                            && let Ok(geom) =
                                serde_json::from_str::<nodedb_types::geometry::Geometry>(s)
                        {
                            spatial.index_document(collection, field, id, &geom);
                        }
                    }
                }
            }
        }
    }

    /// Update the inverted text index after a document write.
    ///
    /// Called by `document_put` to keep the text index in sync.
    /// Concatenates all string fields for full-text indexing.
    pub(crate) fn index_document_text(
        &self,
        collection: &str,
        doc_id: &str,
        fields: &std::collections::HashMap<String, nodedb_types::Value>,
    ) {
        let text: String = fields
            .values()
            .filter_map(|v| match v {
                nodedb_types::Value::String(s) => Some(s.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" ");

        if text.is_empty() {
            return;
        }

        let mut indices = self.text_indices.lock_or_recover();
        let idx = indices.entry(collection.to_string()).or_default();
        idx.index_document(doc_id, &text);
    }

    /// Remove a document from the text index.
    pub(crate) fn remove_document_text(&self, collection: &str, doc_id: &str) {
        let mut indices = self.text_indices.lock_or_recover();
        if let Some(idx) = indices.get_mut(collection) {
            idx.remove_document(doc_id);
        }
    }

    pub(crate) fn ensure_hnsw<'a>(
        indices: &'a mut HashMap<String, HnswIndex>,
        collection: &str,
        dim: usize,
    ) -> &'a mut HnswIndex {
        indices
            .entry(collection.to_string())
            .or_insert_with(|| HnswIndex::new(dim, HnswParams::default()))
    }

    /// Update memory governor with current engine usage.
    pub fn update_memory_stats(&self) {
        if let Ok(indices) = self.hnsw_indices.lock() {
            let hnsw_bytes: usize = indices
                .values()
                .map(|idx| idx.len() * (idx.dim() * 4 + 128))
                .sum();
            self.governor.report_usage(EngineId::Hnsw, hnsw_bytes);
        }
        if let Ok(csr) = self.csr.lock() {
            self.governor
                .report_usage(EngineId::Csr, csr.estimated_memory_bytes());
        }
        if let Ok(crdt) = self.crdt.lock() {
            self.governor
                .report_usage(EngineId::Loro, crdt.estimated_memory_bytes());
        }
    }

    /// List currently loaded HNSW collections.
    pub fn loaded_collections(&self) -> NodeDbResult<Vec<String>> {
        let indices = self.hnsw_indices.lock_or_recover();
        Ok(indices.keys().cloned().collect())
    }

    /// Access the memory governor.
    pub fn governor(&self) -> &MemoryGovernor {
        &self.governor
    }

    /// Access pending CRDT deltas (for sync client).
    pub fn pending_crdt_deltas(
        &self,
    ) -> NodeDbResult<Vec<crate::engine::crdt::engine::PendingDelta>> {
        let crdt = self.crdt.lock_or_recover();
        Ok(crdt.pending_deltas().to_vec())
    }

    /// Acknowledge synced deltas (called after Origin ACK).
    pub fn acknowledge_deltas(&self, acked_id: u64) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();
        crdt.acknowledge(acked_id);
        Ok(())
    }

    /// Import remote deltas from Origin.
    pub fn import_remote_deltas(&self, data: &[u8]) -> NodeDbResult<()> {
        let crdt = self.crdt.lock_or_recover();
        crdt.import_remote(data).map_err(NodeDbError::storage)
    }

    /// Reject a specific delta (rollback optimistic local state).
    pub fn reject_delta(&self, mutation_id: u64) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();
        crdt.reject_delta(mutation_id);
        Ok(())
    }

    /// Start background sync to Origin.
    ///
    /// Spawns a Tokio task that connects to the Origin WebSocket endpoint,
    /// pushes pending deltas, and receives shape updates. Runs forever
    /// with auto-reconnect.
    ///
    /// Returns immediately — the sync runs in the background.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn start_sync(
        self: &Arc<Self>,
        config: crate::sync::SyncConfig,
    ) -> Arc<crate::sync::SyncClient> {
        let client = Arc::new(crate::sync::SyncClient::new(config, self.peer_id()));
        let delegate: Arc<dyn crate::sync::SyncDelegate> = Arc::clone(self) as _;
        let client_clone = Arc::clone(&client);
        tokio::spawn(async move {
            crate::sync::run_sync_loop(client_clone, delegate).await;
        });
        client
    }

    /// Get the peer ID (from the CRDT engine).
    pub fn peer_id(&self) -> u64 {
        self.crdt.lock().map(|c| c.peer_id()).unwrap_or(0)
    }
}
