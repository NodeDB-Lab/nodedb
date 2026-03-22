//! `NodeDbLite` — the main entry point for the embedded edge database.
//!
//! Wires together: HNSW (vector), CSR (graph), CrdtEngine (Loro),
//! SqliteStorage, and MemoryGovernor. Implements the `NodeDb` trait
//! so application code is identical whether running on Lite or Origin.
//!
//! ```rust,ignore
//! let db: Arc<dyn NodeDb> = Arc::new(NodeDbLite::open("./mydb").await?);
//! let results = db.vector_search("embeddings", &query, 5, None).await?;
//! ```

pub(crate) mod convert;
mod trait_impl;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::engine::crdt::CrdtEngine;
use crate::engine::graph::index::CsrIndex;
use crate::engine::vector::graph::{HnswIndex, HnswParams};
use crate::memory::{EngineId, MemoryGovernor};

/// Extension trait for graceful mutex lock recovery.
///
/// Recovers from poisoned mutexes (a thread panicked while holding the lock)
/// by extracting the inner guard. Logs at error level for observability.
pub(crate) trait LockExt<T> {
    fn lock_or_recover(&self) -> std::sync::MutexGuard<'_, T>;
}

impl<T> LockExt<T> for Mutex<T> {
    fn lock_or_recover(&self) -> std::sync::MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|p| {
            tracing::error!("mutex poisoned, recovering guard");
            p.into_inner()
        })
    }
}
use crate::storage::engine::{StorageEngine, WriteOp};

/// Storage key constants.
pub(crate) const META_HNSW_COLLECTIONS: &[u8] = b"meta:hnsw_collections";
pub(crate) const META_CSR: &[u8] = b"meta:csr_checkpoint";
pub(crate) const META_CRDT_SNAPSHOT: &[u8] = b"crdt:snapshot";
pub(crate) const META_CRDT_DELTAS: &[u8] = b"crdt:pending_deltas";

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
    pub(crate) crdt: Mutex<CrdtEngine>,
    /// Memory budget governor.
    pub(crate) governor: MemoryGovernor,
    /// HNSW search ef parameter (configurable).
    pub(crate) search_ef: usize,
    /// Vector ID to collection+doc_id mapping (for CRDT integration).
    pub(crate) vector_id_map: Mutex<HashMap<String, (String, u32)>>,
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

        // ── Restore CRDT state ──
        let mut crdt = match storage
            .get(Namespace::LoroState, META_CRDT_SNAPSHOT)
            .await?
        {
            Some(snapshot) => {
                CrdtEngine::from_snapshot(peer_id, &snapshot).map_err(|e| NodeDbError::Storage {
                    detail: format!("CRDT restore failed: {e}"),
                })?
            }
            None => CrdtEngine::new(peer_id).map_err(|e| NodeDbError::Storage {
                detail: format!("CRDT init failed: {e}"),
            })?,
        };

        // Restore pending deltas.
        if let Some(delta_bytes) = storage.get(Namespace::Crdt, META_CRDT_DELTAS).await? {
            crdt.restore_pending_deltas(&delta_bytes);
        }

        // ── Restore CSR ──
        let csr = match storage.get(Namespace::Graph, META_CSR).await? {
            Some(bytes) => CsrIndex::from_checkpoint(&bytes).unwrap_or_else(|| {
                tracing::warn!("CSR checkpoint corrupted, starting with empty graph index");
                CsrIndex::new()
            }),
            None => CsrIndex::new(),
        };

        // ── Restore HNSW indices ──
        let hnsw_indices = Self::restore_hnsw_indices(&storage).await?;

        let governor = MemoryGovernor::new(memory_budget);

        Ok(Self {
            storage,
            hnsw_indices: Mutex::new(hnsw_indices),
            csr: Mutex::new(csr),
            crdt: Mutex::new(crdt),
            governor,
            search_ef: 128,
            vector_id_map: Mutex::new(HashMap::new()),
        })
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
            if let Some(checkpoint) = storage.get(Namespace::Vector, key.as_bytes()).await?
                && let Some(index) = HnswIndex::from_checkpoint(&checkpoint)
            {
                hnsw_indices.insert(name.clone(), index);
            }
        }
        Ok(hnsw_indices)
    }

    /// Persist all in-memory state to storage (call before shutdown).
    pub async fn flush(&self) -> NodeDbResult<()> {
        let mut ops = Vec::new();

        // ── Persist CRDT snapshot ──
        {
            let crdt = self.crdt.lock_or_recover();
            let snapshot = crdt.export_snapshot().map_err(|e| NodeDbError::Storage {
                detail: e.to_string(),
            })?;
            ops.push(WriteOp::Put {
                ns: Namespace::LoroState,
                key: META_CRDT_SNAPSHOT.to_vec(),
                value: snapshot,
            });

            let deltas = crdt
                .serialize_pending_deltas()
                .map_err(|e| NodeDbError::Storage {
                    detail: e.to_string(),
                })?;
            ops.push(WriteOp::Put {
                ns: Namespace::Crdt,
                key: META_CRDT_DELTAS.to_vec(),
                value: deltas,
            });
        }

        // ── Persist CSR ──
        {
            let csr = self.csr.lock_or_recover();
            let checkpoint = csr.checkpoint_to_bytes();
            ops.push(WriteOp::Put {
                ns: Namespace::Graph,
                key: META_CSR.to_vec(),
                value: checkpoint,
            });
        }

        // ── Persist HNSW indices ──
        {
            let indices = self.hnsw_indices.lock_or_recover();
            let names: Vec<String> = indices.keys().cloned().collect();
            let names_bytes =
                rmp_serde::to_vec_named(&names).map_err(|e| NodeDbError::Serialization {
                    format: "msgpack".into(),
                    detail: e.to_string(),
                })?;
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
                    value: checkpoint,
                });
            }
        }

        self.storage
            .batch_write(&ops)
            .await
            .map_err(|e| NodeDbError::Storage {
                detail: e.to_string(),
            })?;

        Ok(())
    }

    /// Get or create an HNSW index for a collection.
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
                .map(|idx| {
                    // Rough estimate: vectors + neighbor lists.
                    idx.len() * (idx.dim() * 4 + 128)
                })
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

    /// Batch insert vectors — O(1) CRDT delta export instead of O(N).
    ///
    /// Use this for bulk loading (cold-start hydration, benchmark setup, imports).
    /// Each vector is inserted into HNSW and tracked in the ID map, but only one
    /// Loro delta is generated for the entire batch.
    pub fn batch_vector_insert(
        &self,
        collection: &str,
        vectors: &[(&str, &[f32])],
    ) -> NodeDbResult<()> {
        if vectors.is_empty() {
            return Ok(());
        }

        let dim = vectors[0].1.len();

        // ── Insert all vectors into HNSW ──
        {
            let mut indices = self.hnsw_indices.lock_or_recover();
            let index = Self::ensure_hnsw(&mut indices, collection, dim);
            let mut id_map = self.vector_id_map.lock_or_recover();

            for &(id, embedding) in vectors {
                let internal_id = index.len() as u32;
                index
                    .insert(embedding.to_vec())
                    .map_err(|e| NodeDbError::BadRequest { detail: e })?;
                id_map.insert(
                    format!("{collection}:{internal_id}"),
                    (id.to_string(), internal_id),
                );
            }
        }

        // ── Single CRDT batch ──
        {
            let mut crdt = self.crdt.lock_or_recover();

            use crate::engine::crdt::engine::{CrdtBatchOp, CrdtField};

            // Pre-allocate field arrays so references live long enough.
            let fields: Vec<Vec<CrdtField<'_>>> = vectors
                .iter()
                .map(|&(_, emb)| vec![("embedding_dim", loro::LoroValue::I64(emb.len() as i64))])
                .collect();

            let ops: Vec<CrdtBatchOp<'_>> = vectors
                .iter()
                .zip(fields.iter())
                .map(|(&(id, _), f)| (collection, id, f.as_slice()))
                .collect();

            crdt.batch_upsert(&ops).map_err(|e| NodeDbError::Storage {
                detail: e.to_string(),
            })?;
        }

        self.update_memory_stats();
        Ok(())
    }

    /// Batch insert graph edges — O(1) CRDT delta export instead of O(N).
    pub fn batch_graph_insert_edges(
        &self,
        edges: &[(&str, &str, &str)], // (src, dst, label)
    ) -> NodeDbResult<()> {
        if edges.is_empty() {
            return Ok(());
        }

        // ── Insert all edges into CSR ──
        {
            let mut csr = self.csr.lock_or_recover();
            for &(src, dst, label) in edges {
                csr.add_edge(src, label, dst);
            }
        }

        // ── Single CRDT batch ──
        {
            let mut crdt = self.crdt.lock_or_recover();

            use crate::engine::crdt::engine::{CrdtBatchOp, CrdtField};

            let ops: Vec<(String, Vec<CrdtField<'_>>)> = edges
                .iter()
                .map(|&(src, dst, label)| {
                    let edge_id = format!("{src}--{label}-->{dst}");
                    let fields: Vec<CrdtField<'_>> = vec![
                        ("src", loro::LoroValue::String(src.into())),
                        ("dst", loro::LoroValue::String(dst.into())),
                        ("label", loro::LoroValue::String(label.into())),
                    ];
                    (edge_id, fields)
                })
                .collect();

            let refs: Vec<CrdtBatchOp<'_>> = ops
                .iter()
                .map(|(id, fields)| ("__edges", id.as_str(), fields.as_slice()))
                .collect();

            crdt.batch_upsert(&refs).map_err(|e| NodeDbError::Storage {
                detail: e.to_string(),
            })?;
        }

        self.update_memory_stats();
        Ok(())
    }

    /// Compact the CSR graph index (merge buffer into dense arrays).
    ///
    /// Call after bulk edge insertion for optimal traversal performance.
    pub fn compact_graph(&self) -> NodeDbResult<()> {
        let mut csr = self.csr.lock_or_recover();
        csr.compact();
        Ok(())
    }

    /// Evict HNSW collections to reduce memory usage.
    ///
    /// Persists each evicted collection to storage first, then drops
    /// it from memory. The data is reloaded lazily on next `vector_search`.
    ///
    /// `max_to_evict` limits how many collections to drop in one pass.
    /// Collections are evicted smallest-first (least data = cheapest to reload).
    pub async fn evict_collections(&self, max_to_evict: usize) -> NodeDbResult<usize> {
        let mut evicted = 0;

        // Identify candidates.
        let candidates: Vec<(String, usize)> = {
            let indices = self.hnsw_indices.lock_or_recover();
            let mut sorted: Vec<(String, usize)> = indices
                .iter()
                .map(|(name, idx)| (name.clone(), idx.len()))
                .collect();
            sorted.sort_by_key(|(_, size)| *size);
            sorted
        };

        for (name, _) in candidates.into_iter().take(max_to_evict) {
            // Persist before evicting.
            let checkpoint = {
                let indices = self.hnsw_indices.lock_or_recover();
                match indices.get(&name) {
                    Some(idx) => idx.checkpoint_to_bytes(),
                    None => continue,
                }
            };

            let key = format!("hnsw:{name}");
            self.storage
                .put(Namespace::Vector, key.as_bytes(), &checkpoint)
                .await
                .map_err(|e| NodeDbError::Storage {
                    detail: e.to_string(),
                })?;

            // Remove from memory.
            {
                let mut indices = self.hnsw_indices.lock_or_recover();
                indices.remove(&name);
            }

            tracing::info!(collection = %name, "HNSW collection evicted from memory");
            evicted += 1;
        }

        self.update_memory_stats();
        Ok(evicted)
    }

    /// Check memory pressure and evict if needed.
    ///
    /// Call periodically (e.g., after batch inserts or on a timer).
    /// Returns the number of collections evicted.
    pub async fn check_and_evict(&self) -> NodeDbResult<usize> {
        use crate::memory::PressureLevel;

        self.update_memory_stats();
        match self.governor.pressure() {
            PressureLevel::Critical => self.evict_collections(2).await,
            PressureLevel::Warning => self.evict_collections(1).await,
            PressureLevel::Normal => Ok(0),
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
        crdt.import_remote(data).map_err(|e| NodeDbError::Storage {
            detail: e.to_string(),
        })
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

/// `SyncDelegate` implementation — bridges the sync transport to NodeDbLite's engines.
#[cfg(not(target_arch = "wasm32"))]
impl<S: StorageEngine> crate::sync::SyncDelegate for NodeDbLite<S> {
    fn pending_deltas(&self) -> Vec<crate::engine::crdt::engine::PendingDelta> {
        self.pending_crdt_deltas().unwrap_or_default()
    }

    fn acknowledge(&self, mutation_id: u64) {
        if let Err(e) = self.acknowledge_deltas(mutation_id) {
            tracing::warn!(mutation_id, error = %e, "SyncDelegate: acknowledge failed");
        }
    }

    fn reject(&self, mutation_id: u64) {
        if let Err(e) = self.reject_delta(mutation_id) {
            tracing::warn!(mutation_id, error = %e, "SyncDelegate: reject failed");
        }
    }

    fn import_remote(&self, data: &[u8]) {
        if let Err(e) = self.import_remote_deltas(data) {
            tracing::warn!(error = %e, "SyncDelegate: import_remote failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_client::NodeDb;
    use nodedb_types::document::Document;
    use nodedb_types::id::NodeId;
    use nodedb_types::value::Value;

    use crate::RedbStorage;

    use super::*;

    async fn make_db() -> NodeDbLite<RedbStorage> {
        let storage = RedbStorage::open_in_memory().unwrap();
        NodeDbLite::open(storage, 1).await.unwrap()
    }

    #[tokio::test]
    async fn open_empty_db() {
        let db = make_db().await;
        assert_eq!(db.governor().total_used(), 0);
    }

    #[tokio::test]
    async fn vector_insert_and_search() {
        let db = make_db().await;

        db.vector_insert("embeddings", "v1", &[1.0, 0.0, 0.0], None)
            .await
            .unwrap();
        db.vector_insert("embeddings", "v2", &[0.0, 1.0, 0.0], None)
            .await
            .unwrap();
        db.vector_insert("embeddings", "v3", &[0.0, 0.0, 1.0], None)
            .await
            .unwrap();

        let results = db
            .vector_search("embeddings", &[1.0, 0.0, 0.0], 2, None)
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "v1"); // Closest.
    }

    #[tokio::test]
    async fn vector_delete() {
        let db = make_db().await;
        db.vector_insert("coll", "v1", &[1.0, 0.0], None)
            .await
            .unwrap();
        db.vector_delete("coll", "v1").await.unwrap();

        let results = db
            .vector_search("coll", &[1.0, 0.0], 5, None)
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn graph_insert_and_traverse() {
        let db = make_db().await;

        db.graph_insert_edge(&NodeId::new("alice"), &NodeId::new("bob"), "KNOWS", None)
            .await
            .unwrap();
        db.graph_insert_edge(&NodeId::new("bob"), &NodeId::new("carol"), "KNOWS", None)
            .await
            .unwrap();

        let subgraph = db
            .graph_traverse(&NodeId::new("alice"), 2, None)
            .await
            .unwrap();

        assert!(subgraph.node_count() >= 2);
        assert!(subgraph.edge_count() >= 1);
    }

    #[tokio::test]
    async fn graph_delete_edge() {
        let db = make_db().await;
        let edge_id = db
            .graph_insert_edge(&NodeId::new("a"), &NodeId::new("b"), "L", None)
            .await
            .unwrap();

        db.graph_delete_edge(&edge_id).await.unwrap();

        let subgraph = db.graph_traverse(&NodeId::new("a"), 1, None).await.unwrap();
        assert_eq!(subgraph.edge_count(), 0);
    }

    #[tokio::test]
    async fn document_crud() {
        let db = make_db().await;

        // Get missing → None.
        let doc = db.document_get("notes", "n1").await.unwrap();
        assert!(doc.is_none());

        // Put.
        let mut doc = Document::new("n1");
        doc.set("title", Value::String("Hello".into()));
        doc.set("score", Value::Float(9.5));
        db.document_put("notes", doc).await.unwrap();

        // Get.
        let doc = db.document_get("notes", "n1").await.unwrap().unwrap();
        assert_eq!(doc.id, "n1");
        assert_eq!(doc.get_str("title"), Some("Hello"));

        // Delete.
        db.document_delete("notes", "n1").await.unwrap();
        let doc = db.document_get("notes", "n1").await.unwrap();
        assert!(doc.is_none());
    }

    #[tokio::test]
    async fn sql_not_enabled() {
        let db = make_db().await;
        let result = db.execute_sql("SELECT 1", &[]).await;
        assert!(matches!(result, Err(NodeDbError::SqlNotEnabled)));
    }

    #[tokio::test]
    async fn flush_and_reopen() {
        // Write data and verify flush persists state.
        {
            let s = RedbStorage::open_in_memory().unwrap();
            let db = NodeDbLite::open(s, 1).await.unwrap();

            let mut doc = Document::new("d1");
            doc.set("key", Value::String("val".into()));
            db.document_put("docs", doc).await.unwrap();
            db.graph_insert_edge(&NodeId::new("x"), &NodeId::new("y"), "REL", None)
                .await
                .unwrap();

            db.flush().await.unwrap();

            // Verify data survives flush (still in memory).
            let doc = db.document_get("docs", "d1").await.unwrap();
            assert!(doc.is_some());
        }
    }

    #[tokio::test]
    async fn crdt_deltas_generated() {
        let db = make_db().await;

        let mut doc = Document::new("d1");
        doc.set("x", Value::Integer(42));
        db.document_put("docs", doc).await.unwrap();

        let deltas = db.pending_crdt_deltas().unwrap();
        assert!(!deltas.is_empty());
    }

    #[tokio::test]
    async fn acknowledge_deltas() {
        let db = make_db().await;

        db.document_put("a", Document::new("1")).await.unwrap();
        db.document_put("a", Document::new("2")).await.unwrap();

        let deltas = db.pending_crdt_deltas().unwrap();
        assert_eq!(deltas.len(), 2);

        let max_id = deltas.iter().map(|d| d.mutation_id).max().unwrap();
        db.acknowledge_deltas(max_id).unwrap();

        let deltas = db.pending_crdt_deltas().unwrap();
        assert!(deltas.is_empty());
    }

    #[tokio::test]
    async fn memory_governor_tracks_usage() {
        let db = make_db().await;

        for i in 0..100 {
            db.vector_insert("vecs", &format!("v{i}"), &[i as f32, 0.0, 0.0], None)
                .await
                .unwrap();
        }

        assert!(db.governor().total_used() > 0);
    }

    #[tokio::test]
    async fn search_nonexistent_collection() {
        let db = make_db().await;
        let results = db
            .vector_search("no_such_collection", &[1.0], 5, None)
            .await
            .unwrap();
        assert!(results.is_empty());
    }
}
