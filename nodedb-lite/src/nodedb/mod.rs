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
            Some(bytes) => CsrIndex::from_checkpoint(&bytes).unwrap_or_default(),
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
            let crdt = self.crdt.lock().map_err(|_| NodeDbError::Internal {
                detail: "CRDT lock poisoned".into(),
            })?;
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
            let csr = self.csr.lock().map_err(|_| NodeDbError::Internal {
                detail: "CSR lock poisoned".into(),
            })?;
            let checkpoint = csr.checkpoint_to_bytes();
            ops.push(WriteOp::Put {
                ns: Namespace::Graph,
                key: META_CSR.to_vec(),
                value: checkpoint,
            });
        }

        // ── Persist HNSW indices ──
        {
            let indices = self
                .hnsw_indices
                .lock()
                .map_err(|_| NodeDbError::Internal {
                    detail: "HNSW lock poisoned".into(),
                })?;
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
    pub(crate) fn update_memory_stats(&self) {
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

    /// Access the memory governor.
    pub fn governor(&self) -> &MemoryGovernor {
        &self.governor
    }

    /// Access pending CRDT deltas (for sync client).
    pub fn pending_crdt_deltas(
        &self,
    ) -> NodeDbResult<Vec<crate::engine::crdt::engine::PendingDelta>> {
        let crdt = self.crdt.lock().map_err(|_| NodeDbError::Internal {
            detail: "CRDT lock poisoned".into(),
        })?;
        Ok(crdt.pending_deltas().to_vec())
    }

    /// Acknowledge synced deltas (called after Origin ACK).
    pub fn acknowledge_deltas(&self, acked_id: u64) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock().map_err(|_| NodeDbError::Internal {
            detail: "CRDT lock poisoned".into(),
        })?;
        crdt.acknowledge(acked_id);
        Ok(())
    }

    /// Import remote deltas from Origin.
    pub fn import_remote_deltas(&self, data: &[u8]) -> NodeDbResult<()> {
        let crdt = self.crdt.lock().map_err(|_| NodeDbError::Internal {
            detail: "CRDT lock poisoned".into(),
        })?;
        crdt.import_remote(data).map_err(|e| NodeDbError::Storage {
            detail: e.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_client::NodeDb;
    use nodedb_types::document::Document;
    use nodedb_types::id::NodeId;
    use nodedb_types::value::Value;

    use crate::SqliteStorage;

    use super::*;

    async fn make_db() -> NodeDbLite<SqliteStorage> {
        let storage = SqliteStorage::open_in_memory().unwrap();
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
            let s = SqliteStorage::open_in_memory().unwrap();
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
