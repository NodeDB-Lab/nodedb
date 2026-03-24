//! JavaScript/TypeScript bindings for NodeDB-Lite via wasm-bindgen.
//!
//! Uses `redb` for storage — same engine as native.
//! - `open()` — in-memory (no persistence across page reloads)
//! - `openPersistent()` — OPFS-backed (data survives reloads, Web Worker only)
//!
//! ```js
//! // In-memory:
//! const db = await NodeDbLiteWasm.open(1n);
//!
//! // Persistent (must run in a Web Worker):
//! const db = await NodeDbLiteWasm.openPersistent("mydb.redb", 1n);
//! ```

pub mod opfs_backend;

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, RedbStorage};
use nodedb_types::document::Document;
use nodedb_types::id::NodeId;
use nodedb_types::value::Value;

/// NodeDB-Lite instance for browser/WASM environments.
#[wasm_bindgen]
pub struct NodeDbLiteWasm {
    db: NodeDbLite<RedbStorage>,
}

#[wasm_bindgen]
impl NodeDbLiteWasm {
    /// Create a new in-memory NodeDB-Lite database (no persistence).
    #[wasm_bindgen]
    pub async fn open(peer_id: u64) -> Result<NodeDbLiteWasm, JsError> {
        let storage = RedbStorage::open_in_memory().map_err(|e| JsError::new(&e.to_string()))?;
        let db = NodeDbLite::open(storage, peer_id)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        Ok(Self { db })
    }

    /// Create a persistent NodeDB-Lite database backed by OPFS.
    ///
    /// **Must run in a Web Worker** — OPFS SyncAccessHandle is not
    /// available on the main thread.
    ///
    /// Data survives page reloads and browser restarts.
    #[wasm_bindgen(js_name = "openPersistent")]
    pub async fn open_persistent(filename: &str, peer_id: u64) -> Result<NodeDbLiteWasm, JsError> {
        // Get OPFS root directory.
        let global: web_sys::WorkerGlobalScope = js_sys::global()
            .dyn_into()
            .map_err(|_| JsError::new("openPersistent must be called from a Web Worker"))?;
        let storage = global.navigator().storage();
        let root: web_sys::FileSystemDirectoryHandle = JsFuture::from(storage.get_directory())
            .await
            .map_err(|e| JsError::new(&format!("OPFS getDirectory failed: {e:?}")))?
            .dyn_into()
            .map_err(|_| JsError::new("expected FileSystemDirectoryHandle"))?;

        // Get or create the database file.
        let opts = web_sys::FileSystemGetFileOptions::new();
        opts.set_create(true);
        let file_handle: web_sys::FileSystemFileHandle =
            JsFuture::from(root.get_file_handle_with_options(filename, &opts))
                .await
                .map_err(|e| JsError::new(&format!("OPFS getFileHandle failed: {e:?}")))?
                .dyn_into()
                .map_err(|_| JsError::new("expected FileSystemFileHandle"))?;

        // Create sync access handle.
        let sync_handle: web_sys::FileSystemSyncAccessHandle =
            JsFuture::from(file_handle.create_sync_access_handle())
                .await
                .map_err(|e| JsError::new(&format!("OPFS createSyncAccessHandle failed: {e:?}")))?
                .dyn_into()
                .map_err(|_| JsError::new("expected FileSystemSyncAccessHandle"))?;

        // Create redb with OPFS backend.
        let backend = opfs_backend::OpfsBackend::new(sync_handle);
        let db_inner = redb::Database::builder()
            .create_with_backend(backend)
            .map_err(|e| JsError::new(&format!("redb create with OPFS failed: {e}")))?;

        // Wrap in RedbStorage.
        let storage = RedbStorage::from_database(db_inner);
        let db = NodeDbLite::open(storage, peer_id)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        Ok(Self { db })
    }

    /// Insert a vector into a collection.
    #[wasm_bindgen(js_name = "vectorInsert")]
    pub async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
    ) -> Result<(), JsError> {
        self.db
            .vector_insert(collection, id, embedding, None)
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// Search for the k nearest vectors. Returns JSON array.
    #[wasm_bindgen(js_name = "vectorSearch")]
    pub async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
    ) -> Result<JsValue, JsError> {
        let results = self
            .db
            .vector_search(collection, query, k, None)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;

        let json: Vec<serde_json::Value> = results
            .iter()
            .map(|r| serde_json::json!({"id": r.id, "distance": r.distance}))
            .collect();

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Delete a vector by ID.
    #[wasm_bindgen(js_name = "vectorDelete")]
    pub async fn vector_delete(&self, collection: &str, id: &str) -> Result<(), JsError> {
        self.db
            .vector_delete(collection, id)
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// Insert a directed graph edge.
    #[wasm_bindgen(js_name = "graphInsertEdge")]
    pub async fn graph_insert_edge(
        &self,
        from: &str,
        to: &str,
        edge_type: &str,
    ) -> Result<String, JsError> {
        let from_id = NodeId::new(from);
        let to_id = NodeId::new(to);
        let edge_id = self
            .db
            .graph_insert_edge(&from_id, &to_id, edge_type, None)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;
        Ok(edge_id.as_str().to_string())
    }

    /// Traverse the graph from a start node. Returns JSON.
    #[wasm_bindgen(js_name = "graphTraverse")]
    pub async fn graph_traverse(&self, start: &str, depth: u8) -> Result<JsValue, JsError> {
        let start_id = NodeId::new(start);
        let subgraph = self
            .db
            .graph_traverse(&start_id, depth, None)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;

        let json = serde_json::json!({
            "nodes": subgraph.nodes.iter().map(|n| serde_json::json!({
                "id": n.id.as_str(),
                "depth": n.depth,
            })).collect::<Vec<_>>(),
            "edges": subgraph.edges.iter().map(|e| serde_json::json!({
                "from": e.from.as_str(),
                "to": e.to.as_str(),
                "label": e.label,
            })).collect::<Vec<_>>(),
        });

        serde_wasm_bindgen::to_value(&json).map_err(|e| JsError::new(&e.to_string()))
    }

    /// Get a document by ID. Returns JSON or null.
    #[wasm_bindgen(js_name = "documentGet")]
    pub async fn document_get(&self, collection: &str, id: &str) -> Result<JsValue, JsError> {
        let doc = self
            .db
            .document_get(collection, id)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;

        match doc {
            Some(d) => serde_wasm_bindgen::to_value(&d).map_err(|e| JsError::new(&e.to_string())),
            None => Ok(JsValue::NULL),
        }
    }

    /// Put (insert or update) a document. Takes a JSON string of fields.
    ///
    /// If `id` is empty, a UUIDv7 is auto-generated.
    /// Returns the document ID (useful when auto-generated).
    #[wasm_bindgen(js_name = "documentPut")]
    pub async fn document_put(
        &self,
        collection: &str,
        id: &str,
        fields_json: &str,
    ) -> Result<String, JsError> {
        let fields: std::collections::HashMap<String, Value> =
            serde_json::from_str(fields_json).map_err(|e| JsError::new(&e.to_string()))?;

        let doc_id = if id.is_empty() {
            nodedb_types::id_gen::uuid_v7()
        } else {
            id.to_string()
        };

        let mut doc = Document::new(&doc_id);
        for (k, v) in fields {
            doc.set(k, v);
        }

        self.db
            .document_put(collection, doc)
            .await
            .map_err(|e| JsError::new(&e.to_string()))?;

        Ok(doc_id)
    }

    /// Delete a document by ID.
    #[wasm_bindgen(js_name = "documentDelete")]
    pub async fn document_delete(&self, collection: &str, id: &str) -> Result<(), JsError> {
        self.db
            .document_delete(collection, id)
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }

    /// Flush all in-memory state to storage.
    #[wasm_bindgen]
    pub async fn flush(&self) -> Result<(), JsError> {
        self.db
            .flush()
            .await
            .map_err(|e| JsError::new(&e.to_string()))
    }

    // ─── ID Generation ──────────────────────────────────────────────

    /// Generate a UUIDv7 (time-sortable, recommended for primary keys).
    #[wasm_bindgen(js_name = "generateId")]
    pub fn generate_id() -> String {
        nodedb_types::id_gen::uuid_v7()
    }

    /// Generate an ID of the specified type.
    ///
    /// Supported types: "uuidv7", "uuidv4", "ulid", "cuid2", "nanoid".
    #[wasm_bindgen(js_name = "generateIdTyped")]
    pub fn generate_id_typed(id_type: &str) -> Result<String, JsError> {
        nodedb_types::id_gen::generate_by_type(id_type).ok_or_else(|| {
            JsError::new(&format!(
                "unknown ID type '{id_type}': use uuidv7, uuidv4, ulid, cuid2, or nanoid"
            ))
        })
    }
}
