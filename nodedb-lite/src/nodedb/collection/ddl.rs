//! Collection DDL: create, drop, list collections with metadata.

use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::super::{LockExt, NodeDbLite};
use crate::storage::engine::StorageEngine;

/// Collection metadata stored in redb.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CollectionMeta {
    pub name: String,
    pub collection_type: String,
    pub created_at_ms: u64,
    pub fields: Vec<(String, String)>,
    /// Optional JSON-serialized engine config (e.g., `KvConfig` for KV collections,
    /// `StrictSchema` for strict collections). Empty for schemaless document collections.
    #[serde(default)]
    pub config_json: Option<String>,
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Create a collection with optional schema.
    ///
    /// If the collection already exists, returns Ok (idempotent).
    /// Schema is advisory — documents are schemaless by default.
    pub async fn create_collection(
        &self,
        name: &str,
        fields: &[(String, String)],
    ) -> NodeDbResult<()> {
        let meta = CollectionMeta {
            name: name.to_string(),
            collection_type: "document".to_string(),
            created_at_ms: now_ms(),
            fields: fields.to_vec(),
            config_json: None,
        };
        let key = format!("collection:{name}");
        let bytes = serde_json::to_vec(&meta).map_err(|e| NodeDbError::storage(e.to_string()))?;
        self.storage
            .put(nodedb_types::Namespace::Meta, key.as_bytes(), &bytes)
            .await?;
        Ok(())
    }

    /// Create a KV collection with typed schema and optional TTL.
    ///
    /// Stores the `KvConfig` as JSON in the collection metadata so that the
    /// KV engine can reconstruct the schema on startup.
    pub async fn create_kv_collection(
        &self,
        name: &str,
        config: &nodedb_types::KvConfig,
    ) -> NodeDbResult<()> {
        let fields: Vec<(String, String)> = config
            .schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.column_type.to_string()))
            .collect();

        let config_json =
            serde_json::to_string(config).map_err(|e| NodeDbError::storage(e.to_string()))?;

        let meta = CollectionMeta {
            name: name.to_string(),
            collection_type: "kv".to_string(),
            created_at_ms: now_ms(),
            fields,
            config_json: Some(config_json),
        };
        let key = format!("collection:{name}");
        let bytes = serde_json::to_vec(&meta).map_err(|e| NodeDbError::storage(e.to_string()))?;
        self.storage
            .put(nodedb_types::Namespace::Meta, key.as_bytes(), &bytes)
            .await?;
        Ok(())
    }

    /// Drop a collection — deletes all documents and metadata.
    ///
    /// Uses `clear_collection` for single-batch deletion (one Loro delta
    /// for all document removals). Also removes the text index.
    pub async fn drop_collection(&self, name: &str) -> NodeDbResult<()> {
        // Batch-delete all documents in one delta.
        {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.clear_collection(name).map_err(NodeDbError::storage)?;
        }

        // Remove text index for this collection.
        {
            let mut indices = self.text_indices.lock().unwrap_or_else(|p| p.into_inner());
            indices.remove(name);
        }

        // Delete collection metadata from redb.
        let key = format!("collection:{name}");
        self.storage
            .delete(nodedb_types::Namespace::Meta, key.as_bytes())
            .await?;
        Ok(())
    }

    /// List all collections.
    pub async fn list_collections(&self) -> NodeDbResult<Vec<CollectionMeta>> {
        let pairs = self
            .storage
            .scan_prefix(nodedb_types::Namespace::Meta, b"collection:")
            .await?;
        let mut result = Vec::new();
        for (_, value) in &pairs {
            if let Ok(meta) = serde_json::from_slice::<CollectionMeta>(value) {
                result.push(meta);
            }
        }
        // Also include implicit collections (from CRDT state without explicit DDL).
        let crdt = self.crdt.lock_or_recover();
        let crdt_names = crdt.collection_names();
        let explicit: std::collections::HashSet<String> =
            result.iter().map(|m| m.name.clone()).collect();
        for name in crdt_names {
            if !name.starts_with("__") && !explicit.contains(&name) {
                result.push(CollectionMeta {
                    name,
                    collection_type: "document".to_string(),
                    created_at_ms: 0,
                    fields: Vec::new(),
                    config_json: None,
                });
            }
        }
        Ok(result)
    }
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
