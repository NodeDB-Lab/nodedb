//! `DocumentEngine` — MessagePack document storage with secondary indexing.
//!
//! NOT a separate storage engine — extends the existing [`SparseEngine`] (redb)
//! with MessagePack encoding and automatic secondary index extraction.
//!
//! Documents are stored as MessagePack-encoded blobs keyed by
//! `(collection, doc_id)`. On write, declared index paths are extracted
//! and stored in redb B-Trees as `(collection, path, value) -> doc_id`.

use std::collections::HashMap;

use crate::engine::sparse::btree::SparseEngine;

use super::config::CollectionConfig;
use super::extract::{
    extract_index_values, extract_index_values_rmpv, json_to_msgpack, rmpv_to_json,
};

pub struct DocumentEngine<'a> {
    sparse: &'a SparseEngine,
    tenant_id: u32,
    configs: HashMap<String, CollectionConfig>,
}

impl<'a> DocumentEngine<'a> {
    pub fn new(sparse: &'a SparseEngine, tenant_id: u32) -> Self {
        Self {
            sparse,
            tenant_id,
            configs: HashMap::new(),
        }
    }

    /// Register a collection configuration with index paths.
    pub fn register_collection(&mut self, config: CollectionConfig) {
        self.configs.insert(config.name.clone(), config);
    }

    /// Put a document (JSON value) into a collection.
    pub fn put(
        &self,
        collection: &str,
        doc_id: &str,
        document: &serde_json::Value,
    ) -> crate::Result<()> {
        let msgpack = json_to_msgpack(document);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("encode: {e}"),
        })?;

        self.sparse.put(self.tenant_id, collection, doc_id, &buf)?;

        if let Some(config) = self.configs.get(collection) {
            for index_path in &config.index_paths {
                let values = extract_index_values(document, &index_path.path, index_path.is_array);
                for value in values {
                    self.sparse.index_put(
                        self.tenant_id,
                        collection,
                        &index_path.path,
                        &value,
                        doc_id,
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Put a document from raw MessagePack bytes.
    pub fn put_raw(
        &self,
        collection: &str,
        doc_id: &str,
        msgpack_bytes: &[u8],
    ) -> crate::Result<()> {
        self.sparse
            .put(self.tenant_id, collection, doc_id, msgpack_bytes)?;

        if let Some(config) = self.configs.get(collection)
            && let Ok(value) = rmpv::decode::read_value(&mut &msgpack_bytes[..])
        {
            for index_path in &config.index_paths {
                let values =
                    extract_index_values_rmpv(&value, &index_path.path, index_path.is_array);
                for v in values {
                    self.sparse.index_put(
                        self.tenant_id,
                        collection,
                        &index_path.path,
                        &v,
                        doc_id,
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Get a document and deserialize from MessagePack to JSON.
    pub fn get(&self, collection: &str, doc_id: &str) -> crate::Result<Option<serde_json::Value>> {
        match self.sparse.get(self.tenant_id, collection, doc_id)? {
            Some(bytes) => {
                let rmpv_val = rmpv::decode::read_value(&mut bytes.as_slice()).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("decode: {e}"),
                    }
                })?;
                Ok(Some(rmpv_to_json(&rmpv_val)))
            }
            None => Ok(None),
        }
    }

    /// Get raw MessagePack bytes (zero-copy path for DataFusion UDFs).
    pub fn get_raw(&self, collection: &str, doc_id: &str) -> crate::Result<Option<Vec<u8>>> {
        self.sparse.get(self.tenant_id, collection, doc_id)
    }

    /// Delete a document and its secondary index entries.
    ///
    /// Returns whether a row was actually removed. The prior bytes returned
    /// by `sparse.delete` are not needed at this boundary.
    pub fn delete(&self, collection: &str, doc_id: &str) -> crate::Result<bool> {
        self.sparse
            .delete_indexes_for_document(self.tenant_id, collection, doc_id)?;
        Ok(self
            .sparse
            .delete(self.tenant_id, collection, doc_id)?
            .is_some())
    }

    /// Drop all secondary index entries for a field across the entire collection.
    pub fn drop_field_index(&self, collection: &str, field: &str) -> crate::Result<usize> {
        self.sparse
            .delete_index_entries_for_field(self.tenant_id, collection, field)
    }

    /// Lookup documents by a secondary index value.
    pub fn index_lookup(
        &self,
        collection: &str,
        path: &str,
        value: &str,
    ) -> crate::Result<Vec<String>> {
        let prefix_with_value = format!("{value}:");
        let results = self.sparse.range_scan(
            self.tenant_id,
            collection,
            path,
            Some(prefix_with_value.as_bytes()),
            None,
            1000,
        )?;

        let mut doc_ids = Vec::new();
        for (key, _) in results {
            if let Some(doc_id) = key.rsplit(':').next() {
                let expected_prefix = format!("{}:{collection}:{path}:{value}:", self.tenant_id);
                if key.starts_with(&expected_prefix) {
                    doc_ids.push(doc_id.to_string());
                }
            }
        }
        Ok(doc_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::super::extract::json_to_msgpack;
    use super::*;

    fn make_engine() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = SparseEngine::open(&dir.path().join("doc.redb")).unwrap();
        (engine, dir)
    }

    #[test]
    fn put_and_get_document() {
        let (sparse, _dir) = make_engine();
        let doc_engine = DocumentEngine::new(&sparse, 1);

        let doc = serde_json::json!({
            "name": "Alice",
            "email": "alice@example.com",
            "age": 30
        });

        doc_engine.put("users", "u1", &doc).unwrap();
        let retrieved = doc_engine.get("users", "u1").unwrap().unwrap();

        assert_eq!(retrieved["name"], "Alice");
        assert_eq!(retrieved["email"], "alice@example.com");
        assert_eq!(retrieved["age"], 30);
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let (sparse, _dir) = make_engine();
        let doc_engine = DocumentEngine::new(&sparse, 1);
        assert!(doc_engine.get("users", "missing").unwrap().is_none());
    }

    #[test]
    fn delete_document() {
        let (sparse, _dir) = make_engine();
        let doc_engine = DocumentEngine::new(&sparse, 1);

        let doc = serde_json::json!({"name": "Bob"});
        doc_engine.put("users", "u1", &doc).unwrap();
        assert!(doc_engine.delete("users", "u1").unwrap());
        assert!(doc_engine.get("users", "u1").unwrap().is_none());
    }

    #[test]
    fn overwrite_document() {
        let (sparse, _dir) = make_engine();
        let doc_engine = DocumentEngine::new(&sparse, 1);

        doc_engine
            .put("users", "u1", &serde_json::json!({"v": 1}))
            .unwrap();
        doc_engine
            .put("users", "u1", &serde_json::json!({"v": 2}))
            .unwrap();

        let doc = doc_engine.get("users", "u1").unwrap().unwrap();
        assert_eq!(doc["v"], 2);
    }

    #[test]
    fn secondary_index_extraction() {
        let (sparse, _dir) = make_engine();
        let mut doc_engine = DocumentEngine::new(&sparse, 1);

        doc_engine.register_collection(CollectionConfig::new("users").with_index("$.email"));

        doc_engine
            .put(
                "users",
                "u1",
                &serde_json::json!({"name": "Alice", "email": "alice@example.com"}),
            )
            .unwrap();
        doc_engine
            .put(
                "users",
                "u2",
                &serde_json::json!({"name": "Bob", "email": "bob@example.com"}),
            )
            .unwrap();

        let results = doc_engine
            .index_lookup("users", "$.email", "alice@example.com")
            .unwrap();
        assert_eq!(results, vec!["u1"]);
    }

    #[test]
    fn array_index_extraction() {
        let (sparse, _dir) = make_engine();
        let mut doc_engine = DocumentEngine::new(&sparse, 1);

        doc_engine.register_collection(CollectionConfig::new("users").with_index("$.tags[]"));

        doc_engine
            .put(
                "users",
                "u1",
                &serde_json::json!({"name": "Alice", "tags": ["admin", "editor"]}),
            )
            .unwrap();

        let results = doc_engine.index_lookup("users", "$.tags", "admin").unwrap();
        assert_eq!(results, vec!["u1"]);

        let results = doc_engine
            .index_lookup("users", "$.tags", "editor")
            .unwrap();
        assert_eq!(results, vec!["u1"]);
    }

    #[test]
    fn nested_field_index() {
        let (sparse, _dir) = make_engine();
        let mut doc_engine = DocumentEngine::new(&sparse, 1);

        doc_engine.register_collection(CollectionConfig::new("docs").with_index("$.metadata.lang"));

        doc_engine
            .put(
                "docs",
                "d1",
                &serde_json::json!({"title": "Hello", "metadata": {"lang": "en"}}),
            )
            .unwrap();

        let results = doc_engine
            .index_lookup("docs", "$.metadata.lang", "en")
            .unwrap();
        assert_eq!(results, vec!["d1"]);
    }

    #[test]
    fn raw_msgpack_roundtrip() {
        let (sparse, _dir) = make_engine();
        let doc_engine = DocumentEngine::new(&sparse, 1);

        let doc = serde_json::json!({"key": "value", "num": 42});
        let rmpv_val = json_to_msgpack(&doc);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &rmpv_val).unwrap();

        doc_engine.put_raw("col", "id1", &buf).unwrap();

        let raw = doc_engine.get_raw("col", "id1").unwrap().unwrap();
        assert_eq!(raw, buf);

        let decoded = doc_engine.get("col", "id1").unwrap().unwrap();
        assert_eq!(decoded["key"], "value");
        assert_eq!(decoded["num"], 42);
    }

    #[test]
    fn collections_are_isolated() {
        let (sparse, _dir) = make_engine();
        let doc_engine = DocumentEngine::new(&sparse, 1);

        doc_engine
            .put("users", "id1", &serde_json::json!({"type": "user"}))
            .unwrap();
        doc_engine
            .put("orders", "id1", &serde_json::json!({"type": "order"}))
            .unwrap();

        let user = doc_engine.get("users", "id1").unwrap().unwrap();
        let order = doc_engine.get("orders", "id1").unwrap().unwrap();
        assert_eq!(user["type"], "user");
        assert_eq!(order["type"], "order");
    }

    #[test]
    fn put_raw_with_index_extraction() {
        let (sparse, _dir) = make_engine();
        let mut doc_engine = DocumentEngine::new(&sparse, 1);

        doc_engine.register_collection(CollectionConfig::new("items").with_index("$.category"));

        let doc = serde_json::json!({"name": "Widget", "category": "tools"});
        let rmpv_val = json_to_msgpack(&doc);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &rmpv_val).unwrap();

        doc_engine.put_raw("items", "i1", &buf).unwrap();

        let results = doc_engine
            .index_lookup("items", "$.category", "tools")
            .unwrap();
        assert_eq!(results, vec!["i1"]);
    }
}
