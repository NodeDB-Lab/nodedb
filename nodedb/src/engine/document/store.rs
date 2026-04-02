use crate::engine::sparse::btree::SparseEngine;

/// Index path declaration for automatic secondary index extraction.
///
/// Example: `IndexPath::new("$.user.email")` extracts the `user.email` field
/// from each document and indexes it in redb for efficient lookups.
#[derive(Debug, Clone)]
pub struct IndexPath {
    /// JSON-path-like expression (e.g., `$.user.email`, `$.tags[]`).
    pub path: String,
    /// Whether this path extracts array elements individually.
    pub is_array: bool,
}

impl IndexPath {
    pub fn new(path: &str) -> Self {
        let is_array = path.ends_with("[]");
        let path = path.strip_suffix("[]").unwrap_or(path).to_string();
        Self { path, is_array }
    }
}

/// Collection configuration for the Document Engine.
#[derive(Debug, Clone)]
pub struct CollectionConfig {
    pub name: String,
    /// Declared secondary index paths.
    pub index_paths: Vec<IndexPath>,
    /// Whether this collection uses CRDT-backed storage (Loro).
    pub crdt_enabled: bool,
    /// Storage encoding mode (schemaless MessagePack or strict Binary Tuple).
    pub storage_mode: crate::bridge::physical_plan::StorageMode,
    /// Accounting enforcement options.
    pub accounting: crate::bridge::physical_plan::AccountingOptions,
}

impl CollectionConfig {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            index_paths: Vec::new(),
            crdt_enabled: false,
            storage_mode: crate::bridge::physical_plan::StorageMode::Schemaless,
            accounting: crate::bridge::physical_plan::AccountingOptions::default(),
        }
    }

    pub fn with_index(mut self, path: &str) -> Self {
        self.index_paths.push(IndexPath::new(path));
        self
    }

    pub fn with_crdt(mut self) -> Self {
        self.crdt_enabled = true;
        self
    }

    pub fn with_storage_mode(mut self, mode: crate::bridge::physical_plan::StorageMode) -> Self {
        self.storage_mode = mode;
        self
    }
}

/// Document Engine.
///
/// NOT a separate storage engine — extends the existing SparseEngine (redb)
/// with MessagePack encoding and automatic secondary index extraction.
///
/// Documents are stored as MessagePack-encoded blobs keyed by
/// `(collection, doc_id)`. On write, declared index paths are extracted
/// and stored in redb B-Trees as `(collection, path, value) -> doc_id`.
pub struct DocumentEngine<'a> {
    sparse: &'a SparseEngine,
    tenant_id: u32,
    configs: std::collections::HashMap<String, CollectionConfig>,
}

impl<'a> DocumentEngine<'a> {
    pub fn new(sparse: &'a SparseEngine, tenant_id: u32) -> Self {
        Self {
            sparse,
            tenant_id,
            configs: std::collections::HashMap::new(),
        }
    }

    /// Register a collection configuration with index paths.
    pub fn register_collection(&mut self, config: CollectionConfig) {
        self.configs.insert(config.name.clone(), config);
    }

    /// Put a document (JSON value) into a collection.
    ///
    /// The document is serialized to MessagePack and stored in redb.
    /// Secondary indexes are extracted and written atomically.
    pub fn put(
        &self,
        collection: &str,
        doc_id: &str,
        document: &serde_json::Value,
    ) -> crate::Result<()> {
        // Serialize to MessagePack.
        let msgpack = json_to_msgpack(document);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("encode: {e}"),
        })?;

        // Store the document blob.
        self.sparse.put(self.tenant_id, collection, doc_id, &buf)?;

        // Extract and write secondary indexes.
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
    ///
    /// Useful when the caller already has MessagePack data (e.g., from network).
    pub fn put_raw(
        &self,
        collection: &str,
        doc_id: &str,
        msgpack_bytes: &[u8],
    ) -> crate::Result<()> {
        self.sparse
            .put(self.tenant_id, collection, doc_id, msgpack_bytes)?;

        // Extract indexes from the MessagePack blob.
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
    pub fn delete(&self, collection: &str, doc_id: &str) -> crate::Result<bool> {
        // Clean up secondary index entries for this document before deleting it.
        self.sparse
            .delete_indexes_for_document(self.tenant_id, collection, doc_id)?;
        self.sparse.delete(self.tenant_id, collection, doc_id)
    }

    /// Drop all secondary index entries for a field across the entire collection.
    ///
    /// Used by ALTER COLLECTION DROP INDEX. Returns the number of entries removed.
    pub fn drop_field_index(&self, collection: &str, field: &str) -> crate::Result<usize> {
        self.sparse
            .delete_index_entries_for_field(self.tenant_id, collection, field)
    }

    /// Lookup documents by a secondary index value.
    ///
    /// Returns document IDs matching `path = value` in the given collection.
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
            // Key format: "{tenant_id}:{collection}:{path}:{value}:{doc_id}"
            // After range_scan, we get the full key; extract doc_id.
            if let Some(doc_id) = key.rsplit(':').next() {
                // Verify the value portion matches exactly (not just prefix).
                let expected_prefix = format!("{}:{collection}:{path}:{value}:", self.tenant_id);
                if key.starts_with(&expected_prefix) {
                    doc_ids.push(doc_id.to_string());
                }
            }
        }
        Ok(doc_ids)
    }
}

/// Strip the leading `$.` or `$` prefix from a JSON path expression.
pub(crate) fn normalize_path(path: &str) -> &str {
    path.strip_prefix("$.")
        .or_else(|| path.strip_prefix('$'))
        .unwrap_or(path)
}

/// Extract scalar values at a JSON path for secondary indexing.
pub(crate) fn extract_index_values(
    doc: &serde_json::Value,
    path: &str,
    is_array: bool,
) -> Vec<String> {
    let path = normalize_path(path);
    let target = navigate_json(doc, path);

    match target {
        Some(serde_json::Value::Array(arr)) if is_array => {
            arr.iter().filter_map(json_scalar_to_string).collect()
        }
        Some(val) => json_scalar_to_string(val).into_iter().collect(),
        None => Vec::new(),
    }
}

/// Extract values from an rmpv::Value at a path.
fn extract_index_values_rmpv(value: &rmpv::Value, path: &str, is_array: bool) -> Vec<String> {
    let path = normalize_path(path);
    let target = navigate_rmpv(value, path);

    match target {
        Some(rmpv::Value::Array(arr)) if is_array => {
            arr.iter().filter_map(rmpv_scalar_to_string).collect()
        }
        Some(val) => rmpv_scalar_to_string(val).into_iter().collect(),
        None => Vec::new(),
    }
}

fn navigate_json<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    if path.is_empty() {
        return Some(value);
    }
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

fn navigate_rmpv<'a>(value: &'a rmpv::Value, path: &str) -> Option<&'a rmpv::Value> {
    if path.is_empty() {
        return Some(value);
    }
    let mut current = value;
    for segment in path.split('.') {
        let map = current.as_map()?;
        let mut found = false;
        for (k, v) in map {
            if k.as_str() == Some(segment) {
                current = v;
                found = true;
                break;
            }
        }
        if !found {
            return None;
        }
    }
    Some(current)
}

fn json_scalar_to_string(val: &serde_json::Value) -> Option<String> {
    match val {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn rmpv_scalar_to_string(val: &rmpv::Value) -> Option<String> {
    match val {
        rmpv::Value::String(s) => Some(s.as_str()?.to_string()),
        rmpv::Value::Integer(i) => Some(i.to_string()),
        rmpv::Value::Boolean(b) => Some(b.to_string()),
        rmpv::Value::F32(f) => Some(f.to_string()),
        rmpv::Value::F64(f) => Some(f.to_string()),
        _ => None,
    }
}

/// Convert serde_json::Value to rmpv::Value for MessagePack serialization.
pub(crate) fn json_to_msgpack(val: &serde_json::Value) -> rmpv::Value {
    match val {
        serde_json::Value::Null => rmpv::Value::Nil,
        serde_json::Value::Bool(b) => rmpv::Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rmpv::Value::Integer(rmpv::Integer::from(i))
            } else if let Some(u) = n.as_u64() {
                rmpv::Value::Integer(rmpv::Integer::from(u))
            } else {
                rmpv::Value::F64(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => rmpv::Value::String(rmpv::Utf8String::from(s.as_str())),
        serde_json::Value::Array(arr) => {
            rmpv::Value::Array(arr.iter().map(json_to_msgpack).collect())
        }
        serde_json::Value::Object(obj) => rmpv::Value::Map(
            obj.iter()
                .map(|(k, v)| {
                    (
                        rmpv::Value::String(rmpv::Utf8String::from(k.as_str())),
                        json_to_msgpack(v),
                    )
                })
                .collect(),
        ),
    }
}

/// Convert rmpv::Value back to serde_json::Value.
fn rmpv_to_json(val: &rmpv::Value) -> serde_json::Value {
    match val {
        rmpv::Value::Nil => serde_json::Value::Null,
        rmpv::Value::Boolean(b) => serde_json::Value::Bool(*b),
        rmpv::Value::Integer(i) => {
            if let Some(n) = i.as_i64() {
                serde_json::json!(n)
            } else if let Some(n) = i.as_u64() {
                serde_json::json!(n)
            } else {
                serde_json::Value::Null
            }
        }
        rmpv::Value::F32(f) => serde_json::json!(*f),
        rmpv::Value::F64(f) => serde_json::json!(*f),
        rmpv::Value::String(s) => serde_json::Value::String(s.as_str().unwrap_or("").to_string()),
        rmpv::Value::Binary(b) => serde_json::Value::String(base64_encode(b)),
        rmpv::Value::Array(arr) => serde_json::Value::Array(arr.iter().map(rmpv_to_json).collect()),
        rmpv::Value::Map(entries) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in entries {
                let key = match k {
                    rmpv::Value::String(s) => s.as_str().unwrap_or("").to_string(),
                    other => format!("{other}"),
                };
                obj.insert(key, rmpv_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        rmpv::Value::Ext(_, _) => serde_json::Value::Null,
    }
}

fn base64_encode(data: &[u8]) -> String {
    // Simple base64 encoding without pulling in a dependency.
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((n >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((n >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((n >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(n & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

#[cfg(test)]
mod tests {
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

        // Lookup by email index.
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
    fn msgpack_is_compact() {
        let doc = serde_json::json!({
            "name": "Alice Wonderland",
            "age": 30,
            "tags": ["admin", "user"],
            "active": true
        });

        let json_bytes = serde_json::to_vec(&doc).unwrap();
        let rmpv_val = json_to_msgpack(&doc);
        let mut msgpack_bytes = Vec::new();
        rmpv::encode::write_value(&mut msgpack_bytes, &rmpv_val).unwrap();

        // MessagePack should be more compact than JSON.
        assert!(
            msgpack_bytes.len() < json_bytes.len(),
            "msgpack {} bytes vs json {} bytes",
            msgpack_bytes.len(),
            json_bytes.len()
        );
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

    #[test]
    fn json_msgpack_roundtrip_preserves_types() {
        let doc = serde_json::json!({
            "string": "hello",
            "int": 42,
            "float": 1.234,
            "bool": true,
            "null": null,
            "array": [1, "two", false],
            "nested": {"a": {"b": 3}}
        });

        let rmpv_val = json_to_msgpack(&doc);
        let recovered = rmpv_to_json(&rmpv_val);

        assert_eq!(recovered["string"], "hello");
        assert_eq!(recovered["int"], 42);
        assert_eq!(recovered["bool"], true);
        assert!(recovered["null"].is_null());
        assert_eq!(recovered["array"][0], 1);
        assert_eq!(recovered["array"][1], "two");
        assert_eq!(recovered["nested"]["a"]["b"], 3);
    }
}
