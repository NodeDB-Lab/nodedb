// SPDX-License-Identifier: BUSL-1.1

//! `DocumentEngine` struct, constructor, registration, and index lookups.

use std::collections::HashMap;

use nodedb_types::OrdinalClock;

use crate::engine::document::store::StorageKey;
use crate::engine::document::store::config::CollectionConfig;
use crate::engine::sparse::btree::SparseEngine;

pub struct DocumentEngine<'a> {
    pub(super) sparse: &'a SparseEngine,
    pub(super) database_id: u64,
    pub(super) tenant_id: u64,
    pub(super) configs: HashMap<String, CollectionConfig>,
    /// System-time ordinals for this struct API's writes. The `CoreLoop`
    /// write paths stamp from the core's shared clock instead, so this clock
    /// never reaches a Calvin or replicated write.
    pub(super) clock: OrdinalClock,
}

impl<'a> DocumentEngine<'a> {
    pub fn new(sparse: &'a SparseEngine, database_id: u64, tenant_id: u64) -> Self {
        Self {
            sparse,
            database_id,
            tenant_id,
            configs: HashMap::new(),
            clock: OrdinalClock::new(),
        }
    }

    /// Register a collection configuration with index paths.
    pub fn register_collection(&mut self, config: CollectionConfig) {
        self.configs.insert(config.name.clone(), config);
    }

    /// Drop all secondary index entries for a field across the entire
    /// collection: every value, every row, every version.
    pub fn drop_field_index(&self, collection: &str, field: &str) -> crate::Result<usize> {
        let txn = self.sparse.begin_write()?;
        let removed = self.sparse.versioned_index_drop_field_in_txn(
            &txn,
            self.database_id,
            self.tenant_id,
            collection,
            field,
        )?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("commit index drop: {e}"),
        })?;
        Ok(removed)
    }

    /// The documents whose version visible at `sys_cutoff` (`None` =
    /// current) holds exactly `value` under the index `path`, in doc-id
    /// order. Every match is returned.
    pub fn index_lookup(
        &self,
        collection: &str,
        path: &str,
        value: &str,
        sys_cutoff: Option<i64>,
    ) -> crate::Result<Vec<StorageKey>> {
        self.sparse.versioned_index_lookup_as_of(
            self.database_id,
            self.tenant_id,
            collection,
            path,
            value,
            sys_cutoff,
        )
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use crate::engine::document::store::extract::json_to_msgpack;

    use super::*;

    fn make_engine() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = SparseEngine::open(&dir.path().join("doc.redb")).unwrap();
        (engine, dir)
    }

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(surrogate))
    }

    #[test]
    fn secondary_index_extraction() {
        let (sparse, _dir) = make_engine();
        let mut doc_engine = DocumentEngine::new(&sparse, 0, 1);

        doc_engine.register_collection(CollectionConfig::new("users").with_index("$.email"));

        doc_engine
            .put(
                "users",
                &key(1),
                &serde_json::json!({"name": "Alice", "email": "alice@example.com"}),
            )
            .unwrap();
        doc_engine
            .put(
                "users",
                &key(2),
                &serde_json::json!({"name": "Bob", "email": "bob@example.com"}),
            )
            .unwrap();

        let results = doc_engine
            .index_lookup("users", "$.email", "alice@example.com", None)
            .unwrap();
        assert_eq!(results, vec![key(1)]);
    }

    #[test]
    fn array_index_extraction() {
        let (sparse, _dir) = make_engine();
        let mut doc_engine = DocumentEngine::new(&sparse, 0, 1);

        doc_engine.register_collection(CollectionConfig::new("users").with_index("$.tags[]"));

        doc_engine
            .put(
                "users",
                &key(1),
                &serde_json::json!({"name": "Alice", "tags": ["admin", "editor"]}),
            )
            .unwrap();

        let results = doc_engine
            .index_lookup("users", "$.tags", "admin", None)
            .unwrap();
        assert_eq!(results, vec![key(1)]);

        let results = doc_engine
            .index_lookup("users", "$.tags", "editor", None)
            .unwrap();
        assert_eq!(results, vec![key(1)]);
    }

    #[test]
    fn nested_field_index() {
        let (sparse, _dir) = make_engine();
        let mut doc_engine = DocumentEngine::new(&sparse, 0, 1);

        doc_engine.register_collection(CollectionConfig::new("docs").with_index("$.metadata.lang"));

        doc_engine
            .put(
                "docs",
                &key(1),
                &serde_json::json!({"title": "Hello", "metadata": {"lang": "en"}}),
            )
            .unwrap();

        let results = doc_engine
            .index_lookup("docs", "$.metadata.lang", "en", None)
            .unwrap();
        assert_eq!(results, vec![key(1)]);
    }

    #[test]
    fn put_raw_with_index_extraction() {
        let (sparse, _dir) = make_engine();
        let mut doc_engine = DocumentEngine::new(&sparse, 0, 1);

        doc_engine.register_collection(CollectionConfig::new("items").with_index("$.category"));

        let doc = serde_json::json!({"name": "Widget", "category": "tools"});
        let rmpv_val = json_to_msgpack(&doc);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &rmpv_val).unwrap();

        doc_engine.put_raw("items", &key(1), &buf).unwrap();

        let results = doc_engine
            .index_lookup("items", "$.category", "tools", None)
            .unwrap();
        assert_eq!(results, vec![key(1)]);
    }
}
