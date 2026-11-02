// SPDX-License-Identifier: BUSL-1.1

//! Document write paths: JSON and raw-MessagePack entry points.

use std::collections::BTreeSet;

use super::batch::DocumentEngine;
use crate::engine::document::store::StorageKey;
use crate::engine::document::store::extract::{extract_index_values_rmpv, json_to_msgpack};
use crate::engine::sparse::btree_versioned::{VersionedIndexEntry, VersionedPut};

impl<'a> DocumentEngine<'a> {
    /// Put a document (JSON value) into a collection.
    pub fn put(
        &self,
        collection: &str,
        doc_id: &StorageKey,
        document: &serde_json::Value,
    ) -> crate::Result<()> {
        let msgpack = json_to_msgpack(document);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("encode: {e}"),
        })?;
        self.put_raw(collection, doc_id, &buf)
    }

    /// Put a document from raw MessagePack bytes: append a version at a
    /// fresh ordinal of this engine's clock, and move the row's index
    /// entries to the new body's values at the same ordinal. A value the new
    /// body drops is tombstoned. A value it gains is added.
    pub fn put_raw(
        &self,
        collection: &str,
        doc_id: &StorageKey,
        msgpack_bytes: &[u8],
    ) -> crate::Result<()> {
        let sys_from = self.next_row_ordinal(collection, doc_id)?;
        let old_values = match self.sparse.versioned_get_current(
            self.database_id,
            self.tenant_id,
            collection,
            doc_id,
        )? {
            Some(body) => self.index_values(collection, &body)?,
            None => BTreeSet::new(),
        };
        let new_values = self.index_values(collection, msgpack_bytes)?;

        let txn = self.sparse.begin_write()?;
        self.sparse.versioned_put_in_txn(
            &txn,
            VersionedPut {
                database_id: self.database_id,
                tenant: self.tenant_id,
                coll: collection,
                doc_id,
                sys_from,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                body: msgpack_bytes,
            },
        )?;
        for (field, value) in old_values.difference(&new_values) {
            self.sparse.versioned_index_tombstone_in_txn(
                &txn,
                self.entry(collection, field, value, doc_id, sys_from),
            )?;
        }
        for (field, value) in new_values.difference(&old_values) {
            self.sparse.versioned_index_put_in_txn(
                &txn,
                self.entry(collection, field, value, doc_id, sys_from),
            )?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("commit document put: {e}"),
        })?;
        Ok(())
    }

    /// The `(path, value)` index entries `body` yields under the
    /// collection's index paths. A body that does not decode fails: a row
    /// whose entries cannot be derived cannot keep its index exact.
    pub(super) fn index_values(
        &self,
        collection: &str,
        body: &[u8],
    ) -> crate::Result<BTreeSet<(String, String)>> {
        let mut out = BTreeSet::new();
        let Some(config) = self.configs.get(collection) else {
            return Ok(out);
        };
        if config.index_paths.is_empty() {
            return Ok(out);
        }
        let value = crate::util::bounded_msgpack::read_value(body).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("index decode of '{collection}' row: {e}"),
            }
        })?;
        for index_path in &config.index_paths {
            for v in extract_index_values_rmpv(&value, &index_path.path, index_path.is_array) {
                out.insert((index_path.path.clone(), v));
            }
        }
        Ok(out)
    }

    /// The ordinal a write of `doc_id` stamps: a fresh ordinal of this
    /// engine's clock, raised one above the row's newest stored version
    /// when that version sits at or above it.
    pub(super) fn next_row_ordinal(
        &self,
        collection: &str,
        doc_id: &StorageKey,
    ) -> crate::Result<i64> {
        let fresh = self.clock.next_ordinal();
        let newest = self.sparse.versioned_latest_sys_from(
            self.database_id,
            self.tenant_id,
            collection,
            doc_id,
        )?;
        let ordinal = match newest {
            Some(newest) if newest >= fresh => newest.saturating_add(1),
            _ => fresh,
        };
        self.clock.update_from_remote(ordinal);
        Ok(ordinal)
    }

    /// One index entry of `doc_id` at `sys_from`.
    pub(super) fn entry<'e>(
        &self,
        collection: &'e str,
        field: &'e str,
        value: &'e str,
        doc_id: &'e StorageKey,
        sys_from: i64,
    ) -> VersionedIndexEntry<'e> {
        VersionedIndexEntry {
            database_id: self.database_id,
            tenant: self.tenant_id,
            coll: collection,
            field,
            value,
            doc_id,
            sys_from,
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use crate::engine::sparse::btree::SparseEngine;

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
    fn put_and_get_document() {
        let (sparse, _dir) = make_engine();
        let doc_engine = DocumentEngine::new(&sparse, 0, 1);

        let doc = serde_json::json!({
            "name": "Alice",
            "email": "alice@example.com",
            "age": 30
        });

        doc_engine.put("users", &key(1), &doc).unwrap();
        let retrieved = doc_engine.get("users", &key(1)).unwrap().unwrap();

        assert_eq!(retrieved["name"], "Alice");
        assert_eq!(retrieved["email"], "alice@example.com");
        assert_eq!(retrieved["age"], 30);
    }

    #[test]
    fn overwrite_document() {
        let (sparse, _dir) = make_engine();
        let doc_engine = DocumentEngine::new(&sparse, 0, 1);

        doc_engine
            .put("users", &key(1), &serde_json::json!({"v": 1}))
            .unwrap();
        doc_engine
            .put("users", &key(1), &serde_json::json!({"v": 2}))
            .unwrap();

        let doc = doc_engine.get("users", &key(1)).unwrap().unwrap();
        assert_eq!(doc["v"], 2);
    }

    #[test]
    fn overwrite_moves_the_index_and_keeps_history() {
        use crate::engine::document::store::config::CollectionConfig;
        let (sparse, _dir) = make_engine();
        let mut doc_engine = DocumentEngine::new(&sparse, 0, 1);
        doc_engine.register_collection(CollectionConfig::new("users").with_index("$.city"));

        doc_engine
            .put("users", &key(1), &serde_json::json!({"city": "oslo"}))
            .unwrap();
        let first = sparse
            .versioned_latest_sys_from(0, 1, "users", &key(1))
            .unwrap()
            .unwrap();
        doc_engine
            .put("users", &key(1), &serde_json::json!({"city": "rome"}))
            .unwrap();

        assert!(
            doc_engine
                .index_lookup("users", "$.city", "oslo", None)
                .unwrap()
                .is_empty(),
            "the overwrite tombstones the old value"
        );
        assert_eq!(
            doc_engine
                .index_lookup("users", "$.city", "rome", None)
                .unwrap(),
            vec![key(1)]
        );
        assert_eq!(
            doc_engine
                .index_lookup("users", "$.city", "oslo", Some(first))
                .unwrap(),
            vec![key(1)],
            "a read at the first version's cut sees the old value"
        );
        assert!(
            doc_engine
                .index_lookup("users", "$.city", "rome", Some(first))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn raw_msgpack_roundtrip() {
        let (sparse, _dir) = make_engine();
        let doc_engine = DocumentEngine::new(&sparse, 0, 1);

        let doc = serde_json::json!({"key": "value", "num": 42});
        let rmpv_val = json_to_msgpack(&doc);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &rmpv_val).unwrap();

        doc_engine.put_raw("col", &key(1), &buf).unwrap();

        let raw = doc_engine.get_raw("col", &key(1)).unwrap().unwrap();
        assert_eq!(raw, buf);

        let decoded = doc_engine.get("col", &key(1)).unwrap().unwrap();
        assert_eq!(decoded["key"], "value");
        assert_eq!(decoded["num"], 42);
    }
}
