// SPDX-License-Identifier: BUSL-1.1

//! Document write paths: JSON and raw-MessagePack entry points.

use super::batch::{DocumentEngine, wall_now_ms};
use crate::engine::document::store::StorageKey;
use crate::engine::document::store::extract::{
    extract_index_values_rmpv, json_to_msgpack, rmpv_to_json,
};

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

        // Delegate to put_raw so both JSON and raw-MessagePack entry points
        // share the same bitemporal-aware write path.
        self.put_raw(collection, doc_id, &buf)?;

        let _ = document;
        Ok(())
    }

    /// Put a document from raw MessagePack bytes.
    pub fn put_raw(
        &self,
        collection: &str,
        doc_id: &StorageKey,
        msgpack_bytes: &[u8],
    ) -> crate::Result<()> {
        let bitemporal = self.is_bitemporal(collection);

        if bitemporal {
            let sys_from = wall_now_ms();
            self.sparse
                .versioned_put(crate::engine::sparse::btree_versioned::VersionedPut {
                    database_id: self.database_id,
                    tenant: self.tenant_id,
                    coll: collection,
                    doc_id,
                    sys_from_ms: sys_from,
                    valid_from_ms: i64::MIN,
                    valid_until_ms: i64::MAX,
                    body: msgpack_bytes,
                })?;
        } else {
            self.sparse.put(
                self.database_id,
                self.tenant_id,
                collection,
                doc_id,
                msgpack_bytes,
            )?;
        }

        if let Some(config) = self.configs.get(collection)
            && let Ok(value) = crate::util::bounded_msgpack::read_value(msgpack_bytes)
        {
            // INDEXES_VERSIONED still keys on the storage key as text.
            let doc_id_str = doc_id.to_string();
            for index_path in &config.index_paths {
                let values =
                    extract_index_values_rmpv(&value, &index_path.path, index_path.is_array);
                for v in values {
                    if bitemporal {
                        let sys_from = wall_now_ms();
                        self.sparse.versioned_index_put(
                            crate::engine::sparse::btree_versioned::VersionedIndexEntry {
                                database_id: self.database_id,
                                tenant: self.tenant_id,
                                coll: collection,
                                field: &index_path.path,
                                value: &v,
                                doc_id: &doc_id_str,
                                sys_from_ms: sys_from,
                            },
                        )?;
                    } else {
                        self.sparse.index_put(
                            self.database_id,
                            self.tenant_id,
                            collection,
                            &index_path.path,
                            &v,
                            doc_id,
                        )?;
                    }
                }
            }
        }

        let _ = rmpv_to_json;
        Ok(())
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
