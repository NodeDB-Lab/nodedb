// SPDX-License-Identifier: BUSL-1.1

//! Document read paths.

use super::batch::DocumentEngine;
use crate::engine::document::store::StorageKey;
use crate::engine::document::store::extract::rmpv_to_json;

impl<'a> DocumentEngine<'a> {
    /// Get a document and deserialize from MessagePack to JSON.
    pub fn get(
        &self,
        collection: &str,
        doc_id: &StorageKey,
    ) -> crate::Result<Option<serde_json::Value>> {
        let bytes_opt = if self.is_bitemporal(collection) {
            self.sparse.versioned_get_current(
                self.database_id,
                self.tenant_id,
                collection,
                &doc_id.to_string(),
            )?
        } else {
            self.sparse
                .get(self.database_id, self.tenant_id, collection, doc_id)?
        };
        match bytes_opt {
            Some(bytes) => {
                let rmpv_val = crate::util::bounded_msgpack::read_value(&bytes).map_err(|e| {
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
    pub fn get_raw(&self, collection: &str, doc_id: &StorageKey) -> crate::Result<Option<Vec<u8>>> {
        if self.is_bitemporal(collection) {
            self.sparse.versioned_get_current(
                self.database_id,
                self.tenant_id,
                collection,
                &doc_id.to_string(),
            )
        } else {
            self.sparse
                .get(self.database_id, self.tenant_id, collection, doc_id)
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
    fn get_nonexistent_returns_none() {
        let (sparse, _dir) = make_engine();
        let doc_engine = DocumentEngine::new(&sparse, 0, 1);
        assert!(doc_engine.get("users", &key(1)).unwrap().is_none());
    }

    #[test]
    fn collections_are_isolated() {
        let (sparse, _dir) = make_engine();
        let doc_engine = DocumentEngine::new(&sparse, 0, 1);

        doc_engine
            .put("users", &key(1), &serde_json::json!({"type": "user"}))
            .unwrap();
        doc_engine
            .put("orders", &key(1), &serde_json::json!({"type": "order"}))
            .unwrap();

        let user = doc_engine.get("users", &key(1)).unwrap().unwrap();
        let order = doc_engine.get("orders", &key(1)).unwrap().unwrap();
        assert_eq!(user["type"], "user");
        assert_eq!(order["type"], "order");
    }
}
