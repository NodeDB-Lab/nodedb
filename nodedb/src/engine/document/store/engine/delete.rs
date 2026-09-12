// SPDX-License-Identifier: BUSL-1.1

//! Document delete path.
//!
//! On bitemporal collections this appends a tombstone version so AS-OF
//! queries still see prior history; on non-bitemporal collections the
//! row is removed in place.

use super::batch::{DocumentEngine, wall_now_ms};
use crate::engine::document::store::StorageKey;
use crate::engine::document::store::extract::extract_index_values_rmpv;

impl<'a> DocumentEngine<'a> {
    pub fn delete(&self, collection: &str, doc_id: &StorageKey) -> crate::Result<bool> {
        if self.is_bitemporal(collection) {
            let prior_body = self.sparse.versioned_get_current(
                self.database_id,
                self.tenant_id,
                collection,
                doc_id,
            )?;
            let Some(body) = prior_body else {
                return Ok(false);
            };
            let sys_from = wall_now_ms();
            self.sparse.versioned_tombstone(
                self.database_id,
                self.tenant_id,
                collection,
                doc_id,
                sys_from,
            )?;
            if let Some(config) = self.configs.get(collection)
                && let Ok(rmpv_val) = crate::util::bounded_msgpack::read_value(&body)
            {
                // INDEXES_VERSIONED still keys on the storage key as text.
                let doc_id_str = doc_id.to_string();
                for index_path in &config.index_paths {
                    for v in
                        extract_index_values_rmpv(&rmpv_val, &index_path.path, index_path.is_array)
                    {
                        self.sparse.versioned_index_tombstone(
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
                    }
                }
            }
            return Ok(true);
        }
        self.sparse.delete_indexes_for_document(
            self.database_id,
            self.tenant_id,
            collection,
            doc_id,
        )?;
        Ok(self
            .sparse
            .delete(self.database_id, self.tenant_id, collection, doc_id)?
            .is_some())
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
    fn delete_document() {
        let (sparse, _dir) = make_engine();
        let doc_engine = DocumentEngine::new(&sparse, 0, 1);

        let doc = serde_json::json!({"name": "Bob"});
        doc_engine.put("users", &key(1), &doc).unwrap();
        assert!(doc_engine.delete("users", &key(1)).unwrap());
        assert!(doc_engine.get("users", &key(1)).unwrap().is_none());
    }
}
