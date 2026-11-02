// SPDX-License-Identifier: BUSL-1.1

//! Document delete path: append a tombstone version and tombstone the row's
//! index entries at the same ordinal, so AS-OF reads still see the history.

use super::batch::DocumentEngine;
use crate::engine::document::store::StorageKey;

impl<'a> DocumentEngine<'a> {
    /// Delete `doc_id`. Returns `false` when the row had no live version.
    pub fn delete(&self, collection: &str, doc_id: &StorageKey) -> crate::Result<bool> {
        let Some(body) = self.sparse.versioned_get_current(
            self.database_id,
            self.tenant_id,
            collection,
            doc_id,
        )?
        else {
            return Ok(false);
        };
        let old_values = self.index_values(collection, &body)?;
        let sys_from = self.next_row_ordinal(collection, doc_id)?;
        let txn = self.sparse.begin_write()?;
        self.sparse.versioned_tombstone_in_txn(
            &txn,
            self.database_id,
            self.tenant_id,
            collection,
            doc_id,
            sys_from,
        )?;
        for (field, value) in &old_values {
            self.sparse.versioned_index_tombstone_in_txn(
                &txn,
                self.entry(collection, field, value, doc_id, sys_from),
            )?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("commit document delete: {e}"),
        })?;
        Ok(true)
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
        assert!(!doc_engine.delete("users", &key(1)).unwrap());
        let tombstone = sparse
            .versioned_latest_sys_from(0, 1, "users", &key(1))
            .unwrap()
            .unwrap();
        assert!(
            sparse
                .versioned_get_as_of(0, 1, "users", &key(1), Some(tombstone - 1), None)
                .unwrap()
                .is_some(),
            "the deleted row stays visible below its tombstone"
        );
    }
}
