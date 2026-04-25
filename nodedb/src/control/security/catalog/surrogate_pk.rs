//! Surrogate ↔ PK catalog ops for the `_system.surrogate_pk{,_rev}` tables.
//!
//! Forward + reverse mapping between user-visible primary keys and the
//! global `Surrogate` allocator. Every method writes both
//! tables atomically in a single redb write transaction so the two
//! directions can never drift.
//!
//! Mirrors the `arrays.rs` style — typed put/get/delete plus a bulk
//! scan + bulk delete used by drop-collection / drop-array cleanup.

use nodedb_types::Surrogate;
use redb::ReadableTable;

use super::types::{SURROGATE_PK, SURROGATE_PK_REV, SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Insert or overwrite a surrogate ↔ PK binding. Writes both the
    /// forward (`(collection, pk_bytes) -> surrogate`) and reverse
    /// (`(collection, surrogate) -> pk_bytes`) rows in one txn.
    ///
    /// Idempotent: re-binding the same `(collection, pk_bytes)` to the
    /// same surrogate is a no-op-on-disk overwrite.
    pub fn put_surrogate(
        &self,
        collection: &str,
        pk_bytes: &[u8],
        surrogate: Surrogate,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_pk write txn", e))?;
        {
            let mut fwd = txn
                .open_table(SURROGATE_PK)
                .map_err(|e| catalog_err("open surrogate_pk", e))?;
            fwd.insert((collection, pk_bytes), surrogate.as_u32())
                .map_err(|e| catalog_err("insert surrogate_pk", e))?;
            let mut rev = txn
                .open_table(SURROGATE_PK_REV)
                .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
            rev.insert((collection, surrogate.as_u32()), pk_bytes)
                .map_err(|e| catalog_err("insert surrogate_pk_rev", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("surrogate_pk commit", e))
    }

    /// Look up the surrogate previously bound to a `(collection, pk_bytes)`.
    /// Returns `None` if no binding exists.
    pub fn get_surrogate_for_pk(
        &self,
        collection: &str,
        pk_bytes: &[u8],
    ) -> crate::Result<Option<Surrogate>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_pk read txn", e))?;
        let table = txn
            .open_table(SURROGATE_PK)
            .map_err(|e| catalog_err("open surrogate_pk", e))?;
        match table
            .get((collection, pk_bytes))
            .map_err(|e| catalog_err("get surrogate_pk", e))?
        {
            Some(v) => Ok(Some(Surrogate::new(v.value()))),
            None => Ok(None),
        }
    }

    /// Look up the PK previously bound to a `(collection, surrogate)`.
    /// Returns `None` if no binding exists.
    pub fn get_pk_for_surrogate(
        &self,
        collection: &str,
        surrogate: Surrogate,
    ) -> crate::Result<Option<Vec<u8>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_pk_rev read txn", e))?;
        let table = txn
            .open_table(SURROGATE_PK_REV)
            .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
        match table
            .get((collection, surrogate.as_u32()))
            .map_err(|e| catalog_err("get surrogate_pk_rev", e))?
        {
            Some(v) => Ok(Some(v.value().to_vec())),
            None => Ok(None),
        }
    }

    /// Remove a surrogate ↔ PK binding. Removes both directions
    /// atomically. Idempotent: removing a missing binding succeeds
    /// silently (mirrors `delete_array`'s tolerant shape).
    pub fn delete_surrogate(&self, collection: &str, pk_bytes: &[u8]) -> crate::Result<()> {
        // We need the surrogate value to remove the reverse row, so
        // the read happens inside the same write txn the removal
        // commits in — guarantees the two rows can never observe a
        // half-deleted state under concurrent compaction.
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_pk delete txn", e))?;
        {
            let mut fwd = txn
                .open_table(SURROGATE_PK)
                .map_err(|e| catalog_err("open surrogate_pk", e))?;
            let removed = fwd
                .remove((collection, pk_bytes))
                .map_err(|e| catalog_err("remove surrogate_pk", e))?;
            if let Some(v) = removed {
                let surrogate = v.value();
                let mut rev = txn
                    .open_table(SURROGATE_PK_REV)
                    .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
                rev.remove((collection, surrogate))
                    .map_err(|e| catalog_err("remove surrogate_pk_rev", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("surrogate_pk delete commit", e))
    }

    /// Scan every binding for a collection. Returns
    /// `Vec<(pk_bytes, surrogate)>` in redb's natural order over the
    /// `(collection, pk_bytes)` composite key.
    ///
    /// Used by drop-collection cleanup (to know what to wipe) and by
    /// integration tests asserting bulk-delete leaves nothing behind.
    pub fn scan_surrogates_for_collection(
        &self,
        collection: &str,
    ) -> crate::Result<Vec<(Vec<u8>, Surrogate)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_pk scan txn", e))?;
        let table = txn
            .open_table(SURROGATE_PK)
            .map_err(|e| catalog_err("open surrogate_pk", e))?;
        // Range over `(collection, &[]) ..= (collection, &[0xFF; ...])`
        // is unwieldy with borrowed slice keys; iterate everything and
        // filter. The catalog is an ops surface, not a hot path.
        let mut out = Vec::new();
        let iter = table
            .iter()
            .map_err(|e| catalog_err("iter surrogate_pk", e))?;
        for row in iter {
            let (k, v) = row.map_err(|e| catalog_err("iter surrogate_pk row", e))?;
            let (coll, pk) = k.value();
            if coll == collection {
                out.push((pk.to_vec(), Surrogate::new(v.value())));
            }
        }
        Ok(out)
    }

    /// Bulk-delete every surrogate binding for a collection. Drains
    /// both forward and reverse tables of every row whose collection
    /// component matches `collection`.
    ///
    /// Called from `DROP COLLECTION` / `DROP ARRAY` paths so dropping
    /// a collection wipes its surrogate map. Idempotent: dropping a
    /// collection with no bindings yet is a successful no-op.
    pub fn delete_all_surrogates_for_collection(&self, collection: &str) -> crate::Result<()> {
        // Two-step: gather PKs + surrogates inside a read txn (so the
        // iterator doesn't conflict with the write txn that removes
        // them), then remove inside one write txn.
        let to_remove = self.scan_surrogates_for_collection(collection)?;
        if to_remove.is_empty() {
            return Ok(());
        }
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_pk bulk-delete txn", e))?;
        {
            let mut fwd = txn
                .open_table(SURROGATE_PK)
                .map_err(|e| catalog_err("open surrogate_pk", e))?;
            let mut rev = txn
                .open_table(SURROGATE_PK_REV)
                .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
            for (pk, surrogate) in &to_remove {
                fwd.remove((collection, pk.as_slice()))
                    .map_err(|e| catalog_err("bulk remove surrogate_pk", e))?;
                rev.remove((collection, surrogate.as_u32()))
                    .map_err(|e| catalog_err("bulk remove surrogate_pk_rev", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("surrogate_pk bulk-delete commit", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, cat)
    }

    #[test]
    fn put_then_get_roundtrip() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate("users", b"alice", Surrogate::new(7))
            .unwrap();
        assert_eq!(
            cat.get_surrogate_for_pk("users", b"alice").unwrap(),
            Some(Surrogate::new(7))
        );
        assert_eq!(
            cat.get_pk_for_surrogate("users", Surrogate::new(7))
                .unwrap(),
            Some(b"alice".to_vec())
        );
    }

    #[test]
    fn missing_returns_none() {
        let (_dir, cat) = open_catalog();
        assert_eq!(cat.get_surrogate_for_pk("users", b"nobody").unwrap(), None);
        assert_eq!(
            cat.get_pk_for_surrogate("users", Surrogate::new(42))
                .unwrap(),
            None
        );
    }

    #[test]
    fn delete_is_idempotent_and_removes_both_directions() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate("users", b"alice", Surrogate::new(7))
            .unwrap();
        cat.delete_surrogate("users", b"alice").unwrap();
        // both gone
        assert_eq!(cat.get_surrogate_for_pk("users", b"alice").unwrap(), None);
        assert_eq!(
            cat.get_pk_for_surrogate("users", Surrogate::new(7))
                .unwrap(),
            None
        );
        // re-delete: still ok
        cat.delete_surrogate("users", b"alice").unwrap();
    }

    #[test]
    fn put_overwrites_existing_pk_with_same_surrogate() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate("users", b"alice", Surrogate::new(7))
            .unwrap();
        cat.put_surrogate("users", b"alice", Surrogate::new(7))
            .unwrap();
        assert_eq!(
            cat.get_surrogate_for_pk("users", b"alice").unwrap(),
            Some(Surrogate::new(7))
        );
    }

    #[test]
    fn scan_returns_only_named_collection() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate("users", b"alice", Surrogate::new(1))
            .unwrap();
        cat.put_surrogate("users", b"bob", Surrogate::new(2))
            .unwrap();
        cat.put_surrogate("orders", b"alice", Surrogate::new(3))
            .unwrap();
        let mut got = cat.scan_surrogates_for_collection("users").unwrap();
        got.sort();
        assert_eq!(
            got,
            vec![
                (b"alice".to_vec(), Surrogate::new(1)),
                (b"bob".to_vec(), Surrogate::new(2)),
            ]
        );
        let other = cat.scan_surrogates_for_collection("orders").unwrap();
        assert_eq!(other, vec![(b"alice".to_vec(), Surrogate::new(3))]);
    }

    #[test]
    fn delete_all_wipes_collection_and_leaves_others_intact() {
        let (_dir, cat) = open_catalog();
        cat.put_surrogate("users", b"alice", Surrogate::new(1))
            .unwrap();
        cat.put_surrogate("users", b"bob", Surrogate::new(2))
            .unwrap();
        cat.put_surrogate("orders", b"o1", Surrogate::new(3))
            .unwrap();
        cat.delete_all_surrogates_for_collection("users").unwrap();
        assert!(
            cat.scan_surrogates_for_collection("users")
                .unwrap()
                .is_empty()
        );
        // orders untouched
        assert_eq!(
            cat.get_surrogate_for_pk("orders", b"o1").unwrap(),
            Some(Surrogate::new(3))
        );
        // reverse direction also gone for users
        assert_eq!(
            cat.get_pk_for_surrogate("users", Surrogate::new(1))
                .unwrap(),
            None
        );
        // double-delete is a no-op
        cat.delete_all_surrogates_for_collection("users").unwrap();
    }

    #[test]
    fn delete_all_on_empty_collection_is_noop() {
        let (_dir, cat) = open_catalog();
        cat.delete_all_surrogates_for_collection("nonexistent")
            .unwrap();
    }
}
