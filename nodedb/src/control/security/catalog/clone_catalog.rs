// SPDX-License-Identifier: BUSL-1.1

//! Catalog persistence for the clone CoW subsystem: `clone_copyups` maps source
//! surrogate → target surrogate; `clone_tombstones` records deleted source rows;
//! `clone_lineage` maps source db → child db ids for DROP DATABASE checks.

use nodedb_types::DatabaseId;
use redb::{ReadableDatabase, ReadableTable};

use super::types::{
    CLONE_COPYUPS, CLONE_KV_TOMBSTONES, CLONE_LINEAGE, CLONE_TOMBSTONES, SystemCatalog, catalog_err,
};

impl SystemCatalog {
    // ── clone_copyups ─────────────────────────────────────────────────────────

    /// Record a copy-up: `source_surrogate` in `target_collection` is now
    /// represented by `target_surrogate` in target storage.
    pub fn put_clone_copyup(
        &self,
        target_collection_key: &str,
        source_surrogate: u32,
        target_surrogate: u32,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("clone_copyups begin_write", e))?;
        {
            let mut table = txn
                .open_table(CLONE_COPYUPS)
                .map_err(|e| catalog_err("open clone_copyups", e))?;
            table
                .insert((target_collection_key, source_surrogate), target_surrogate)
                .map_err(|e| catalog_err("insert clone_copyups", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("clone_copyups commit", e))
    }

    /// Look up the target surrogate for a copied-up source row.
    /// Returns `None` if no copy-up has been recorded for this surrogate.
    pub fn get_clone_copyup(
        &self,
        target_collection_key: &str,
        source_surrogate: u32,
    ) -> crate::Result<Option<u32>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("clone_copyups begin_read", e))?;
        let table = txn
            .open_table(CLONE_COPYUPS)
            .map_err(|e| catalog_err("open clone_copyups read", e))?;
        let val = table
            .get((target_collection_key, source_surrogate))
            .map_err(|e| catalog_err("get clone_copyups", e))?
            .map(|v| v.value());
        Ok(val)
    }

    /// Source surrogates copied up into `target_collection_key`.
    /// Clone reads must suppress the source row a copy-up superseded,
    /// the same role `list_clone_tombstones` plays for deletes.
    pub fn list_clone_copyups(
        &self,
        target_collection_key: &str,
    ) -> crate::Result<std::collections::HashSet<u32>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("clone_copyups list begin_read", e))?;
        let table = txn
            .open_table(CLONE_COPYUPS)
            .map_err(|e| catalog_err("open clone_copyups list", e))?;
        let mut set = std::collections::HashSet::new();
        for row in table
            .iter()
            .map_err(|e| catalog_err("iter clone_copyups list", e))?
        {
            let (k, _) = row.map_err(|e| catalog_err("iter clone_copyups list row", e))?;
            if k.value().0 == target_collection_key {
                set.insert(k.value().1);
            }
        }
        Ok(set)
    }

    /// Remove a copy-up record (called when the collection is fully
    /// materialised and the CoW tables are reaped).
    pub fn delete_clone_copyup(
        &self,
        target_collection_key: &str,
        source_surrogate: u32,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("clone_copyups delete begin_write", e))?;
        {
            let mut table = txn
                .open_table(CLONE_COPYUPS)
                .map_err(|e| catalog_err("open clone_copyups delete", e))?;
            table
                .remove((target_collection_key, source_surrogate))
                .map_err(|e| catalog_err("remove clone_copyups", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("clone_copyups delete commit", e))
    }

    /// Delete all copy-up records for a specific `target_collection_key`
    /// (used during post-materialization reap).
    pub fn delete_all_clone_copyups_for_collection(
        &self,
        target_collection_key: &str,
    ) -> crate::Result<u64> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("clone_copyups reap begin_write", e))?;
        let removed = {
            let mut table = txn
                .open_table(CLONE_COPYUPS)
                .map_err(|e| catalog_err("open clone_copyups reap", e))?;
            // Collect keys first to avoid borrow issues.
            let keys: Vec<u32> = {
                let iter = table
                    .iter()
                    .map_err(|e| catalog_err("iter clone_copyups reap", e))?;
                let mut acc = Vec::new();
                for row in iter {
                    let (k, _) = row.map_err(|e| catalog_err("iter clone_copyups row", e))?;
                    if k.value().0 == target_collection_key {
                        acc.push(k.value().1);
                    }
                }
                acc
            };
            let count = keys.len() as u64;
            for surrogate in keys {
                table
                    .remove((target_collection_key, surrogate))
                    .map_err(|e| catalog_err("remove clone_copyups reap", e))?;
            }
            count
        };
        txn.commit()
            .map_err(|e| catalog_err("clone_copyups reap commit", e))?;
        Ok(removed)
    }

    // ── clone_tombstones ──────────────────────────────────────────────────────

    /// Record a tombstone: `source_surrogate` was deleted from the clone.
    /// Future reads will consult this before falling back to source storage.
    pub fn put_clone_tombstone(
        &self,
        target_collection_key: &str,
        source_surrogate: u32,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("clone_tombstones begin_write", e))?;
        {
            let mut table = txn
                .open_table(CLONE_TOMBSTONES)
                .map_err(|e| catalog_err("open clone_tombstones", e))?;
            table
                .insert((target_collection_key, source_surrogate), ())
                .map_err(|e| catalog_err("insert clone_tombstones", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("clone_tombstones commit", e))
    }

    /// Returns `true` if `source_surrogate` has been tombstoned in this clone.
    pub fn is_clone_tombstoned(
        &self,
        target_collection_key: &str,
        source_surrogate: u32,
    ) -> crate::Result<bool> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("clone_tombstones begin_read", e))?;
        let table = txn
            .open_table(CLONE_TOMBSTONES)
            .map_err(|e| catalog_err("open clone_tombstones read", e))?;
        let exists = table
            .get((target_collection_key, source_surrogate))
            .map_err(|e| catalog_err("get clone_tombstones", e))?
            .is_some();
        Ok(exists)
    }

    /// All tombstoned source surrogates for a collection, used to filter
    /// source rows before merging into the target result set.
    pub fn list_clone_tombstones(
        &self,
        target_collection_key: &str,
    ) -> crate::Result<std::collections::HashSet<u32>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("clone_tombstones list begin_read", e))?;
        let table = txn
            .open_table(CLONE_TOMBSTONES)
            .map_err(|e| catalog_err("open clone_tombstones list", e))?;
        let mut set = std::collections::HashSet::new();
        for row in table
            .iter()
            .map_err(|e| catalog_err("iter clone_tombstones list", e))?
        {
            let (k, _) = row.map_err(|e| catalog_err("iter clone_tombstones list row", e))?;
            if k.value().0 == target_collection_key {
                set.insert(k.value().1);
            }
        }
        Ok(set)
    }

    /// Delete all tombstone records for a specific `target_collection_key`
    /// (used during post-materialization reap).
    pub fn delete_all_clone_tombstones_for_collection(
        &self,
        target_collection_key: &str,
    ) -> crate::Result<u64> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("clone_tombstones reap begin_write", e))?;
        let removed = {
            let mut table = txn
                .open_table(CLONE_TOMBSTONES)
                .map_err(|e| catalog_err("open clone_tombstones reap", e))?;
            let keys: Vec<u32> = {
                let iter = table
                    .iter()
                    .map_err(|e| catalog_err("iter clone_tombstones reap", e))?;
                let mut acc = Vec::new();
                for row in iter {
                    let (k, _) = row.map_err(|e| catalog_err("iter clone_tombstones row", e))?;
                    if k.value().0 == target_collection_key {
                        acc.push(k.value().1);
                    }
                }
                acc
            };
            let count = keys.len() as u64;
            for surrogate in keys {
                table
                    .remove((target_collection_key, surrogate))
                    .map_err(|e| catalog_err("remove clone_tombstones reap", e))?;
            }
            count
        };
        txn.commit()
            .map_err(|e| catalog_err("clone_tombstones reap commit", e))?;
        Ok(removed)
    }

    // ── clone_kv_tombstones ───────────────────────────────────────────────────

    /// Record a KV tombstone: `kv_key` was deleted from the KV clone collection.
    /// Future reads will exclude source rows with this key from results.
    pub fn put_kv_clone_tombstone(
        &self,
        target_collection_key: &str,
        kv_key: &str,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("clone_kv_tombstones begin_write", e))?;
        {
            let mut table = txn
                .open_table(CLONE_KV_TOMBSTONES)
                .map_err(|e| catalog_err("open clone_kv_tombstones", e))?;
            table
                .insert((target_collection_key, kv_key), ())
                .map_err(|e| catalog_err("insert clone_kv_tombstones", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("clone_kv_tombstones commit", e))
    }

    /// Returns `true` if `kv_key` has been tombstoned in this KV clone.
    pub fn is_kv_clone_tombstoned(
        &self,
        target_collection_key: &str,
        kv_key: &str,
    ) -> crate::Result<bool> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("clone_kv_tombstones begin_read", e))?;
        let table = txn
            .open_table(CLONE_KV_TOMBSTONES)
            .map_err(|e| catalog_err("open clone_kv_tombstones read", e))?;
        Ok(table
            .get((target_collection_key, kv_key))
            .map_err(|e| catalog_err("get clone_kv_tombstones", e))?
            .is_some())
    }

    /// All KV tombstoned keys for a clone collection, used to filter source
    /// KV scan results before merging into the target result set.
    pub fn list_kv_clone_tombstones(
        &self,
        target_collection_key: &str,
    ) -> crate::Result<std::collections::HashSet<String>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("clone_kv_tombstones list begin_read", e))?;
        let table = txn
            .open_table(CLONE_KV_TOMBSTONES)
            .map_err(|e| catalog_err("open clone_kv_tombstones list", e))?;
        let mut set = std::collections::HashSet::new();
        for row in table
            .iter()
            .map_err(|e| catalog_err("iter clone_kv_tombstones list", e))?
        {
            let (k, _) = row.map_err(|e| catalog_err("iter clone_kv_tombstones row", e))?;
            if k.value().0 == target_collection_key {
                set.insert(k.value().1.to_owned());
            }
        }
        Ok(set)
    }

    /// Delete every KV tombstone of `target_collection_key`.
    pub fn delete_all_kv_clone_tombstones_for_collection(
        &self,
        target_collection_key: &str,
    ) -> crate::Result<u64> {
        let keys = self.list_kv_clone_tombstones(target_collection_key)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("clone_kv_tombstones reap begin_write", e))?;
        {
            let mut table = txn
                .open_table(CLONE_KV_TOMBSTONES)
                .map_err(|e| catalog_err("open clone_kv_tombstones reap", e))?;
            for kv_key in &keys {
                table
                    .remove((target_collection_key, kv_key.as_str()))
                    .map_err(|e| catalog_err("remove clone_kv_tombstones reap", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("clone_kv_tombstones reap commit", e))?;
        Ok(keys.len() as u64)
    }

    // ── clone_lineage ─────────────────────────────────────────────────────────

    /// Return the list of child database ids that are clones of `source_db_id`.
    /// Returns an empty vec if no clones exist.
    pub fn get_clone_children(&self, source_db_id: DatabaseId) -> crate::Result<Vec<DatabaseId>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("clone_lineage begin_read", e))?;
        let table = txn
            .open_table(CLONE_LINEAGE)
            .map_err(|e| catalog_err("open clone_lineage read", e))?;
        match table
            .get(source_db_id.as_u64())
            .map_err(|e| catalog_err("get clone_lineage", e))?
        {
            None => Ok(Vec::new()),
            Some(v) => {
                let ids: Vec<u64> = zerompk::from_msgpack(v.value())
                    .map_err(|e| catalog_err("deser clone_lineage", e))?;
                Ok(ids.into_iter().map(DatabaseId::new).collect())
            }
        }
    }

    /// The clone children of `source_db_id` whose database still exists.
    ///
    /// A dropped child keeps its lineage edge, because the edge marks its
    /// clone as applied for metadata-log replay. Every user-visible listing
    /// and every dependency check reads this instead of the raw edge.
    pub fn get_live_clone_children(
        &self,
        source_db_id: DatabaseId,
    ) -> crate::Result<Vec<DatabaseId>> {
        let mut live = Vec::new();
        for child in self.get_clone_children(source_db_id)? {
            if self.get_database(child)?.is_some() {
                live.push(child);
            }
        }
        Ok(live)
    }

    /// Add `child_db_id` as a clone child of `source_db_id`.
    /// Idempotent — safe to call multiple times with the same child.
    pub fn add_clone_child(
        &self,
        source_db_id: DatabaseId,
        child_db_id: DatabaseId,
    ) -> crate::Result<()> {
        let mut children = self.get_clone_children(source_db_id)?;
        if !children.contains(&child_db_id) {
            children.push(child_db_id);
        }
        self.write_clone_children(source_db_id, &children)
    }

    /// Remove `child_db_id` from the clone children of `source_db_id`.
    /// Idempotent — safe to call when the child is already absent.
    pub fn remove_clone_child(
        &self,
        source_db_id: DatabaseId,
        child_db_id: DatabaseId,
    ) -> crate::Result<()> {
        let mut children = self.get_clone_children(source_db_id)?;
        children.retain(|id| *id != child_db_id);
        if children.is_empty() {
            self.delete_clone_lineage_entry(source_db_id)
        } else {
            self.write_clone_children(source_db_id, &children)
        }
    }

    fn write_clone_children(
        &self,
        source_db_id: DatabaseId,
        children: &[DatabaseId],
    ) -> crate::Result<()> {
        let ids: Vec<u64> = children.iter().map(|id| id.as_u64()).collect();
        let bytes =
            zerompk::to_msgpack_vec(&ids).map_err(|e| catalog_err("serialize clone_lineage", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("clone_lineage begin_write", e))?;
        {
            let mut table = txn
                .open_table(CLONE_LINEAGE)
                .map_err(|e| catalog_err("open clone_lineage write", e))?;
            table
                .insert(source_db_id.as_u64(), bytes.as_slice())
                .map_err(|e| catalog_err("insert clone_lineage", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("clone_lineage commit", e))
    }

    fn delete_clone_lineage_entry(&self, source_db_id: DatabaseId) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("clone_lineage delete begin_write", e))?;
        {
            let mut table = txn
                .open_table(CLONE_LINEAGE)
                .map_err(|e| catalog_err("open clone_lineage delete", e))?;
            table
                .remove(source_db_id.as_u64())
                .map_err(|e| catalog_err("remove clone_lineage", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("clone_lineage delete commit", e))
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

    fn db(n: u64) -> DatabaseId {
        DatabaseId::new(n)
    }

    #[test]
    fn copyup_roundtrip() {
        let (_dir, cat) = open_catalog();
        cat.put_clone_copyup("db1:0:users", 42, 99).unwrap();
        assert_eq!(cat.get_clone_copyup("db1:0:users", 42).unwrap(), Some(99));
        assert_eq!(cat.get_clone_copyup("db1:0:users", 43).unwrap(), None);
    }

    #[test]
    fn copyup_delete() {
        let (_dir, cat) = open_catalog();
        cat.put_clone_copyup("db1:0:users", 1, 10).unwrap();
        cat.delete_clone_copyup("db1:0:users", 1).unwrap();
        assert_eq!(cat.get_clone_copyup("db1:0:users", 1).unwrap(), None);
    }

    #[test]
    fn copyup_delete_all_for_collection() {
        let (_dir, cat) = open_catalog();
        cat.put_clone_copyup("db1:0:users", 1, 10).unwrap();
        cat.put_clone_copyup("db1:0:users", 2, 11).unwrap();
        cat.put_clone_copyup("db1:0:posts", 5, 50).unwrap();
        let removed = cat
            .delete_all_clone_copyups_for_collection("db1:0:users")
            .unwrap();
        assert_eq!(removed, 2);
        assert_eq!(cat.get_clone_copyup("db1:0:users", 1).unwrap(), None);
        assert_eq!(cat.get_clone_copyup("db1:0:posts", 5).unwrap(), Some(50));
    }

    #[test]
    fn copyup_list_is_scoped_to_one_collection() {
        let (_dir, cat) = open_catalog();
        cat.put_clone_copyup("db1:0:users", 1, 10).unwrap();
        cat.put_clone_copyup("db1:0:users", 2, 11).unwrap();
        cat.put_clone_copyup("db1:0:posts", 5, 50).unwrap();
        let set = cat.list_clone_copyups("db1:0:users").unwrap();
        assert_eq!(set.len(), 2);
        assert!(set.contains(&1));
        assert!(set.contains(&2));
        assert!(!set.contains(&5));
        assert!(cat.list_clone_copyups("db1:0:absent").unwrap().is_empty());
    }

    #[test]
    fn tombstone_roundtrip() {
        let (_dir, cat) = open_catalog();
        assert!(!cat.is_clone_tombstoned("db1:0:users", 7).unwrap());
        cat.put_clone_tombstone("db1:0:users", 7).unwrap();
        assert!(cat.is_clone_tombstoned("db1:0:users", 7).unwrap());
        assert!(!cat.is_clone_tombstoned("db1:0:users", 8).unwrap());
    }

    #[test]
    fn tombstone_delete_all_for_collection() {
        let (_dir, cat) = open_catalog();
        cat.put_clone_tombstone("db1:0:users", 1).unwrap();
        cat.put_clone_tombstone("db1:0:users", 2).unwrap();
        cat.put_clone_tombstone("db1:0:posts", 9).unwrap();
        let removed = cat
            .delete_all_clone_tombstones_for_collection("db1:0:users")
            .unwrap();
        assert_eq!(removed, 2);
        assert!(!cat.is_clone_tombstoned("db1:0:users", 1).unwrap());
        assert!(cat.is_clone_tombstoned("db1:0:posts", 9).unwrap());
    }

    #[test]
    fn lineage_empty_by_default() {
        let (_dir, cat) = open_catalog();
        assert!(cat.get_clone_children(db(1)).unwrap().is_empty());
    }

    #[test]
    fn lineage_add_and_remove() {
        let (_dir, cat) = open_catalog();
        cat.add_clone_child(db(1), db(2)).unwrap();
        cat.add_clone_child(db(1), db(3)).unwrap();
        let children = cat.get_clone_children(db(1)).unwrap();
        assert!(children.contains(&db(2)));
        assert!(children.contains(&db(3)));

        cat.remove_clone_child(db(1), db(2)).unwrap();
        let children = cat.get_clone_children(db(1)).unwrap();
        assert!(!children.contains(&db(2)));
        assert!(children.contains(&db(3)));
    }

    #[test]
    fn lineage_add_idempotent() {
        let (_dir, cat) = open_catalog();
        cat.add_clone_child(db(1), db(2)).unwrap();
        cat.add_clone_child(db(1), db(2)).unwrap();
        assert_eq!(cat.get_clone_children(db(1)).unwrap().len(), 1);
    }

    #[test]
    fn lineage_remove_last_cleans_up() {
        let (_dir, cat) = open_catalog();
        cat.add_clone_child(db(1), db(2)).unwrap();
        cat.remove_clone_child(db(1), db(2)).unwrap();
        assert!(cat.get_clone_children(db(1)).unwrap().is_empty());
        // Idempotent — removing absent child is fine.
        cat.remove_clone_child(db(1), db(2)).unwrap();
    }

    /// A dropped child keeps its edge but leaves the live listing.
    #[test]
    fn live_children_skip_a_dropped_child() {
        use crate::control::security::catalog::database_types::{
            DatabaseDescriptor, DatabaseStatus,
        };

        let (_dir, cat) = open_catalog();
        for child in [db(2), db(3)] {
            cat.put_database(&DatabaseDescriptor {
                id: child,
                name: format!("child_{}", child.as_u64()),
                status: DatabaseStatus::Active,
                created_at_lsn: 0,
                quota_ref: 0,
                parent_clone: None,
                mirror_origin: None,
                audit_dml: nodedb_types::AuditDmlMode::None,
                idle_session_timeout_secs: 0,
            })
            .unwrap();
            cat.add_clone_child(db(1), child).unwrap();
        }
        cat.delete_database(db(2)).unwrap();

        assert_eq!(cat.get_clone_children(db(1)).unwrap(), vec![db(2), db(3)]);
        assert_eq!(cat.get_live_clone_children(db(1)).unwrap(), vec![db(3)]);
    }
}
