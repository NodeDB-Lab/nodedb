// SPDX-License-Identifier: BUSL-1.1

//! Durable set of the document index paths each core maintains.
//!
//! A write maintains the entries of exactly the paths this set holds. The
//! set changes only in the transaction of an index build or purge, so it
//! follows the data log and never the catalog. A build and its set change
//! commit together, and so do a purge and its set change.
//!
//! Storage: its own `document_maintained_indexes` redb table, keyed
//! `"{database_id}:{tenant_id}:{collection}"`. The value is one encoded
//! [`MaintainedIndexSet`]. A collection whose set keeps nothing has no row.

use redb::{ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};

use super::engine::SparseEngine;
use super::maintained_index_record::{MaintainedIndexSet, maintained_set_key};
use super::tables::redb_err;

/// Table definition for maintained index sets.
pub(super) const MAINTAINED_INDEXES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("document_maintained_indexes");

impl SparseEngine {
    /// One collection's set as `txn` sees it. A collection with no row
    /// gives an empty set.
    pub fn maintained_indexes_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<MaintainedIndexSet> {
        let table = txn
            .open_table(MAINTAINED_INDEXES)
            .map_err(|e| redb_err("open maintained indexes", e))?;
        let key = maintained_set_key(database_id, tenant_id, collection);
        match table
            .get(key.as_str())
            .map_err(|e| redb_err("get maintained indexes", e))?
        {
            Some(value) => MaintainedIndexSet::from_bytes(&key, value.value()),
            None => Ok(MaintainedIndexSet::empty(
                database_id,
                tenant_id,
                collection,
            )),
        }
    }

    /// Store `set` inside `txn`. An empty set removes the collection's row.
    pub fn put_maintained_indexes_in_txn(
        &self,
        txn: &WriteTransaction,
        set: &MaintainedIndexSet,
    ) -> crate::Result<()> {
        let mut table = txn
            .open_table(MAINTAINED_INDEXES)
            .map_err(|e| redb_err("open maintained indexes", e))?;
        let key = set.key();
        if set.is_empty() {
            table
                .remove(key.as_str())
                .map_err(|e| redb_err("remove maintained indexes", e))?;
        } else {
            let bytes = set.to_bytes()?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| redb_err("put maintained indexes", e))?;
        }
        Ok(())
    }

    /// Remove one collection's row inside `txn`, as a collection purge does.
    pub fn remove_maintained_indexes_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<()> {
        let mut table = txn
            .open_table(MAINTAINED_INDEXES)
            .map_err(|e| redb_err("open maintained indexes", e))?;
        let key = maintained_set_key(database_id, tenant_id, collection);
        table
            .remove(key.as_str())
            .map_err(|e| redb_err("remove maintained indexes", e))?;
        Ok(())
    }

    /// Every stored set, as a core loads them at open.
    pub fn load_maintained_indexes(&self) -> crate::Result<Vec<MaintainedIndexSet>> {
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(MAINTAINED_INDEXES)
            .map_err(|e| redb_err("open maintained indexes", e))?;
        let mut out = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| redb_err("iter maintained indexes", e))?
        {
            let (key, value) = entry.map_err(|e| redb_err("scan maintained indexes", e))?;
            out.push(MaintainedIndexSet::from_bytes(key.value(), value.value())?);
        }
        Ok(out)
    }

    /// Every stored row of `(database_id, tenant_id)`, as a snapshot
    /// carries them: `("{database_id}:{tenant_id}:{collection}", bytes)`.
    pub fn maintained_indexes_for_tenant(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(MAINTAINED_INDEXES)
            .map_err(|e| redb_err("open maintained indexes", e))?;
        let prefix = format!("{database_id}:{tenant_id}:");
        let mut out = Vec::new();
        for entry in table
            .range(prefix.as_str()..)
            .map_err(|e| redb_err("range maintained indexes", e))?
        {
            let (key, value) = entry.map_err(|e| redb_err("scan maintained indexes", e))?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            out.push((key.value().to_string(), value.value().to_vec()));
        }
        Ok(out)
    }

    /// Store a snapshot's row and return the set it holds. A row whose
    /// bytes do not decode, or whose key does not name its set, is refused
    /// before any write.
    pub fn put_maintained_indexes_raw(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> crate::Result<MaintainedIndexSet> {
        let set = MaintainedIndexSet::from_bytes(key, bytes)?;
        if set.key() != key {
            return Err(crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!(
                    "maintained index set row '{key}' holds the set of '{}'",
                    set.key()
                ),
            });
        }
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        self.put_maintained_indexes_in_txn(&txn, &set)?;
        txn.commit()
            .map_err(|e| redb_err("commit maintained indexes", e))?;
        Ok(set)
    }

    /// One collection's committed set. A collection with no row gives an
    /// empty set.
    pub fn maintained_indexes(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<MaintainedIndexSet> {
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(MAINTAINED_INDEXES)
            .map_err(|e| redb_err("open maintained indexes", e))?;
        let key = maintained_set_key(database_id, tenant_id, collection);
        match table
            .get(key.as_str())
            .map_err(|e| redb_err("get maintained indexes", e))?
        {
            Some(value) => MaintainedIndexSet::from_bytes(&key, value.value()),
            None => Ok(MaintainedIndexSet::empty(
                database_id,
                tenant_id,
                collection,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::maintained_index_record::MaintainedOwner;
    use super::*;
    use nodedb_physical::physical_plan::DocIndexOwner;

    fn engine() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = SparseEngine::open(&dir.path().join("sparse.redb")).expect("open");
        (engine, dir)
    }

    fn email_set(tenant_id: u64) -> MaintainedIndexSet {
        let mut set = MaintainedIndexSet::empty(0, tenant_id, "users");
        set.add_owner(
            "$.email",
            MaintainedOwner {
                owner: DocIndexOwner::Declared,
                is_array: false,
                unique: false,
                case_insensitive: false,
                predicate: None,
            },
        );
        set
    }

    #[test]
    fn a_set_commits_with_its_transaction_and_lists_by_tenant() {
        let (engine, _dir) = engine();
        let txn = engine.begin_write().expect("txn");
        engine
            .put_maintained_indexes_in_txn(&txn, &email_set(7))
            .expect("put");
        txn.commit().expect("commit");

        assert_eq!(
            engine.load_maintained_indexes().expect("load"),
            vec![email_set(7)]
        );
        let listed = engine.maintained_indexes_for_tenant(0, 7).expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].0, "0:7:users");
        assert!(
            engine
                .maintained_indexes_for_tenant(0, 8)
                .expect("list")
                .is_empty()
        );

        assert_eq!(
            engine.maintained_indexes(0, 7, "users").expect("read"),
            email_set(7)
        );
        assert!(
            engine
                .maintained_indexes(0, 8, "users")
                .expect("read")
                .is_empty()
        );

        let txn = engine.begin_write().expect("txn");
        engine
            .remove_maintained_indexes_in_txn(&txn, 0, 7, "users")
            .expect("del");
        txn.commit().expect("commit");
        assert!(engine.load_maintained_indexes().expect("load").is_empty());
    }

    #[test]
    fn an_empty_set_leaves_no_row() {
        let (engine, _dir) = engine();
        let txn = engine.begin_write().expect("txn");
        engine
            .put_maintained_indexes_in_txn(&txn, &email_set(7))
            .expect("put");
        let mut cleared = email_set(7);
        cleared.remove_owner("$.email", &DocIndexOwner::Declared);
        engine
            .put_maintained_indexes_in_txn(&txn, &cleared)
            .expect("put empty");
        txn.commit().expect("commit");
        assert!(engine.load_maintained_indexes().expect("load").is_empty());
    }

    #[test]
    fn a_snapshot_row_installs_and_a_mismatched_key_is_refused() {
        let (engine, _dir) = engine();
        let set = email_set(7);
        let bytes = set.to_bytes().expect("encode");
        assert_eq!(
            engine
                .put_maintained_indexes_raw(&set.key(), &bytes)
                .expect("install"),
            set
        );
        assert!(
            engine
                .put_maintained_indexes_raw("0:9:users", &bytes)
                .is_err()
        );
    }
}
