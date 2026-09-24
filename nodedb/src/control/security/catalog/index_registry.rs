// SPDX-License-Identifier: BUSL-1.1

//! Catalog operations on `_system.index_registry`.
//!
//! Key format: `"{database_id}:{tenant_id}:{index_name}"`. Every read that
//! spans more than one index scans the `"{database_id}:{tenant_id}:"` prefix,
//! so an index is always found under the database that owns its collection —
//! not under a fixed database 0 the way the legacy per-kind ownership rows
//! were filed.

use super::index_record::{IndexKind, StoredIndexRecord};
use super::types::{INDEX_REGISTRY, SystemCatalog, catalog_err};
use redb::{ReadableDatabase, ReadableTable};

/// Key of one index record.
fn index_key(database_id: u64, tenant_id: u64, name: &str) -> String {
    format!("{database_id}:{tenant_id}:{name}")
}

/// Prefix shared by every index record of one (database, tenant).
fn tenant_prefix(database_id: u64, tenant_id: u64) -> String {
    format!("{database_id}:{tenant_id}:")
}

/// Prefix shared by every index record of one database.
fn database_prefix(database_id: u64) -> String {
    format!("{database_id}:")
}

/// Exclusive upper bound for [`database_prefix`].
///
/// The prefix ends with `:`. The next byte after `:` is `;`, so this key sorts
/// immediately past every tenant of the database.
fn database_upper_bound(database_id: u64) -> String {
    format!("{database_id};")
}

impl SystemCatalog {
    /// Upsert one index record.
    pub fn put_index_record(&self, record: &StoredIndexRecord) -> crate::Result<()> {
        let key = index_key(record.database_id, record.tenant_id, &record.name);
        let bytes = zerompk::to_msgpack_vec(record)
            .map_err(|e| catalog_err("serialize index record", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(INDEX_REGISTRY)
                .map_err(|e| catalog_err("open index_registry", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert index record", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Read one index record by name, with the calling connection's buffered
    /// transactional DDL merged in — a `CREATE INDEX` this same transaction
    /// has buffered but not yet committed resolves here too.
    pub fn get_index_record(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredIndexRecord>> {
        let committed = self.get_committed_index_record(database_id, tenant_id, name)?;
        Ok(crate::control::catalog_overlay::resolve_index_record(
            database_id,
            tenant_id,
            name,
            committed,
        ))
    }

    /// Committed-only read, bypassing the transaction DDL overlay.
    pub fn get_committed_index_record(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredIndexRecord>> {
        let key = index_key(database_id, tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(INDEX_REGISTRY)
            .map_err(|e| catalog_err("open index_registry", e))?;
        match table.get(key.as_str()) {
            Ok(Some(value)) => Ok(Some(
                zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser index record", e))?,
            )),
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get index record", e)),
        }
    }

    /// The collection an index of `kind` was built on.
    ///
    /// The registry is the only place an index name is bound to a collection,
    /// so a read that names an index and nothing else — `TOPK(<index>, k)` and
    /// its siblings — resolves the collection it must be authorized against
    /// here. `None` when no such index exists, when it belongs to another
    /// kind, or when its collection is soft-dropped: all three leave the read
    /// with no collection to authorize, which is a refusal rather than an
    /// unguarded read.
    pub fn index_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
        kind: IndexKind,
    ) -> crate::Result<Option<String>> {
        Ok(self
            .get_index_record(database_id, tenant_id, name)?
            .filter(|record| record.kind == kind && record.is_visible())
            .map(|record| record.collection))
    }

    /// Remove one index record. Returns whether a record was present.
    pub fn delete_index_record(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let key = index_key(database_id, tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let removed = {
            let mut table = write_txn
                .open_table(INDEX_REGISTRY)
                .map_err(|e| catalog_err("open index_registry", e))?;
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove index record", e))?
                .is_some()
        };
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(removed)
    }

    /// Every index record of one (database, tenant), in key order, with the
    /// calling connection's buffered transactional DDL merged in.
    pub fn list_index_records(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredIndexRecord>> {
        let committed = self.list_committed_index_records(database_id, tenant_id)?;
        Ok(crate::control::catalog_overlay::resolve_index_records(
            database_id,
            tenant_id,
            committed,
        ))
    }

    /// Committed-only listing, bypassing the transaction DDL overlay.
    pub fn list_committed_index_records(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredIndexRecord>> {
        let prefix = tenant_prefix(database_id, tenant_id);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(INDEX_REGISTRY)
            .map_err(|e| catalog_err("open index_registry", e))?;
        let mut out = Vec::new();
        for item in table
            .range(prefix.as_str()..)
            .map_err(|e| catalog_err("range index_registry", e))?
        {
            let (key, value) = item.map_err(|e| catalog_err("read index record", e))?;
            if !key.value().starts_with(prefix.as_str()) {
                break;
            }
            out.push(
                zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser index record", e))?,
            );
        }
        Ok(out)
    }

    /// Every index record of one database, across every tenant, in key order.
    ///
    /// The scan is bounded to the database's key range.
    pub fn list_index_records_in_database(
        &self,
        database_id: u64,
    ) -> crate::Result<Vec<StoredIndexRecord>> {
        let prefix = database_prefix(database_id);
        let upper = database_upper_bound(database_id);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(INDEX_REGISTRY)
            .map_err(|e| catalog_err("open index_registry", e))?;
        let mut out = Vec::new();
        for item in table
            .range(prefix.as_str()..upper.as_str())
            .map_err(|e| catalog_err("range index_registry", e))?
        {
            let (_, value) = item.map_err(|e| catalog_err("read index record", e))?;
            out.push(
                zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser index record", e))?,
            );
        }
        Ok(out)
    }

    /// Every index record attached to one collection.
    pub fn list_index_records_for_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<Vec<StoredIndexRecord>> {
        Ok(self
            .list_index_records(database_id, tenant_id)?
            .into_iter()
            .filter(|r| r.collection == collection)
            .collect())
    }

    /// Every index record in the catalog, across databases and tenants. Used
    /// by the boot-time registry seeding and by the integrity verifier.
    pub fn list_all_index_records(&self) -> crate::Result<Vec<StoredIndexRecord>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(INDEX_REGISTRY)
            .map_err(|e| catalog_err("open index_registry", e))?;
        let mut out = Vec::new();
        for item in table
            .range(..)
            .map_err(|e| catalog_err("range index_registry", e))?
        {
            let (_, value) = item.map_err(|e| catalog_err("read index record", e))?;
            out.push(
                zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser index record", e))?,
            );
        }
        Ok(out)
    }

    /// Flip `is_active` on every index record of one collection, mirroring the
    /// collection's own soft-delete state. Returns the number of records
    /// changed.
    pub fn set_index_records_active_for_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        is_active: bool,
    ) -> crate::Result<usize> {
        let mut changed = 0usize;
        for mut record in
            self.list_index_records_for_collection(database_id, tenant_id, collection)?
        {
            if record.is_active == is_active {
                continue;
            }
            record.is_active = is_active;
            self.put_index_record(&record)?;
            changed += 1;
        }
        Ok(changed)
    }

    /// Remove every index record of one collection. Returns the number
    /// removed.
    pub fn delete_index_records_for_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<usize> {
        let mut removed = 0usize;
        for record in self.list_index_records_for_collection(database_id, tenant_id, collection)? {
            if self.delete_index_record(database_id, tenant_id, &record.name)? {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::super::index_record::{IndexKind, StoredIndexRecord};
    use super::*;
    use tempfile::TempDir;

    fn catalog() -> (SystemCatalog, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let catalog = SystemCatalog::open(&tmp.path().join("system.redb")).expect("open catalog");
        (catalog, tmp)
    }

    fn record(name: &str, collection: &str, kind: IndexKind) -> StoredIndexRecord {
        StoredIndexRecord {
            database_id: 0,
            tenant_id: 1,
            name: name.to_string(),
            kind,
            collection: collection.to_string(),
            fields: vec!["embedding".to_string()],
            is_active: true,
        }
    }

    #[test]
    fn put_get_delete_round_trip() {
        let (catalog, _tmp) = catalog();
        let rec = record("idx_a", "docs", IndexKind::Vector);
        catalog.put_index_record(&rec).unwrap();
        assert_eq!(catalog.get_index_record(0, 1, "idx_a").unwrap(), Some(rec));
        assert!(catalog.delete_index_record(0, 1, "idx_a").unwrap());
        assert_eq!(catalog.get_index_record(0, 1, "idx_a").unwrap(), None);
        assert!(!catalog.delete_index_record(0, 1, "idx_a").unwrap());
    }

    #[test]
    fn listing_is_scoped_to_database_and_tenant() {
        let (catalog, _tmp) = catalog();
        catalog
            .put_index_record(&record("idx_a", "docs", IndexKind::Vector))
            .unwrap();
        let mut other_tenant = record("idx_b", "docs", IndexKind::Secondary);
        other_tenant.tenant_id = 2;
        catalog.put_index_record(&other_tenant).unwrap();
        let mut other_db = record("idx_c", "docs", IndexKind::Secondary);
        other_db.database_id = 7;
        catalog.put_index_record(&other_db).unwrap();

        let listed = catalog.list_index_records(0, 1).unwrap();
        assert_eq!(listed.len(), 1, "{listed:?}");
        assert_eq!(listed[0].name, "idx_a");
        assert_eq!(catalog.list_all_index_records().unwrap().len(), 3);
    }

    #[test]
    fn index_collection_resolves_only_the_matching_visible_kind() {
        let (catalog, _tmp) = catalog();
        catalog
            .put_index_record(&record("board", "scores", IndexKind::Sorted))
            .unwrap();

        assert_eq!(
            catalog
                .index_collection(0, 1, "board", IndexKind::Sorted)
                .unwrap(),
            Some("scores".to_string())
        );
        // A different kind under the same name is not this index.
        assert_eq!(
            catalog
                .index_collection(0, 1, "board", IndexKind::Vector)
                .unwrap(),
            None
        );
        // An unknown name resolves to nothing.
        assert_eq!(
            catalog
                .index_collection(0, 1, "missing", IndexKind::Sorted)
                .unwrap(),
            None
        );
        // A soft-dropped collection hides its indexes from resolution.
        catalog
            .set_index_records_active_for_collection(0, 1, "scores", false)
            .unwrap();
        assert_eq!(
            catalog
                .index_collection(0, 1, "board", IndexKind::Sorted)
                .unwrap(),
            None
        );
    }

    #[test]
    fn collection_scoped_activation_and_removal() {
        let (catalog, _tmp) = catalog();
        catalog
            .put_index_record(&record("idx_a", "docs", IndexKind::Vector))
            .unwrap();
        catalog
            .put_index_record(&record("idx_b", "docs", IndexKind::FullText))
            .unwrap();
        catalog
            .put_index_record(&record("idx_c", "other", IndexKind::Secondary))
            .unwrap();

        assert_eq!(
            catalog
                .set_index_records_active_for_collection(0, 1, "docs", false)
                .unwrap(),
            2
        );
        let inactive: Vec<_> = catalog
            .list_index_records_for_collection(0, 1, "docs")
            .unwrap();
        assert!(inactive.iter().all(|r| !r.is_active));
        assert!(
            catalog
                .get_index_record(0, 1, "idx_c")
                .unwrap()
                .expect("other collection untouched")
                .is_active
        );

        assert_eq!(
            catalog
                .delete_index_records_for_collection(0, 1, "docs")
                .unwrap(),
            2
        );
        assert_eq!(catalog.list_index_records(0, 1).unwrap().len(), 1);
    }
}
