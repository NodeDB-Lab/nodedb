// SPDX-License-Identifier: BUSL-1.1

//! The collection key of `_system.index_registry`.
//!
//! `INDEX_REGISTRY` keys a record by its name, so a name lookup is one get.
//! `INDEX_REGISTRY_BY_COLLECTION` keys the same record by
//! `(database_id, tenant_id, collection, name)`, so one collection's records
//! are a prefix scan. Every write of a record moves its collection key in
//! the same write transaction.

use redb::{ReadTransaction, ReadableDatabase, ReadableTable, WriteTransaction};

use super::index_record::StoredIndexRecord;
use super::tables::{INDEX_REGISTRY, INDEX_REGISTRY_BY_COLLECTION};
use super::types::{SystemCatalog, catalog_err};

/// Decode one stored index record.
pub(super) fn decode_index_record(bytes: &[u8]) -> crate::Result<StoredIndexRecord> {
    zerompk::from_msgpack(bytes).map_err(|e| catalog_err("deser index record", e))
}

/// Move the collection key of `record` inside `txn`: drop the key of
/// `prior`, the record the write replaced, and insert the key of `record`.
pub(super) fn put_collection_key(
    txn: &WriteTransaction,
    prior: Option<&StoredIndexRecord>,
    record: &StoredIndexRecord,
) -> crate::Result<()> {
    let mut table = txn
        .open_table(INDEX_REGISTRY_BY_COLLECTION)
        .map_err(|e| catalog_err("open index_registry_by_collection", e))?;
    if let Some(prior) = prior
        && prior.collection != record.collection
    {
        table
            .remove(collection_key(prior))
            .map_err(|e| catalog_err("remove index collection key", e))?;
    }
    table
        .insert(collection_key(record), ())
        .map_err(|e| catalog_err("insert index collection key", e))?;
    Ok(())
}

/// Drop the collection key of `record` inside `txn`.
pub(super) fn remove_collection_key(
    txn: &WriteTransaction,
    record: &StoredIndexRecord,
) -> crate::Result<()> {
    let mut table = txn
        .open_table(INDEX_REGISTRY_BY_COLLECTION)
        .map_err(|e| catalog_err("open index_registry_by_collection", e))?;
    table
        .remove(collection_key(record))
        .map_err(|e| catalog_err("remove index collection key", e))?;
    Ok(())
}

/// The collection key of `record`.
fn collection_key(record: &StoredIndexRecord) -> (u64, u64, &str, &str) {
    (
        record.database_id,
        record.tenant_id,
        record.collection.as_str(),
        record.name.as_str(),
    )
}

/// Every committed index record of `collection` in one (database, tenant),
/// in name order, read inside `read_txn`. The scan covers the collection's
/// keys only.
pub(super) fn list_committed_index_records_for_collection_in(
    read_txn: &ReadTransaction,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> crate::Result<Vec<StoredIndexRecord>> {
    let keys = read_txn
        .open_table(INDEX_REGISTRY_BY_COLLECTION)
        .map_err(|e| catalog_err("open index_registry_by_collection", e))?;
    let records = read_txn
        .open_table(INDEX_REGISTRY)
        .map_err(|e| catalog_err("open index_registry", e))?;
    let mut out = Vec::new();
    for item in keys
        .range((database_id, tenant_id, collection, "")..)
        .map_err(|e| catalog_err("range index_registry_by_collection", e))?
    {
        let (key, _) = item.map_err(|e| catalog_err("read index collection key", e))?;
        let (db, tenant, owner, name) = key.value();
        if db != database_id || tenant != tenant_id || owner != collection {
            break;
        }
        let record_key = format!("{database_id}:{tenant_id}:{name}");
        let value = records
            .get(record_key.as_str())
            .map_err(|e| catalog_err("get index record", e))?
            .ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "index registry: collection key ({database_id}, {tenant_id}, \
                     {collection}, {name}) names no index record; the catalog is \
                     inconsistent, restore it from a backup"
                ),
            })?;
        out.push(decode_index_record(value.value())?);
    }
    Ok(out)
}

impl SystemCatalog {
    /// Every committed index record of one collection, bypassing the
    /// transaction DDL overlay. The scan covers the collection's keys only.
    pub fn list_committed_index_records_for_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<Vec<StoredIndexRecord>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        list_committed_index_records_for_collection_in(
            &read_txn,
            database_id,
            tenant_id,
            collection,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::super::index_record::{IndexKind, IndexRecordState};
    use super::*;
    use tempfile::TempDir;

    fn catalog() -> (SystemCatalog, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let catalog = SystemCatalog::open(&tmp.path().join("system.redb")).expect("open catalog");
        (catalog, tmp)
    }

    fn record(name: &str, collection: &str) -> StoredIndexRecord {
        StoredIndexRecord {
            database_id: 0,
            tenant_id: 1,
            name: name.to_string(),
            kind: IndexKind::FullText,
            collection: collection.to_string(),
            fields: vec!["body".to_string()],
            is_active: true,
            state: IndexRecordState::Ready,
            analyzer: None,
            fuzzy_default: None,
        }
    }

    fn names_of(catalog: &SystemCatalog, collection: &str) -> Vec<String> {
        catalog
            .list_committed_index_records_for_collection(0, 1, collection)
            .expect("list")
            .into_iter()
            .map(|r| r.name)
            .collect()
    }

    #[test]
    fn a_collection_scan_reads_only_its_own_records() {
        let (catalog, _tmp) = catalog();
        for (name, collection) in [("a", "doc"), ("b", "docs"), ("c", "doc"), ("d", "zeta")] {
            catalog
                .put_index_record(&record(name, collection))
                .expect("put");
        }
        let mut other_tenant = record("e", "doc");
        other_tenant.tenant_id = 2;
        catalog.put_index_record(&other_tenant).expect("put");

        assert_eq!(names_of(&catalog, "doc"), vec!["a", "c"]);
        assert_eq!(names_of(&catalog, "docs"), vec!["b"]);
        assert!(names_of(&catalog, "missing").is_empty());
    }

    #[test]
    fn a_rewrite_and_a_delete_move_the_collection_key() {
        let (catalog, _tmp) = catalog();
        catalog.put_index_record(&record("a", "doc")).expect("put");
        catalog
            .put_index_record(&record("a", "notes"))
            .expect("rewrite");
        assert!(names_of(&catalog, "doc").is_empty());
        assert_eq!(names_of(&catalog, "notes"), vec!["a"]);

        assert!(catalog.delete_index_record(0, 1, "a").expect("delete"));
        assert!(names_of(&catalog, "notes").is_empty());
    }
}
