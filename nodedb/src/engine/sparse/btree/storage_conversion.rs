// SPDX-License-Identifier: BUSL-1.1

//! Durable record of the last `CONVERT COLLECTION` a core applied.
//!
//! The record commits in the redb transaction that rewrites the rows, so it
//! always names the encoding the rows hold. It is what makes a re-delivered
//! conversion answer without a rewrite. It also lets a restart pick the rows'
//! encoding when the catalog row and the data-group log stand apart.
//!
//! Storage: its own `storage_conversions` redb table, keyed
//! `"{database_id}:{tenant_id}:{collection}"`.

use nodedb_physical::physical_plan::StorageMode;
use nodedb_types::Hlc;
use redb::{ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};

use super::engine::SparseEngine;
use super::tables::redb_err;

/// Table definition for stored conversion records.
pub(super) const STORAGE_CONVERSIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("storage_conversions");

/// The last conversion a core applied to one collection.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StorageConversionRecord {
    /// The conversion's generation.
    pub generation: u64,
    /// The collection incarnation it rewrote.
    pub incarnation: Hlc,
    /// The mode the rows hold since it applied.
    pub target: StorageMode,
}

impl StorageConversionRecord {
    /// The record's stored bytes.
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("encode storage conversion: {e}"),
        })
    }

    /// A record from its stored bytes.
    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("decode storage conversion: {e}"),
        })
    }
}

fn record_key(database_id: u64, tenant_id: u64, collection: &str) -> String {
    format!("{database_id}:{tenant_id}:{collection}")
}

impl SparseEngine {
    /// One collection's conversion record, `None` when no conversion applied.
    pub fn get_storage_conversion(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<Option<StorageConversionRecord>> {
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(STORAGE_CONVERSIONS)
            .map_err(|e| redb_err("open storage conversions", e))?;
        let key = record_key(database_id, tenant_id, collection);
        match table
            .get(key.as_str())
            .map_err(|e| redb_err("get storage conversion", e))?
        {
            Some(value) => Ok(Some(StorageConversionRecord::from_bytes(value.value())?)),
            None => Ok(None),
        }
    }

    /// Store `record` inside `txn`, which rewrites the collection's rows.
    pub fn put_storage_conversion_in_txn(
        &self,
        txn: &WriteTransaction,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        record: &StorageConversionRecord,
    ) -> crate::Result<()> {
        let bytes = record.to_bytes()?;
        let mut table = txn
            .open_table(STORAGE_CONVERSIONS)
            .map_err(|e| redb_err("open storage conversions", e))?;
        let key = record_key(database_id, tenant_id, collection);
        table
            .insert(key.as_str(), bytes.as_slice())
            .map_err(|e| redb_err("put storage conversion", e))?;
        Ok(())
    }

    /// Store `key`'s encoded record, as a snapshot install does.
    pub fn put_storage_conversion_raw(&self, key: &str, bytes: &[u8]) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = txn
                .open_table(STORAGE_CONVERSIONS)
                .map_err(|e| redb_err("open storage conversions", e))?;
            table
                .insert(key, bytes)
                .map_err(|e| redb_err("put storage conversion", e))?;
        }
        txn.commit()
            .map_err(|e| redb_err("commit storage conversion", e))?;
        Ok(())
    }

    /// Remove one collection's record, as a snapshot install's clear does.
    pub fn delete_storage_conversion(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = txn
                .open_table(STORAGE_CONVERSIONS)
                .map_err(|e| redb_err("open storage conversions", e))?;
            let key = record_key(database_id, tenant_id, collection);
            table
                .remove(key.as_str())
                .map_err(|e| redb_err("delete storage conversion", e))?;
        }
        txn.commit()
            .map_err(|e| redb_err("commit storage conversion", e))?;
        Ok(())
    }

    /// Every record of `(database_id, tenant_id)`, keyed
    /// `"{database_id}:{tenant_id}:{collection}"`.
    pub fn storage_conversions_for_tenant(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(STORAGE_CONVERSIONS)
            .map_err(|e| redb_err("open storage conversions", e))?;
        let prefix = format!("{database_id}:{tenant_id}:");
        let mut out = Vec::new();
        for entry in table
            .range(prefix.as_str()..)
            .map_err(|e| redb_err("range storage conversions", e))?
        {
            let (key, value) = entry.map_err(|e| redb_err("scan storage conversion", e))?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            out.push((key.value().to_string(), value.value().to_vec()));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = SparseEngine::open(&dir.path().join("sparse.redb")).expect("open");
        (engine, dir)
    }

    #[test]
    fn a_record_round_trips_inside_a_transaction_and_lists_by_tenant() {
        let (engine, _dir) = engine();
        let record = StorageConversionRecord {
            generation: 3,
            incarnation: Hlc::new(9, 0),
            target: StorageMode::Schemaless,
        };
        let txn = engine.begin_write().expect("txn");
        engine
            .put_storage_conversion_in_txn(&txn, 0, 7, "docs", &record)
            .expect("put");
        txn.commit().expect("commit");

        assert_eq!(
            engine.get_storage_conversion(0, 7, "docs").expect("get"),
            Some(record)
        );
        assert_eq!(
            engine.get_storage_conversion(0, 8, "docs").expect("get"),
            None
        );
        let listed = engine.storage_conversions_for_tenant(0, 7).expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].0, "0:7:docs");

        engine.delete_storage_conversion(0, 7, "docs").expect("del");
        assert_eq!(
            engine.get_storage_conversion(0, 7, "docs").expect("get"),
            None
        );
    }
}
