// SPDX-License-Identifier: BUSL-1.1

//! Tenant export and raw import of the versioned document and index tables.
//!
//! A `bitemporal=true` collection writes only these tables, so a tenant
//! snapshot that reads `DOCUMENTS` and `INDEXES` alone carries none of its
//! rows. Keys and values move verbatim: every version keeps its system time.

use redb::TableDefinition;

use super::doc::DOCUMENTS_VERSIONED;
use super::index::INDEXES_VERSIONED;
use crate::engine::sparse::btree::{SparseEngine, redb_err};

impl SparseEngine {
    /// Every document version of `tenant_id` in `database_id`, as
    /// `("{db}:{tid}:{coll}:{doc_id}\x00{sys_from:020}", versioned value)`.
    pub fn scan_versioned_documents_for_tenant(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        self.scan_table_for_tenant(
            DOCUMENTS_VERSIONED,
            database_id,
            tenant_id,
            "versioned document scan",
        )
    }

    /// Every versioned index entry of `tenant_id` in `database_id`.
    pub fn scan_versioned_indexes_for_tenant(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        self.scan_table_for_tenant(
            INDEXES_VERSIONED,
            database_id,
            tenant_id,
            "versioned index scan",
        )
    }

    /// Install one document version under its exported key.
    pub fn put_versioned_document_raw(&self, key: &str, value: &[u8]) -> crate::Result<()> {
        self.put_table_raw(DOCUMENTS_VERSIONED, key, value)
    }

    /// Install one versioned index entry under its exported key.
    pub fn put_versioned_index_raw(&self, key: &str, value: &[u8]) -> crate::Result<()> {
        self.put_table_raw(INDEXES_VERSIONED, key, value)
    }

    fn put_table_raw(
        &self,
        table_def: TableDefinition<&str, &[u8]>,
        key: &str,
        value: &[u8],
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut table = txn
                .open_table(table_def)
                .map_err(|e| redb_err("open table", e))?;
            table
                .insert(key, value)
                .map_err(|e| redb_err("raw insert", e))?;
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{StorageKey, Surrogate};

    use super::super::value::VersionedPut;
    use super::*;

    fn open_temp() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = SparseEngine::open(&dir.path().join("s.redb")).unwrap();
        (engine, dir)
    }

    #[test]
    fn exported_versions_install_verbatim_on_another_engine() {
        let (source, _source_dir) = open_temp();
        let key = StorageKey::for_surrogate(Surrogate::new(7));
        for (sys, body) in [(100, b"v1".as_slice()), (200, b"v2".as_slice())] {
            source
                .versioned_put(VersionedPut {
                    database_id: 0,
                    tenant: 3,
                    coll: "ledger",
                    doc_id: &key,
                    sys_from_ms: sys,
                    valid_from_ms: 0,
                    valid_until_ms: i64::MAX,
                    body,
                })
                .unwrap();
        }
        source
            .versioned_put(VersionedPut {
                database_id: 0,
                tenant: 4,
                coll: "ledger",
                doc_id: &key,
                sys_from_ms: 100,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
                body: b"other tenant",
            })
            .unwrap();

        let exported = source.scan_versioned_documents_for_tenant(0, 3).unwrap();
        assert_eq!(exported.len(), 2, "only tenant 3's versions export");

        let (target, _target_dir) = open_temp();
        for (key, value) in &exported {
            target.put_versioned_document_raw(key, value).unwrap();
        }
        assert_eq!(
            target
                .versioned_get_as_of(0, 3, "ledger", &key, Some(150), None)
                .unwrap()
                .as_deref(),
            Some(b"v1".as_slice())
        );
        assert_eq!(
            target
                .versioned_get_current(0, 3, "ledger", &key)
                .unwrap()
                .as_deref(),
            Some(b"v2".as_slice())
        );
    }
}
