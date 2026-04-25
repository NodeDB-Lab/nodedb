//! Array-catalog redb ops for the `_system.arrays` table.
//!
//! Mirrors the `triggers.rs` shape: typed put/get/delete plus a bulk
//! loader for startup. Keyed by `name` (already globally scoped in the
//! `ArrayCatalogEntry` via `ArrayId`'s tenant field — a second-level
//! tenant prefix would only duplicate that information).

use redb::ReadableTable;

use crate::control::array_catalog::ArrayCatalogEntry;

use super::types::{ARRAYS, SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Insert or overwrite an array catalog entry.
    pub fn put_array(&self, entry: &ArrayCatalogEntry) -> crate::Result<()> {
        let bytes =
            zerompk::to_msgpack_vec(entry).map_err(|e| catalog_err("serialize array", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(ARRAYS)
                .map_err(|e| catalog_err("open arrays", e))?;
            table
                .insert(entry.name.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert array", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Fetch one entry by name.
    pub fn get_array(&self, name: &str) -> crate::Result<Option<ArrayCatalogEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(ARRAYS)
            .map_err(|e| catalog_err("open arrays", e))?;
        match table.get(name) {
            Ok(Some(value)) => {
                let entry: ArrayCatalogEntry = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deser array", e))?;
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get array", e)),
        }
    }

    /// Delete by name. Returns whether the entry existed.
    pub fn delete_array(&self, name: &str) -> crate::Result<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(ARRAYS)
                .map_err(|e| catalog_err("open arrays", e))?;
            existed = table
                .remove(name)
                .map_err(|e| catalog_err("remove array", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Load every entry. Used by `ArrayCatalog::load_from_catalog` at
    /// startup.
    pub fn load_all_arrays(&self) -> crate::Result<Vec<ArrayCatalogEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(ARRAYS)
            .map_err(|e| catalog_err("open arrays", e))?;
        let mut out = Vec::new();
        let iter = table.iter().map_err(|e| catalog_err("iter arrays", e))?;
        for row in iter {
            let (_, value) = row.map_err(|e| catalog_err("iter row", e))?;
            let entry: ArrayCatalogEntry =
                zerompk::from_msgpack(value.value()).map_err(|e| catalog_err("deser array", e))?;
            out.push(entry);
        }
        Ok(out)
    }
}
