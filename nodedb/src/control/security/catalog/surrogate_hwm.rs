//! Surrogate hwm catalog ops for the `_system.surrogate_hwm` table.
//!
//! Singleton table — one row keyed `"global"` holding the highest
//! surrogate ever allocated. See
//! `nodedb::control::surrogate::persist` for the trait + bootstrap
//! that consumes these methods.

use super::types::{SystemCatalog, catalog_err};

/// Redb table: singleton `"global"` -> highest allocated surrogate (`u32`).
pub const SURROGATE_HWM: redb::TableDefinition<&str, u32> =
    redb::TableDefinition::new("_system.surrogate_hwm");

/// Singleton row key.
const HWM_KEY: &str = "global";

impl SystemCatalog {
    /// Persist the surrogate allocator high-watermark. Overwrites the
    /// singleton row.
    pub fn put_surrogate_hwm(&self, hwm: u32) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_hwm write txn", e))?;
        {
            let mut table = txn
                .open_table(SURROGATE_HWM)
                .map_err(|e| catalog_err("open surrogate_hwm", e))?;
            table
                .insert(HWM_KEY, hwm)
                .map_err(|e| catalog_err("insert surrogate_hwm", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("surrogate_hwm commit", e))
    }

    /// Load the persisted surrogate hwm, or `0` if none recorded yet
    /// (fresh database).
    pub fn get_surrogate_hwm(&self) -> crate::Result<u32> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_hwm read txn", e))?;
        let table = txn
            .open_table(SURROGATE_HWM)
            .map_err(|e| catalog_err("open surrogate_hwm", e))?;
        match table
            .get(HWM_KEY)
            .map_err(|e| catalog_err("get surrogate_hwm", e))?
        {
            Some(v) => Ok(v.value()),
            None => Ok(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let catalog = SystemCatalog::open(&path).unwrap();
        assert_eq!(catalog.get_surrogate_hwm().unwrap(), 0);
    }

    #[test]
    fn put_then_get_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let catalog = SystemCatalog::open(&path).unwrap();
        catalog.put_surrogate_hwm(42).unwrap();
        assert_eq!(catalog.get_surrogate_hwm().unwrap(), 42);
        catalog.put_surrogate_hwm(1_000_000).unwrap();
        assert_eq!(catalog.get_surrogate_hwm().unwrap(), 1_000_000);
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        {
            let catalog = SystemCatalog::open(&path).unwrap();
            catalog.put_surrogate_hwm(7777).unwrap();
        }
        let catalog = SystemCatalog::open(&path).unwrap();
        assert_eq!(catalog.get_surrogate_hwm().unwrap(), 7777);
    }
}
