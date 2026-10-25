// SPDX-License-Identifier: BUSL-1.1

//! Surrogate hwm catalog ops for the `_system.surrogate_hwm` table.
//!
//! Singleton table — one row keyed `"global"` holding the highest
//! surrogate ever allocated. See
//! `nodedb::control::surrogate::persist` for the trait + bootstrap
//! that consumes these methods.

use super::types::{SystemCatalog, catalog_err};
use redb::{ReadableDatabase, ReadableTable};

/// Redb table: singleton `"global"` -> highest allocated surrogate (`u32`).
pub const SURROGATE_HWM: redb::TableDefinition<&str, u32> =
    redb::TableDefinition::new("_system.surrogate_hwm");

/// Redb table: singleton `"global"` -> highest metadata Raft log index whose
/// `SurrogateReserve` has been folded into the global watermark `G` (`u64`).
/// Persisted ATOMICALLY with `SURROGATE_HWM` in cluster mode so a crash can
/// never leave the seeded `G` and the applied-reserve cursor inconsistent
/// (which diverges `G` across nodes on the next restart replay).
pub const SURROGATE_RESERVE_INDEX: redb::TableDefinition<&str, u64> =
    redb::TableDefinition::new("_system.surrogate_reserve_index");

/// Singleton row key (shared by both surrogate-state tables).
pub(super) const HWM_KEY: &str = "global";

impl SystemCatalog {
    /// Persist the surrogate allocator high-watermark. Overwrites the
    /// singleton row.
    pub fn put_surrogate_hwm(&self, hwm: u32) -> crate::Result<()> {
        #[cfg(test)]
        if self
            .faults
            .fail_next_surrogate_write
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(catalog_err(
                "surrogate watermark write",
                "injected surrogate write failure",
            ));
        }
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

    /// Cluster-mode: persist the global watermark `hwm` AND the
    /// applied-reserve cursor `reserve_index` together in a SINGLE redb write
    /// transaction. Atomicity is mandatory: if these two values were
    /// written separately, a crash between them seeds the next restart
    /// with a mismatched `(G, cursor)` pair, causing metadata-log replay to
    /// re-apply (or wrongly skip) reservations and diverge `G` across nodes.
    pub fn put_surrogate_reserve_state(&self, hwm: u32, reserve_index: u64) -> crate::Result<()> {
        #[cfg(test)]
        if self
            .faults
            .fail_next_surrogate_write
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(catalog_err(
                "surrogate watermark write",
                "injected surrogate write failure",
            ));
        }
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("surrogate_reserve_state write txn", e))?;
        {
            let mut hwm_table = txn
                .open_table(SURROGATE_HWM)
                .map_err(|e| catalog_err("open surrogate_hwm", e))?;
            hwm_table
                .insert(HWM_KEY, hwm)
                .map_err(|e| catalog_err("insert surrogate_hwm", e))?;
            let mut idx_table = txn
                .open_table(SURROGATE_RESERVE_INDEX)
                .map_err(|e| catalog_err("open surrogate_reserve_index", e))?;
            idx_table
                .insert(HWM_KEY, reserve_index)
                .map_err(|e| catalog_err("insert surrogate_reserve_index", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("surrogate_reserve_state commit", e))
    }

    /// Load the persisted applied-reserve cursor, or `0` if none recorded yet
    /// (fresh database / single-node history). Seeds the registry's
    /// `last_reserve_index` on restart.
    pub fn get_surrogate_reserve_index(&self) -> crate::Result<u64> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("surrogate_reserve_index read txn", e))?;
        let table = txn
            .open_table(SURROGATE_RESERVE_INDEX)
            .map_err(|e| catalog_err("open surrogate_reserve_index", e))?;
        match table
            .get(HWM_KEY)
            .map_err(|e| catalog_err("get surrogate_reserve_index", e))?
        {
            Some(v) => Ok(v.value()),
            None => Ok(0),
        }
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
    fn reserve_state_fresh_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let catalog = SystemCatalog::open(&path).unwrap();
        assert_eq!(catalog.get_surrogate_reserve_index().unwrap(), 0);
    }

    #[test]
    fn reserve_state_atomic_roundtrip_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        {
            let catalog = SystemCatalog::open(&path).unwrap();
            catalog.put_surrogate_reserve_state(8192, 42).unwrap();
            // Both keys written in one txn — readable via the existing
            // hwm reader (tests rely on get_surrogate_hwm) and the cursor.
            assert_eq!(catalog.get_surrogate_hwm().unwrap(), 8192);
            assert_eq!(catalog.get_surrogate_reserve_index().unwrap(), 42);
        }
        // Survives reopen.
        let catalog = SystemCatalog::open(&path).unwrap();
        assert_eq!(catalog.get_surrogate_hwm().unwrap(), 8192);
        assert_eq!(catalog.get_surrogate_reserve_index().unwrap(), 42);
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
