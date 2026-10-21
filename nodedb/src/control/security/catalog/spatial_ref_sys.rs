// SPDX-License-Identifier: BUSL-1.1

//! User-defined CRSs (`CREATE SPATIAL_REF_SYS`) in the system catalog.
//!
//! `_system.spatial_ref_sys` keys on the SRID code and holds one
//! MessagePack `CustomCrs` per code: a live definition, or the tombstone a
//! `DROP SPATIAL_REF_SYS` leaves. A dropped code is never reused, so stored
//! data that names it fails to resolve instead of changing meaning.
//!
//! The catalog keeps the whole set in memory as an `Arc<CustomCrsSet>`.
//! Readers take a share of the current set. A write builds the next set,
//! commits its row, then swaps the share, all under the set's write lock.

use std::sync::Arc;

use nodedb_crs::{CrsDef, CustomCrs, CustomCrsSet, CustomCrsState, Srid};
use redb::{ReadableDatabase, ReadableTable};

use super::tables::SPATIAL_REF_SYS;
use super::types::{SystemCatalog, catalog_err};

/// What a write did to the user-defined CRS set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CustomCrsWrite {
    /// The row changed.
    Written,
    /// The set already holds the change, or refuses it. Every node applies
    /// the same log, so every node leaves the set alike. The DDL that
    /// proposed the entry reads the set back to tell which.
    Unchanged,
}

impl SystemCatalog {
    /// A share of the current user-defined CRS set.
    pub fn custom_crs_set(&self) -> Arc<CustomCrsSet> {
        Arc::clone(&self.crs_set.read().unwrap_or_else(|p| p.into_inner()))
    }

    /// Define `def` under its SRID. A code that is live or dropped leaves
    /// the set unchanged.
    pub fn define_custom_crs(&self, def: &CrsDef) -> crate::Result<CustomCrsWrite> {
        let mut current = self.crs_set.write().unwrap_or_else(|p| p.into_inner());
        let mut next = CustomCrsSet::clone(&current);
        if next.define(def.clone()).is_err() {
            return Ok(CustomCrsWrite::Unchanged);
        }
        let row = CustomCrs {
            srid: def.srid,
            state: CustomCrsState::Live(Box::new(def.clone())),
        };
        self.write_custom_crs_row(&row)?;
        *current = Arc::new(next);
        Ok(CustomCrsWrite::Written)
    }

    /// Retire the live CRS `srid`. A code that is not live leaves the set
    /// unchanged.
    pub fn drop_custom_crs(&self, srid: Srid) -> crate::Result<CustomCrsWrite> {
        let mut current = self.crs_set.write().unwrap_or_else(|p| p.into_inner());
        let mut next = CustomCrsSet::clone(&current);
        if next.drop_crs(srid).is_err() {
            return Ok(CustomCrsWrite::Unchanged);
        }
        let row = CustomCrs {
            srid,
            state: CustomCrsState::Dropped,
        };
        self.write_custom_crs_row(&row)?;
        *current = Arc::new(next);
        Ok(CustomCrsWrite::Written)
    }

    /// Reload the in-memory set from the table, after open or after a
    /// metadata image replaced the table.
    pub fn reload_custom_crs_set(&self) -> crate::Result<()> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("spatial_ref_sys read txn", e))?;
        let set = load_custom_crs_set_in(&read_txn)?;
        *self.crs_set.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(set);
        Ok(())
    }

    fn write_custom_crs_row(&self, row: &CustomCrs) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(row)
            .map_err(|e| catalog_err("serialize spatial_ref_sys row", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("spatial_ref_sys write txn", e))?;
        {
            let mut table = write_txn
                .open_table(SPATIAL_REF_SYS)
                .map_err(|e| catalog_err("open spatial_ref_sys", e))?;
            table
                .insert(row.srid.code(), bytes.as_slice())
                .map_err(|e| catalog_err("insert spatial_ref_sys row", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit spatial_ref_sys", e))
    }
}

/// The user-defined CRS set the table holds in `read_txn`.
pub(super) fn load_custom_crs_set_in(
    read_txn: &redb::ReadTransaction,
) -> crate::Result<CustomCrsSet> {
    let table = read_txn
        .open_table(SPATIAL_REF_SYS)
        .map_err(|e| catalog_err("open spatial_ref_sys", e))?;
    let mut entries = Vec::new();
    for row in table
        .iter()
        .map_err(|e| catalog_err("iter spatial_ref_sys", e))?
    {
        let (key, value) = row.map_err(|e| catalog_err("read spatial_ref_sys row", e))?;
        let entry: CustomCrs = zerompk::from_msgpack(value.value())
            .map_err(|e| catalog_err("decode spatial_ref_sys row", e))?;
        if entry.srid.code() != key.value() {
            return Err(catalog_err(
                "spatial_ref_sys row",
                format!(
                    "row under key {} holds SRID {}",
                    key.value(),
                    entry.srid.code()
                ),
            ));
        }
        entries.push(entry);
    }
    CustomCrsSet::from_entries(entries).map_err(|e| catalog_err("spatial_ref_sys rows", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_crs::parse_proj_string;

    fn def(code: u32) -> CrsDef {
        let srid = Srid::new(code).unwrap();
        parse_proj_string(srid, "user tm", "+proj=tmerc +lon_0=101 +ellps=GRS80").unwrap()
    }

    #[test]
    fn define_drop_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        {
            let catalog = SystemCatalog::open(&path).unwrap();
            assert_eq!(
                catalog.define_custom_crs(&def(910_010)).unwrap(),
                CustomCrsWrite::Written
            );
            assert_eq!(
                catalog.define_custom_crs(&def(910_010)).unwrap(),
                CustomCrsWrite::Unchanged
            );
            let srid = Srid::new(910_010).unwrap();
            assert_eq!(
                catalog.drop_custom_crs(srid).unwrap(),
                CustomCrsWrite::Written
            );
            assert_eq!(
                catalog.drop_custom_crs(srid).unwrap(),
                CustomCrsWrite::Unchanged
            );
            assert_eq!(
                catalog.define_custom_crs(&def(910_010)).unwrap(),
                CustomCrsWrite::Unchanged
            );
            catalog.define_custom_crs(&def(910_011)).unwrap();
            assert_eq!(
                catalog.define_custom_crs(&def(4326)).unwrap(),
                CustomCrsWrite::Unchanged
            );
        }
        let reopened = SystemCatalog::open(&path).unwrap();
        let set = reopened.custom_crs_set();
        assert_eq!(set.len(), 2);
        assert_eq!(
            set.get(Srid::new(910_010).unwrap()),
            Some(&CustomCrsState::Dropped)
        );
        assert!(matches!(
            set.get(Srid::new(910_011).unwrap()),
            Some(CustomCrsState::Live(_))
        ));
    }
}
