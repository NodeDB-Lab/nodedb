// SPDX-License-Identifier: BUSL-1.1

//! Catalog persistence for the mirror subsystem: `_system.mirror_cut_state`.
//!
//! The table holds one [`MirrorCutState`] per mirror database. Only a
//! committed metadata entry writes it, so every node holds the same row, and
//! a group 0 snapshot carries it.

use nodedb_types::{DatabaseId, MirrorCutState};
use redb::{ReadableDatabase, ReadableTable};

use super::types::{MIRROR_CUT_STATE, SystemCatalog, catalog_err};

impl SystemCatalog {
    /// The cut state of a mirror database, or `None` before its first cut.
    pub fn get_mirror_cut_state(
        &self,
        mirror_db_id: DatabaseId,
    ) -> crate::Result<Option<MirrorCutState>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("mirror_cut_state begin_read", e))?;
        let table = txn
            .open_table(MIRROR_CUT_STATE)
            .map_err(|e| catalog_err("open mirror_cut_state", e))?;
        let row = table
            .get(mirror_db_id.as_u64())
            .map_err(|e| catalog_err("get mirror_cut_state", e))?;
        let Some(bytes) = row else {
            return Ok(None);
        };
        zerompk::from_msgpack(bytes.value()).map(Some).map_err(|e| {
            catalog_err(
                &format!("deserialize mirror_cut_state db={}", mirror_db_id.as_u64()),
                e,
            )
        })
    }

    /// Store `state` unless the row is already past it, and return whether it
    /// wrote. A replay or a former applier's late state moves nothing back.
    pub fn raise_mirror_cut_state(
        &self,
        mirror_db_id: DatabaseId,
        state: &MirrorCutState,
    ) -> crate::Result<bool> {
        let bytes = zerompk::to_msgpack_vec(state)
            .map_err(|e| catalog_err("serialize mirror_cut_state", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("mirror_cut_state begin_write", e))?;
        let raised = {
            let mut table = txn
                .open_table(MIRROR_CUT_STATE)
                .map_err(|e| catalog_err("open mirror_cut_state (raise)", e))?;
            let current: Option<MirrorCutState> = table
                .get(mirror_db_id.as_u64())
                .map_err(|e| catalog_err("get mirror_cut_state (raise)", e))?
                .map(|row| zerompk::from_msgpack(row.value()))
                .transpose()
                .map_err(|e| catalog_err("deserialize mirror_cut_state (raise)", e))?;
            let raises = current.is_none_or(|current| state.is_at_or_past(&current));
            if raises {
                table
                    .insert(mirror_db_id.as_u64(), bytes.as_slice())
                    .map_err(|e| catalog_err("insert mirror_cut_state", e))?;
            }
            raises
        };
        txn.commit()
            .map_err(|e| catalog_err("mirror_cut_state commit", e))?;
        Ok(raised)
    }

    /// Remove the cut state of a database that no longer follows a source.
    pub fn delete_mirror_cut_state(&self, mirror_db_id: DatabaseId) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("mirror_cut_state begin_write (delete)", e))?;
        {
            let mut table = txn
                .open_table(MIRROR_CUT_STATE)
                .map_err(|e| catalog_err("open mirror_cut_state (delete)", e))?;
            table
                .remove(mirror_db_id.as_u64())
                .map_err(|e| catalog_err("remove mirror_cut_state", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("mirror_cut_state delete commit", e))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use nodedb_types::{DatabaseId, MirrorCutState, MirrorGroupPosition};
    use tempfile::TempDir;

    use super::*;

    fn open_tmp_catalog(tmp: &TempDir) -> SystemCatalog {
        let path: PathBuf = tmp.path().join("system.redb");
        SystemCatalog::open(&path).expect("open catalog")
    }

    fn state(ready_cut: u64, applied_cut: u64) -> MirrorCutState {
        MirrorCutState {
            ready_cut,
            applied_cut,
            applied_cut_hlc_ns: 1_700_000_000_000_000_000,
            cap_ordinal: 9,
            positions: vec![MirrorGroupPosition {
                group_id: 1,
                log_index: 42,
            }],
        }
    }

    #[test]
    fn a_cut_state_round_trips() {
        let tmp = TempDir::new().expect("tempdir");
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1024);
        assert!(
            catalog
                .raise_mirror_cut_state(db_id, &state(2, 1))
                .expect("raise")
        );
        assert_eq!(
            catalog.get_mirror_cut_state(db_id).expect("read"),
            Some(state(2, 1))
        );
    }

    #[test]
    fn an_unknown_database_has_no_cut_state() {
        let tmp = TempDir::new().expect("tempdir");
        let catalog = open_tmp_catalog(&tmp);
        assert!(
            catalog
                .get_mirror_cut_state(DatabaseId::new(9999))
                .expect("read")
                .is_none()
        );
    }

    #[test]
    fn a_late_state_moves_nothing_back() {
        let tmp = TempDir::new().expect("tempdir");
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1025);
        catalog
            .raise_mirror_cut_state(db_id, &state(5, 4))
            .expect("raise");
        assert!(
            !catalog
                .raise_mirror_cut_state(db_id, &state(4, 4))
                .expect("raise")
        );
        assert_eq!(
            catalog.get_mirror_cut_state(db_id).expect("read"),
            Some(state(5, 4))
        );
    }

    #[test]
    fn a_delete_removes_the_row_and_replays_cleanly() {
        let tmp = TempDir::new().expect("tempdir");
        let catalog = open_tmp_catalog(&tmp);
        let db_id = DatabaseId::new(1026);
        catalog
            .raise_mirror_cut_state(db_id, &state(1, 1))
            .expect("raise");
        for _ in 0..2 {
            catalog.delete_mirror_cut_state(db_id).expect("delete");
            assert!(catalog.get_mirror_cut_state(db_id).expect("read").is_none());
        }
    }
}
