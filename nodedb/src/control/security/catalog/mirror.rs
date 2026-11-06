// SPDX-License-Identifier: BUSL-1.1

//! Catalog persistence for the mirror subsystem: `_system.mirror_cut_state`.
//!
//! The table holds one [`MirrorCutState`] per mirror database. Only a
//! committed metadata entry writes it, so every node holds the same row, and
//! a group 0 snapshot carries it.
//!
//! `_system.mirror_pins` holds one [`MirrorPin`] per link that follows a
//! database of this cluster. It replicates the same way.

use nodedb_types::{DatabaseId, MirrorCutState, MirrorPin, MirrorPinState};
use redb::{ReadableDatabase, ReadableTable};

use super::types::{MIRROR_CUT_STATE, MIRROR_PINS, SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Every link's pin.
    pub fn list_mirror_pins(&self) -> crate::Result<Vec<MirrorPin>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("mirror_pins begin_read", e))?;
        let table = txn
            .open_table(MIRROR_PINS)
            .map_err(|e| catalog_err("open mirror_pins", e))?;
        let mut pins = Vec::new();
        for row in table
            .iter()
            .map_err(|e| catalog_err("iter mirror_pins", e))?
        {
            let (_, value) = row.map_err(|e| catalog_err("read mirror_pins", e))?;
            pins.push(
                zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deserialize mirror_pins", e))?,
            );
        }
        Ok(pins)
    }

    /// Store `pin`, merged with the link's row: a late ack moves nothing back,
    /// and a pin that stopped holding stays stopped.
    pub fn raise_mirror_pin(&self, pin: &MirrorPin) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("mirror_pins begin_write", e))?;
        {
            let mut table = txn
                .open_table(MIRROR_PINS)
                .map_err(|e| catalog_err("open mirror_pins (raise)", e))?;
            let current: Option<MirrorPin> = table
                .get(pin.link_id)
                .map_err(|e| catalog_err("get mirror_pins (raise)", e))?
                .map(|row| zerompk::from_msgpack(row.value()))
                .transpose()
                .map_err(|e| catalog_err("deserialize mirror_pins (raise)", e))?;
            let next = current.map_or_else(|| pin.clone(), |current| current.merged(pin));
            let bytes = zerompk::to_msgpack_vec(&next)
                .map_err(|e| catalog_err("serialize mirror_pins", e))?;
            table
                .insert(pin.link_id, bytes.as_slice())
                .map_err(|e| catalog_err("insert mirror_pins", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("mirror_pins commit", e))
    }

    /// Remove the pin of `link_id`.
    pub fn delete_mirror_pin(&self, link_id: u64) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("mirror_pins begin_write (delete)", e))?;
        {
            let mut table = txn
                .open_table(MIRROR_PINS)
                .map_err(|e| catalog_err("open mirror_pins (delete)", e))?;
            table
                .remove(link_id)
                .map_err(|e| catalog_err("remove mirror_pins", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("mirror_pins delete commit", e))
    }

    /// Stop every pin that follows the dropped database `database`. The row
    /// stays, so its link's next fetch learns the source dropped.
    pub fn mark_mirror_pins_source_dropped(&self, database: u64) -> crate::Result<()> {
        for pin in self.list_mirror_pins()? {
            if pin.database == database {
                self.raise_mirror_pin(&MirrorPin {
                    state: MirrorPinState::SourceDropped,
                    ..pin
                })?;
            }
        }
        Ok(())
    }

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

    /// Forget every cut and position of a mirror that starts over, keeping only
    /// its fence. A copy of a former epoch then skips on every replica.
    pub fn reset_mirror_cut_state(&self, mirror_db_id: DatabaseId) -> crate::Result<()> {
        let fence_epoch = self
            .get_mirror_cut_state(mirror_db_id)?
            .map_or(0, |state| state.fence_epoch);
        let state = MirrorCutState {
            fence_epoch,
            ..MirrorCutState::default()
        };
        let bytes = zerompk::to_msgpack_vec(&state)
            .map_err(|e| catalog_err("serialize mirror_cut_state (reset)", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("mirror_cut_state begin_write (reset)", e))?;
        {
            let mut table = txn
                .open_table(MIRROR_CUT_STATE)
                .map_err(|e| catalog_err("open mirror_cut_state (reset)", e))?;
            table
                .insert(mirror_db_id.as_u64(), bytes.as_slice())
                .map_err(|e| catalog_err("insert mirror_cut_state (reset)", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("mirror_cut_state reset commit", e))
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
            epochs: Vec::new(),
            fence_epoch: 0,
            source_timeline: None,
        }
    }

    /// A rebuild forgets every position and cut, and keeps the fence.
    #[test]
    fn a_reset_keeps_only_the_fence() {
        let tmp = TempDir::new().unwrap();
        let catalog = open_tmp_catalog(&tmp);
        let db = DatabaseId::new(1300);
        let held = MirrorCutState {
            fence_epoch: 7,
            ..state(5, 4)
        };
        catalog.raise_mirror_cut_state(db, &held).unwrap();
        catalog.reset_mirror_cut_state(db).unwrap();
        assert_eq!(
            catalog.get_mirror_cut_state(db).unwrap(),
            Some(MirrorCutState {
                fence_epoch: 7,
                ..MirrorCutState::default()
            })
        );
    }

    fn pin(link_id: u64, database: u64, log_index: u64) -> MirrorPin {
        MirrorPin {
            link_id,
            client: "dr".to_string(),
            database,
            positions: vec![MirrorGroupPosition {
                group_id: 1,
                log_index,
            }],
            acked_ms: 5,
            state: MirrorPinState::Live,
            mode: nodedb_types::MirrorMode::Async,
            acked_ordinal: 0,
        }
    }

    #[test]
    fn a_pin_merges_on_raise_and_a_delete_removes_it() {
        let tmp = TempDir::new().expect("tempdir");
        let catalog = open_tmp_catalog(&tmp);
        catalog.raise_mirror_pin(&pin(1, 1024, 40)).expect("raise");
        catalog.raise_mirror_pin(&pin(1, 1024, 30)).expect("raise");
        assert_eq!(
            catalog.list_mirror_pins().expect("list"),
            vec![pin(1, 1024, 40)]
        );
        catalog.delete_mirror_pin(1).expect("delete");
        assert!(catalog.list_mirror_pins().expect("list").is_empty());
    }

    #[test]
    fn a_database_drop_stops_only_its_own_pins() {
        let tmp = TempDir::new().expect("tempdir");
        let catalog = open_tmp_catalog(&tmp);
        catalog.raise_mirror_pin(&pin(1, 1024, 40)).expect("raise");
        catalog.raise_mirror_pin(&pin(2, 1025, 40)).expect("raise");
        catalog.mark_mirror_pins_source_dropped(1024).expect("mark");
        let pins = catalog.list_mirror_pins().expect("list");
        assert_eq!(pins[0].state, MirrorPinState::SourceDropped);
        assert_eq!(pins[1].state, MirrorPinState::Live);
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
