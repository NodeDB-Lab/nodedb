// SPDX-License-Identifier: BUSL-1.1

//! The version a rolled-back row write appended, and its removal.
//!
//! A sparse-store row write (a spatial geometry row, a vector sidecar)
//! appends one version, stamped above the row's newest stored version. The
//! undo entry records that newest version before the write: the row's
//! floor. The rollback undoes entries in reverse on one core, so when an
//! entry is undone the row's only version above its floor is the one its
//! write appended. The undo removes exactly that version. The row then
//! reads as it did before the write, at every system time, with no trace of
//! the rolled-back version in its history.

use nodedb_types::StorageKey;

use crate::data::executor::core_loop::CoreLoop;

use super::UndoError;

/// One row a write names: where it is stored, and its version floor.
pub(in crate::data::executor) struct RowVersionFloor {
    pub key: StorageKey,
    /// The system time of the row's newest stored version before the
    /// write, `None` when the row held no version.
    pub floor: Option<i64>,
}

/// The rows of one collection whose written versions an undo removes.
pub(super) struct WrittenRows<'a> {
    pub entry_index: usize,
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
}

impl CoreLoop {
    /// The version floor of row `key` of `collection`: its newest stored
    /// version's system time, read before a write appends to it.
    pub(in crate::data::executor) fn row_version_floor(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        key: StorageKey,
    ) -> crate::Result<RowVersionFloor> {
        let floor = self
            .sparse
            .versioned_latest_sys_from(database_id, tid, collection, &key)?;
        Ok(RowVersionFloor { key, floor })
    }

    /// Remove the version each rolled-back write appended to a row of
    /// `rows`: the row's newest version, when it sits above the floor. A row
    /// with no version above its floor took no write. A second version above
    /// the floor is a mismatch: the entry did not undo the write that
    /// appended it.
    pub(super) fn remove_written_row_versions(
        &mut self,
        rows: WrittenRows<'_>,
        floors: &[RowVersionFloor],
    ) -> Result<(), UndoError> {
        let WrittenRows {
            entry_index,
            database_id,
            tid,
            collection,
        } = rows;
        let failed = |action: &str, key: &StorageKey, e: crate::Error| {
            UndoError::failed(
                entry_index,
                format!("{action} the written version of {key} in '{collection}'"),
                e,
            )
        };
        let above = |sys: i64, floor: Option<i64>| floor.is_none_or(|f| sys > f);
        let mut written = Vec::with_capacity(floors.len());
        for RowVersionFloor { key, floor } in floors {
            let newest = self
                .sparse
                .versioned_latest_sys_from(database_id, tid, collection, key)
                .map_err(|e| failed("reading", key, e))?;
            if let Some(sys_from) = newest.filter(|sys| above(*sys, *floor)) {
                written.push((key, *floor, sys_from));
            }
        }
        if written.is_empty() {
            return Ok(());
        }
        let txn = self.sparse.begin_write().map_err(|e| {
            UndoError::failed(
                entry_index,
                format!("opening the version removal in '{collection}'"),
                e,
            )
        })?;
        for (key, _, sys_from) in &written {
            self.sparse
                .versioned_remove_in_txn(&txn, database_id, tid, collection, key, *sys_from)
                .map_err(|e| failed("removing", key, e))?;
        }
        txn.commit().map_err(|e| {
            UndoError::failed(
                entry_index,
                format!("committing the version removal in '{collection}'"),
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: e.to_string(),
                },
            )
        })?;
        for (key, floor, _) in written {
            self.doc_cache.invalidate(database_id, tid, collection, key);
            let newest = self
                .sparse
                .versioned_latest_sys_from(database_id, tid, collection, key)
                .map_err(|e| failed("reading", key, e))?;
            if newest.is_some_and(|sys| above(sys, floor)) {
                return Err(UndoError::mismatch(
                    entry_index,
                    format!(
                        "row {key} in '{collection}' holds more than one version above its \
                         undo floor"
                    ),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use nodedb_types::Surrogate;

    const TID: u64 = 1;
    const COLL: &str = "rows";

    fn key(n: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(n))
    }

    fn rows() -> WrittenRows<'static> {
        WrittenRows {
            entry_index: 0,
            database_id: 0,
            tid: TID,
            collection: COLL,
        }
    }

    fn as_of(core: &CoreLoop, k: &StorageKey, sys: i64) -> Option<Vec<u8>> {
        core.sparse
            .versioned_get_as_of(0, TID, COLL, k, Some(sys), None)
            .expect("read as of")
    }

    #[test]
    fn a_rolled_back_overwrite_leaves_no_history() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let k = key(7);
        core.sparse.put(0, TID, COLL, &k, b"old", 100).expect("put");
        let floor = core.row_version_floor(0, TID, COLL, k).expect("floor");
        assert_eq!(floor.floor, Some(100));
        core.sparse.put(0, TID, COLL, &k, b"new", 200).expect("put");

        core.remove_written_row_versions(rows(), &[floor])
            .expect("undo");

        assert_eq!(
            core.sparse.get(0, TID, COLL, &k).unwrap(),
            Some(b"old".to_vec())
        );
        assert_eq!(as_of(&core, &k, 250), Some(b"old".to_vec()));
        assert_eq!(
            core.sparse
                .versioned_latest_sys_from(0, TID, COLL, &k)
                .unwrap(),
            Some(100),
            "no version stamped at or after the rolled-back write"
        );
    }

    #[test]
    fn a_rolled_back_insert_and_delete_remove_their_versions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let (fresh, gone) = (key(1), key(2));
        core.sparse
            .put(0, TID, COLL, &gone, b"kept", 100)
            .expect("put");
        let floors = [
            core.row_version_floor(0, TID, COLL, fresh).expect("floor"),
            core.row_version_floor(0, TID, COLL, gone).expect("floor"),
        ];
        core.sparse
            .put(0, TID, COLL, &fresh, b"x", 300)
            .expect("put");
        core.sparse
            .delete(0, TID, COLL, &gone, 300)
            .expect("delete");

        core.remove_written_row_versions(rows(), &floors)
            .expect("undo");

        assert_eq!(core.sparse.get(0, TID, COLL, &fresh).unwrap(), None);
        assert_eq!(
            core.sparse
                .versioned_latest_sys_from(0, TID, COLL, &fresh)
                .unwrap(),
            None,
            "the inserted row leaves no version"
        );
        assert_eq!(
            core.sparse.get(0, TID, COLL, &gone).unwrap(),
            Some(b"kept".to_vec())
        );
        assert_eq!(as_of(&core, &gone, 400), Some(b"kept".to_vec()));
    }

    #[test]
    fn a_row_the_write_did_not_touch_is_left_alone() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let k = key(3);
        core.sparse.put(0, TID, COLL, &k, b"v", 100).expect("put");
        let floor = core.row_version_floor(0, TID, COLL, k).expect("floor");
        core.remove_written_row_versions(rows(), &[floor])
            .expect("undo");
        assert_eq!(
            core.sparse.get(0, TID, COLL, &k).unwrap(),
            Some(b"v".to_vec())
        );
    }

    #[test]
    fn two_versions_above_the_floor_are_a_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let k = key(4);
        let floor = core.row_version_floor(0, TID, COLL, k).expect("floor");
        core.sparse.put(0, TID, COLL, &k, b"a", 100).expect("put");
        core.sparse.put(0, TID, COLL, &k, b"b", 200).expect("put");
        assert!(core.remove_written_row_versions(rows(), &[floor]).is_err());
    }
}
