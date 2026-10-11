// SPDX-License-Identifier: BUSL-1.1

//! The data groups whose replica on this node owes a snapshot install,
//! backing `_system.redo_snapshot_owed`.
//!
//! A node that leaves a data group drops the group's open chunked redo
//! streams. A later remount resumes the group's old log, which can hold a
//! later chunk or final entry of a dropped stream. The row makes the remount
//! catch up by snapshot instead. A snapshot install of the group removes it.
//!
//! Table: `group_id` -> `()`.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::types::{SystemCatalog, catalog_err};

pub(super) const REDO_SNAPSHOT_OWED: TableDefinition<u64, ()> =
    TableDefinition::new("_system.redo_snapshot_owed");

impl SystemCatalog {
    /// Every data group whose replica here owes a snapshot install.
    pub fn load_redo_snapshot_owed(&self) -> crate::Result<Vec<u64>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("redo_snapshot_owed read txn", e))?;
        let table = txn
            .open_table(REDO_SNAPSHOT_OWED)
            .map_err(|e| catalog_err("open redo_snapshot_owed", e))?;
        let mut out = Vec::new();
        for row in table
            .iter()
            .map_err(|e| catalog_err("iterate redo_snapshot_owed", e))?
        {
            let (group_id, _) = row.map_err(|e| catalog_err("read redo_snapshot_owed", e))?;
            out.push(group_id.value());
        }
        Ok(out)
    }

    /// Record in one transaction that the replicas of `groups` here owe a
    /// snapshot install.
    pub fn save_redo_snapshot_owed(&self, groups: &[u64]) -> crate::Result<()> {
        if groups.is_empty() {
            return Ok(());
        }
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("redo_snapshot_owed write txn", e))?;
        {
            let mut table = txn
                .open_table(REDO_SNAPSHOT_OWED)
                .map_err(|e| catalog_err("open redo_snapshot_owed", e))?;
            for &group_id in groups {
                table
                    .insert(group_id, ())
                    .map_err(|e| catalog_err("insert redo_snapshot_owed", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("redo_snapshot_owed commit", e))
    }

    /// Remove the row of `group_id`. Removing an absent row succeeds.
    pub fn remove_redo_snapshot_owed(&self, group_id: u64) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("redo_snapshot_owed write txn", e))?;
        {
            let mut table = txn
                .open_table(REDO_SNAPSHOT_OWED)
                .map_err(|e| catalog_err("open redo_snapshot_owed", e))?;
            table
                .remove(group_id)
                .map_err(|e| catalog_err("remove redo_snapshot_owed", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("redo_snapshot_owed commit", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owed_groups_save_reload_and_remove() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        assert!(catalog.load_redo_snapshot_owed().expect("load").is_empty());

        catalog.save_redo_snapshot_owed(&[4, 9]).expect("save");
        catalog.save_redo_snapshot_owed(&[4]).expect("save again");
        assert_eq!(catalog.load_redo_snapshot_owed().expect("load"), vec![4, 9]);

        catalog.remove_redo_snapshot_owed(4).expect("remove");
        catalog
            .remove_redo_snapshot_owed(5)
            .expect("remove an absent row");
        assert_eq!(catalog.load_redo_snapshot_owed().expect("load"), vec![9]);
    }
}
