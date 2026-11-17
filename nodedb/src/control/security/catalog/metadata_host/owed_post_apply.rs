// SPDX-License-Identifier: BUSL-1.1

//! The catalog entry whose post-apply this node owes, backing
//! `_system.metadata_owed_post_apply`.
//!
//! The metadata applier writes the row before an entry's catalog write and
//! removes it once every post-apply effect finished. A re-delivery of the
//! entry, including the full-log replay after a restart, finds the row and
//! runs the post-apply again. The row is local to this node: no snapshot
//! image carries it.
//!
//! Table: `raft_index` -> entry payload. It holds at most one row, because
//! the applier stops at the entry that owes its post-apply.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::super::types::{SystemCatalog, catalog_err};

pub(in crate::control::security::catalog) const METADATA_OWED_POST_APPLY: TableDefinition<
    u64,
    &[u8],
> = TableDefinition::new("_system.metadata_owed_post_apply");

impl SystemCatalog {
    /// Record that the entry `payload` at `raft_index` owes its post-apply.
    /// The row replaces any other row in the same transaction.
    pub fn put_owed_post_apply(&self, raft_index: u64, payload: &[u8]) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("metadata_owed_post_apply write txn", e))?;
        {
            let mut table = txn
                .open_table(METADATA_OWED_POST_APPLY)
                .map_err(|e| catalog_err("open metadata_owed_post_apply", e))?;
            let mut held = Vec::new();
            for row in table
                .iter()
                .map_err(|e| catalog_err("iterate metadata_owed_post_apply", e))?
            {
                let (index, _) =
                    row.map_err(|e| catalog_err("read metadata_owed_post_apply", e))?;
                held.push(index.value());
            }
            for index in held {
                table
                    .remove(index)
                    .map_err(|e| catalog_err("remove metadata_owed_post_apply", e))?;
            }
            table
                .insert(raft_index, payload)
                .map_err(|e| catalog_err("insert metadata_owed_post_apply", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit metadata_owed_post_apply", e))
    }

    /// The `(raft_index, payload)` of the entry that owes its post-apply, if
    /// any. More than one row is a corrupt table.
    pub fn load_owed_post_apply(&self) -> crate::Result<Option<(u64, Vec<u8>)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("metadata_owed_post_apply read txn", e))?;
        let table = txn
            .open_table(METADATA_OWED_POST_APPLY)
            .map_err(|e| catalog_err("open metadata_owed_post_apply", e))?;
        let mut owed = None;
        for row in table
            .iter()
            .map_err(|e| catalog_err("iterate metadata_owed_post_apply", e))?
        {
            let (index, payload) =
                row.map_err(|e| catalog_err("read metadata_owed_post_apply", e))?;
            if let Some((held, _)) = &owed {
                return Err(catalog_err(
                    "decode metadata_owed_post_apply",
                    format!(
                        "two entries owe their post-apply: raft indexes {held} and {}",
                        index.value()
                    ),
                ));
            }
            owed = Some((index.value(), payload.value().to_vec()));
        }
        Ok(owed)
    }

    /// Remove the row of the entry `payload` at `raft_index`. A row of
    /// another entry stays. Removing an absent row succeeds.
    pub fn remove_owed_post_apply(&self, raft_index: u64, payload: &[u8]) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("metadata_owed_post_apply write txn", e))?;
        {
            let mut table = txn
                .open_table(METADATA_OWED_POST_APPLY)
                .map_err(|e| catalog_err("open metadata_owed_post_apply", e))?;
            let matches = table
                .get(raft_index)
                .map_err(|e| catalog_err("get metadata_owed_post_apply", e))?
                .is_some_and(|held| held.value() == payload);
            if matches {
                table
                    .remove(raft_index)
                    .map_err(|e| catalog_err("remove metadata_owed_post_apply", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("commit metadata_owed_post_apply", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owed_row_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        {
            let catalog = SystemCatalog::open(&path).unwrap();
            assert_eq!(catalog.load_owed_post_apply().unwrap(), None);
            catalog.put_owed_post_apply(7, b"first").unwrap();
            catalog.put_owed_post_apply(9, b"second").unwrap();
        }
        let catalog = SystemCatalog::open(&path).unwrap();
        assert_eq!(
            catalog.load_owed_post_apply().unwrap(),
            Some((9, b"second".to_vec()))
        );
    }

    #[test]
    fn remove_keeps_a_row_of_another_entry() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        catalog.put_owed_post_apply(9, b"second").unwrap();
        catalog.remove_owed_post_apply(9, b"other").unwrap();
        catalog.remove_owed_post_apply(8, b"second").unwrap();
        assert_eq!(
            catalog.load_owed_post_apply().unwrap(),
            Some((9, b"second".to_vec()))
        );
        catalog.remove_owed_post_apply(9, b"second").unwrap();
        assert_eq!(catalog.load_owed_post_apply().unwrap(), None);
        catalog.remove_owed_post_apply(9, b"second").unwrap();
    }
}
