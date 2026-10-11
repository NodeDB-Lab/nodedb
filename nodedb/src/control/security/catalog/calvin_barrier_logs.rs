// SPDX-License-Identifier: BUSL-1.1

//! Persistent dependent-read barrier logs backing
//! `_system.calvin_barrier_logs`.
//!
//! The data-group apply loop folds each `CalvinReadResult` and
//! `CalvinReadTimeout` entry into the row of its `(vShard, txn)` before the
//! entry counts as applied. Boot resumes Raft delivery above the durable
//! applied floor, so an entry at or below it is never delivered again. Its
//! row is what a barrier of the txn reads after a restart.
//!
//! Each row holds the encoded barrier log of one txn on one vShard. This
//! module stores the bytes and never decodes them.

use std::collections::BTreeSet;

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::types::{SystemCatalog, catalog_err};

/// Table: `(vshard_id, epoch, position)` -> encoded barrier log.
pub(super) const CALVIN_BARRIER_LOGS: TableDefinition<(u32, u64, u32), &[u8]> =
    TableDefinition::new("_system.calvin_barrier_logs");

/// One stored barrier log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBarrierLog {
    pub vshard_id: u32,
    pub epoch: u64,
    pub position: u32,
    /// The encoded barrier log.
    pub log: Vec<u8>,
}

/// The key range of every row of `vshard_id`.
fn vshard_range(vshard_id: u32) -> std::ops::RangeInclusive<(u32, u64, u32)> {
    (vshard_id, 0, 0)..=(vshard_id, u64::MAX, u32::MAX)
}

/// Append every row of `range` to `out`.
fn collect_rows(
    out: &mut Vec<StoredBarrierLog>,
    range: redb::Range<'_, (u32, u64, u32), &'static [u8]>,
) -> crate::Result<()> {
    for row in range {
        let (key, value) = row.map_err(|e| catalog_err("read calvin_barrier_logs", e))?;
        let (vshard_id, epoch, position) = key.value();
        out.push(StoredBarrierLog {
            vshard_id,
            epoch,
            position,
            log: value.value().to_vec(),
        });
    }
    Ok(())
}

impl SystemCatalog {
    /// Replace the row of `(vshard_id, epoch, position)` with what `fold`
    /// returns for the row it holds, in one transaction. A fold that
    /// returns `None` keeps the row, and nothing commits.
    pub fn fold_calvin_barrier_log(
        &self,
        vshard_id: u32,
        epoch: u64,
        position: u32,
        fold: impl FnOnce(Option<&[u8]>) -> crate::Result<Option<Vec<u8>>>,
    ) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("fold_calvin_barrier_log txn", e))?;
        {
            let mut table = write_txn
                .open_table(CALVIN_BARRIER_LOGS)
                .map_err(|e| catalog_err("open calvin_barrier_logs", e))?;
            let key = (vshard_id, epoch, position);
            let folded = {
                let held = table
                    .get(key)
                    .map_err(|e| catalog_err("get calvin_barrier_logs", e))?;
                fold(held.as_ref().map(|row| row.value()))?
            };
            let Some(folded) = folded else {
                return Ok(());
            };
            table
                .insert(key, folded.as_slice())
                .map_err(|e| catalog_err("insert calvin_barrier_logs", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit calvin_barrier_logs", e))
    }

    /// The row of `(vshard_id, epoch, position)`, when one is stored.
    pub fn load_calvin_barrier_log(
        &self,
        vshard_id: u32,
        epoch: u64,
        position: u32,
    ) -> crate::Result<Option<Vec<u8>>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("load_calvin_barrier_log read txn", e))?;
        let table = read_txn
            .open_table(CALVIN_BARRIER_LOGS)
            .map_err(|e| catalog_err("open calvin_barrier_logs", e))?;
        let row = table
            .get((vshard_id, epoch, position))
            .map_err(|e| catalog_err("get calvin_barrier_logs", e))?;
        Ok(row.map(|row| row.value().to_vec()))
    }

    /// Every stored row of the vShards in `vshards`, or of every vShard
    /// when `vshards` is `None`, in key order.
    pub fn load_calvin_barrier_logs(
        &self,
        vshards: Option<&BTreeSet<u32>>,
    ) -> crate::Result<Vec<StoredBarrierLog>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("load_calvin_barrier_logs read txn", e))?;
        let table = read_txn
            .open_table(CALVIN_BARRIER_LOGS)
            .map_err(|e| catalog_err("open calvin_barrier_logs", e))?;
        let mut out = Vec::new();
        match vshards {
            None => collect_rows(
                &mut out,
                table
                    .iter()
                    .map_err(|e| catalog_err("iter calvin_barrier_logs", e))?,
            )?,
            Some(vshards) => {
                for vshard_id in vshards {
                    collect_rows(
                        &mut out,
                        table
                            .range(vshard_range(*vshard_id))
                            .map_err(|e| catalog_err("range calvin_barrier_logs", e))?,
                    )?;
                }
            }
        }
        Ok(out)
    }

    /// Remove the rows of `keys`, each `(vshard_id, epoch, position)`, in
    /// one transaction. A key with no row changes nothing.
    pub fn remove_calvin_barrier_logs(&self, keys: &[(u32, u64, u32)]) -> crate::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("remove_calvin_barrier_logs txn", e))?;
        {
            let mut table = write_txn
                .open_table(CALVIN_BARRIER_LOGS)
                .map_err(|e| catalog_err("open calvin_barrier_logs", e))?;
            for key in keys {
                table
                    .remove(*key)
                    .map_err(|e| catalog_err("remove calvin_barrier_logs", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit calvin_barrier_logs", e))
    }

    /// Replace every row of the vShards in `vshards` with `rows`, in one
    /// transaction. A data-group snapshot install brings the rows the
    /// builder held for the group's vShards: they replace this node's.
    pub fn replace_calvin_barrier_logs(
        &self,
        vshards: &BTreeSet<u32>,
        rows: &[StoredBarrierLog],
    ) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("replace_calvin_barrier_logs txn", e))?;
        {
            let mut table = write_txn
                .open_table(CALVIN_BARRIER_LOGS)
                .map_err(|e| catalog_err("open calvin_barrier_logs", e))?;
            for vshard_id in vshards {
                table
                    .retain_in(vshard_range(*vshard_id), |_, _| false)
                    .map_err(|e| catalog_err("clear calvin_barrier_logs", e))?;
            }
            for row in rows {
                table
                    .insert((row.vshard_id, row.epoch, row.position), row.log.as_slice())
                    .map_err(|e| catalog_err("insert calvin_barrier_logs", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit calvin_barrier_logs", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(vshard_id: u32, epoch: u64, position: u32, log: &[u8]) -> StoredBarrierLog {
        StoredBarrierLog {
            vshard_id,
            epoch,
            position,
            log: log.to_vec(),
        }
    }

    fn put(catalog: &SystemCatalog, row: &StoredBarrierLog) {
        let bytes = row.log.clone();
        catalog
            .fold_calvin_barrier_log(row.vshard_id, row.epoch, row.position, |_| Ok(Some(bytes)))
            .expect("fold");
    }

    /// A fold sees the row it replaces, and the row reloads.
    #[test]
    fn a_fold_sees_the_held_row() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        put(&catalog, &row(3, 7, 1, b"a"));
        catalog
            .fold_calvin_barrier_log(3, 7, 1, |held| {
                assert_eq!(held, Some(b"a".as_slice()));
                Ok(Some(b"ab".to_vec()))
            })
            .expect("fold");
        assert_eq!(
            catalog.load_calvin_barrier_log(3, 7, 1).expect("load"),
            Some(b"ab".to_vec())
        );
        assert_eq!(
            catalog.load_calvin_barrier_log(3, 7, 2).expect("load"),
            None
        );
        // A fold that keeps the row commits nothing.
        catalog
            .fold_calvin_barrier_log(3, 7, 1, |_| Ok(None))
            .expect("fold");
        catalog
            .fold_calvin_barrier_log(3, 7, 2, |_| Ok(None))
            .expect("fold");
        assert_eq!(
            catalog.load_calvin_barrier_log(3, 7, 1).expect("load"),
            Some(b"ab".to_vec())
        );
        assert_eq!(
            catalog.load_calvin_barrier_log(3, 7, 2).expect("load"),
            None
        );
    }

    /// A load of some vShards returns their rows alone, in key order.
    #[test]
    fn rows_load_per_vshard_in_key_order() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        put(&catalog, &row(4, 2, 0, b"y"));
        put(&catalog, &row(3, 9, 0, b"x"));
        put(&catalog, &row(3, 1, 5, b"w"));
        let all = catalog.load_calvin_barrier_logs(None).expect("load");
        assert_eq!(
            all,
            vec![row(3, 1, 5, b"w"), row(3, 9, 0, b"x"), row(4, 2, 0, b"y")]
        );
        let only = catalog
            .load_calvin_barrier_logs(Some(&BTreeSet::from([4])))
            .expect("load");
        assert_eq!(only, vec![row(4, 2, 0, b"y")]);
    }

    /// A replace drops every row of the named vShards and keeps the rest.
    #[test]
    fn a_replace_swaps_the_named_vshards_rows() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        put(&catalog, &row(3, 1, 0, b"old"));
        put(&catalog, &row(4, 1, 0, b"kept"));
        catalog
            .replace_calvin_barrier_logs(&BTreeSet::from([3]), &[row(3, 2, 0, b"new")])
            .expect("replace");
        let all = catalog.load_calvin_barrier_logs(None).expect("load");
        assert_eq!(all, vec![row(3, 2, 0, b"new"), row(4, 1, 0, b"kept")]);
        catalog
            .remove_calvin_barrier_logs(&[(3, 2, 0), (9, 9, 9)])
            .expect("remove");
        let all = catalog.load_calvin_barrier_logs(None).expect("load");
        assert_eq!(all, vec![row(4, 1, 0, b"kept")]);
    }
}
