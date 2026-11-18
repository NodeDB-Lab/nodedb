// SPDX-License-Identifier: BUSL-1.1

//! Durable progress of a committed redo install that commits batch by batch.
//!
//! A batched install commits each batch's rows before the record's write set
//! is stored. Three tables keep what a crash then needs:
//!
//! - The fold ledger names, per record LSN, how many of the record's ops
//!   folded their materialized-sum deltas durably. Restart replay folds only
//!   the ops at or above it.
//! - The install targets hold each fold target row those ops wrote, keyed by
//!   record LSN, op index and the write's index within the op. A row written
//!   twice keeps both writes. The record's write set holds them in key
//!   order, then the targets replay folds.
//! - The install cursors hold the position of an install in progress.
//!
//! A row and its ledger entry and target rows commit in one transaction. The
//! record's write set and the removal of its ledger and target rows commit
//! in one transaction too. A live install's write set commit removes its
//! cursor as well. Boot replay keeps a record's cursor until every arm
//! replayed the record, then drops it. Boot never clears these tables
//! wholesale.

use redb::{ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};

use super::engine::SparseEngine;
use super::tables::redb_err;

/// Key: the record LSN. Value: the count of its ops folded, exclusive.
pub(super) const REDO_FOLD_LEDGER: TableDefinition<u64, u64> =
    TableDefinition::new("redo_fold_ledger");

/// Key: `(record LSN, op index, write index within the op)`. Value: the
/// encoded target row write.
pub(super) const REDO_INSTALL_TARGETS: TableDefinition<(u64, u64, u32), &[u8]> =
    TableDefinition::new("redo_install_targets");

/// Key: the record LSN. Value: the encoded install cursor.
pub(super) const REDO_INSTALL_CURSORS: TableDefinition<u64, &[u8]> =
    TableDefinition::new("redo_install_cursors");

impl SparseEngine {
    /// Record within `txn` that the first `folded_ops` ops of the record at
    /// `lsn` folded.
    pub fn put_fold_ledger_in_txn(
        &self,
        txn: &WriteTransaction,
        lsn: u64,
        folded_ops: u64,
    ) -> crate::Result<()> {
        txn.open_table(REDO_FOLD_LEDGER)
            .map_err(|e| redb_err("open redo fold ledger", e))?
            .insert(lsn, folded_ops)
            .map_err(|e| redb_err("store redo fold ledger", e))?;
        Ok(())
    }

    /// Store within `txn` the `index`th fold target row write of op `op` of
    /// the record at `lsn`.
    pub fn put_install_target_in_txn(
        &self,
        txn: &WriteTransaction,
        lsn: u64,
        op: u64,
        index: u32,
        target: &[u8],
    ) -> crate::Result<()> {
        txn.open_table(REDO_INSTALL_TARGETS)
            .map_err(|e| redb_err("open redo install targets", e))?
            .insert((lsn, op, index), target)
            .map_err(|e| redb_err("store redo install target", e))?;
        Ok(())
    }

    /// Store within `txn` the encoded install cursor of the record at `lsn`.
    pub fn put_install_cursor_in_txn(
        &self,
        txn: &WriteTransaction,
        lsn: u64,
        cursor: &[u8],
    ) -> crate::Result<()> {
        txn.open_table(REDO_INSTALL_CURSORS)
            .map_err(|e| redb_err("open redo install cursors", e))?
            .insert(lsn, cursor)
            .map_err(|e| redb_err("store redo install cursor", e))?;
        Ok(())
    }

    /// Every stored install cursor, as `(lsn, encoded cursor)` in LSN order.
    pub fn load_install_cursors(&self) -> crate::Result<Vec<(u64, Vec<u8>)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("read redo install cursors", e))?;
        let table = txn
            .open_table(REDO_INSTALL_CURSORS)
            .map_err(|e| redb_err("open redo install cursors", e))?;
        let mut cursors = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| redb_err("scan redo install cursors", e))?
        {
            let (lsn, cursor) = entry.map_err(|e| redb_err("read redo install cursor", e))?;
            cursors.push((lsn.value(), cursor.value().to_vec()));
        }
        Ok(cursors)
    }

    /// Every fold ledger entry, as `(lsn, folded_ops)` in LSN order.
    pub fn load_fold_ledgers(&self) -> crate::Result<Vec<(u64, u64)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("read redo fold ledger", e))?;
        let table = txn
            .open_table(REDO_FOLD_LEDGER)
            .map_err(|e| redb_err("open redo fold ledger", e))?;
        let mut ledgers = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| redb_err("scan redo fold ledger", e))?
        {
            let (lsn, folded) = entry.map_err(|e| redb_err("read redo fold ledger", e))?;
            ledgers.push((lsn.value(), folded.value()));
        }
        Ok(ledgers)
    }

    /// Hand `visit` every stored fold target row write of the record at
    /// `lsn`, in write order.
    pub fn scan_install_targets(
        &self,
        lsn: u64,
        mut visit: impl FnMut(&[u8]) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("read redo install targets", e))?;
        let table = txn
            .open_table(REDO_INSTALL_TARGETS)
            .map_err(|e| redb_err("open redo install targets", e))?;
        for entry in table
            .range((lsn, 0u64, 0u32)..)
            .map_err(|e| redb_err("scan redo install targets", e))?
        {
            let (key, value) = entry.map_err(|e| redb_err("read redo install target", e))?;
            if key.value().0 != lsn {
                break;
            }
            visit(value.value())?;
        }
        Ok(())
    }

    /// Remove within `txn` the fold ledger entry, the fold target rows and
    /// the install cursor of each record in `lsns`.
    pub fn remove_redo_install_in_txn(
        &self,
        txn: &WriteTransaction,
        lsns: &[u64],
    ) -> crate::Result<()> {
        if lsns.is_empty() {
            return Ok(());
        }
        self.remove_fold_rows_in_txn(txn, lsns)?;
        let mut cursors = txn
            .open_table(REDO_INSTALL_CURSORS)
            .map_err(|e| redb_err("open redo install cursors", e))?;
        for lsn in lsns {
            cursors
                .remove(*lsn)
                .map_err(|e| redb_err("drop redo install cursor", e))?;
        }
        Ok(())
    }

    /// Set within `txn` the fold ledger of the record at `lsn` back to
    /// `folded_ops`, and remove its fold target rows of every op at or above
    /// it. A ledger of 0 removes the entry.
    pub fn rewind_fold_ledger_in_txn(
        &self,
        txn: &WriteTransaction,
        lsn: u64,
        folded_ops: u64,
    ) -> crate::Result<()> {
        let mut ledger = txn
            .open_table(REDO_FOLD_LEDGER)
            .map_err(|e| redb_err("open redo fold ledger", e))?;
        if folded_ops == 0 {
            ledger
                .remove(lsn)
                .map_err(|e| redb_err("drop redo fold ledger", e))?;
        } else {
            ledger
                .insert(lsn, folded_ops)
                .map_err(|e| redb_err("rewind redo fold ledger", e))?;
        }
        let mut targets = txn
            .open_table(REDO_INSTALL_TARGETS)
            .map_err(|e| redb_err("open redo install targets", e))?;
        remove_targets_from(&mut targets, lsn, folded_ops)
    }

    /// Remove within `txn` the fold ledger entry and the fold target rows of
    /// each record in `lsns`. Their install cursors stay.
    pub fn remove_fold_rows_in_txn(
        &self,
        txn: &WriteTransaction,
        lsns: &[u64],
    ) -> crate::Result<()> {
        if lsns.is_empty() {
            return Ok(());
        }
        let mut ledger = txn
            .open_table(REDO_FOLD_LEDGER)
            .map_err(|e| redb_err("open redo fold ledger", e))?;
        let mut targets = txn
            .open_table(REDO_INSTALL_TARGETS)
            .map_err(|e| redb_err("open redo install targets", e))?;
        for lsn in lsns {
            ledger
                .remove(*lsn)
                .map_err(|e| redb_err("drop redo fold ledger", e))?;
            remove_targets_from(&mut targets, *lsn, 0)?;
        }
        Ok(())
    }
}

/// Remove from `targets` the fold target rows of the record at `lsn` whose
/// op is at or above `from_op`.
fn remove_targets_from(
    targets: &mut redb::Table<'_, (u64, u64, u32), &'static [u8]>,
    lsn: u64,
    from_op: u64,
) -> crate::Result<()> {
    let mut keys: Vec<(u64, u32)> = Vec::new();
    for entry in targets
        .range((lsn, from_op, 0u32)..)
        .map_err(|e| redb_err("scan redo install targets", e))?
    {
        let (key, _) = entry.map_err(|e| redb_err("read redo install target", e))?;
        let (at, op, index) = key.value();
        if at != lsn {
            break;
        }
        keys.push((op, index));
    }
    for (op, index) in keys {
        targets
            .remove((lsn, op, index))
            .map_err(|e| redb_err("drop redo install target", e))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn targets_of(sparse: &SparseEngine, lsn: u64) -> Vec<Vec<u8>> {
        let mut rows = Vec::new();
        sparse
            .scan_install_targets(lsn, |bytes| {
                rows.push(bytes.to_vec());
                Ok(())
            })
            .unwrap();
        rows
    }

    #[test]
    fn install_rows_store_scan_and_drop_by_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let sparse = SparseEngine::open(&dir.path().join("s.redb")).unwrap();
        let txn = sparse.begin_write().unwrap();
        sparse.put_fold_ledger_in_txn(&txn, 7, 2).unwrap();
        sparse.put_fold_ledger_in_txn(&txn, 9, 1).unwrap();
        sparse
            .put_install_target_in_txn(&txn, 7, 1, 0, b"op1")
            .unwrap();
        sparse
            .put_install_target_in_txn(&txn, 7, 0, 1, b"op0-second")
            .unwrap();
        sparse
            .put_install_target_in_txn(&txn, 7, 0, 0, b"op0-first")
            .unwrap();
        sparse
            .put_install_target_in_txn(&txn, 9, 0, 0, b"nine")
            .unwrap();
        sparse.put_install_cursor_in_txn(&txn, 7, b"at7").unwrap();
        sparse.put_install_cursor_in_txn(&txn, 9, b"at9").unwrap();
        txn.commit().unwrap();

        assert_eq!(
            sparse.load_install_cursors().unwrap(),
            [(7, b"at7".to_vec()), (9, b"at9".to_vec())]
        );
        assert_eq!(sparse.load_fold_ledgers().unwrap(), [(7, 2), (9, 1)]);
        assert_eq!(
            targets_of(&sparse, 7),
            [
                b"op0-first".to_vec(),
                b"op0-second".to_vec(),
                b"op1".to_vec()
            ],
            "every write stays, in write order"
        );
        assert_eq!(targets_of(&sparse, 9), [b"nine".to_vec()]);
        assert!(targets_of(&sparse, 8).is_empty());

        let txn = sparse.begin_write().unwrap();
        sparse.remove_redo_install_in_txn(&txn, &[7]).unwrap();
        txn.commit().unwrap();
        assert_eq!(sparse.load_fold_ledgers().unwrap(), [(9, 1)]);
        assert_eq!(
            sparse.load_install_cursors().unwrap(),
            [(9, b"at9".to_vec())]
        );
        assert!(targets_of(&sparse, 7).is_empty());
        assert_eq!(targets_of(&sparse, 9), [b"nine".to_vec()]);
    }

    #[test]
    fn a_rewind_keeps_the_ledger_and_targets_below_it() {
        let dir = tempfile::tempdir().unwrap();
        let sparse = SparseEngine::open(&dir.path().join("s.redb")).unwrap();
        let txn = sparse.begin_write().unwrap();
        sparse.put_fold_ledger_in_txn(&txn, 7, 4).unwrap();
        for op in 0..4u64 {
            sparse
                .put_install_target_in_txn(&txn, 7, op, 0, format!("op{op}").as_bytes())
                .unwrap();
        }
        sparse
            .put_install_target_in_txn(&txn, 8, 3, 0, b"eight")
            .unwrap();
        txn.commit().unwrap();

        let txn = sparse.begin_write().unwrap();
        sparse.rewind_fold_ledger_in_txn(&txn, 7, 2).unwrap();
        txn.commit().unwrap();
        assert_eq!(sparse.load_fold_ledgers().unwrap(), [(7, 2)]);
        assert_eq!(targets_of(&sparse, 7), [b"op0".to_vec(), b"op1".to_vec()]);
        assert_eq!(targets_of(&sparse, 8), [b"eight".to_vec()]);

        let txn = sparse.begin_write().unwrap();
        sparse.rewind_fold_ledger_in_txn(&txn, 7, 0).unwrap();
        txn.commit().unwrap();
        assert!(sparse.load_fold_ledgers().unwrap().is_empty());
        assert!(targets_of(&sparse, 7).is_empty());
    }
}
