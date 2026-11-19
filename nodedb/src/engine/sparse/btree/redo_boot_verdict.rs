// SPDX-License-Identifier: BUSL-1.1

//! The verdicts restart replay reached on committed records with no
//! outcome.
//!
//! Replay judges such a record at its place in the stream, and stores the
//! verdict durably before it applies anything after the record. Boot then
//! journals the verdict in the WAL: a cancel for a refusal, a closed group
//! for a pass. A crash before those records are durable leaves the record
//! with no outcome in the WAL, and the stored verdict decides it on the next
//! boot without a second validate: the stores then hold writes of records
//! after it. Boot drops a stored verdict once the WAL settles its record.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::engine::SparseEngine;
use super::tables::redb_err;

/// Key: the record LSN. Value: the encoded verdict.
pub(super) const REDO_BOOT_VERDICTS: TableDefinition<u64, &[u8]> =
    TableDefinition::new("redo_boot_verdicts");

impl SparseEngine {
    /// Store each `(lsn, encoded verdict)` of `verdicts` in one durable
    /// commit.
    pub fn put_boot_verdicts(&self, verdicts: &[(u64, Vec<u8>)]) -> crate::Result<()> {
        let txn = self.begin_durable_write()?;
        {
            let mut table = txn
                .open_table(REDO_BOOT_VERDICTS)
                .map_err(|e| redb_err("open redo boot verdicts", e))?;
            for (lsn, verdict) in verdicts {
                table
                    .insert(*lsn, verdict.as_slice())
                    .map_err(|e| redb_err("store redo boot verdict", e))?;
            }
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("commit {} redo boot verdicts: {e}", verdicts.len()),
        })
    }

    /// Every stored verdict, as `(lsn, encoded verdict)` in LSN order.
    pub fn load_boot_verdicts(&self) -> crate::Result<Vec<(u64, Vec<u8>)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("read redo boot verdicts", e))?;
        let table = txn
            .open_table(REDO_BOOT_VERDICTS)
            .map_err(|e| redb_err("open redo boot verdicts", e))?;
        let mut verdicts = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| redb_err("scan redo boot verdicts", e))?
        {
            let (lsn, verdict) = entry.map_err(|e| redb_err("read redo boot verdict", e))?;
            verdicts.push((lsn.value(), verdict.value().to_vec()));
        }
        Ok(verdicts)
    }

    /// Remove the stored verdict on each record in `lsns` in one durable
    /// commit.
    pub fn remove_boot_verdicts(&self, lsns: &[u64]) -> crate::Result<()> {
        if lsns.is_empty() {
            return Ok(());
        }
        let txn = self.begin_durable_write()?;
        {
            let mut table = txn
                .open_table(REDO_BOOT_VERDICTS)
                .map_err(|e| redb_err("open redo boot verdicts", e))?;
            for lsn in lsns {
                table
                    .remove(*lsn)
                    .map_err(|e| redb_err("drop redo boot verdict", e))?;
            }
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("commit the drop of {} redo boot verdicts: {e}", lsns.len()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdicts_store_load_and_drop() {
        let dir = tempfile::tempdir().unwrap();
        let sparse = SparseEngine::open(&dir.path().join("sparse.redb")).unwrap();
        assert!(sparse.load_boot_verdicts().unwrap().is_empty());
        sparse
            .put_boot_verdicts(&[(9, b"at9".to_vec()), (7, b"at7".to_vec())])
            .unwrap();
        assert_eq!(
            sparse.load_boot_verdicts().unwrap(),
            vec![(7, b"at7".to_vec()), (9, b"at9".to_vec())]
        );
        sparse.remove_boot_verdicts(&[7]).unwrap();
        assert_eq!(
            sparse.load_boot_verdicts().unwrap(),
            vec![(9, b"at9".to_vec())]
        );
    }
}
