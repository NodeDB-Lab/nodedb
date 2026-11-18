// SPDX-License-Identifier: BUSL-1.1

//! Stored write sets of journalled writes, keyed by their group origin.
//!
//! A write whose Control Plane journals its write set after apply keeps its
//! effects non-durable until the core stores the write set here, in the
//! transaction that persists them (see [`crate::engine::durability_gate`]).
//! A crash after that commit leaves the effects and the write set together,
//! and boot journals the write set as the group's parts. A crash before it
//! leaves neither. Once the parts are durable in the WAL, the stored write
//! set is dropped.
//!
//! The value is the encoded [`crate::wal::WriteSetCapture`].

use redb::{ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};

use super::engine::SparseEngine;
use super::tables::redb_err;
use crate::engine::durability_gate::DurableWrite;

/// Key: the group origin LSN. Value: the encoded write set.
pub(super) const WRITE_SET_CAPTURES: TableDefinition<u64, &[u8]> =
    TableDefinition::new("write_set_captures");

impl SparseEngine {
    /// Begin a write transaction that persists its commit and every deferred
    /// commit before it.
    pub fn begin_durable_write(&self) -> crate::Result<DurableWrite<'_>> {
        self.db
            .begin_durable_write()
            .map_err(|e| redb_err("begin durable write txn", e))
    }

    /// Store the write set of the group at `origin` within `txn`.
    pub fn put_write_set_capture_in_txn(
        &self,
        txn: &WriteTransaction,
        origin: u64,
        capture: &[u8],
    ) -> crate::Result<()> {
        txn.open_table(WRITE_SET_CAPTURES)
            .map_err(|e| redb_err("open write set captures", e))?
            .insert(origin, capture)
            .map_err(|e| redb_err("store write set capture", e))?;
        Ok(())
    }

    /// Drop the stored write sets of `origins` within `txn`.
    pub fn remove_write_set_captures_in_txn(
        &self,
        txn: &WriteTransaction,
        origins: &[u64],
    ) -> crate::Result<()> {
        let mut table = txn
            .open_table(WRITE_SET_CAPTURES)
            .map_err(|e| redb_err("open write set captures", e))?;
        for origin in origins {
            table
                .remove(*origin)
                .map_err(|e| redb_err("drop write set capture", e))?;
        }
        Ok(())
    }

    /// Every stored write set, by origin.
    pub fn load_write_set_captures(&self) -> crate::Result<Vec<(u64, Vec<u8>)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("read write set captures", e))?;
        let table = txn
            .open_table(WRITE_SET_CAPTURES)
            .map_err(|e| redb_err("open write set captures", e))?;
        let mut captures = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| redb_err("scan write set captures", e))?
        {
            let (origin, value) = entry.map_err(|e| redb_err("read write set capture", e))?;
            captures.push((origin.value(), value.value().to_vec()));
        }
        Ok(captures)
    }

    /// Drop every stored write set, durably.
    pub fn clear_write_set_captures(&self) -> crate::Result<()> {
        let txn = self
            .db
            .begin_durable_write()
            .map_err(|e| redb_err("begin write set capture clear", e))?;
        {
            let mut table = txn
                .open_table(WRITE_SET_CAPTURES)
                .map_err(|e| redb_err("open write set captures", e))?;
            let origins: Vec<u64> = table
                .iter()
                .map_err(|e| redb_err("scan write set captures", e))?
                .map(|entry| entry.map(|(origin, _)| origin.value()))
                .collect::<Result<_, _>>()
                .map_err(|e| redb_err("read write set capture", e))?;
            for origin in origins {
                table
                    .remove(origin)
                    .map_err(|e| redb_err("drop write set capture", e))?;
            }
        }
        txn.commit()
            .map_err(|e| redb_err("commit write set capture clear", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captures_store_load_and_clear() {
        let dir = tempfile::tempdir().unwrap();
        let sparse = SparseEngine::open(&dir.path().join("s.redb")).unwrap();
        let txn = sparse.begin_write().unwrap();
        sparse
            .put_write_set_capture_in_txn(&txn, 7, b"seven")
            .unwrap();
        sparse
            .put_write_set_capture_in_txn(&txn, 9, b"nine")
            .unwrap();
        txn.commit().unwrap();
        assert_eq!(
            sparse.load_write_set_captures().unwrap(),
            [(7, b"seven".to_vec()), (9, b"nine".to_vec())]
        );

        let txn = sparse.begin_write().unwrap();
        sparse.remove_write_set_captures_in_txn(&txn, &[7]).unwrap();
        txn.commit().unwrap();
        assert_eq!(sparse.load_write_set_captures().unwrap().len(), 1);

        sparse.clear_write_set_captures().unwrap();
        assert!(sparse.load_write_set_captures().unwrap().is_empty());
    }
}
