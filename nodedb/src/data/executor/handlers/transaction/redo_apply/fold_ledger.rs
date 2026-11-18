// SPDX-License-Identifier: BUSL-1.1

//! The fold ledger a batched install writes in each source row's
//! transaction.
//!
//! A batched install commits a batch's rows before the record's write set
//! is stored. Each source row's transaction then also records how many of
//! the record's ops folded, and every fold target row the row's fold wrote.
//! A crash leaves the row, its fold and its ledger entry together, or none
//! of them. Restart replay folds only the ops at or above the ledger. The
//! record's write set holds the stored target row writes in write order,
//! then the writes replay folds: the entry sequence of a whole install.

use std::sync::Arc;

use nodedb_physical::physical_plan::StorageMode;
use redb::WriteTransaction;

use crate::bridge::envelope::WriteSetEntry;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::data::executor::enforcement::write_hook::target_write_set;
use crate::wal::CapturedEntry;

use super::document::CommittedDocWrite;

/// Where a batched install's batch sits in its record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) struct FoldLedger {
    /// The record's LSN.
    pub lsn: u64,
    /// The record op index of the batch's first op. A batch record's op `i`
    /// is the record's op `op_base + i`.
    pub op_base: u64,
}

/// One stored fold target row: the entry and the schema table it indexes.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
struct StoredTargetRow {
    entry: CapturedEntry,
    schemas: Vec<StorageMode>,
}

/// The stored form of one fold target row.
pub(super) fn encode_target_row(entry: &WriteSetEntry) -> crate::Result<Vec<u8>> {
    let mut schemas = Vec::new();
    let entry = CapturedEntry::from_entry(entry, &mut schemas)?;
    zerompk::to_msgpack_vec(&StoredTargetRow { entry, schemas }).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("redo install target encode: {e}"),
        }
    })
}

fn decode_target_row(lsn: u64, bytes: &[u8]) -> crate::Result<WriteSetEntry> {
    let row: StoredTargetRow = zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!("redo install target of the record at lsn {lsn} does not decode: {e}"),
    })?;
    let schemas: Vec<Arc<StorageMode>> = row.schemas.into_iter().map(Arc::new).collect();
    row.entry.into_entry(&schemas)
}

impl CoreLoop {
    /// Write the fold ledger entry of `row` and each target row in `targets`
    /// within `txn`, the transaction that commits the row. Writes nothing
    /// unless the open scope carries a fold ledger.
    pub(super) fn write_fold_ledger_in_txn(
        &self,
        txn: &WriteTransaction,
        row: &CommittedDocWrite<'_>,
        targets: &[TargetWrite],
    ) -> crate::Result<()> {
        let Some(ledger) = self
            .redo_apply
            .scope
            .as_ref()
            .and_then(|scope| scope.ledger)
        else {
            return Ok(());
        };
        if row.record_lsn != ledger.lsn {
            return Err(crate::Error::Internal {
                detail: format!(
                    "a document write of the record at lsn {} ran under the fold ledger of the \
                     record at lsn {}",
                    row.record_lsn, ledger.lsn
                ),
            });
        }
        let op = row.op.ok_or(crate::Error::Internal {
            detail: format!(
                "a document write of the record at lsn {} under a fold ledger carries no op \
                 index",
                ledger.lsn
            ),
        })?;
        let counted = ledger
            .op_base
            .checked_add(op)
            .and_then(|record_op| record_op.checked_add(1).map(|folded| (record_op, folded)));
        let Some((record_op, folded)) = counted else {
            return Err(crate::Error::Internal {
                detail: format!(
                    "the fold ledger of the record at lsn {} overflows at batch op {op}",
                    ledger.lsn
                ),
            });
        };
        self.sparse
            .put_fold_ledger_in_txn(txn, ledger.lsn, folded)?;
        for (index, entry) in target_write_set(targets).iter().enumerate() {
            let index = u32::try_from(index).map_err(|_| crate::Error::Internal {
                detail: format!(
                    "op {record_op} of the record at lsn {} writes more fold target rows than \
                     a u32 counts",
                    ledger.lsn
                ),
            })?;
            let bytes = encode_target_row(entry)?;
            self.sparse
                .put_install_target_in_txn(txn, ledger.lsn, record_op, index, &bytes)?;
        }
        Ok(())
    }

    /// The fold target row writes a batched install of the record at `lsn`
    /// stored, in write order.
    pub(super) fn stored_install_targets(&self, lsn: u64) -> crate::Result<Vec<WriteSetEntry>> {
        let mut rows = Vec::new();
        self.sparse.scan_install_targets(lsn, |bytes| {
            rows.push(decode_target_row(lsn, bytes)?);
            Ok(())
        })?;
        Ok(rows)
    }
}
