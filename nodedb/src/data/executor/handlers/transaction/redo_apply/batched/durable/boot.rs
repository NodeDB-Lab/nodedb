// SPDX-License-Identifier: BUSL-1.1

//! Boot's settle of the batched installs a crash left part way.
//!
//! A cursor in the sparse store names a record whose install made effects
//! durable and stored no write set capture (see `cursor`). Boot reads the
//! cursors before replay. Replay then completes each record forward from
//! op 0 at its own LSN:
//!
//! - The durable stores re-apply each op at its exact key: a document
//!   version at its stamp, an edge at its stamped key. Ops the crash kept
//!   rewrite what they wrote.
//! - The memory engines apply it from op 0, past the ops a flush between
//!   its batches made durable: a timeseries partition or an array segment
//!   names those ops of the record as a partial record, and replay skips
//!   exactly them. No checkpoint runs between its batches.
//! - The document arm folds only the ops at or above the fold ledger, and
//!   the record's write set merges the stored fold target rows. That write
//!   set's commit keeps the cursor: a crash before the cursors drop leaves
//!   the write set beside the cursor, and the next boot replays the record
//!   plainly with its labels versioned again.
//! - The graph label arm versions every op of the record again: a batch's
//!   versions commit with the record's LSN mark, so the mark alone would
//!   skip the batches the crash cut.
//!
//! A cursor marker whose chunks are gone refuses replay
//! (`RedoInstallError::ChunksMissing`). Once replay ends, a cursor whose
//! marker the WAL does not hold is `RedoInstallError::CursorWithoutRecord`,
//! and every other cursor drops in one durable commit.

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::wal::redo::install_cursor::{InstallCursor, InstallCursors, RedoInstallError};
use crate::wal::{RedoRecord, RedoStreamRef};

impl CoreLoop {
    /// Read the batched install cursors of this core's store, before
    /// replay. A store that cannot be read fail-stops the core: replay
    /// without them versions a record's node labels in part.
    pub(crate) fn load_install_cursors(&mut self) -> crate::Result<()> {
        let loaded = self.sparse.load_install_cursors().and_then(|stored| {
            stored
                .into_iter()
                .map(|(lsn, bytes)| InstallCursor::from_bytes(lsn, &bytes))
                .collect::<crate::Result<Vec<_>>>()
        });
        match loaded {
            Ok(cursors) => {
                self.redo_apply.install_cursors = InstallCursors::new(cursors);
                Ok(())
            }
            Err(error) => {
                self.fail_stop_core(
                    FailStopCause::StoreUnreadable,
                    &format!("the batched install cursors cannot be read: {error}"),
                );
                Err(error)
            }
        }
    }

    /// The cursors boot read, until boot drops them.
    pub(crate) fn boot_install_cursors(&self) -> &InstallCursors {
        &self.redo_apply.install_cursors
    }

    /// Settle the cursors once replay of `records` completed their records:
    /// refuse one whose marker `records` lacks, then drop every cursor in
    /// one durable commit.
    pub(crate) fn finish_replayed_installs(&mut self, records: &[WalRecord]) -> crate::Result<()> {
        if self.redo_apply.install_cursors.is_empty() {
            return Ok(());
        }
        let orphaned = first_orphaned(records, &self.redo_apply.install_cursors);
        let orphaned = match orphaned {
            Ok(orphaned) => orphaned,
            Err(error) => {
                self.fail_stop_core(
                    FailStopCause::ReplayRecordUnapplied,
                    &format!("a batched install cursor's record does not decode: {error}"),
                );
                return Err(error);
            }
        };
        if let Some(lsn) = orphaned {
            let error = RedoInstallError::CursorWithoutRecord { lsn };
            self.fail_stop_core(FailStopCause::InstallCursorOrphaned, &error.to_string());
            return Err(error.into());
        }
        let lsns = self.redo_apply.install_cursors.lsns();
        if let Err(error) = self.drop_install_cursors(&lsns) {
            self.fail_stop_core(
                FailStopCause::InstallProgressUnpersisted,
                &format!(
                    "the {} batched install cursors replay completed could not be dropped: \
                     {error}",
                    lsns.len()
                ),
            );
            return Err(error);
        }
        self.redo_apply.install_cursors = InstallCursors::default();
        Ok(())
    }

    /// Persist what replay wrote to the edge store, then remove the
    /// install rows of `lsns` in one durable sparse commit. The edges go
    /// first: once a cursor is gone, the next boot no longer versions its
    /// record's node labels again.
    fn drop_install_cursors(&mut self, lsns: &[u64]) -> crate::Result<()> {
        self.edge_store.persist()?;
        let txn = self.sparse.begin_durable_write()?;
        self.sparse.remove_redo_install_in_txn(&txn, lsns)?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!(
                "commit the drop of {} batched install cursors: {e}",
                lsns.len()
            ),
        })
    }
}

/// The LSN of the first cursor among `cursors` whose marker `records`
/// does not hold.
fn first_orphaned(records: &[WalRecord], cursors: &InstallCursors) -> crate::Result<Option<u64>> {
    for cursor in cursors.iter() {
        if !holds_marker(records, cursor)? {
            return Ok(Some(cursor.lsn));
        }
    }
    Ok(None)
}

/// Whether `records` holds the stream marker `cursor` names: a
/// `TransactionRedo` record at its LSN whose reference names its stream.
/// Replay decoded the same records, so a decode error is no verdict here:
/// it refuses the boot.
fn holds_marker(records: &[WalRecord], cursor: &InstallCursor) -> crate::Result<bool> {
    for record in records {
        if record.header.lsn != cursor.lsn
            || RecordType::from_raw(record.logical_record_type())
                != Some(RecordType::TransactionRedo)
        {
            continue;
        }
        let redo = RedoRecord::from_bytes(&record.payload)?;
        if RedoStreamRef::of_marker(&redo)?
            .is_some_and(|reference| reference.stream == cursor.stream)
        {
            return Ok(true);
        }
    }
    Ok(false)
}
