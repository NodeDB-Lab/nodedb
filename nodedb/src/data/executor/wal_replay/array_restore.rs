// SPDX-License-Identifier: BUSL-1.1

//! Replay arm for restored array cells (`crate::wal::ArrayRestoreRedo`).
//!
//! A RESTORE re-issues every backed-up version of an array cell at its own
//! system time. The resolve adds the versions that set a coordinate's
//! backed-up state again above the destination's newer ones, each at a stamp
//! the resolve names. Every stamp comes from the record, never from the
//! apply, so every replica and every replay installs the same versions. The
//! highest stamp folds into the shared clock, so a later write stamps above
//! every restored version.
//!
//! The arm honours the array manifest's replay stamp as every array record
//! does: a record a flushed segment already holds replays nothing.

use nodedb_wal::WalRecord;

use super::array::ArrayOpen;
use crate::data::executor::core_loop::CoreLoop;
use crate::wal::ArrayRestoreRedo;

/// What replaying one restored-array record did.
pub(super) enum RestoreReplay {
    /// The cells installed. `in_flight` when the array's replay stamp names
    /// an LSN above the record.
    Applied { cells: usize, in_flight: bool },
    /// The record installed nothing.
    Skipped,
    /// A validate pass claimed the record: nothing is written.
    Claimed,
}

impl CoreLoop {
    /// Install the restored cells `redo` of `record`.
    pub(super) fn replay_array_restore(
        &mut self,
        record: &WalRecord,
        redo: ArrayRestoreRedo,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> RestoreReplay {
        let record_lsn = record.header.lsn;
        let ArrayRestoreRedo {
            array_id,
            vshard_id: placement,
            puts,
            deletes,
        } = redo;
        if tombstones.is_key_tombstoned(
            nodedb_types::CollectionKey::from_bare(array_id.database_id, &array_id.name),
            record.header.tenant_id,
            record_lsn,
        ) {
            return RestoreReplay::Skipped;
        }
        match self.ensure_array_open_for_replay(&array_id) {
            Ok(ArrayOpen::Ready) => {}
            Ok(ArrayOpen::NoCatalogEntry) => {
                self.replay_record_rejected(
                    "array",
                    record_lsn,
                    None,
                    &format!(
                        "array '{}' has no catalog entry; its restored cells are skipped",
                        array_id.name
                    ),
                );
                return RestoreReplay::Skipped;
            }
            Err(e) => {
                self.replay_record_unapplied(
                    "array",
                    "open",
                    record_lsn,
                    &format!("array '{}' could not be opened: {e}", array_id.name),
                );
                return RestoreReplay::Skipped;
            }
        }
        if self.array_replay_skips(&array_id, record_lsn) {
            return RestoreReplay::Skipped;
        }
        if self.claim_for_validation() {
            return RestoreReplay::Claimed;
        }
        // Every version keeps the stamp and the placement the record carries.
        self.fold_restored_array_stamps(&puts, &deletes);
        if self.recording_redo_undo()
            && !self.record_array_tiles_undo(
                &array_id,
                puts.iter()
                    .map(|c| (c.coord.as_slice(), c.system_from_ms))
                    .chain(
                        deletes
                            .iter()
                            .map(|c| (c.coord.as_slice(), c.system_from_ms)),
                    ),
                None,
            )
        {
            return RestoreReplay::Skipped;
        }
        let cells = puts.len() + deletes.len();
        let in_flight = self.array_stamp_passes(&array_id, record_lsn);
        if let Err(e) = self
            .array_engine
            .put_cells(&array_id, placement, puts, record_lsn)
        {
            self.replay_record_unapplied(
                "array",
                "restore_put_cells",
                record_lsn,
                &format!("restored cells could not be re-applied: {e}"),
            );
            return RestoreReplay::Skipped;
        }
        if let Err(e) = self
            .array_engine
            .delete_cells(&array_id, placement, deletes, record_lsn)
        {
            self.replay_record_unapplied(
                "array",
                "restore_delete_cells",
                record_lsn,
                &format!("restored tombstones could not be re-applied: {e}"),
            );
            return RestoreReplay::Skipped;
        }
        self.note_redo_array_written(&array_id);
        RestoreReplay::Applied { cells, in_flight }
    }
}
