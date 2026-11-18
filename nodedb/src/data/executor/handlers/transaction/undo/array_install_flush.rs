// SPDX-License-Identifier: BUSL-1.1

//! Undo of a batched install's array flush between two batches.
//!
//! The flush wrote one segment holding the install's cells below the
//! batch's end, and published a manifest naming it with the part of the
//! record it holds. A rollback publishes the manifest without the segment
//! and with the stamps it had before the flush, then removes the segment
//! file and fsyncs the array's directory (`withdraw_flushed_segment`). The
//! manifest write is the one commit point: a crash before it keeps the
//! flush whole, and a crash after it leaves an unreferenced file.
//!
//! The memtable is not here: the install flushed every array whose
//! memtable held cells and that the record could fill before its first
//! array batch, so the oldest tile snapshot of each tile the install wrote
//! restores it, and applies after every flush undo.

use nodedb_array::types::ArrayId;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::partial_record::PartialRecords;
use crate::types::replay_stamp::ReplayStamp;

use super::UndoError;

/// What one array flush between two batches of a batched install changed.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor) struct ArrayInstallFlushUndo {
    pub array_id: ArrayId,
    /// The segment the flush wrote.
    pub segment: String,
    /// The manifest's replay stamp before the flush.
    pub replay_before: ReplayStamp,
    /// The manifest's partial records before the flush.
    pub partial_before: PartialRecords,
}

impl ArrayInstallFlushUndo {
    /// Heap bytes the entry holds.
    pub(in crate::data::executor) fn held_bytes(&self) -> usize {
        self.array_id.name.len()
            + self.segment.len()
            + std::mem::size_of_val(self.replay_before.applied_above.as_slice())
            + std::mem::size_of_val(self.partial_before.records())
    }
}

impl CoreLoop {
    /// Reverse the flush `undo` names. Fatal on failure: a segment the
    /// manifest still names holds cells of a rolled-back record.
    pub(super) fn apply_undo_array_install_flush(
        &mut self,
        entry_index: usize,
        undo: ArrayInstallFlushUndo,
    ) -> Result<(), UndoError> {
        let ArrayInstallFlushUndo {
            array_id,
            segment,
            replay_before,
            partial_before,
        } = undo;
        self.array_engine
            .withdraw_flushed_segment(&array_id, &segment, replay_before, partial_before)
            .map_err(|e| {
                UndoError::failed(
                    entry_index,
                    format!(
                        "array install flush undo: withdraw segment {segment} of array '{}'",
                        array_id.name
                    ),
                    e,
                )
            })
    }
}
