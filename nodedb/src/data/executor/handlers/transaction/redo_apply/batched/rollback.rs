// SPDX-License-Identifier: BUSL-1.1

//! The rollback of a batched install that failed: every batch it wrote,
//! newest first.
//!
//! The entries come off both lanes of the install's undo log in the
//! reverse of the order they were recorded in, and each applies through
//! the undo a whole-record rollback applies it through. An entry that does
//! not reverse stops the rollback, and the `RollbackFailed` answer
//! fail-stops the core as it leaves.
//!
//! Once every entry of a batch reversed, the rollback makes the reversal
//! durable with a `RollingBack` cursor (see `cursor`). The state is then the
//! forward install through the batches before it, so a crash leaves a
//! forward prefix, and restart replay completes the record forward. The
//! rollback ends with a durable commit that removes the cursor, the fold
//! ledger and the fold target rows, before the refusal leaves the core.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::UndoError;
use crate::wal::RedoStreamId;
use crate::wal::redo::install_cursor::CursorState;

use super::super::passes::{PassRefusal, rollback_end_failed};
use super::state::FoldedThrough;
use super::undo::{BatchAt, DrainStop, SpillableUndo};

/// What a batched rollback reverses.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct BatchedRollback {
    pub database_id: u64,
    pub tid: u64,
    pub undo: SpillableUndo,
    /// The LSN of the record the install wrote.
    pub lsn: u64,
    /// The stream the record's marker names.
    pub stream: RedoStreamId,
    /// The node-label LSN mark the install's first version commit found.
    pub label_mark_before: Option<u64>,
    /// The fold ledger each durable document batch left.
    pub folded: FoldedThrough,
}

impl CoreLoop {
    /// Reverse every write of a batched install that failed with `cause`.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn roll_back_batched(
        &mut self,
        rollback: BatchedRollback,
        cause: ErrorCode,
    ) -> PassRefusal {
        let BatchedRollback {
            database_id,
            tid,
            undo,
            lsn,
            stream,
            label_mark_before,
            folded,
        } = rollback;
        // A rollback lowers stored versions in a way no write note
        // describes: the newest-ordinal cache forgets every value.
        if !undo.is_empty() {
            self.forget_newest_ordinals();
        }
        #[cfg(test)]
        {
            self.redo_apply.last_rollback = super::super::state::UndoReport {
                spill_lane: undo.spill_lane_len(),
                spill_lane_hot_bytes: undo.hot_bytes(),
                ram_lane: undo.ram_lane_len(),
            };
        }
        // Each entry names the batch whose write it reverses, and one
        // batch's entries come together.
        let mut reversing: Option<BatchAt> = None;
        let drained = undo.drain_newest_first(|entry_index, at, entry| {
            if let Some(reversed) = reversing
                && reversed != at
            {
                self.checkpoint_rollback(lsn, stream, at, &folded)
                    .map_err(|error| {
                        UndoError::failed(
                            entry_index,
                            format!(
                                "storing the rollback checkpoint after reversing batch {} of {:?}",
                                reversed.batch, reversed.arm
                            ),
                            error,
                        )
                    })?;
            }
            reversing = Some(at);
            self.apply_undo_entry(database_id, tid, entry_index, entry)
        });
        match drained {
            Ok(()) => self.end_batched_rollback(cause, lsn, label_mark_before),
            Err(DrainStop::Undo(mut undo_error)) => {
                undo_error.action = format!(
                    "rolling back a committed redo install that failed with {cause:?}: {}",
                    undo_error.action
                );
                PassRefusal::RollbackFailed(ErrorCode::from(undo_error))
            }
            Err(DrainStop::Read(error)) => PassRefusal::RollbackFailed(ErrorCode::RollbackFailed {
                entry_index: 0,
                detail: format!(
                    "rolling back a committed redo install that failed with {cause:?}: its \
                     spilled undo entries could not be read back: {error}"
                ),
                cause: None,
            }),
        }
    }

    /// Make the reversal durable: the install's state is its forward
    /// install through batch `kept`, whose entries the rollback reverses
    /// next. The fold ledger goes back to the ops that state folded, with
    /// the fold target rows above it, so restart replay folds the reversed
    /// ops again. A crash test kills the process once it is durable.
    fn checkpoint_rollback(
        &mut self,
        lsn: u64,
        stream: RedoStreamId,
        kept: BatchAt,
        folded: &FoldedThrough,
    ) -> crate::Result<()> {
        let state = CursorState::RollingBack {
            arm: kept.arm,
            batch: kept.batch,
        };
        let rewind = folded.rewind_to(lsn, kept)?;
        self.commit_batch_durable(lsn, stream, state, rewind)?;
        crate::fail_point!(
            self.fail_scope,
            "redo_apply::batched::rollback_after_checkpoint"
        );
        Ok(())
    }

    /// End a rollback whose entries all reversed: lower the node-label LSN
    /// mark back to `label_mark_before`, then remove the cursor, the fold
    /// ledger and the fold target rows of the record at `lsn` in a durable
    /// commit.
    fn end_batched_rollback(
        &mut self,
        cause: ErrorCode,
        lsn: u64,
        label_mark_before: Option<u64>,
    ) -> PassRefusal {
        // A crash test kills the process here: every entry reversed, and
        // the reversal after the last checkpoint is not durable.
        crate::fail_point!(self.fail_scope, "redo_apply::batched::before_rollback_end");
        if let Err(error) = self.restore_label_mark(label_mark_before) {
            return rollback_end_failed(&cause, "restoring the node-label LSN mark", &error);
        }
        if let Err(error) = self.end_install_durable(lsn) {
            return rollback_end_failed(&cause, "the durable rollback end", &error);
        }
        PassRefusal::RolledBack(cause)
    }
}
