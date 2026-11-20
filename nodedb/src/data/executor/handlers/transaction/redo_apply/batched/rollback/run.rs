// SPDX-License-Identifier: BUSL-1.1

//! The rollback of a batched install that failed: every batch it wrote,
//! newest first, one batch per slice.
//!
//! The entries come off both lanes of the install's undo log in the
//! reverse of the order they were recorded in, and each applies through
//! the undo a whole-record rollback applies it through. An entry that does
//! not reverse stops the rollback, and the `RollbackFailed` answer
//! fail-stops the core as it leaves.
//!
//! A slice reverses every entry of one batch, then makes the reversal
//! durable with a `RollingBack` cursor (see `durable::cursor`) that keeps the batch
//! the next entry reverses. The state is then the forward install through
//! the kept batch, so a crash leaves a forward prefix, and restart replay
//! completes the record forward. A checkpoint that cannot be stored stops
//! the core and seals the stores, as a forward batch's failed commit does.
//! A parked install gives the core back after each such slice. The slice that reverses the oldest batch ends
//! the rollback with a durable commit that removes the cursor, the fold
//! ledger and the fold target rows, before the refusal leaves the core.
//!
//! Between slices the install fence holds every collection the undo
//! entries touch. What other tasks can change meanwhile is core-wide, and
//! each undo leaves it as such a task left it:
//!
//! - the timeseries ingest timer restores with `TimerRestore::KeepNewer`;
//! - a sync high-water mark lowers only while it holds the install's value;
//! - each slice forgets the cached newest ordinals before it lowers
//!   versions;
//! - the node-label LSN mark lowers once, at the end. A mark another
//!   database's record raised meanwhile lowers with it, and restart replay
//!   versions that record's labels again, idempotently.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::{TimerRestore, UndoError};
use crate::wal::RedoStreamId;
use crate::wal::redo::install_cursor::CursorState;

use super::super::super::passes::{PassRefusal, rollback_end_failed};
use super::super::state::FoldedThrough;
use super::undo::{BatchAt, DrainStop, SpillableUndo};
use super::undo_drain::{BatchDrained, UndoDrain};

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

/// A batched rollback between its slices.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct RollbackRun {
    database_id: u64,
    tid: u64,
    lsn: u64,
    stream: RedoStreamId,
    label_mark_before: Option<u64>,
    folded: FoldedThrough,
    /// The entries left to reverse.
    drain: UndoDrain,
    /// Whether the log held an entry when the rollback began.
    lowers: bool,
    /// The error the install failed with.
    cause: ErrorCode,
}

impl RollbackRun {
    /// The RAM the rollback holds between slices.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn held_bytes(&self) -> usize {
        self.drain.held_bytes()
    }
}

/// How one slice of a rollback ended.
pub(in crate::data::executor::handlers::transaction::redo_apply) enum RollbackSlice {
    /// One batch reversed, and the checkpoint that keeps `kept` is
    /// durable. More is left.
    Checkpointed {
        run: Box<RollbackRun>,
        kept: BatchAt,
    },
    /// The rollback ended with this refusal.
    Ended(PassRefusal),
}

impl CoreLoop {
    /// Start the rollback of a batched install that failed with `cause`.
    /// Nothing is reversed yet.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn begin_batched_rollback(
        &mut self,
        rollback: BatchedRollback,
        cause: ErrorCode,
    ) -> Box<RollbackRun> {
        let BatchedRollback {
            database_id,
            tid,
            undo,
            lsn,
            stream,
            label_mark_before,
            folded,
        } = rollback;
        #[cfg(test)]
        {
            self.redo_apply.last_rollback = super::super::super::state::UndoReport {
                #[cfg(feature = "failpoints")]
                spill_lane: undo.spill_lane_len(),
                #[cfg(feature = "failpoints")]
                spill_lane_hot_bytes: undo.hot_bytes(),
                ram_lane: undo.ram_lane_len(),
            };
        }
        Box::new(RollbackRun {
            database_id,
            tid,
            lsn,
            stream,
            label_mark_before,
            folded,
            lowers: !undo.is_empty(),
            drain: undo.into_drain(),
            cause,
        })
    }

    /// Run one slice of `run`: reverse every entry of the newest batch
    /// left, then store the checkpoint that keeps the batch before it, or
    /// end the rollback once no entry is left.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn step_batched_rollback(
        &mut self,
        mut run: Box<RollbackRun>,
    ) -> RollbackSlice {
        // A rollback lowers stored versions in a way no write note
        // describes: the newest-ordinal cache forgets every value. A task
        // between slices can fill it again, so each slice forgets.
        if run.lowers {
            self.forget_newest_ordinals();
        }
        let (database_id, tid) = (run.database_id, run.tid);
        let drained = run.drain.drain_batch(|entry_index, _, entry| {
            self.apply_undo_entry(
                database_id,
                tid,
                entry_index,
                entry,
                TimerRestore::KeepNewer,
            )
        });
        match drained {
            Ok(BatchDrained::Boundary { kept }) => {
                if let Err(error) = self.checkpoint_rollback(run.lsn, run.stream, kept, &run.folded)
                {
                    // The stores hold a reversal past the last durable
                    // cursor. The core stops and seals them, as after a
                    // forward batch's failed commit, and the next boot
                    // completes the record forward from that cursor.
                    self.fail_install_progress(run.lsn, &error);
                    return RollbackSlice::Ended(PassRefusal::Unpersisted(ErrorCode::from(error)));
                }
                RollbackSlice::Checkpointed { run, kept }
            }
            Ok(BatchDrained::Empty) => {
                let RollbackRun {
                    cause,
                    lsn,
                    label_mark_before,
                    ..
                } = *run;
                RollbackSlice::Ended(self.end_batched_rollback(cause, lsn, label_mark_before))
            }
            Err(DrainStop::Undo(undo_error)) => {
                RollbackSlice::Ended(undo_failed(&run.cause, undo_error))
            }
            Err(DrainStop::Read { entry_index, error }) => {
                RollbackSlice::Ended(PassRefusal::RollbackFailed(ErrorCode::RollbackFailed {
                    entry_index,
                    detail: format!(
                        "rolling back a committed redo install that failed with {:?}: its \
                         spilled undo entries could not be read back: {error}",
                        run.cause
                    ),
                    cause: None,
                }))
            }
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

/// The refusal of a rollback that stopped at `undo_error`, of an install
/// that failed with `cause`.
fn undo_failed(cause: &ErrorCode, mut undo_error: UndoError) -> PassRefusal {
    undo_error.action = format!(
        "rolling back a committed redo install that failed with {cause:?}: {}",
        undo_error.action
    );
    PassRefusal::RollbackFailed(ErrorCode::from(undo_error))
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, TenantId};

    use super::super::super::rollback_tests::roll_back_batched;
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::transaction::undo::{TimeseriesIngestUndo, UndoEntry};
    use crate::wal::redo::replay_arm::RedoArm;

    /// An ingest to another collection stamps a newer timer between the
    /// install's batches. The rollback reverses the install's ingest and
    /// leaves that timer in place.
    #[test]
    fn a_batched_rollback_keeps_the_timer_of_an_interleaved_ingest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let before = std::time::Instant::now();
        let interleaved = before + std::time::Duration::from_secs(5);

        let mut undo = SpillableUndo::default();
        undo.enter(BatchAt {
            arm: RedoArm::Timeseries,
            batch: 0,
        });
        undo.record([UndoEntry::TimeseriesIngest(TimeseriesIngestUndo {
            collection_key: (DatabaseId::DEFAULT, TenantId::new(1), "installed".into()),
            memtable_before: None,
            memtable_config_before: None,
            memtable_memory_bytes_before: None,
            last_value_cache_before: None,
            series_catalog_before: None,
            last_ts_ingest_before: Some(before),
            reservation_bytes_before: None,
        })]);
        core.last_ts_ingest = Some(interleaved);

        // The answer depends on the durable end of the rollback, which this
        // test does not set up. The timer restores before that end.
        let _ = roll_back_batched(
            &mut core,
            BatchedRollback {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tid: 1,
                undo,
                lsn: 10,
                stream: RedoStreamId::Session {
                    vshard: 0,
                    idempotency_key: 1,
                },
                label_mark_before: None,
                folded: FoldedThrough::default(),
            },
            ErrorCode::Internal {
                detail: "failed install".into(),
            },
        );
        assert_eq!(core.last_ts_ingest, Some(interleaved));
    }
}
