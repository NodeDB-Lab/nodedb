// SPDX-License-Identifier: BUSL-1.1

//! One replay arm's pass over a stream marker's content, one batch per
//! step.
//!
//! A pass reads the chunk records back through this core's io_uring cursor
//! and cuts them into the batches the validate cut. Each step decodes one
//! batch's ops, raises them to the stream's stamp floor, and runs the arm
//! over the record that holds them. The step then runs the arm's flushes
//! after the batch, spills what the scope holds past the threshold, and
//! commits the batch durably with its cursor.

use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::data::io::uring_wal_cursor::UringWalCursor;
use crate::wal::redo::frame::{FrameBatcher, RedoFrameTag, RedoFrames, decode_frame};
use crate::wal::redo::install_cursor::CursorState;
use crate::wal::redo::replay_arm::RedoArm;
use crate::wal::{RedoStreamReader, RedoSubRecord};

use super::super::fold_ledger::FoldLedger;
use super::super::passes::arm_panic;
use super::super::streamed::unreadable;
use super::batch_record::batch_wal_record;
use super::state::BatchedInstall;
use super::undo::{BatchAt, SpillableUndo};

/// Why an arm's pass over the stream stopped.
pub(super) enum ArmStop {
    /// The pass cannot go on. The install rolls back.
    Refused(ErrorCode),
    /// The install's durable progress could not be stored. The core
    /// fail-stopped, and the install rolls nothing back.
    Unpersisted(ErrorCode),
}

impl From<ErrorCode> for ArmStop {
    fn from(code: ErrorCode) -> Self {
        Self::Refused(code)
    }
}

/// What one step of a pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BatchStep {
    /// One batch ran, and the pass has more to read.
    Ran,
    /// The pass ended: the stream holds no later batch, or an arm recorded
    /// an error.
    Ended,
}

/// Where one batch sits in the stream.
#[derive(Clone, Copy)]
struct BatchCut {
    /// The batch's index among every batch of the stream.
    batch: u32,
    /// The record op index of the batch's first op.
    op_base: u64,
    /// The batch's rows record the fold ledger.
    ledgered: bool,
}

/// One pass of `arm` over the stream.
pub(super) struct ArmPass {
    arm: RedoArm,
    batcher: FrameBatcher<RedoStreamReader<UringWalCursor>>,
    /// The index of the next batch among every batch of the stream.
    index: u64,
    /// The record op index of the next batch's first op.
    op_base: u64,
}

impl ArmPass {
    /// Open `arm`'s pass over the stream `install` names.
    pub(super) fn open(
        core: &CoreLoop,
        install: &BatchedInstall,
        arm: RedoArm,
    ) -> Result<Self, ArmStop> {
        let reader =
            RedoStreamReader::<UringWalCursor>::open(&core.wal_source, install.reference.clone())
                .map_err(|error| unreadable(&install.reference, error))?;
        Ok(Self {
            arm,
            batcher: FrameBatcher::new(reader, install.limits.batch_bytes),
            index: 0,
            op_base: 0,
        })
    }

    /// The arm the pass runs.
    pub(super) fn arm(&self) -> RedoArm {
        self.arm
    }

    /// The index of the last batch the pass ran, among every batch of the
    /// stream.
    pub(super) fn last_batch(&self) -> u64 {
        self.index.saturating_sub(1)
    }

    /// The RAM the pass's read buffer holds.
    pub(super) fn held_bytes(&self) -> usize {
        self.batcher.held_bytes()
    }

    /// Run the pass's next batch. A batch that ran ends durable with its
    /// cursor. `ledgered` writes the fold ledger. `Err` when the pass cannot
    /// go on.
    pub(super) fn step_batch(
        &mut self,
        core: &mut CoreLoop,
        task: &ExecutionTask,
        install: &mut BatchedInstall,
        ledgered: bool,
    ) -> Result<BatchStep, ArmStop> {
        let arm = self.arm;
        let bytes = match self.batcher.next_batch() {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                // A test fails the arm's last batch here, after it wrote.
                crate::fail_point_err!(
                    core.fail_scope,
                    &format!("redo_apply::batched::install_batch_fails::{arm:?}"),
                    |detail: String| ArmStop::Refused(ErrorCode::Internal { detail })
                );
                return Ok(BatchStep::Ended);
            }
            Err(error) => return Err(unreadable(&install.reference, error).into()),
        };
        let ops = batch_ops(&bytes, install.reference.stamp_floor)?;
        let count = ops.len() as u64;
        let op_base = self.op_base;
        if !ops.is_empty() {
            let batch = u32::try_from(self.index).map_err(|_| ErrorCode::Internal {
                detail: format!(
                    "the committed redo stream holds more than {} batches",
                    u32::MAX
                ),
            })?;
            let at = BatchCut {
                batch,
                op_base,
                ledgered,
            };
            let undo_before = core.install_undo_len();
            core.install_batch(task, install, arm, at, ops)?;
            if core.install_stopped() {
                return Ok(BatchStep::Ended);
            }
            let ops_through = op_base.saturating_add(count);
            // The flushes run before the batch's durable commit, and their
            // undo entries are the batch's.
            core.flush_after_batch(install, arm, batch, ops_through)?;
            core.spill_held(install)?;
            // Every write of the install records its undo, so a batch this
            // arm recorded none for wrote nothing to make durable.
            if core.install_undo_len() != undo_before {
                let state = CursorState::Installing {
                    arm,
                    batch,
                    ops_through,
                };
                core.batch_durable(install, state)?;
            }
            if arm == RedoArm::Document {
                install.folded.record(batch, ops_through);
            }
        }
        self.op_base = op_base.saturating_add(count);
        self.index = self.index.saturating_add(1);
        Ok(BatchStep::Ran)
    }
}

impl CoreLoop {
    /// The entries the open install's undo log holds.
    fn install_undo_len(&self) -> u64 {
        self.redo_apply
            .scope
            .as_ref()
            .and_then(|scope| scope.spillable.as_ref())
            .map_or(0, SpillableUndo::len)
    }

    /// Commit the batch `state` names durably with its cursor. A crash test
    /// kills the process on either side of the commit.
    fn batch_durable(
        &mut self,
        install: &BatchedInstall,
        state: CursorState,
    ) -> Result<(), ArmStop> {
        #[cfg(feature = "failpoints")]
        let at = super::cursor::batch_point(state);
        crate::fail_point!(
            self.fail_scope,
            &format!("redo_apply::batched::before_batch_durable::{at}")
        );
        let lsn = install.lsn.as_u64();
        if let Err(error) = self.commit_batch_durable(lsn, install.reference.stream, state, None) {
            self.fail_install_progress(lsn, &error);
            return Err(ArmStop::Unpersisted(ErrorCode::from(error)));
        }
        if let CursorState::Installing { arm, batch, .. } = state {
            tracing::debug!(
                core = self.core_id,
                lsn,
                arm = ?arm,
                batch,
                "batched install batch durable"
            );
        }
        crate::fail_point!(
            self.fail_scope,
            &format!("redo_apply::batched::after_batch_durable::{at}")
        );
        Ok(())
    }

    /// Run `arm` over the batch `ops`, at `at`. `Err` when the batch cannot
    /// run: its record does not build, its reconstitution fails, or the arm
    /// panics.
    fn install_batch(
        &mut self,
        task: &ExecutionTask,
        install: &BatchedInstall,
        arm: RedoArm,
        at: BatchCut,
        ops: Vec<RedoSubRecord>,
    ) -> Result<(), ErrorCode> {
        let record = batch_wal_record(task, install.lsn, &install.header, &install.stamps, ops)?;
        if let Some(scope) = self.redo_apply.scope.as_mut() {
            scope.batch = at.batch;
            scope.op_base = at.op_base;
            scope.ledger = at.ledgered.then_some(FoldLedger {
                lsn: install.lsn.as_u64(),
                op_base: at.op_base,
            });
            if let Some(undo) = scope.spillable.as_mut() {
                undo.enter(BatchAt {
                    arm,
                    batch: at.batch,
                });
            }
        }
        // The arms route a record to `vshard_id % num_cores`. A committed
        // record carries no collection tombstone of its own.
        let num_cores = self.redo_apply.num_cores;
        let applied = catch_unwind(AssertUnwindSafe(|| {
            self.replay_arm_in_lsn_order(
                arm,
                std::slice::from_ref(&record),
                num_cores,
                &nodedb_wal::TombstoneSet::new(),
            )
        }));
        match applied {
            Ok(applied) => applied.map_err(ErrorCode::from),
            Err(payload) => Err(arm_panic(payload.as_ref())),
        }
    }
}

/// The ops of one batch of framed records, raised to the stream's stamp
/// floor as the validate raised them. The validate decoded the same bytes,
/// so an error here is no verdict on the record: the install rolls back.
fn batch_ops(batch: &[u8], stamp_floor: Option<i64>) -> Result<Vec<RedoSubRecord>, ErrorCode> {
    let mut ops = Vec::new();
    for frame in RedoFrames::new(batch) {
        let (tag, body) = frame.map_err(ErrorCode::from)?;
        if tag != RedoFrameTag::Op {
            continue;
        }
        let mut sub: RedoSubRecord = decode_frame(tag, body).map_err(ErrorCode::from)?;
        if let Some(floor) = stamp_floor {
            sub.raise_stamp_to(floor).map_err(ErrorCode::from)?;
        }
        ops.push(sub);
    }
    Ok(ops)
}
