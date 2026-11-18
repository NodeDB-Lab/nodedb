// SPDX-License-Identifier: BUSL-1.1

//! The arm-major install of a stream marker's content.
//!
//! Every replay arm that claimed an op in the validate runs, in
//! [`RedoArm::DISPATCH`](crate::wal::redo::replay_arm::RedoArm::DISPATCH)
//! order, and each takes one pass over the stream (see `arm_pass`).
//! A pass reads the chunk records back through this core's io_uring
//! cursor and cuts them into the batches the validate cut. It decodes each
//! batch's ops, raises them to the stream's stamp floor, and runs the arm
//! over the record that holds them. So each arm applies the record's ops in
//! record order, after every earlier arm applied all of them: the order a
//! whole-record pass applies them in.
//!
//! One install scope stays open across every pass. It holds the undo log,
//! and in spools the events, the document rows and the write notes of the
//! whole record, beside what the validate kept for the events. After each
//! batch the spools and the undo log spill into the install's space once
//! they pass the threshold. The first error stops the install, and the
//! undo log rolls every batch back (`roll_back_batched`).
//!
//! The install is a job (see `job`) that runs one slice per step: one
//! batch of an arm's pass, the end of the arms, or the finish. The task
//! that validated the stream runs every step, with no yield between them.
//!
//! Under a root journal the scope also holds the fold ledger of the batch
//! that runs: each source row's transaction records how many of the
//! record's ops folded. A task with no root journal stores no write set,
//! and writes no ledger row, as a whole-record install writes none.
//!
//! After each batch the install makes everything through the batch durable
//! with its cursor (see `cursor`). Restart replay completes a record whose
//! install ended part way: its durable stores re-apply each op at its
//! exact key, its memory engines re-apply from op 0, and its folds skip
//! the ops the fold ledger names.
//!
//! Around the timeseries and array arms' batches the install runs those
//! engines' flushes itself (see `partial_flush`): before the arm's first
//! batch by the load the validate measured, and after each batch for each
//! memtable at its budget, stamped with the part of the record it holds.

use nodedb_physical::physical_plan::RedoOrigin;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_set_journal::RootJournalPermit;
use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;
use crate::wal::{RedoRecord, RedoStreamRef};

use super::super::state::TsInstalled;
use super::super::stream_validate::{StreamLimits, StreamValidated};
use super::job::StreamInstallJob;
use super::state::BatchedInstall;

/// One validated stream marker's install.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct StreamInstallInput<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    /// The marker record. Its Calvin stamp and cross-shard key are the
    /// content's.
    pub marker: &'a RedoRecord,
    pub reference: &'a RedoStreamRef,
    pub origin: RedoOrigin,
    /// The marker's LSN: every batch record takes it.
    pub lsn: Lsn,
    /// The validate's bounds, so the install cuts the batches it cut.
    pub limits: StreamLimits,
    pub permit: Option<RootJournalPermit>,
    /// Every collection the record writes.
    pub collections: &'a [String],
}

impl CoreLoop {
    /// Install the content of the stream `input` names, which the validate
    /// passed as `validated`, then settle it as a whole record settles.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn install_redo_stream(
        &mut self,
        input: StreamInstallInput<'_>,
        validated: StreamValidated,
        ts_installs: &mut Vec<TsInstalled>,
    ) -> Response {
        let task = input.task;
        match StreamInstallJob::open(self, input, validated) {
            Ok(job) => job.run_to_end(self, task, ts_installs),
            Err(refusal) => self.response_error(task, refusal),
        }
    }

    /// Whether an arm recorded an error, or the scope is gone: no later
    /// batch runs.
    pub(super) fn install_stopped(&self) -> bool {
        self.redo_apply
            .scope
            .as_ref()
            .is_none_or(|scope| scope.error.is_some())
    }
}

impl CoreLoop {
    /// Spill what the open scope holds into the install's space once it
    /// passed the threshold. `Err` when a spill file cannot be written: the
    /// install rolls back.
    pub(super) fn spill_held(&mut self, install: &mut BatchedInstall) -> Result<(), ErrorCode> {
        let Some(scope) = self.redo_apply.scope.as_mut() else {
            return Ok(());
        };
        // The fold targets' write set entries and the undo log's snapshots
        // cannot spill: they count against the threshold, so the spools
        // spill sooner. The undo log spills with the spools.
        let (undo_hot, undo_pinned) = scope
            .spillable
            .as_ref()
            .map_or((0, 0), |undo| (undo.hot_bytes(), undo.pinned_bytes()));
        let pinned = scope
            .target_set_bytes
            .saturating_add(undo_hot)
            .saturating_add(undo_pinned);
        let space = install.space.space_mut();
        let over = scope.held.hot_bytes().saturating_add(pinned) > space.threshold();
        let spilled = scope
            .held
            .spill_past_threshold(space, pinned)
            .and_then(|()| match scope.spillable.as_mut() {
                Some(undo) if over => undo.spill(space),
                _ => Ok(()),
            });
        spilled.map_err(|error| ErrorCode::RetryableRefusal {
            reason: format!(
                "the batched install of a committed redo failed to spill what it holds \
                 on this node: {error}"
            ),
        })
    }
}
