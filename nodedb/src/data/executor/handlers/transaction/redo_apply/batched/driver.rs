// SPDX-License-Identifier: BUSL-1.1

//! The arm-major install of a stream marker's content.
//!
//! Every replay arm that claimed an op in the validate runs, in
//! [`RedoArm::DISPATCH`] order, and each takes one pass over the stream.
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
//! undo log rolls every batch back (`roll_back_batched`). The install runs
//! in the task that validated the stream, with no yield between its
//! batches.
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

use std::panic::{AssertUnwindSafe, catch_unwind};

use nodedb_physical::physical_plan::RedoOrigin;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_set_journal::RootJournalPermit;
use crate::data::executor::task::ExecutionTask;
use crate::data::io::uring_wal_cursor::UringWalCursor;
use crate::types::Lsn;
use crate::wal::redo::frame::{FrameBatcher, RedoFrameTag, RedoFrames, decode_frame};
use crate::wal::redo::install_cursor::CursorState;
use crate::wal::redo::replay_arm::RedoArm;
use crate::wal::{RedoRecord, RedoStreamReader, RedoStreamRef, RedoSubRecord};

use super::super::entry::unrecorded_fold;
use super::super::fold_ledger::FoldLedger;
use super::super::passes::{PassRefusal, arm_panic, lost_scope};
use super::super::state::{RedoApplyPass, RedoApplyScope, TsInstalled};
use super::super::stream_validate::{StreamLimits, StreamValidated};
use super::super::streamed::unreadable;
use super::batch_record::{BatchStamps, batch_wal_record};
use super::finish::{BatchedFinish, InstallFinish};
use super::rollback::BatchedRollback;
use super::spools::InstallSpools;
use super::state::{BatchedInstall, BatchedInstallOpen, FoldedThrough, InstallLoads};
use super::undo::{BatchAt, SpillableUndo};

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

impl CoreLoop {
    /// Install the content of the stream `input` names, which the validate
    /// passed as `validated`, then settle it as a whole record settles.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn install_redo_stream(
        &mut self,
        input: StreamInstallInput<'_>,
        validated: StreamValidated,
        ts_installs: &mut Vec<TsInstalled>,
    ) -> Response {
        let StreamValidated {
            header,
            active_arms,
            keyed,
            held,
            space,
            ts_loads,
            array_loads,
            spill,
        } = validated;
        let task = input.task;
        // Each batch makes its folds durable, and only a root journal keeps
        // the fold ledger that counts them, so an open journal alone does
        // not record them.
        let folds = !header.sum_targets.is_empty();
        if let Some(refusal) = unrecorded_fold(input.lsn, folds, input.permit.is_some()) {
            return self.response_error(task, refusal);
        }
        let mut install = BatchedInstall::new(BatchedInstallOpen {
            lsn: input.lsn,
            tid: input.tid,
            database_id: task.request.database_id.as_u64(),
            origin: input.origin,
            reference: input.reference.clone(),
            stamps: BatchStamps::of(input.marker),
            header,
            active_arms,
            limits: input.limits,
            space,
            loads: InstallLoads {
                ts: ts_loads,
                arrays: array_loads,
            },
        });
        // Each write the arms apply takes the record's stamp: its vShard and
        // its LSN.
        let stamp = self.task_write_stamp_at(task, input.lsn);
        self.record_stamps.insert(stamp);
        // Every KV write of the record, and every undo of a refused install,
        // stamps one system-time ordinal.
        let apply_ordinal = self.kv_apply_ordinal();
        let kv_scope = self.kv_engine.open_write_scope(apply_ordinal);
        let ledgered = input.permit.is_some();
        install.folded = FoldedThrough::new(ledgered);
        let installed = self.install_stream_arms(task, &mut install, ledgered, held);
        self.kv_engine.close_write_scope(kv_scope);
        // The install ran over the validated content: its spill directory
        // goes.
        drop(spill);
        self.record_stamps.remove(input.lsn);
        let mut scope = match installed {
            Ok(scope) => scope,
            Err(refusal) => return self.response_error(task, refusal.into_code()),
        };
        let Some(undo) = scope.spillable.take() else {
            return self.response_error(task, lost_scope().into_code());
        };
        self.finish_redo_install(
            InstallFinish {
                task,
                tid: input.tid,
                lsn: input.lsn,
                stamp,
                apply_ordinal,
                permit: input.permit,
                collections: input.collections,
                keyed: &keyed,
                reader: install.space.reader(),
                batched: Some(BatchedFinish {
                    undo,
                    stream: install.reference.stream,
                    folded: std::mem::take(&mut install.folded),
                }),
            },
            scope,
            ts_installs,
        )
    }

    /// Run every active arm over the stream with one install scope open,
    /// holding `held`, what the validate kept. `ledgered` writes the fold
    /// ledger, under a root journal only. On success the scope carries what
    /// the arms wrote. On a failure every write is rolled back first, with
    /// the fold ledger rows of the record, and the rollback ends durable.
    /// A batch whose durable commit fails stops the core instead, and
    /// nothing rolls back.
    fn install_stream_arms(
        &mut self,
        task: &ExecutionTask,
        install: &mut BatchedInstall,
        ledgered: bool,
        held: InstallSpools,
    ) -> Result<RedoApplyScope, PassRefusal> {
        let mut scope =
            RedoApplyScope::new(RedoApplyPass::Install, install.header.sum_targets.clone())
                .with_origin(install.origin);
        // The spillable log marks the scope batched: the arms run no
        // timeseries flush of their own, and the install runs them.
        scope.held = held;
        scope.spillable = Some(SpillableUndo::default());
        self.redo_apply.scope = Some(scope);
        let arms: Vec<RedoArm> = install.active_arms.active().collect();
        let mut stopped = None;
        for arm in arms {
            // The memtables flush here as a whole-record install flushes
            // them before the record's first row of the arm's engine.
            if !self.flush_before_arm(install, arm) {
                break;
            }
            match self.install_arm(task, install, arm, ledgered) {
                Ok(()) => {}
                Err(ArmStop::Refused(cause)) => {
                    stopped = Some(cause);
                    break;
                }
                Err(ArmStop::Unpersisted(code)) => {
                    // The stores hold effects past the last cursor: a
                    // rollback would leave them with no cursor.
                    self.redo_apply.scope = None;
                    return Err(PassRefusal::Unpersisted(code));
                }
            }
            if self.install_stopped() {
                break;
            }
        }
        let Some(mut scope) = self.redo_apply.scope.take() else {
            return Err(lost_scope());
        };
        // A pass that could not go on outranks an arm's error, as a
        // reconstitution error or a panic does in a whole-record pass.
        let Some(cause) = stopped.or(scope.error.take()) else {
            return Ok(scope);
        };
        let Some(undo) = scope.spillable.take() else {
            return Err(lost_scope());
        };
        Err(self.roll_back_batched(
            BatchedRollback {
                database_id: install.database_id,
                tid: install.tid,
                undo,
                lsn: install.lsn.as_u64(),
                stream: install.reference.stream,
                label_mark_before: scope.label_mark_before,
                folded: std::mem::take(&mut install.folded),
            },
            cause,
        ))
    }

    /// The entries the open install's undo log holds.
    fn install_undo_len(&self) -> u64 {
        self.redo_apply
            .scope
            .as_ref()
            .and_then(|scope| scope.spillable.as_ref())
            .map_or(0, SpillableUndo::len)
    }

    /// Whether an arm recorded an error, or the scope is gone: no later
    /// batch runs.
    pub(super) fn install_stopped(&self) -> bool {
        self.redo_apply
            .scope
            .as_ref()
            .is_none_or(|scope| scope.error.is_some())
    }

    /// One pass of `arm` over the stream, batch by batch. Each batch that
    /// ran ends durable with its cursor. `Err` when the pass cannot go on.
    fn install_arm(
        &mut self,
        task: &ExecutionTask,
        install: &mut BatchedInstall,
        arm: RedoArm,
        ledgered: bool,
    ) -> Result<(), ArmStop> {
        let reader =
            RedoStreamReader::<UringWalCursor>::open(&self.wal_source, install.reference.clone())
                .map_err(|error| unreadable(&install.reference, error))?;
        let mut batcher = FrameBatcher::new(reader, install.limits.batch_bytes);
        let mut index: u64 = 0;
        let mut op_base: u64 = 0;
        loop {
            let bytes = match batcher.next_batch() {
                Ok(Some(bytes)) => bytes,
                Ok(None) => {
                    // A test fails the arm's last batch here, after it wrote.
                    crate::fail_point_err!(
                        self.fail_scope,
                        &format!("redo_apply::batched::install_batch_fails::{arm:?}"),
                        |detail: String| ArmStop::Refused(ErrorCode::Internal { detail })
                    );
                    return Ok(());
                }
                Err(error) => return Err(unreadable(&install.reference, error).into()),
            };
            let ops = batch_ops(&bytes, install.reference.stamp_floor)?;
            let count = ops.len() as u64;
            if !ops.is_empty() {
                let batch = u32::try_from(index).map_err(|_| ErrorCode::Internal {
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
                let undo_before = self.install_undo_len();
                self.install_batch(task, install, arm, at, ops)?;
                if self.install_stopped() {
                    return Ok(());
                }
                let ops_through = op_base.saturating_add(count);
                // The flushes run before the batch's durable commit, and
                // their undo entries are the batch's.
                self.flush_after_batch(install, arm, batch, ops_through)?;
                self.spill_held(install)?;
                // Every write of the install records its undo, so a batch
                // this arm recorded none for wrote nothing to make durable.
                if self.install_undo_len() != undo_before {
                    let state = CursorState::Installing {
                        arm,
                        batch,
                        ops_through,
                    };
                    self.batch_durable(install, state)?;
                }
                if arm == RedoArm::Document {
                    install.folded.record(batch, ops_through);
                }
            }
            op_base = op_base.saturating_add(count);
            index = index.saturating_add(1);
        }
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

impl CoreLoop {
    /// Spill what the open scope holds into the install's space once it
    /// passed the threshold. `Err` when a spill file cannot be written: the
    /// install rolls back.
    fn spill_held(&mut self, install: &mut BatchedInstall) -> Result<(), ErrorCode> {
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
