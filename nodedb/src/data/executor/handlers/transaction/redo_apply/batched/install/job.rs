// SPDX-License-Identifier: BUSL-1.1

//! A stream marker's batched install as a job that runs one slice per
//! step.
//!
//! The job owns every piece of the install's state: the install, the
//! validate's results, the KV write scope it opened, and the apply scope
//! once the arms are done with it. A slice is one batch of the current
//! arm's pass, the end of the arms, or the whole finish. A batch that wrote
//! ends with its durable `Installing` cursor commit. A batch with no ops, a
//! batch whose arm recorded no undo, and the end of the arms write nothing
//! after the last durable cursor. So every slice of the arms ends at a
//! durable cursor, or wrote nothing since the last one.
//!
//! The arms' apply scope sits in the core while the arms run, since the
//! arms read it there. The slice that ends the last pass takes it back and
//! closes the KV write scope.
//!
//! Arms that fail roll back one batch per slice (see `rollback`): a
//! refused batch, or a panic in an arm. The failing slice reverses the
//! newest batch, and each later slice the batch before it. Every slice
//! ends at the durable checkpoint that keeps the batch the next one
//! reverses. The KV write scope stays open through the rollback, and the
//! last slice answers.
//!
//! A finish that cannot prepare its capture rolls nothing back: every
//! batch is durable under the install's cursor. The journal fails and the
//! core fail-stops at once, and the next boot completes the record
//! forward.
//!
//! A task that `run_task` runs parks its job between slices (see `park`):
//! the job's ambient core state leaves the core, other tasks run under the
//! install fence, and `tick()` drives the next slice (see `park::drive`). Any
//! other caller runs the job to its end in one call.

use std::collections::BTreeSet;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_note::WriteStamp;
use crate::data::executor::core_loop::write_set_journal::RootJournalPermit;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::engine::KvWriteScopeGuard;
use crate::wal::redo::replay_arm::RedoArm;

use super::super::super::entry::unrecorded_fold;
use super::super::super::passes::{PassRefusal, lost_scope};
use super::super::super::state::{RedoApplyPass, RedoApplyScope, TsInstalled};
use super::super::super::stream_validate::{StreamValidated, ValidateSpill};
use super::super::rollback::run::{BatchedRollback, RollbackRun, RollbackSlice};
use super::super::rollback::undo::SpillableUndo;
use super::super::state::{BatchedInstall, BatchedInstallOpen, FoldedThrough, InstallLoads};
use super::arm_pass::{ArmPass, ArmStop, BatchStep};
use super::batch_record::BatchStamps;
use super::driver::StreamInstallInput;
use super::finish::{BatchedFinish, InstallFinish};
use super::job_stage::{ArmsFailed, FinishInput, JobStage, ParkPoint, StepOutcome};

/// One stream marker's batched install, run slice by slice.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct StreamInstallJob {
    install: BatchedInstall,
    stage: JobStage,
    /// The arms that claim an op, in dispatch order.
    arms: Vec<RedoArm>,
    /// The batches record the fold ledger: under a root journal only.
    ledgered: bool,
    /// The collections whose rows the arms note by key.
    keyed: BTreeSet<String>,
    /// The validate's spill files. They go once the arms ran.
    spill: Option<ValidateSpill>,
    /// The stamp every write of the record takes.
    stamp: WriteStamp,
    /// The ordinal every KV write of the record stamps.
    apply_ordinal: Option<i64>,
    /// The KV write scope the arms and a rollback write in. `None` once
    /// it closed.
    kv_scope: Option<KvWriteScopeGuard>,
    permit: Option<RootJournalPermit>,
    /// Every collection the record writes.
    collections: Vec<String>,
    /// What each resolved timeseries batch the install stored.
    ts_installs: Vec<TsInstalled>,
}

impl StreamInstallJob {
    /// Open the install of the stream `input` names, which the validate
    /// passed as `validated`. The apply scope and the KV write scope open
    /// on `core`. `Err` when the record cannot install: it folds with no
    /// root journal to store its write set.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn open(
        core: &mut CoreLoop,
        input: StreamInstallInput<'_>,
        validated: StreamValidated,
    ) -> Result<Self, ErrorCode> {
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
            return Err(refusal);
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
        let stamp = core.task_write_stamp_at(task, input.lsn);
        core.record_stamps.insert(stamp);
        // Every KV write of the record, and every undo of a refused install,
        // stamps one system-time ordinal.
        let apply_ordinal = core.kv_apply_ordinal();
        let kv_scope = core.kv_engine.open_write_scope(apply_ordinal);
        let ledgered = input.permit.is_some();
        install.folded = FoldedThrough::new(ledgered);
        let mut scope =
            RedoApplyScope::new(RedoApplyPass::Install, install.header.sum_targets.clone())
                .with_origin(install.origin);
        // The spillable log marks the scope batched: the arms run no
        // timeseries flush of their own, and the install runs them.
        scope.held = held;
        scope.spillable = Some(SpillableUndo::default());
        core.redo_apply.scope = Some(scope);
        let arms = install.active_arms.active().collect();
        Ok(Self {
            install,
            stage: JobStage::Arms {
                next_arm: 0,
                pass: None,
            },
            arms,
            ledgered,
            keyed,
            spill: Some(spill),
            stamp,
            apply_ordinal,
            kv_scope: Some(kv_scope),
            permit: input.permit,
            collections: input.collections.to_vec(),
            ts_installs: Vec::new(),
        })
    }

    /// Run steps until the job answers, then hand `ts_installs` what each
    /// resolved timeseries batch stored.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn run_to_end(
        mut self,
        core: &mut CoreLoop,
        task: &ExecutionTask,
        ts_installs: &mut Vec<TsInstalled>,
    ) -> Response {
        loop {
            match self.step(core, task) {
                Ok(StepOutcome::Ran) => {}
                Ok(StepOutcome::Done(response)) => {
                    ts_installs.append(&mut self.ts_installs);
                    return response;
                }
                Err(code) => return core.response_error(task, code),
            }
        }
    }

    /// Hand over what each resolved timeseries batch the install stored.
    pub(in crate::data::executor::handlers::transaction::redo_apply::batched) fn take_ts_installs(
        &mut self,
    ) -> Vec<TsInstalled> {
        std::mem::take(&mut self.ts_installs)
    }

    /// Where the job stands between slices.
    pub(in crate::data::executor::handlers::transaction::redo_apply::batched) fn park_point(
        &self,
    ) -> ParkPoint {
        match &self.stage {
            JobStage::Arms {
                pass: Some(pass), ..
            } => ParkPoint::Batch {
                arm: pass.arm(),
                batch: pass.last_batch(),
            },
            JobStage::RollingBack { kept, .. } => ParkPoint::RollingBack(*kept),
            JobStage::Arms { pass: None, .. } | JobStage::Finish(_) | JobStage::Done => {
                ParkPoint::Finish
            }
        }
    }

    /// The RAM the job holds between slices: its read buffer, and the hot
    /// parts of its spools, fold target entries and undo log. While the
    /// arms run, the apply scope is `core_scope`, the one in the core.
    pub(in crate::data::executor::handlers::transaction::redo_apply::batched) fn resident_bytes(
        &self,
        core_scope: Option<&RedoApplyScope>,
    ) -> usize {
        let (scope, undo, read) = match &self.stage {
            JobStage::Arms { pass, .. } => (
                core_scope,
                core_scope.and_then(|scope| scope.spillable.as_ref()),
                pass.as_ref().map_or(0, |pass| pass.held_bytes()),
            ),
            JobStage::Finish(input) => (Some(&input.scope), Some(&input.undo), 0),
            JobStage::RollingBack { run, .. } => (None, None, run.held_bytes()),
            JobStage::Done => (None, None, 0),
        };
        let held = scope.map_or(0, RedoApplyScope::resident_bytes);
        let undo = undo.map_or(0, |undo| {
            undo.hot_bytes().saturating_add(undo.pinned_bytes())
        });
        read.saturating_add(held).saturating_add(undo)
    }

    /// Run one slice for `task`, the task that opened the job. `Err` when
    /// the job answered already.
    pub(in crate::data::executor::handlers::transaction::redo_apply::batched) fn step(
        &mut self,
        core: &mut CoreLoop,
        task: &ExecutionTask,
    ) -> Result<StepOutcome, ErrorCode> {
        match std::mem::replace(&mut self.stage, JobStage::Done) {
            JobStage::Arms { next_arm, pass } => Ok(self.step_arms(core, task, next_arm, pass)),
            JobStage::Finish(tail) => Ok(self.finish(core, task, *tail)),
            JobStage::RollingBack { run, .. } => Ok(self.step_rollback(core, task, run)),
            JobStage::Done => Err(ErrorCode::Internal {
                detail: format!(
                    "the batched install at lsn {} ran a step after it answered",
                    self.install.lsn.as_u64()
                ),
            }),
        }
    }

    /// Run the next batch of the arms. A pass that ended moves on to the
    /// next arm in the same slice. The slice after the last batch ends the
    /// arms.
    fn step_arms(
        &mut self,
        core: &mut CoreLoop,
        task: &ExecutionTask,
        mut next_arm: usize,
        mut pass: Option<Box<ArmPass>>,
    ) -> StepOutcome {
        loop {
            let mut current = match pass.take() {
                Some(current) => current,
                None => {
                    let Some(&arm) = self.arms.get(next_arm) else {
                        return self.end_arms(core, task, None);
                    };
                    // The memtables flush here as a whole-record install
                    // flushes them before the record's first row of the
                    // arm's engine.
                    if !core.flush_before_arm(&self.install, arm) {
                        return self.end_arms(core, task, None);
                    }
                    match ArmPass::open(core, &self.install, arm) {
                        Ok(opened) => Box::new(opened),
                        Err(stop) => return self.end_arms(core, task, Some(stop)),
                    }
                }
            };
            match current.step_batch(core, task, &mut self.install, self.ledgered) {
                Ok(BatchStep::Ran) => {
                    self.stage = JobStage::Arms {
                        next_arm,
                        pass: Some(current),
                    };
                    return StepOutcome::Ran;
                }
                Ok(BatchStep::Ended) => {
                    next_arm = next_arm.saturating_add(1);
                    if core.install_stopped() {
                        return self.end_arms(core, task, None);
                    }
                }
                Err(stop) => return self.end_arms(core, task, Some(stop)),
            }
        }
    }

    /// End the arms, which stopped at `stop` or ran every pass. On success
    /// the KV write scope closes, the validate's spill files go, and the
    /// record's stamp is forgotten. A failure rolls back first, and the
    /// rollback's last slice closes them.
    fn end_arms(
        &mut self,
        core: &mut CoreLoop,
        task: &ExecutionTask,
        stop: Option<ArmStop>,
    ) -> StepOutcome {
        let mut scope = match self.take_scope(core, stop) {
            Ok(scope) => scope,
            Err(ArmsFailed::RollBack(run)) => {
                return self.step_rollback(core, task, run);
            }
            Err(ArmsFailed::Refused(refusal)) => {
                self.close_arms(core);
                return StepOutcome::Done(core.response_error(task, refusal.into_code()));
            }
        };
        self.close_arms(core);
        let Some(undo) = scope.spillable.take() else {
            return StepOutcome::Done(core.response_error(task, lost_scope().into_code()));
        };
        self.stage = JobStage::Finish(Box::new(FinishInput { scope, undo }));
        StepOutcome::Ran
    }

    /// Close what the arms held open: the KV write scope, the validate's
    /// spill files, which the install ran over, and the record's stamp.
    fn close_arms(&mut self, core: &mut CoreLoop) {
        if let Some(kv_scope) = self.kv_scope.take() {
            core.kv_engine.close_write_scope(kv_scope);
        }
        drop(self.spill.take());
        core.record_stamps.remove(self.install.lsn);
    }

    /// Take the apply scope out of the core once the arms stopped at
    /// `stop`. On success the scope carries what the arms wrote. On a
    /// failure every write rolls back, with the fold ledger rows of the
    /// record, and the rollback ends durable. A batch whose durable commit
    /// failed stopped the core instead, and nothing rolls back.
    fn take_scope(
        &mut self,
        core: &mut CoreLoop,
        stop: Option<ArmStop>,
    ) -> Result<RedoApplyScope, ArmsFailed> {
        let stopped = match stop {
            None => None,
            Some(ArmStop::Refused(cause)) => Some(cause),
            Some(ArmStop::Unpersisted(code)) => {
                // The stores hold effects past the last cursor: a rollback
                // would leave them with no cursor.
                core.redo_apply.scope = None;
                return Err(ArmsFailed::Refused(PassRefusal::Unpersisted(code)));
            }
        };
        let Some(mut scope) = core.redo_apply.scope.take() else {
            return Err(ArmsFailed::Refused(lost_scope()));
        };
        // A pass that could not go on outranks an arm's error, as a
        // reconstitution error or a panic does in a whole-record pass.
        let Some(cause) = stopped.or(scope.error.take()) else {
            return Ok(scope);
        };
        let Some(undo) = scope.spillable.take() else {
            return Err(ArmsFailed::Refused(lost_scope()));
        };
        let label_mark_before = scope.label_mark_before;
        Err(ArmsFailed::RollBack(self.begin_rollback(
            core,
            undo,
            label_mark_before,
            cause,
        )))
    }

    /// Start the rollback of the install's `undo`, after it failed with
    /// `cause`. `label_mark_before` is the node-label LSN mark the install's
    /// first version commit found.
    fn begin_rollback(
        &mut self,
        core: &mut CoreLoop,
        undo: SpillableUndo,
        label_mark_before: Option<u64>,
        cause: ErrorCode,
    ) -> Box<RollbackRun> {
        let install = &mut self.install;
        core.begin_batched_rollback(
            BatchedRollback {
                database_id: install.database_id,
                tid: install.tid,
                undo,
                lsn: install.lsn.as_u64(),
                stream: install.reference.stream,
                label_mark_before,
                folded: std::mem::take(&mut install.folded),
            },
            cause,
        )
    }

    /// Run one slice of the rollback `run`. The slice that ends it closes
    /// what the arms held open, and the job answers with its refusal.
    fn step_rollback(
        &mut self,
        core: &mut CoreLoop,
        task: &ExecutionTask,
        run: Box<RollbackRun>,
    ) -> StepOutcome {
        match core.step_batched_rollback(run) {
            RollbackSlice::Checkpointed { run, kept } => {
                self.stage = JobStage::RollingBack { run, kept };
                StepOutcome::Ran
            }
            RollbackSlice::Ended(refusal) => {
                self.close_arms(core);
                StepOutcome::Done(core.response_error(task, refusal.into_code()))
            }
        }
    }

    /// Settle the record the arms wrote, as a whole record settles. A
    /// capture that cannot be prepared rolls nothing back: see the module
    /// doc.
    fn finish(
        &mut self,
        core: &mut CoreLoop,
        task: &ExecutionTask,
        tail: FinishInput,
    ) -> StepOutcome {
        let FinishInput { mut scope, undo } = tail;
        let finish = InstallFinish {
            task,
            tid: self.install.tid,
            lsn: self.install.lsn,
            stamp: self.stamp,
            apply_ordinal: self.apply_ordinal,
            collections: &self.collections,
            keyed: &self.keyed,
            reader: self.install.space.reader(),
        };
        let response =
            match core.prepare_install_capture(&finish, true, self.permit.take(), &mut scope) {
                Ok(capture) => core.finish_redo_install(
                    finish,
                    capture,
                    Some(BatchedFinish { undo }),
                    scope,
                    &mut self.ts_installs,
                ),
                Err(error) => core.refuse_batched_capture(task, self.install.lsn, &error),
            };
        StepOutcome::Done(response)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::RedoOrigin;

    use super::super::super::super::install_refusal_tests as refusal;
    use super::super::super::super::stream_validate::tests::{
        LSN, TID, default_task, framed, hold, plain_core,
    };
    use super::super::super::super::stream_validate::{StreamLimits, StreamValidateInput};
    use super::super::super::equivalence_tests::{
        BATCH_BYTES, FILL, digest, install_batched_at, resolved, spread,
    };
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::enforcement::retention::judged_instant_ms;
    use crate::types::Lsn;

    #[test]
    fn a_job_run_slice_by_slice_matches_run_to_end() {
        let plans = [refusal::kv_predicate_delete()];
        let mut redo = resolved(&plans, refusal::seed_kv);
        redo.ops = spread(std::mem::take(&mut redo.ops));
        let mut collections = nodedb_physical::physical_plan::written_collections(&plans);
        collections.push(FILL.to_string());

        let mut whole = hold(plain_core);
        refusal::seed_kv(&mut whole.core);
        let ran = install_batched_at(&mut whole, &redo, &[], &collections, usize::MAX);
        assert_eq!(ran.status, Status::Ok, "{:?}", ran.error_code);

        let mut stepped = hold(plain_core);
        refusal::seed_kv(&mut stepped.core);
        let reference = stepped.wal.write(&framed(&redo, &[], &[]));
        let marker = reference.marker(None, None, Vec::new()).expect("marker");
        let task = default_task();
        let limits = StreamLimits {
            batch_bytes: BATCH_BYTES,
            spill_threshold: usize::MAX,
        };
        let lsn = Lsn::new(LSN);
        let validated = stepped
            .core
            .validate_redo_stream(&StreamValidateInput {
                task: &task,
                tid: TID,
                marker: &marker,
                reference: &reference,
                origin: RedoOrigin::Commit,
                snapshot: None,
                judged_at_ms: judged_instant_ms(None, task.request.system_ordinal),
                lsn,
                limits,
            })
            .expect("validate");
        let mut job = StreamInstallJob::open(
            &mut stepped.core,
            StreamInstallInput {
                task: &task,
                tid: TID,
                marker: &marker,
                reference: &reference,
                origin: RedoOrigin::Commit,
                lsn,
                limits,
                permit: None,
                collections: &collections,
            },
            validated,
        )
        .expect("open the job");
        let mut slices = 0_usize;
        let response = loop {
            match job.step(&mut stepped.core, &task).expect("step") {
                StepOutcome::Ran => slices += 1,
                StepOutcome::Done(response) => break response,
            }
        };

        assert_eq!(response.status, ran.status, "{:?}", response.error_code);
        assert!(
            slices > job.arms.len(),
            "the record spans several batches of each arm: {slices} slices"
        );
        assert_eq!(digest(&stepped.core), digest(&whole.core));
        assert!(
            job.step(&mut stepped.core, &task).is_err(),
            "a job that answered runs no later step"
        );
    }
}
