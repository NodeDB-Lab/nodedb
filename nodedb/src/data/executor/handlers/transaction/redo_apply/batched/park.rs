// SPDX-License-Identifier: BUSL-1.1

//! The start of a stream install that gives the core back between its
//! slices.
//!
//! The install fence is armed before the validate, over every collection
//! the record writes, every materialized-sum source and target, the node
//! labels of the task's tenant, and the vShard the record's events carry.
//! A stream marker names no op, so whether the content holds label ops is
//! unknown before the validate, and the labels are always fenced. The
//! validate's header names the content's sum targets, and they join the
//! fence before the first batch.
//!
//! The first slice runs inside the task. When the job has more to run, its
//! ambient state leaves the core, the heartbeat stays below the marker's
//! LSN, and the task answers with a placeholder that never leaves the core
//! (see `parked`). A name that is no stored collection name fences nothing
//! it names, so that install runs to its end in its task.
//!
//! Everything from the fence's arming through the park runs inside one
//! unwind boundary. A panic there stops the core, drops the job, releases
//! the fence, and the task answers that its install stopped part way.

use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

use nodedb_physical::physical_plan::RedoSumTargets;
use nodedb_types::QualifiedCollection;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::data::executor::core_loop::install_fence::FenceKey;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, Lsn};

use super::super::state::TsInstalled;
use super::super::stream_validate::{StreamValidateInput, StreamValidated};
use super::driver::StreamInstallInput;
use super::job::{StepOutcome, StreamInstallJob};
use super::parked::{InstallTail, PendingPark};

/// The site a fence key's name failed at, for its report.
const UNFENCED_SITE: &str = "install_fence: the stream install runs to its end in its task";

/// A stream marker's install that may give the core back between its
/// slices: the task runs through `run_task`.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct YieldingStream<'a> {
    pub(in crate::data::executor::handlers::transaction::redo_apply) validate:
        StreamValidateInput<'a>,
    pub(in crate::data::executor::handlers::transaction::redo_apply) install:
        StreamInstallInput<'a>,
    /// The materialized-sum sources and targets the plan names.
    pub(in crate::data::executor::handlers::transaction::redo_apply) sum_targets:
        &'a [RedoSumTargets],
}

impl CoreLoop {
    /// Validate and install `stream`, parking the install between its
    /// slices, inside one unwind boundary.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn install_redo_stream_yielding(
        &mut self,
        stream: YieldingStream<'_>,
        ts_installs: &mut Vec<TsInstalled>,
    ) -> Response {
        let task = stream.install.task;
        let lsn = stream.install.lsn;
        let ran = catch_unwind(AssertUnwindSafe(|| {
            self.start_yielding(stream, ts_installs)
        }));
        match ran {
            Ok(response) => response,
            Err(payload) => {
                let detail = self.stop_panicked_install(lsn, payload.as_ref());
                self.release_install_fence();
                self.response_error(task, unfinished(lsn, &detail))
            }
        }
    }

    /// Arm the fence, validate, and install `stream`. A record the fence
    /// cannot key runs to its end.
    fn start_yielding(
        &mut self,
        stream: YieldingStream<'_>,
        ts_installs: &mut Vec<TsInstalled>,
    ) -> Response {
        let YieldingStream {
            validate,
            install,
            sum_targets,
        } = stream;
        let task = install.task;
        let fenced = self.arm_stream_fence(task, install.collections, sum_targets);
        // A test panics here: the fence is armed, and nothing is validated.
        crate::fail_point!(self.fail_scope, "redo_apply::batched::before_validate");
        let validated = match self.validate_redo_stream(&validate) {
            Ok(validated) => validated,
            Err(code) => {
                if fenced {
                    self.release_install_fence();
                }
                return self.response_error(task, code);
            }
        };
        if fenced {
            self.install_redo_stream_parking(install, validated, ts_installs)
        } else {
            self.install_redo_stream(install, validated, ts_installs)
        }
    }

    /// Arm the install fence over what the record of `task` writes: the
    /// stored `collections`, the sources and targets of `sum_targets`, the
    /// node labels of its tenant, and the vShard its events carry. `false`
    /// when a name is no stored collection name: nothing is armed.
    fn arm_stream_fence(
        &mut self,
        task: &ExecutionTask,
        collections: &[String],
        sum_targets: &[RedoSumTargets],
    ) -> bool {
        let db = task.request.database_id;
        let mut keys = BTreeSet::new();
        keys.insert(FenceKey::GraphLabels {
            db,
            tenant: task.request.tenant_id,
        });
        // Every event of the record carries the task's vShard. A write of
        // that vShard would advance its partition past the held events.
        keys.insert(FenceKey::VShard(task.request.vshard_id));
        match FenceKey::of_stored_collections(collections) {
            Ok(stored) => keys.extend(stored),
            Err(error) => {
                // The error names the collection. The list is the record's.
                crate::diag::stored_collection_name_malformed(
                    UNFENCED_SITE,
                    &collections.join(","),
                    &error,
                );
                return false;
            }
        }
        if !sum_target_keys(db, sum_targets, &mut keys) {
            return false;
        }
        self.arm_install_fence(keys);
        true
    }

    /// Install the stream `input` names, which the validate passed as
    /// `validated`, under the install fence its task armed. The first slice
    /// runs now, and a job with more to run parks. The fence releases when
    /// the job answers.
    fn install_redo_stream_parking(
        &mut self,
        input: StreamInstallInput<'_>,
        validated: StreamValidated,
        ts_installs: &mut Vec<TsInstalled>,
    ) -> Response {
        let task = input.task;
        let lsn = input.lsn;
        let mut targets = BTreeSet::new();
        let parks = sum_target_keys(
            task.request.database_id,
            &validated.header.sum_targets,
            &mut targets,
        );
        if parks {
            self.arm_install_fence(targets);
        }
        let mut job = match StreamInstallJob::open(self, input, validated) {
            Ok(job) => job,
            Err(refusal) => {
                self.release_install_fence();
                return self.response_error(task, refusal);
            }
        };
        if !parks {
            let response = job.run_to_end(self, task, ts_installs);
            self.release_install_fence();
            return response;
        }
        match job.step(self, task) {
            Ok(StepOutcome::Ran) => self.park_job(job, task, lsn),
            Ok(StepOutcome::Done(response)) => {
                ts_installs.append(&mut job.take_ts_installs());
                self.release_install_fence();
                response
            }
            Err(code) => {
                self.release_install_fence();
                self.response_error(task, code)
            }
        }
    }

    /// Park `job`, the install of `task` at `lsn`, after a slice that left
    /// more to run. Returns the placeholder the task answers with inside
    /// its run. A journal window that cannot leave fail-stopped the core:
    /// the task answers, and the job goes.
    fn park_job(&mut self, job: StreamInstallJob, task: &ExecutionTask, lsn: Lsn) -> Response {
        let resident = job.resident_bytes(self.redo_apply.scope.as_ref());
        let context = match self.leave_install() {
            Ok(context) => context,
            Err(error) => {
                drop(job);
                self.release_install_fence();
                return self.response_error(task, unfinished(lsn, &error.to_string()));
            }
        };
        // No consumer sees a watermark past an event the install holds.
        self.hold_event_floor(lsn.as_u64());
        self.note_parked(&job, lsn);
        let reservation = self.charge_parked(task, resident);
        self.redo_apply.parking = Some(Box::new(PendingPark {
            job,
            context,
            tail: InstallTail::Plain,
            lsn,
            reservation,
            span: tracing::Span::current(),
        }));
        self.park_placeholder(task)
    }

    /// Log where `job`, the install at `lsn`, parked. A crash test kills
    /// the process here: the install's state is out of the core, and its
    /// last batch is durable.
    pub(super) fn note_parked(&self, job: &StreamInstallJob, lsn: Lsn) {
        let point = job.park_point();
        if let Some((arm, batch)) = point {
            tracing::debug!(
                core = self.core_id,
                lsn = lsn.as_u64(),
                arm = ?arm,
                batch,
                "batched install parked"
            );
        }
        #[cfg(feature = "failpoints")]
        let at = park_point_name(point);
        crate::fail_point!(
            self.fail_scope,
            &format!("redo_apply::batched::parked::{at}")
        );
    }

    /// Stop the core: a part of the install at `lsn` outside its arms
    /// panicked with `payload`. What the install changed is unknown, so the
    /// parked job and a job parking now go. Returns the detail the task
    /// answers with.
    pub(super) fn stop_panicked_install(
        &mut self,
        lsn: Lsn,
        payload: &(dyn std::any::Any + Send),
    ) -> String {
        let detail = format!(
            "a part of the batched install at lsn {} outside its arms panicked: {}",
            lsn.as_u64(),
            crate::data::panic_payload::panic_payload_to_string(payload)
        );
        // The panic can leave the install's journal window in the core with
        // deferred commits and no held events: seal both stores so no later
        // durable commit or close persists them.
        self.sparse.db().seal();
        self.edge_store.db().seal();
        self.fail_stop_core(FailStopCause::InstallPanicked, &detail);
        self.redo_apply.parking = None;
        self.redo_apply.parked = None;
        detail
    }
}

/// The name a park point takes in a failpoint: `{arm:?}::{batch}`, or
/// `finish` once the arms ended.
#[cfg(feature = "failpoints")]
pub(super) fn park_point_name(
    point: Option<(crate::wal::redo::replay_arm::RedoArm, u64)>,
) -> String {
    match point {
        Some((arm, batch)) => format!("{arm:?}::{batch}"),
        None => "finish".to_owned(),
    }
}

/// The error a parked install's task answers with when the core stopped
/// before the install ended: `detail` names why. Its durable batches and
/// cursor stay, and boot completes the record forward, so the code claims
/// nothing about what applied.
pub(super) fn unfinished(lsn: Lsn, detail: &str) -> ErrorCode {
    ErrorCode::Internal {
        detail: format!(
            "the batched install of the committed redo at lsn {} stopped part way: {detail}; \
             its durable batches complete at the next boot",
            lsn.as_u64()
        ),
    }
}

/// Add the key of every source and target collection of `sum_targets`, in
/// database `db`, to `keys`. `false` when a name is no collection name of
/// `db`: its report is filed.
fn sum_target_keys(
    db: DatabaseId,
    sum_targets: &[RedoSumTargets],
    keys: &mut BTreeSet<FenceKey>,
) -> bool {
    let names: BTreeSet<&str> = sum_targets
        .iter()
        .flat_map(|targets| {
            std::iter::once(targets.collection.as_str()).chain(
                targets
                    .resolved
                    .iter()
                    .map(|target| target.target_collection.as_str()),
            )
        })
        .collect();
    for name in names {
        match QualifiedCollection::from_request(db, name) {
            Ok(qualified) => {
                keys.insert(FenceKey::Collection {
                    db,
                    name: qualified.as_str().to_owned(),
                });
            }
            Err(error) => {
                crate::diag::stored_collection_name_malformed(UNFENCED_SITE, name, &error);
                return false;
            }
        }
    }
    true
}
