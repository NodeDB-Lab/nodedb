// SPDX-License-Identifier: BUSL-1.1

//! The start of a stream install that gives the core back between its
//! slices.
//!
//! The install fence is armed before the validate, over:
//! - every collection the record writes
//! - every materialized-sum source and target the plan names
//! - the vShard the record's events carry
//! - the keys the marker declares: the collection of each read, each sum
//!   source and target its header names, each sync stream its ops advance,
//!   and the graph state of the task's tenant when an op writes it
//!
//! Each validate slice checks the keys it walks against the fence (see
//! `stream_job`). A marker whose keys passed the final entry's room
//! declares none: each slice arms the keys it walks. So no write lands on what the record reads or writes
//! between the validate's verdict and the install's first batch.
//!
//! The job opens validating, and its first slice runs inside the task.
//! When the job has more to run, its ambient state leaves the core, the
//! heartbeat stays below the marker's LSN, and the task answers with a
//! placeholder that never leaves the core (see `parked`). A name that is no
//! stored collection name fences nothing it names, so that install runs to
//! its end in its task.
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
use crate::wal::RedoStreamKeys;

use super::super::super::state::TsInstalled;
use super::super::super::stream_validate::StreamValidateInput;
use super::super::install::driver::StreamInstallInput;
use super::super::install::job_stage::{ParkPoint, StepOutcome};
use super::owned::OwnedStream;
use super::parked::{InstallTail, PendingPark};
use super::stream_job::StreamJob;

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
        let lsn = install.lsn;
        let fenced = self.arm_stream_fence(
            task,
            install.collections,
            sum_targets,
            &validate.reference.keys,
        );
        // A test panics here: the fence is armed, and nothing is validated.
        crate::fail_point!(self.fail_scope, "redo_apply::batched::before_validate");
        if !fenced {
            return match self.validate_redo_stream(&validate) {
                Ok(validated) => self.install_redo_stream(install, validated, ts_installs),
                Err(code) => self.response_error(task, code),
            };
        }
        let job = match StreamJob::open(self, task, OwnedStream::of(&validate, install)) {
            Ok(job) => job,
            Err(code) => {
                self.release_install_fence();
                return self.response_error(task, code);
            }
        };
        self.run_first_slice(job, task, lsn, ts_installs)
    }

    /// Arm the install fence over what the record of `task` touches: the
    /// stored `collections`, the sources and targets of `sum_targets`, the
    /// vShard its events carry, and the keys `declared` names. `false` when
    /// a written or summed name is no collection name: nothing is armed.
    fn arm_stream_fence(
        &mut self,
        task: &ExecutionTask,
        collections: &[String],
        sum_targets: &[RedoSumTargets],
        declared: &RedoStreamKeys,
    ) -> bool {
        let db = task.request.database_id;
        let mut keys = BTreeSet::new();
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
        // A marker that declares no key arms what its walk keys, slice by
        // slice, before the core runs another task.
        let RedoStreamKeys::Declared(declared) = declared else {
            self.arm_install_fence(keys);
            return true;
        };
        let declared_sums = declared.sum_collections.iter().map(String::as_str);
        if !sum_name_keys(db, declared_sums, &mut keys) {
            return false;
        }
        keys.extend(
            declared
                .reads
                .iter()
                .filter_map(|name| FenceKey::of_read(db, name)),
        );
        keys.extend(
            declared
                .sync_streams
                .iter()
                .copied()
                .map(FenceKey::of_sync_stream),
        );
        if declared.graph {
            keys.insert(FenceKey::Graph {
                db,
                tenant: task.request.tenant_id,
            });
        }
        self.arm_install_fence(keys);
        true
    }

    /// Run the first slice of `job`, the install of `task` at `lsn`, under
    /// the install fence its task armed. A job with more to run parks. The
    /// fence releases when the job answers.
    fn run_first_slice(
        &mut self,
        mut job: StreamJob,
        task: &ExecutionTask,
        lsn: Lsn,
        ts_installs: &mut Vec<TsInstalled>,
    ) -> Response {
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
    fn park_job(&mut self, job: StreamJob, task: &ExecutionTask, lsn: Lsn) -> Response {
        // The judged view leaves the core with the apply scope, so the
        // parked charge holds it too.
        let resident = job
            .resident_bytes(self.redo_apply.scope.as_ref())
            .saturating_add(self.apply_scope.judged_resident_bytes());
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
    /// last batch, or its rollback's last checkpoint, is durable. A
    /// validate that parked wrote nothing.
    pub(super) fn note_parked(&self, job: &StreamJob, lsn: Lsn) {
        let point = job.park_point();
        match point {
            ParkPoint::Validating { batch } => tracing::debug!(
                core = self.core_id,
                lsn = lsn.as_u64(),
                batch,
                "batched install parked in its validate"
            ),
            ParkPoint::Batch { arm, batch } => tracing::debug!(
                core = self.core_id,
                lsn = lsn.as_u64(),
                arm = ?arm,
                batch,
                "batched install parked"
            ),
            ParkPoint::RollingBack(kept) => tracing::debug!(
                core = self.core_id,
                lsn = lsn.as_u64(),
                arm = ?kept.arm,
                batch = kept.batch,
                "batched rollback parked"
            ),
            ParkPoint::Finish => {}
        }
        crate::fail_point!(self.fail_scope, &park_fail_point(point, ParkSite::Parked));
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

/// A fail point a parked install passes.
#[cfg(feature = "failpoints")]
#[derive(Clone, Copy)]
pub(super) enum ParkSite {
    /// The install parked.
    Parked,
    /// The install's next slice may run.
    Held,
}

/// The fail point `site` names at `point`:
/// `redo_apply::batched::{validate site}::{batch}` after the validate
/// walked a batch, `redo_apply::batched::{site}::{arm:?}::{batch}` while
/// the arms run, `redo_apply::batched::{site}::finish` once they ended,
/// and `redo_apply::batched::{rollback site}::{arm:?}::{batch}` at the
/// batch a rollback's checkpoint kept. The sites are `validate_parked` and
/// `hold_validate_parked`, `parked` and `hold_parked`, and
/// `rollback_parked` and `hold_rollback_parked`.
#[cfg(feature = "failpoints")]
pub(super) fn park_fail_point(point: ParkPoint, site: ParkSite) -> String {
    let (site, at) = match (point, site) {
        (ParkPoint::Validating { batch }, ParkSite::Parked) => {
            ("validate_parked", batch.to_string())
        }
        (ParkPoint::Validating { batch }, ParkSite::Held) => {
            ("hold_validate_parked", batch.to_string())
        }
        (ParkPoint::Batch { arm, batch }, site) => (site.forward(), format!("{arm:?}::{batch}")),
        (ParkPoint::Finish, site) => (site.forward(), "finish".to_owned()),
        (ParkPoint::RollingBack(kept), ParkSite::Parked) => {
            ("rollback_parked", format!("{:?}::{}", kept.arm, kept.batch))
        }
        (ParkPoint::RollingBack(kept), ParkSite::Held) => (
            "hold_rollback_parked",
            format!("{:?}::{}", kept.arm, kept.batch),
        ),
    };
    format!("redo_apply::batched::{site}::{at}")
}

#[cfg(feature = "failpoints")]
impl ParkSite {
    /// The site's name while the install runs forward.
    fn forward(self) -> &'static str {
        match self {
            Self::Parked => "parked",
            Self::Held => "hold_parked",
        }
    }
}

/// The error a parked install's task answers with when the core stopped
/// before the install ended: `detail` names why. Its durable batches and
/// cursor stay, and boot completes the record forward. A validate that
/// stopped wrote nothing, and boot replays the record from op 0. So the
/// code claims nothing about what applied.
pub(super) fn unfinished(lsn: Lsn, detail: &str) -> ErrorCode {
    ErrorCode::Internal {
        detail: format!(
            "the batched install of the committed redo at lsn {} stopped part way: {detail}; \
             the next boot completes the record",
            lsn.as_u64()
        ),
    }
}

/// Add the key of every source and target collection of `sum_targets`, in
/// database `db`, to `keys`. `false` when a name is no collection name of
/// `db`: its report is filed.
pub(super) fn sum_target_keys(
    db: DatabaseId,
    sum_targets: &[RedoSumTargets],
    keys: &mut BTreeSet<FenceKey>,
) -> bool {
    let names = sum_targets.iter().flat_map(|targets| {
        std::iter::once(targets.collection.as_str()).chain(
            targets
                .resolved
                .iter()
                .map(|target| target.target_collection.as_str()),
        )
    });
    sum_name_keys(db, names, keys)
}

/// Add the key of each materialized-sum source or target collection
/// `names` names, in database `db`, to `keys`. `false` when a name is no
/// collection name of `db`: its report is filed.
fn sum_name_keys<'n>(
    db: DatabaseId,
    names: impl IntoIterator<Item = &'n str>,
    keys: &mut BTreeSet<FenceKey>,
) -> bool {
    let names: BTreeSet<&str> = names.into_iter().collect();
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
