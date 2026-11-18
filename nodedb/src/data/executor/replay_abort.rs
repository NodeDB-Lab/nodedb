// SPDX-License-Identifier: BUSL-1.1

//! The single site where startup WAL replay gives up on a committed record.
//!
//! Every record handed to a `replay_*_wal` arm has already had its CRC
//! verified by the WAL reader, and every record in the replayed suffix was
//! acknowledged to a client as committed. So a record that an engine arm
//! cannot decode, cannot route, or whose handler rejects is not a damaged byte
//! range to step over — it is a committed write that this build cannot apply.
//! Continuing past it opens the database with a hole in the replayed suffix
//! that no later read can distinguish from data that was never written.
//!
//! Recovery therefore stops. The core fail-stops with
//! [`FailStopCause::ReplayRecordUnapplied`]: every replay arm applies no
//! record once the core is stopped, a stopped core publishes no checkpoint,
//! and `replay_all_wal` returns the halt to its caller as an error. Boot then
//! refuses to start, exactly as it does for a redo group replay cannot
//! reconstitute and for a partially-recovered sync HWM idempotency gate. The
//! fail-stop files the one report of the halt, with the engine, the step and
//! the record in its detail. Its grouping key is the cause, so one WAL tail
//! that fails identically on every core files one growing report.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::fail_stop::FailStopCause;

impl CoreLoop {
    /// Stop restart replay because a committed WAL record cannot be applied.
    ///
    /// `engine` names the replay arm (`kv`, `fts`, `spatial`, ...), `stage` the
    /// step inside it that failed (`decode`, `handler`, `geometry`, ...), and
    /// `detail` says why in the detecting site's own words. The caller skips
    /// the record, and every arm applies nothing after it.
    pub(in crate::data::executor) fn halt_replay(
        &mut self,
        engine: &str,
        stage: &str,
        record_lsn: u64,
        detail: &str,
    ) {
        if self.is_fail_stopped() {
            return;
        }
        tracing::error!(
            core_id = self.core_id,
            engine,
            stage,
            record_lsn,
            detail,
            "StartupError: a committed WAL record could not be applied — refusing to \
             start with a hole in the replayed suffix"
        );
        self.fail_stop_core(
            FailStopCause::ReplayRecordUnapplied,
            &format!("committed WAL record at lsn {record_lsn} ({engine} {stage}): {detail}"),
        );
    }

    /// Whether restart replay must apply no further record: a record already
    /// halted it, or the core stopped for another cause. Every replay arm
    /// checks this before each record.
    pub(in crate::data::executor) fn replay_halted(&self) -> bool {
        self.is_fail_stopped()
    }

    /// The error restart replay returns once it halted, `None` while it runs.
    pub(in crate::data::executor) fn replay_halt_error(&self) -> Option<crate::Error> {
        let (cause, detail) = self.fail_stop.cause()?;
        Some(crate::Error::Storage {
            engine: "wal_replay".into(),
            detail: format!(
                "core {} stopped WAL replay ({cause:?}): {detail}; the database cannot \
                 start with a hole in the replayed suffix",
                self.core_id
            ),
        })
    }
}
