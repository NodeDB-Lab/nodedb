// SPDX-License-Identifier: BUSL-1.1

//! The timeseries and array flushes a batched install runs around its
//! batches, so those memtables stay under their budget however large the
//! record is.
//!
//! - Before the arm's first batch, each memtable that cannot take the
//!   record whole flushes, by the load the validate measured. Its stamp
//!   names only records applied before.
//! - After each batch of the arm, each memtable of the record that reached
//!   its budget flushes. The record is not noted applied yet, so the stamp
//!   names the record's ops below the batch's end as a partial record.
//!   Restart replay skips exactly those ops.
//!
//! A flush after a batch runs before the batch's durable cursor commit,
//! and records its undo in the batch's entries. Restart replay decides each
//! op of the record by the flushed stamps alone, never by the cursor, so a
//! crash on either side of that commit replays each row once.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::InstallFlushError;
use crate::types::partial_record::PartialRecord;
use crate::wal::redo::replay_arm::RedoArm;

use super::driver::ArmStop;
use super::state::BatchedInstall;

impl CoreLoop {
    /// The flushes before `arm`'s first batch. `false` when the install
    /// stops: a flush failed, and its error is the install's.
    pub(super) fn flush_before_arm(&mut self, install: &BatchedInstall, arm: RedoArm) -> bool {
        match arm {
            RedoArm::Timeseries => {
                self.enter_redo_arm(arm);
                self.flush_ts_for_stream(install.lsn.as_u64(), &install.loads.ts);
            }
            RedoArm::Array => {
                self.enter_redo_arm(arm);
                if let Err(error) = self.flush_arrays_for_stream(&install.loads.arrays)
                    && let Some(scope) = self.redo_apply.scope.as_mut()
                {
                    scope.record_error(error);
                }
            }
            _ => return true,
        }
        !self.install_stopped()
    }

    /// The flushes after batch `batch` of `arm`, whose ops end at the record
    /// op index `ops_through`. A flush that failed rolls the install back. A
    /// flush whose effects its undo cannot name stops the core.
    pub(super) fn flush_after_batch(
        &mut self,
        install: &mut BatchedInstall,
        arm: RedoArm,
        batch: u32,
        ops_through: u64,
    ) -> Result<(), ArmStop> {
        let lsn = install.lsn.as_u64();
        let partial = PartialRecord { lsn, ops_through };
        let flushed = match arm {
            RedoArm::Timeseries => {
                #[cfg(test)]
                self.note_ts_memtable_peak(install);
                let BatchedInstall {
                    loads, ts_flushed, ..
                } = install;
                match self.flush_ts_install_batch(&loads.ts, partial, ts_flushed) {
                    Ok(flushed) => flushed,
                    Err(InstallFlushError::Failed(error)) => {
                        return Err(ArmStop::Refused(ErrorCode::from(error)));
                    }
                    Err(InstallFlushError::Untracked(error)) => {
                        self.fail_install_progress(lsn, &error);
                        return Err(ArmStop::Unpersisted(ErrorCode::from(error)));
                    }
                }
            }
            RedoArm::Array => self
                .flush_array_install_batch(&install.loads.arrays, partial)
                .map_err(|error| ArmStop::Refused(ErrorCode::from(error)))?,
            _ => 0,
        };
        if flushed > 0 {
            tracing::debug!(
                core = self.core_id,
                lsn,
                arm = ?arm,
                batch,
                ops_through,
                flushed,
                "batched install flushed between batches"
            );
            // A crash test kills the process here: the flush is durable, and
            // the batch's cursor is not.
            crate::fail_point!(
                self.fail_scope,
                &format!("redo_apply::batched::after_partial_flush::{arm:?}::{batch}")
            );
        }
        Ok(())
    }

    /// Note the most resident bytes a timeseries memtable of the install
    /// holds after a batch, before the batch's flush.
    #[cfg(test)]
    fn note_ts_memtable_peak(&mut self, install: &BatchedInstall) {
        let peak = install
            .loads
            .ts
            .keys()
            .filter_map(|key| self.columnar_memtables.get(key))
            .map(|mt| mt.memory_bytes())
            .max()
            .unwrap_or(0);
        self.redo_apply.peak_ts_memtable_bytes = self.redo_apply.peak_ts_memtable_bytes.max(peak);
    }
}
