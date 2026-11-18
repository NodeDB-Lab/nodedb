// SPDX-License-Identifier: BUSL-1.1

//! The arm-major install, run as a job one slice at a time: one batch of
//! an arm's pass, the finish every install shares, or one batch of a
//! rollback. The timeseries and array flushes run around their batches.

pub(super) mod arm_pass;
pub(super) mod batch_record;
pub(super) mod driver;
pub(super) mod finish;
pub(super) mod job;
pub(super) mod job_stage;
pub(super) mod partial_flush;
