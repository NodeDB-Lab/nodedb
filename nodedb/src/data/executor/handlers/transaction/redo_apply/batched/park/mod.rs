// SPDX-License-Identifier: BUSL-1.1

//! An install that gives the core back between its slices under the
//! install fence: its start, the job from its validate to its answer, the
//! parked job and its task, and the drive that runs one slice per tick.

pub(super) mod drive;
pub(super) mod owned;
pub(super) mod parked;
pub(super) mod start;
pub(super) mod stream_job;
