// SPDX-License-Identifier: BUSL-1.1

//! An install that gives the core back between its slices under the
//! install fence: its start, the parked job and its task, and the drive
//! that runs one slice per tick.

pub(super) mod drive;
pub(super) mod parked;
pub(super) mod start;
