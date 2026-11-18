// SPDX-License-Identifier: BUSL-1.1

//! The undo log, spilled past the install's threshold, and the rollback
//! that reverses it one batch at a time.

pub(super) mod run;
pub(super) mod undo;
pub(super) mod undo_drain;
