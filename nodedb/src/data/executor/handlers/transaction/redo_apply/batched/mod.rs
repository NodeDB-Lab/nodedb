// SPDX-License-Identifier: BUSL-1.1

//! The batched install of a stream marker: the content installs one bounded
//! batch at a time, arm by arm.
//!
//! - [`state`]: the state one install keeps across its passes and batches.
//! - [`batch_record`]: the record one batch runs as, in the validate and
//!   the install alike.
//! - [`driver`]: the arm-major install, run as a [`job`] one slice at a
//!   time: one batch of an [`arm_pass`], or the finish. [`partial_flush`]
//!   runs the timeseries and array flushes around their batches.
//! - [`park`]: an install that gives the core back between its slices
//!   under the install fence, [`parked`] while it waits, and [`drive`],
//!   which runs one slice per tick.
//! - [`finish`]: the end every install shares once its arms ran.
//! - [`spools`]: what an install holds for after its arms, spilled past a
//!   threshold, in [`space`].
//! - [`undo`]: the undo log, spilled past the same threshold, and
//!   [`rollback`], which reverses it.
//! - [`cursor`]: the durable commit after each batch, and [`boot`], which
//!   settles the cursors a crash left.

mod arm_pass;
mod batch_record;
mod boot;
#[cfg(test)]
mod boot_tests;
#[cfg(all(test, feature = "failpoints"))]
mod crash_tests;
mod cursor;
mod drive;
mod driver;
#[cfg(test)]
mod equivalence_tests;
mod finish;
mod job;
mod notes;
mod park;
mod parked;
mod partial_flush;
#[cfg(test)]
mod partial_flush_tests;
mod rollback;
#[cfg(test)]
mod rollback_tests;
mod row_sources;
mod space;
#[cfg(test)]
mod spool_tests;
mod spooled;
mod spools;
mod state;
mod undo;
#[cfg(test)]
mod yield_tests;

pub(in crate::data::executor::handlers::transaction::redo_apply) use batch_record::{
    BatchStamps, batch_wal_record,
};
pub(in crate::data::executor::handlers::transaction::redo_apply) use driver::StreamInstallInput;
pub(in crate::data::executor::handlers::transaction::redo_apply) use finish::InstallFinish;
pub(in crate::data::executor::handlers::transaction::redo_apply) use park::YieldingStream;
pub(in crate::data::executor::handlers::transaction::redo_apply) use parked::InstallTail;
pub(in crate::data::executor) use parked::{ParkedInstall, PendingPark, is_park_placeholder};
pub(in crate::data::executor::handlers::transaction::redo_apply) use row_sources::HeldRowSources;
pub(in crate::data::executor::handlers::transaction::redo_apply) use space::InstallSpace;
pub(in crate::data::executor::handlers::transaction::redo_apply) use spooled::SpooledDocWrite;
pub(in crate::data::executor::handlers::transaction::redo_apply) use spools::InstallSpools;
pub(in crate::data::executor::handlers::transaction::redo_apply) use state::ActiveArms;
pub(in crate::data::executor::handlers::transaction::redo_apply) use undo::SpillableUndo;
