// SPDX-License-Identifier: BUSL-1.1

//! The batched install of a stream marker: the content installs one bounded
//! batch at a time, arm by arm, and spills what it holds past a threshold.

mod durable;
mod install;
mod notes;
mod park;
mod rollback;
mod row_sources;
mod space;
mod spooled;
mod spools;
mod state;

#[cfg(test)]
mod boot_tests;
#[cfg(test)]
mod concurrent_redo_tests;
#[cfg(all(test, feature = "failpoints"))]
mod crash_tests;
#[cfg(test)]
mod equivalence_tests;
#[cfg(test)]
mod isolation_keys_tests;
#[cfg(test)]
mod isolation_stream_tests;
#[cfg(test)]
mod isolation_tests;
#[cfg(test)]
mod partial_flush_tests;
#[cfg(test)]
mod rollback_tests;
#[cfg(test)]
mod spool_tests;
#[cfg(test)]
mod yield_tests;

pub(in crate::data::executor::handlers::transaction::redo_apply) use install::batch_record::{
    BatchStamps, batch_wal_record,
};
pub(in crate::data::executor::handlers::transaction::redo_apply) use install::driver::StreamInstallInput;
pub(in crate::data::executor::handlers::transaction::redo_apply) use install::finish::InstallFinish;
pub(in crate::data::executor::handlers::transaction::redo_apply) use park::parked::InstallTail;
pub(in crate::data::executor) use park::parked::{ParkedInstall, PendingPark, is_park_placeholder};
pub(in crate::data::executor::handlers::transaction::redo_apply) use park::start::YieldingStream;
pub(in crate::data::executor::handlers::transaction::redo_apply) use rollback::undo::SpillableUndo;
pub(in crate::data::executor::handlers::transaction::redo_apply) use row_sources::HeldRowSources;
pub(in crate::data::executor::handlers::transaction::redo_apply) use space::InstallSpace;
pub(in crate::data::executor::handlers::transaction::redo_apply) use spooled::SpooledDocWrite;
pub(in crate::data::executor::handlers::transaction::redo_apply) use spools::InstallSpools;
pub(in crate::data::executor::handlers::transaction::redo_apply) use state::ActiveArms;
