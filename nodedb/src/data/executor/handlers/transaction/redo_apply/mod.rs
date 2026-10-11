// SPDX-License-Identifier: BUSL-1.1

//! Committed-redo apply: install one committed transaction's redo record on
//! the core that owns its vShard, through the WAL replay arms.
//!
//! - [`entry`]: the `MetaOp::ApplyTransactionRedo` handler.
//! - [`calvin_install`]: the install of a committed Calvin slice.
//! - [`validate`]: commit-boundary checks that run before any write.
//! - [`passes`]: the validate and install passes over the replay arms.
//! - [`settle`]: the work an install defers until every sub-record landed.
//! - [`document`]: document rows written while the apply scope is open.
//! - [`events`]: the record's Event Plane output.
//! - [`sub_ops`]: typed views of the redo sub-records.
//! - [`state`]: the per-core state and per-record scope.
//! - [`test_commit`]: a test driver for a session commit on one core.
//! - [`install_refusal_tests`]: one committed and one refused install per
//!   engine kind.
//! - [`calvin_fold_tests`]: a Calvin record's fold rows journal as parts,
//!   and restart replay applies them.
//! - [`calvin_install_tests`]: a Calvin slice installs from its stamped
//!   redo entry.
//! - [`cross_shard_fold_tests`]: a record never folds a cross-shard sum
//!   target on the source's core.
//! - [`unique_handover_tests`]: UNIQUE judged on a record's post-state.

#[cfg(test)]
pub(in crate::data::executor) mod calvin_fold_tests;
mod calvin_install;
#[cfg(test)]
mod calvin_install_tests;
#[cfg(test)]
mod cross_shard_fold_tests;
mod document;
mod entry;
mod events;
#[cfg(test)]
mod install_refusal_tests;
mod passes;
mod settle;
mod state;
mod sub_ops;
#[cfg(test)]
pub(in crate::data::executor) mod test_commit;
#[cfg(test)]
mod unique_handover_tests;
mod validate;

pub(in crate::data::executor) use document::CommittedDocWrite;
pub(in crate::data::executor) use entry::CommittedRedo;
pub(in crate::data::executor) use state::{RedoApplyPass, RedoApplyState, TsInstalled};
