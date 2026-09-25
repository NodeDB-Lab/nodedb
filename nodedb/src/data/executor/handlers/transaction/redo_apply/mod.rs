// SPDX-License-Identifier: BUSL-1.1

//! Committed-redo apply: install one committed transaction's redo record on
//! the core that owns its vShard, through the WAL replay arms.
//!
//! - [`entry`]: the `MetaOp::ApplyTransactionRedo` handler.
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
//! - [`calvin_fold_tests`]: a Calvin record folds the same live and in
//!   restart replay.

#[cfg(test)]
mod calvin_fold_tests;
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
mod validate;

pub(in crate::data::executor) use document::CommittedDocWrite;
pub(in crate::data::executor) use entry::CommittedRedo;
pub(in crate::data::executor) use state::{RedoApplyPass, RedoApplyState};
