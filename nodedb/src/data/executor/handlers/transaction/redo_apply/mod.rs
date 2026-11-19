// SPDX-License-Identifier: BUSL-1.1

//! Committed-redo apply: install one committed transaction's redo record on
//! the core that owns its vShard, through the WAL replay arms.
//!
//! - [`entry`]: the `MetaOp::ApplyTransactionRedo` handler.
//! - [`calvin_install`]: the install of a committed Calvin slice.
//! - [`validate`]: commit-boundary checks that run before any write. The
//!   Calvin stage runs the same checks on its staged images.
//! - [`passes`]: the validate and install passes over the replay arms.
//! - [`whole_validate`]: every check a whole record's install runs before
//!   its first write.
//! - [`boot_judge`]: restart replay judges a committed record whose install
//!   has no outcome, at its place in the stream.
//! - [`settle`]: the work an install defers until every sub-record landed.
//! - [`streamed`]: reads of a stream marker's content back from the WAL.
//! - [`batched`]: the install of a stream marker's content, batch by
//!   batch, which a task `run_task` runs parks between its batches.
//! - [`stream_validate`]: the validate of a stream marker's content, run
//!   batch by batch over the stream.
//! - [`document`]: document rows written while the apply scope is open, or
//!   while restart replay folds a record whose install never became durable.
//! - [`replay_fold`]: restart replay's fold of such a record.
//! - [`fold_ledger`]: the ops and target rows a batched install folded
//!   durably.
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
//! - [`retention_instant_tests`]: retention judged at the entry's instant.

mod batched;
mod boot_judge;
#[cfg(test)]
pub(in crate::data::executor) mod calvin_fold_tests;
mod calvin_install;
#[cfg(test)]
mod calvin_install_tests;
mod clone_copy;
#[cfg(test)]
mod cross_shard_fold_tests;
mod document;
mod entry;
mod events;
mod fold_ledger;
mod folded_store;
#[cfg(test)]
mod install_refusal_tests;
mod installed_document;
mod passes;
mod replay_fold;
#[cfg(test)]
mod retention_instant_tests;
mod settle;
mod snapshot_check;
mod state;
mod stream_validate;
mod streamed;
mod sub_ops;
mod sum_fold_check;
#[cfg(test)]
mod sync_refusal_tests;
#[cfg(test)]
pub(in crate::data::executor) mod test_commit;
#[cfg(test)]
mod unique_handover_tests;
mod validate;
mod whole_validate;
mod write_keys;

pub(in crate::data::executor) use batched::is_park_placeholder;
pub(in crate::data::executor) use document::CommittedDocWrite;
pub(in crate::data::executor) use entry::{CommittedRedo, RedoSnapshot};
pub(in crate::data::executor) use state::{
    RedoApplyPass, RedoApplyScope, RedoApplyState, TsInstalled,
};
pub(in crate::data::executor) use sub_ops::{RedoDocOp, RedoKvOp, document_ops, kv_ops};
pub(in crate::data::executor) use validate::CommitBoundary;
