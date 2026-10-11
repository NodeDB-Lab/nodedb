// SPDX-License-Identifier: BUSL-1.1

//! Calvin deterministic executor handlers.
//!
//! Handler entry points (`CoreLoop` = [`crate::data::executor::core_loop::CoreLoop`]):
//!
//! - `CoreLoop::execute_calvin_execute_static`: static-set multi-shard txn
//!   (the common case). It VALIDATES the read-set to compute the local commit
//!   vote and STAGES the transaction's plans into the synthetic overlay and
//!   the commit-pending buffer WITHOUT mutating base or firing side effects,
//!   then returns the vote. The data group's apply of the slice's stamped
//!   redo entry later installs it and consumes the staged state, or
//!   `CoreLoop::execute_calvin_drop` discards it.
//!
//! - `CoreLoop::execute_calvin_execute_passive`: passive participant for a
//!   dependent-read txn. Reads each declared key from base storage and
//!   returns a zerompk-encoded `Vec<(PassiveReadKeyId, Value)>` payload with
//!   a commit vote. The Control Plane scheduler proposes the payload as a
//!   `CalvinReadResult` entry to the data group of every active vShard.
//!
//! - `CoreLoop::execute_calvin_execute_active`: active participant for a
//!   dependent-read txn. Stages the physical plans, with the injected read
//!   values already resolved, the way the static path does. Performs an OLLP
//!   verification hook first: if the active participant detects that the
//!   declared predicate no longer matches the current engine state, it
//!   returns `OllpRetryRequired` and stages nothing. The OLLP orchestrator on
//!   the Control Plane retries via `Inbox::submit`.
//!
//! The data-group leader's scheduler proposes the slice's stamped redo entry
//! to the data group. The Data Plane writes no WAL record for a slice.

mod active_passive;
mod discard;
mod shared;
mod static_stage;
#[cfg(test)]
mod test_commit;

pub(in crate::data::executor) use shared::CalvinExecCtx;
#[cfg(test)]
pub(in crate::data::executor) use shared::test_support;
