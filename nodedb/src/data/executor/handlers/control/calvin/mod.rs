// SPDX-License-Identifier: BUSL-1.1

//! Calvin deterministic executor handlers.
//!
//! Handler entry points (`CoreLoop` = [`crate::data::executor::core_loop::CoreLoop`]):
//!
//! - `CoreLoop::execute_calvin_execute_static`: static-set multi-shard txn
//!   (the common case). It VALIDATES the read-set to compute the local commit
//!   vote and STAGES the transaction's plans into the synthetic overlay and
//!   the commit-pending buffer WITHOUT mutating base or firing side effects,
//!   then returns the vote. `CoreLoop::execute_calvin_flush` later installs
//!   the transaction's committed redo record, or
//!   `CoreLoop::execute_calvin_drop` discards the staged state.
//!
//! - `CoreLoop::execute_calvin_execute_passive`: passive participant for a
//!   dependent-read txn. Reads each declared key from the local engine and
//!   returns a msgpack-encoded `Vec<(PassiveReadKeyId, Value)>` payload. The
//!   Control Plane scheduler proposes a `CalvinReadResult` Raft entry after
//!   receiving this response.
//!
//! - `CoreLoop::execute_calvin_execute_active`: active participant for a
//!   dependent-read txn. Stages the physical plans, with the injected read
//!   values already resolved, the way the static path does. Performs an OLLP
//!   verification hook first: if the active participant detects that the
//!   declared predicate no longer matches the current engine state, it
//!   returns `OllpRetryRequired` and stages nothing. The OLLP orchestrator on
//!   the Control Plane retries via `Inbox::submit`.
//!
//! The scheduler appends the transaction's `TransactionRedo` record, or a
//! `CalvinApplied` marker when the transaction wrote nothing here, on the
//! Control Plane. The Data Plane writes no WAL record.

mod active_passive;
mod discard;
mod flush;
mod shared;
mod static_stage;
#[cfg(test)]
mod test_commit;

pub(in crate::data::executor) use flush::CalvinFlushRedo;
pub(in crate::data::executor) use shared::CalvinExecCtx;
