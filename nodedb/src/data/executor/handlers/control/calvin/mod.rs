// SPDX-License-Identifier: BUSL-1.1

//! Calvin deterministic executor handlers.
//!
//! Handler entry points (`CoreLoop` = [`crate::data::executor::core_loop::CoreLoop`]):
//!
//! - `CoreLoop::execute_calvin_execute_static`: static-set multi-shard txn
//!   (the common case). It VALIDATES the read-set to compute the local commit
//!   vote and STAGES the transaction's plans into the commit-pending buffer
//!   WITHOUT mutating base or firing side effects, then returns the vote.
//!   `CoreLoop::execute_calvin_flush` later replays the staged plans through
//!   the durable apply funnel, or `CoreLoop::execute_calvin_drop` discards
//!   them.
//!
//! - `CoreLoop::execute_calvin_execute_passive`: passive participant for a
//!   dependent-read txn. Reads each declared key from the local engine and
//!   returns a msgpack-encoded `Vec<(PassiveReadKeyId, Value)>` payload. The
//!   Control Plane scheduler proposes a `CalvinReadResult` Raft entry after
//!   receiving this response.
//!
//! - `CoreLoop::execute_calvin_execute_active`: active participant for a
//!   dependent-read txn. Executes the physical plans with the injected read
//!   values already resolved. Performs an OLLP verification hook: if the
//!   active participant detects that the declared predicate no longer matches
//!   the current engine state, it returns `OllpRetryRequired` WITHOUT writing.
//!   The OLLP orchestrator on the Control Plane retries via `Inbox::submit`.
//!
//! The `CalvinApplied` WAL record is written on the Control Plane side (in the
//! scheduler's response path) after a successful response is received through
//! the SPSC bridge; not here in the Data Plane.

mod active_passive;
mod discard;
mod flush;
mod shared;
mod static_stage;

pub(in crate::data::executor) use shared::CalvinExecCtx;
