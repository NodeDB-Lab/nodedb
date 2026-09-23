// SPDX-License-Identifier: BUSL-1.1

//! Calvin scheduler driver core.
//!
//! One [`Scheduler`] task runs per vshard hosted on this node. It receives
//! [`SequencedTxn`]s from the sequencer, acquires deterministic locks,
//! dispatches static / dependent-read transactions to the Data Plane,
//! waits for executor responses, and writes `CalvinApplied` WAL records.
//!
//! Sub-modules (one concern per file):
//!
//! - [`scheduler`] — `Scheduler` struct, ctor, run loop.
//! - [`completion_route`] — routes each executor response (disconnect, OLLP
//!   mismatch, staged commit-resolution state, or direct apply) to its handler.
//! - [`process`] — new-txn processing, dependent-read barrier setup,
//!   txn-completion bookkeeping.
//! - [`catch_up`] — sequencer-fan-out catch-up drain: replays inputs dropped on
//!   this replica (channel Full/Closed) from the committed sequencer Raft log.
//! - [`dispatch`] — static / active dispatch to the Data Plane executor.
//! - [`deferred`] — capacity-safe dispatch: parks a request the bridge refuses
//!   at capacity and re-sends it once capacity frees.
//! - [`routing`] — exhaustive `PhysicalPlan` → vshard routing oracle used by
//!   `dispatch`'s local-plan filtering.
//! - [`commit_resolve`] — verdict-driven flush-or-drop of a staged static
//!   transaction, plus the shared commit tail.
//! - [`staged_vote`] — derives a participant's local commit vote from its
//!   staged executor response, keeping the two abort causes apart.
//! - [`commit_redo`] — resolves a committed staged transaction's post-images
//!   into a replayable `TransactionRedo` WAL record ahead of the flush.
//! - [`read_result`] — `CalvinReadResult` handling and barrier timeouts.
//! - [`propose`] — propose `CalvinReadResult` Raft entries.
//! - [`request`] — shared `Request` construction for already-sequenced Calvin
//!   sub-operations.
//! - [`write_version_record`] — post-apply write-version recording for
//!   committed Calvin transactions (at the CalvinApplied WAL LSN).
//!
//! # Determinism
//!
//! All bookkeeping uses `BTreeMap`/`BTreeSet` — never `HashMap`/`HashSet`.
//! Dispatch order is `(epoch, position)` order.
//!
//! # Timing / `Instant::now()`
//!
//! `Instant::now()` is used for:
//! - Lock-wait latency metrics (observability only).
//! - Dependent-read barrier `timeout_at` (off-WAL path only).
//!
//! Never used for WAL-influencing values.

pub mod catch_up;
pub mod commit_redo;
pub mod commit_resolution_dispatch;
pub mod commit_resolve;
pub mod completion_route;
pub mod deferred;
pub mod dispatch;
pub mod process;
pub mod propose;
pub mod read_result;
pub mod request;
pub mod routing;
pub mod scheduler;
pub mod staged_vote;
#[cfg(test)]
mod test_support;
pub mod write_version_record;

pub use propose::{CalvinReadResultProposal, propose_calvin_read_result};
pub use scheduler::{Scheduler, SchedulerParams};
