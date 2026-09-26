// SPDX-License-Identifier: BUSL-1.1

//! Calvin scheduler driver core.
//!
//! One [`Scheduler`] task runs per vshard hosted on this node. It receives
//! sequenced txns from the sequencer, acquires deterministic locks, dispatches
//! static / dependent-read transactions to the Data Plane, waits for executor
//! responses, and writes `CalvinApplied` WAL records. Each sub-module owns one
//! concern; see that file's own doc comment for what it does.
//!
//! All bookkeeping uses `BTreeMap`/`BTreeSet` — never `HashMap`/`HashSet` —
//! and dispatch order is `(epoch, position)` order. `Instant::now()` is used
//! only for lock-wait latency metrics and the dependent-read barrier
//! `timeout_at`, both off the WAL-influencing path; every call site carries a
//! `// no-determinism:` marker.

pub mod catch_up;
pub mod commit_redo;
pub mod commit_resolution_dispatch;
pub mod commit_resolve;
pub mod completion_route;
mod cut_marker;
pub mod deferred;
pub mod dispatch;
pub mod halt;
pub mod intake;
pub mod owed;
pub mod process;
pub mod propose;
pub mod read_result;
mod redo_window;
pub mod request;
pub mod routing;
pub mod scheduler;
pub mod sequencer_proposer;
pub mod staged_vote;
#[cfg(test)]
mod test_proposer;
#[cfg(test)]
mod test_support;
pub mod write_version_record;

pub use propose::{CalvinReadResultProposal, propose_calvin_read_result};
pub use scheduler::{Scheduler, SchedulerParams};
pub use sequencer_proposer::{RaftSequencerProposer, SequencerProposer};
