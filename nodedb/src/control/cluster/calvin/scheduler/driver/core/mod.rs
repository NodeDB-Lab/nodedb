// SPDX-License-Identifier: BUSL-1.1

//! Calvin scheduler driver core.
//!
//! One [`Scheduler`] task runs per vshard hosted on this node. It receives
//! sequenced txns from the sequencer and acquires deterministic locks on
//! every replica. The vShard's data-group leader stages each granted txn on
//! the Data Plane, votes, and proposes a committed slice's stamped redo to
//! the data group. Every replica installs that redo from the log, and the
//! apply loop reports each install through the scheduler's inbox. Each
//! sub-module owns one concern; see that file's own doc comment for what it
//! does.
//!
//! All bookkeeping uses `BTreeMap`/`BTreeSet` — never `HashMap`/`HashSet` —
//! and dispatch order is `(epoch, position)` order. `Instant::now()` is used
//! only for lock-wait latency metrics, the instant a leader proposes a
//! dependent-read barrier's timeout entry, and the restage backoff, all off
//! the WAL-influencing path; every call site carries a `// no-determinism:`
//! marker.

mod apply_result;
pub mod catch_up;
pub mod commit_redo;
pub mod commit_resolve;
pub mod completion_route;
mod cut_marker;
pub mod deferred;
pub mod dispatch;
mod drop_dispatch;
pub mod halt;
mod install_gate;
pub mod intake;
mod metadata_hold;
pub mod owed;
pub mod parts;
mod parts_error;
pub mod parts_lane;
mod passive_read;
pub mod process;
pub mod propose;
pub mod read_result;
mod read_timeout;
pub mod redo_applied;
pub mod redo_propose;
pub mod request;
pub mod resolve_turn;
mod restage;
pub mod role;
pub mod routing;
pub mod run_loop;
pub mod scheduler;
pub mod sequencer_proposer;
pub mod staged_vote;
#[cfg(test)]
mod test_proposer;
#[cfg(test)]
mod test_support;

pub use propose::{CalvinReadResultProposal, propose_calvin_read_result};
pub use scheduler::{Scheduler, SchedulerParams};
pub use sequencer_proposer::{RaftSequencerProposer, SequencerProposer};
