// SPDX-License-Identifier: BUSL-1.1

//! Raft single-group state machine.
//!
//! Split across files:
//! - [`config`]: `RaftConfig` (including voter/learner lists).
//! - [`core`]: `RaftNode` struct, constructors, simple accessors, `tick`,
//!   `propose`, and the `Ready` output type.
//! - [`durability`]: Applied-index durability floor and log compaction.
//! - [`election`]: The pre-vote and vote rounds a replica starts, and the
//!   guard that keeps a replica whose data diverged out of both.
//! - [`internal`]: Internal state transitions (role changes, replication,
//!   commit advancement) and timeout math.
//! - [`leader_lease`]: Serving a linearizable read from the leader's commit
//!   index without a quorum round, and the vote refusal that makes it safe.
//! - [`membership`]: Dynamic configuration changes — add/remove voters,
//!   add/remove/promote learners.
//! - [`ordinal_floor`]: The group's system-time floor, the highest read cut
//!   any leader closed, held durably in the hard state.
//! - [`quorum_contact`]: Check-quorum — tracking when a majority last
//!   answered the leader, and stepping down when none has.
//! - [`read_index`]: Confirming leadership against a quorum before serving a
//!   linearizable read.
//! - [`rpc`]: Incoming RPC handlers (`AppendEntries`, `PreVote`,
//!   `RequestVote`, `InstallSnapshot`, `TimeoutNow`, and their response
//!   handlers).
//! - [`snapshot_debt`]: A replica awaiting a snapshot refuses entries and
//!   never leads.
//! - [`staleness`]: How far behind the leader a replica is, for
//!   bounded-staleness reads.

pub mod config;
pub mod core;
pub mod durability;
mod election;
mod internal;
pub mod leader_lease;
pub mod membership;
pub mod ordinal_floor;
pub mod peer_contact;
pub mod quorum_contact;
pub mod read_index;
pub mod rpc;
pub mod snapshot_debt;
pub mod staleness;

pub use self::config::RaftConfig;
pub use self::core::{RaftNode, Ready};
pub use self::read_index::{ReadIndexProbe, ReadIndexStatus};
pub use self::staleness::StalenessVerdict;
