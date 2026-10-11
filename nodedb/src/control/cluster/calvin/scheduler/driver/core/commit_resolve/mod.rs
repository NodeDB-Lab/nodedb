// SPDX-License-Identifier: BUSL-1.1

//! Verdict-driven commit resolution for a staged Calvin transaction on the
//! data-group leader, and verdict handling for a follower's held txn.
//!
//! A static or active dispatch STAGES its transaction on the Data Plane
//! (validate the read-set + buffer the plans, no base mutation). Its executor
//! response carries the local commit vote on `stage_vote`. The leader votes,
//! waits for the global verdict, then resolves a committed write slice into
//! its redo and proposes it (see `super::commit_redo`), or drops the staged
//! state of a slice that ends with no log entry.

mod drop_tail;
mod verdict;
mod vote;
