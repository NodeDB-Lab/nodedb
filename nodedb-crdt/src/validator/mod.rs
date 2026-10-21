// SPDX-License-Identifier: Apache-2.0

//! Constraint validator — checks deltas against committed state.
//!
//! This is the core of the CRDT/SQL bridge. When a delta arrives from a peer,
//! the validator checks all applicable constraints against the leader's
//! committed state. If any constraint is violated, the delta is rejected
//! with a compensation hint.

pub mod bitemporal;
mod core;
pub mod incoming;
mod policy_dispatch;
mod types;
mod validate;

pub use core::Validator;
pub use incoming::IncomingDelta;
pub use types::{ProposedChange, ValidationOutcome, Violation};
