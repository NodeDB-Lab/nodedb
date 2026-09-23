// SPDX-License-Identifier: BUSL-1.1

//! Deterministic lock manager for the Calvin scheduler.
//!
//! # Design
//!
//! The lock manager provides a deterministic, totally-ordered lock table over
//! per-key entries keyed by
//! [`LockKey`](crate::control::cluster::calvin::scheduler::lock::LockKey).
//! Locks come in two modes: `Exclusive` (one holder, excludes all others) and
//! `Shared` (many compatible holders). The Calvin batch acquire path takes
//! every key in a transaction's `read_set ∪ write_set` as an `Exclusive`
//! lock; single-key `Shared` locks are available via
//! [`LockManager::acquire_shared`].
//!
//! # Determinism
//!
//! `BTreeMap` is used throughout (not `HashMap`) so that iteration order is
//! deterministic and reproducible across replicas.  This is a correctness
//! requirement, not a style preference.
//!
//! Split by concern:
//! - [`types`]: the lock table struct and its internal decision enums.
//! - [`acquire`]: exclusive lock acquisition and waiter queueing.
//! - [`wound_wait`]: shared-lock reservations and wound-wait conflict
//!   resolution.
//! - [`release`]: lock release and FIFO/shared waiter promotion.
//! - [`try_acquire`]: the non-blocking exclusive fast path.
//! - [`introspection`]: readiness checks and test counters.

mod acquire;
mod introspection;
mod release;
mod try_acquire;
mod types;
mod wound_wait;

pub use types::LockManager;
