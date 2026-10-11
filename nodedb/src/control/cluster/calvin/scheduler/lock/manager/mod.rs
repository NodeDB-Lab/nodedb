// SPDX-License-Identifier: BUSL-1.1

//! Deterministic lock manager for the Calvin scheduler.
//!
//! # Design
//!
//! The lock manager provides a deterministic, totally-ordered lock table over
//! per-key entries keyed by
//! [`LockKey`](crate::control::cluster::calvin::scheduler::lock::LockKey).
//! Locks come in three modes (see
//! [`LockMode`](crate::control::cluster::calvin::scheduler::lock::LockMode)):
//! `Exclusive` (one holder), `Shared` (many readers), and `Intent` (many row
//! writers of one collection). A transaction acquires one map of key to mode.
//! Read reservations take one `Shared` key via [`LockManager::acquire_shared`].
//!
//! # Determinism
//!
//! `BTreeMap` is used throughout (not `HashMap`) so that iteration order is
//! deterministic and reproducible across replicas.  This is a correctness
//! requirement, not a style preference.
//!
//! Split by concern:
//! - [`types`]: the lock table struct and its internal decision enum.
//! - [`classify`]: per-key request classification and grants.
//! - [`acquire`]: moded lock acquisition and FIFO waiter queueing.
//! - [`wound_wait`]: shared-lock reservations and wound-wait conflict
//!   resolution.
//! - [`release`]: lock release and mode-aware FIFO waiter promotion.
//! - [`try_acquire`]: the non-blocking fast path.
//! - [`introspection`]: readiness checks, request contention and test
//!   counters.

mod acquire;
mod classify;
mod introspection;
mod release;
mod try_acquire;
mod types;
mod wound_wait;

pub use introspection::KeyContention;
pub use types::LockManager;
