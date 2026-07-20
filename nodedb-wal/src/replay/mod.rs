// SPDX-License-Identifier: Apache-2.0

//! Replay-time utilities layered over raw [`WalRecord`] streams.
//!
//! The WAL crate does not know the payload shapes of domain writes (those
//! live in `nodedb` / engine crates), so filtering is split: this module
//! owns the tombstone primitive, consumers query it after decoding the
//! collection field from their own payload format.

pub mod filter;

pub use filter::{DatabaseTombstones, TombstoneSet, extract_tombstones};
