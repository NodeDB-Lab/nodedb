// SPDX-License-Identifier: BUSL-1.1

//! Per-core write-version index.
//!
//! Records, for every committed write applied on this Data-Plane core, the
//! version of the write against the written key and against the written
//! collection, on the write's vShard. A write that applies a data-group Raft
//! entry takes the entry's log position as its version, so every replica of
//! the vShard records the same version for it. A write that applies no entry
//! takes its WAL LSN on top of the vShard's latest version.
//!
//! This is the shard-local write-version substrate the optimistic-concurrency
//! commit path validates a transaction's read-set against (see
//! `CoreLoop::read_set_still_current`). Because the index lives on the
//! `!Send` core it is a plain `HashMap`: no atomics, no locks, no cross-core
//! sharing.
//!
//! The per-key map is bounded: horizon GC (run from the periodic maintenance
//! hook) evicts entries far below their vShard's latest version and enforces
//! a hard entry-count backstop. The per-collection map is bounded by the
//! number of live collections and is never GC'd.

pub mod index;
pub mod keys;
mod record;
pub mod record_stamps;
#[cfg(test)]
pub mod tests;
mod validate;

pub use index::{RETAIN_WINDOW, WriteVersionIndex};
#[cfg(test)]
pub use keys::{CollKey, WriteKey};
pub use keys::{KeyRepr, WriteStamp};
pub use record_stamps::RecordStamps;
