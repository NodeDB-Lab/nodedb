// SPDX-License-Identifier: BUSL-1.1

//! CP-side helper that turns a `(collection, pk_bytes)` into a stable
//! `Surrogate`, allocating from the registry on the first call and
//! returning the persisted value on every subsequent call (UPSERT
//! preserves the surrogate).
//!
//! Cross-cutting flush trigger: every successful allocation runs the
//! registry's `should_flush()` check; if true, we persist the new
//! high-watermark to both the catalog row (`_system.surrogate_hwm`)
//! and the WAL (`SurrogateAlloc` record) before returning. The two
//! writes form one logical checkpoint — if either fails we surface
//! the error to the caller rather than silently letting the registry
//! advance past a non-durable hwm.
//!
//! The cross-node HiLo reservation path (multi-node batch reservation +
//! the background refill loop that keeps the blocking metadata-Raft
//! round-trip OFF this hot path) lives in the sibling
//! [`super::cluster_reserve`] module.

mod assign_ops;
mod flush;
mod types;

pub use types::{SurrogateAssigner, SurrogateRegistryHandle};
