// SPDX-License-Identifier: BUSL-1.1

//! Per-engine collection reclaim handlers.
//!
//! Each file in this module unlinks the persistent on-disk surface
//! for one engine for a single `(tenant, collection)` pair. Called
//! from `execute_unregister_collection` after in-memory state has
//! been evicted but before the JSON summary is built, so the handler
//! picks up per-file byte counts for the `bytes_reclaimed` metric.
//!
//! The columnar file is reclaimed for every columnar-family collection
//! (`columnar`, `timeseries` on columnar, `spatial`): a `spatial` collection's
//! rows live in the columnar engine, and the columnar restore rebuilds its
//! R-tree from them.
//!
//! Engines whose persistent state is shared-redb (document,
//! document-strict, FTS, graph edges) or in-memory only (the KV hash
//! index) are documented inline in the parent handler — no separate
//! file unlinks are required. The modules here cover the engines that
//! write per-collection checkpoint or partition files under
//! `{data_dir}/...`.

pub mod columnar;
pub mod crdt;
mod error;
pub mod sparse_vector;
pub mod spatial;
mod stats;
pub mod timeseries;
pub mod vector;

pub use error::{ReclaimError, Result};
pub use stats::ReclaimStats;
