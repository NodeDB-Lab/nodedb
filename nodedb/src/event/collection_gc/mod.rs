// SPDX-License-Identifier: BUSL-1.1

//! Event-Plane sweeper that hard-deletes soft-deleted collections once
//! their retention window has elapsed.
//!
//! Runs on the Tokio runtime. Scans `_system.collections` on each eval
//! tick, filters rows with `is_active = false` whose
//! `deactivated_at_ns + retention_window < now()`, and proposes
//! `CatalogEntry::PurgeCollection` for each. Never touches Data Plane
//! directly — proposes through the normal metadata-raft path, which
//! fans out through the existing apply + post_apply pipeline
//! (including the per-node Data Plane reclaim dispatch). Each tick also
//! proposes the retention prune of the collection purge log.

pub mod l2_cleanup;
pub mod pending_reclaim;
pub mod policy;
mod purge_log_prune;
pub mod sweeper;

pub use l2_cleanup::{L2CleanupWorker, spawn_l2_cleanup};
pub use pending_reclaim::{PendingReclaimWorker, spawn_pending_reclaim};
pub use policy::{PurgeDecision, resolve_retention};
pub use sweeper::{CollectionGcSweeper, spawn_collection_gc};
