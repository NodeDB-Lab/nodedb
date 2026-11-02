// SPDX-License-Identifier: BUSL-1.1

//! KvEngine: per-core KV engine owning hash tables and expiry wheel.
//!
//! `!Send` — owned by a single TPC core. Each collection gets its own
//! hash table; the expiry wheel is shared across all collections on
//! this core (one wheel tick processes all collections).

mod batch_write;
mod checkpoint_export;
mod checkpoint_restore;
mod expiry_due;
mod history_merge;
mod memory;
mod newest;
mod purge;
mod reads;
mod scan_ops;
mod state;
mod undo;
mod versioned_reads;
mod versioned_restore;
mod write_epoch;
mod write_scope;

pub use checkpoint_export::KvCollectionRef;
pub use checkpoint_restore::{RestoreCompositeIndexParams, RestoreFieldIndexParams};
pub use expiry_due::DueExpiry;
pub use history_merge::KvHistoryOverlap;
pub use newest::KvCollectionNewest;
pub use reads::{KvEntryImage, KvKeyRef};
pub use state::{KvEngine, ScanResult};
pub use versioned_reads::{KvVersionRow, KvVersionedScan};
pub use write_scope::{KvWriteScope, KvWriteScopeGuard};
