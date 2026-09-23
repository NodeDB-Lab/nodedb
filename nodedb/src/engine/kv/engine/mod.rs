// SPDX-License-Identifier: BUSL-1.1

//! KvEngine: per-core KV engine owning hash tables and expiry wheel.
//!
//! `!Send` — owned by a single TPC core. Each collection gets its own
//! hash table; the expiry wheel is shared across all collections on
//! this core (one wheel tick processes all collections).

mod batch_write;
mod checkpoint_export;
mod checkpoint_restore;
mod purge;
mod reads;
mod scan_ops;
mod state;
mod write_epoch;

pub use checkpoint_export::KvCollectionRef;
pub use checkpoint_restore::{RestoreCompositeIndexParams, RestoreFieldIndexParams};
pub use reads::{KvEntryImage, KvKeyRef};
pub use state::{KvEngine, ScanResult};
