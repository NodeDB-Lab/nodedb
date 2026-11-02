// SPDX-License-Identifier: BUSL-1.1

mod batch_put;
mod clock;
mod counter_refit;
pub mod engine;
pub mod engine_atomic;
mod engine_helpers;
mod engine_index;
pub mod engine_sorted;
mod engine_stats;
mod engine_write;
pub mod entry;
pub mod expiry_wheel;
mod hash_helpers;
pub mod hash_table;
pub mod history;
pub mod index;
pub mod scan;
pub mod slab;
pub mod sorted_index;
#[cfg(test)]
pub(crate) mod test_support;

pub use batch_put::KvBatchPutParams;
pub use clock::current_ms;
pub use counter_refit::fitted_counter_f64;
pub use engine::{
    DueExpiry, KvEngine, KvEntryImage, KvHistoryOverlap, KvKeyRef, KvVersionRow, KvVersionedScan,
    KvWriteScope, KvWriteScopeGuard, RestoreCompositeIndexParams, RestoreFieldIndexParams,
};
pub use engine_atomic::{
    AtomicError, AtomicImageGate, AtomicKeyCtx, CasResult, GetSetResult, IncrStep, Incremented,
    admit_any,
};
pub use engine_index::RegisterIndexParams;
pub use engine_sorted::SortedIndexRangeParams;
pub use engine_stats::{ExpiredKey, KvStats};
pub use engine_write::{KvPutParams, KvRewriteParams, UnboundKvWrite};
pub use scan::KvScanParams;
