//! KvEngine struct definition, construction, and memory-budget checks.

use std::collections::HashMap;

use crate::engine::kv::expiry_wheel::ExpiryWheel;
use crate::engine::kv::hash_table::KvHashTable;
use crate::engine::kv::index::KvIndexSet;

/// Result of a KV SCAN operation: `(entries, next_cursor_bytes)`.
///
/// Each entry is `(key_bytes, value_bytes)`. `next_cursor` is empty
/// when the scan is complete, otherwise an opaque cursor for continuation.
pub type ScanResult = (Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>);

/// Per-core KV engine.
///
/// Owns a hash table per collection and a shared expiry wheel.
/// Dispatched from the Data Plane executor via `PhysicalPlan::Kv(KvOp)`.
pub struct KvEngine {
    /// Per-collection hash tables. Key: hash of "{database_id}:{tenant_id}:{collection}".
    pub(crate) tables: HashMap<u64, KvHashTable>,
    /// Per-collection secondary index sets. Key: hash of "{database_id}:{tenant_id}:{collection}".
    pub(crate) indexes: HashMap<u64, KvIndexSet>,
    /// Reverse mapping: hash → tenant_id. Enables tenant purge without
    /// reversing the FxHash. Maintained in sync with `tables`.
    pub(crate) hash_to_tenant: HashMap<u64, u64>,
    /// Reverse mapping: hash → collection name. Enables snapshot export
    /// to include human-readable collection names (FxHash is not reversible).
    pub(crate) hash_to_collection: HashMap<u64, String>,
    /// Shared expiry wheel across all collections on this core.
    pub(in crate::engine::kv) expiry: ExpiryWheel,
    /// Default tuning parameters for new collections.
    pub(in crate::engine::kv) default_capacity: usize,
    pub(in crate::engine::kv) load_factor_threshold: f32,
    pub(in crate::engine::kv) rehash_batch_size: usize,
    pub(in crate::engine::kv) inline_threshold: usize,
    /// Memory budget in bytes (0 = unlimited). When total_mem_usage() exceeds
    /// this, new PUTs are rejected with a retriable error.
    memory_budget_bytes: usize,
    /// Sorted index manager: order-statistic trees for leaderboard-style queries.
    pub(in crate::engine::kv) sorted_indexes: crate::engine::kv::sorted_index::SortedIndexManager,
    /// Per-table write epoch, bumped on every row mutation. Key: same
    /// `table_key` hash as `tables`. Read by the Data Plane aggregate result
    /// cache (`write_epoch`) to detect a KV write since a cached result was
    /// computed — see `engine/write_epoch.rs` for the single bump chokepoint.
    pub(in crate::engine::kv) write_epochs: HashMap<u64, u64>,
}

impl KvEngine {
    /// Create a new KV engine with the given tuning parameters.
    pub fn new(
        now_ms: u64,
        default_capacity: usize,
        load_factor_threshold: f32,
        rehash_batch_size: usize,
        inline_threshold: usize,
        expiry_tick_ms: u64,
        expiry_reap_budget: usize,
    ) -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
            hash_to_tenant: HashMap::new(),
            hash_to_collection: HashMap::new(),
            expiry: ExpiryWheel::new(now_ms, expiry_tick_ms, expiry_reap_budget),
            default_capacity,
            load_factor_threshold,
            rehash_batch_size,
            inline_threshold,
            memory_budget_bytes: 0, // 0 = unlimited (set via set_memory_budget).
            sorted_indexes: crate::engine::kv::sorted_index::SortedIndexManager::new(),
            write_epochs: HashMap::new(),
        }
    }

    /// Create a KV engine from `KvTuning` config.
    pub fn from_tuning(now_ms: u64, tuning: &nodedb_types::config::tuning::KvTuning) -> Self {
        Self::new(
            now_ms,
            tuning.default_capacity,
            tuning.rehash_load_factor,
            tuning.rehash_batch_size,
            tuning.default_inline_threshold,
            tuning.expiry_tick_ms,
            tuning.expiry_reap_budget,
        )
    }

    /// Set the memory budget in bytes. 0 = unlimited.
    pub fn set_memory_budget(&mut self, budget_bytes: usize) {
        self.memory_budget_bytes = budget_bytes;
    }

    /// Check if the memory budget is exceeded.
    ///
    /// Returns `true` if the budget is set and current usage exceeds it.
    /// Used by PUT handlers to reject new writes with a retriable error.
    pub fn is_over_budget(&self) -> bool {
        self.memory_budget_bytes > 0 && self.total_mem_usage() > self.memory_budget_bytes
    }
}
