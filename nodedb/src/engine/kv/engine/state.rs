//! KvEngine struct definition and construction.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_types::{DatabaseId, OrdinalClock};

use crate::engine::kv::expiry_wheel::ExpiryWheel;
use crate::engine::kv::hash_table::KvHashTable;
use crate::engine::kv::index::KvIndexSet;

use super::memory::KvMemory;
use super::write_scope::KvWriteScope;

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
    /// Reverse mapping: hash → database. The memory governor charges each
    /// table under this database. Maintained in sync with `hash_to_tenant`.
    pub(in crate::engine::kv) hash_to_database: HashMap<u64, DatabaseId>,
    /// Reverse mapping: hash → collection name. Enables snapshot export
    /// to include human-readable collection names (FxHash is not reversible).
    pub(crate) hash_to_collection: HashMap<u64, String>,
    /// Shared expiry wheel across all collections on this core.
    pub(in crate::engine::kv) expiry: ExpiryWheel,
    /// Expired entries the wheel handed out and no reap applied yet, as
    /// `(composite key, expire_at_ms)`. A core never reaps on its own: the
    /// data group's leader proposes each one, and every replica's apply
    /// removes it (`expiry_due`).
    pub(in crate::engine::kv) due: std::collections::BTreeSet<(Vec<u8>, u64)>,
    /// Default tuning parameters for new collections.
    pub(in crate::engine::kv) default_capacity: usize,
    pub(in crate::engine::kv) load_factor_threshold: f32,
    pub(in crate::engine::kv) rehash_batch_size: usize,
    pub(in crate::engine::kv) inline_threshold: usize,
    /// Governor charges for every table's resident bytes, history included.
    pub(in crate::engine::kv) memory: KvMemory,
    /// The system-time clock a write draws its ordinal from when no apply
    /// scope fixes one. The Data Plane core shares its clock here.
    pub(in crate::engine::kv) clock: Arc<OrdinalClock>,
    /// The apply scope the current write belongs to. See `write_scope.rs`.
    pub(in crate::engine::kv) write_scope: KvWriteScope,
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
            hash_to_database: HashMap::new(),
            hash_to_collection: HashMap::new(),
            expiry: ExpiryWheel::new(now_ms, expiry_tick_ms, expiry_reap_budget),
            due: std::collections::BTreeSet::new(),
            default_capacity,
            load_factor_threshold,
            rehash_batch_size,
            inline_threshold,
            memory: KvMemory::default(),
            clock: Arc::new(OrdinalClock::new()),
            write_scope: KvWriteScope::Closed,
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

    /// Record `tkey`'s database, tenant and collection name in the reverse
    /// maps. The first registration of a table key wins.
    pub(in crate::engine::kv) fn register_table_identity(
        &mut self,
        tkey: u64,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) {
        self.hash_to_database
            .entry(tkey)
            .or_insert(DatabaseId::new(database_id));
        self.hash_to_tenant.entry(tkey).or_insert(tenant_id);
        self.hash_to_collection
            .entry(tkey)
            .or_insert_with(|| collection.to_string());
    }
}
