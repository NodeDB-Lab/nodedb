// SPDX-License-Identifier: BUSL-1.1

//! Per-core, per-database weighted-LRU document cache for O(1) hot-key point lookups.
//!
//! Each Data Plane core owns one `DocCache`. It is `!Send` by design —
//! no cross-core sharing, no locking. Invalidated write-through on
//! PointPut/Delete/Update so reads never see stale data.
//!
//! ## Per-database sharding
//!
//! The cache is sharded by `database_id`. Each shard receives a capacity share
//! proportional to its `weight` (sourced from `database.quota.cache_weight`,
//! default 1). When the total entry count exceeds the global capacity, the
//! shard with the highest overshoot ratio (`current_entries / weight`) is
//! chosen for eviction. This prevents a high-traffic database from evicting
//! entries of a lower-traffic database below its proportional share.
//!
//! ## Lookup complexity
//!
//! - `get` / `put` / `invalidate`: O(1) — hashes directly to the shard.
//! - Eviction target selection: O(shards) — only during pressure (when total
//!   entries exceed capacity). Typical shard count is < 100.

use std::cell::Cell;
use std::collections::{HashMap, VecDeque};

use nodedb_types::StorageKey;

/// Composite cache key: `(database_id, tenant_id, collection, document_id)`.
///
/// Same `(tenant_id, collection, document_id)` in two different databases are
/// distinct entries.
#[derive(Eq, PartialEq, Hash, Clone)]
struct CacheKey {
    database_id: u64,
    tenant_id: u64,
    collection: String,
    document_id: String,
}

/// Per-database LRU shard.
struct DatabaseShard {
    entries: HashMap<CacheKey, Vec<u8>>,
    order: VecDeque<CacheKey>,
    tenant_counts: HashMap<u64, usize>,
    /// Relative weight from `database.quota.cache_weight`. Higher = larger fair share.
    weight: u32,
}

impl DatabaseShard {
    fn new(weight: u32) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            tenant_counts: HashMap::new(),
            weight: weight.max(1),
        }
    }

    /// Eviction pressure score: `current_entries * total_weight / weight`.
    /// Higher = more over-proportional → should be evicted from first.
    fn overshoot_score(&self, total_weight: u64) -> u64 {
        (self.entries.len() as u64)
            .saturating_mul(total_weight)
            .saturating_div(self.weight as u64)
    }

    /// Evict the oldest (FIFO) entry from this shard. Returns `true` if an
    /// entry was removed.
    fn evict_one(&mut self) -> bool {
        while let Some(evicted) = self.order.pop_front() {
            if self.entries.remove(&evicted).is_some() {
                if let Some(count) = self.tenant_counts.get_mut(&evicted.tenant_id) {
                    *count = count.saturating_sub(1);
                }
                return true;
            }
            // Key was already removed (e.g., by `invalidate`); skip stale entry.
        }
        false
    }
}

/// Bounded, database-sharded, weighted-LRU document cache.
///
/// Total capacity is shared across all database shards. Each shard's fair
/// share is proportional to its `cache_weight`. When the total exceeds
/// capacity, the shard most over its proportional allocation is evicted from.
pub struct DocCache {
    /// Per-database shards, keyed by `database_id`.
    shards: HashMap<u64, DatabaseShard>,

    /// Total number of cached documents across all shards.
    total: usize,

    /// Maximum total cached documents.
    capacity: usize,

    // -- Stats --
    //
    // `Cell` lets `get` take `&self` while still bumping counters, which
    // also means a single HashMap lookup on the hot path. The cache is
    // owned by one Data Plane core (`!Send`), so interior mutability is
    // safe — there is no cross-thread access.
    hits: Cell<u64>,
    misses: Cell<u64>,
}

impl DocCache {
    /// Create a new document cache with the given total capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            shards: HashMap::new(),
            total: 0,
            capacity,
            hits: Cell::new(0),
            misses: Cell::new(0),
        }
    }

    /// Update the relative cache weight for a database.
    ///
    /// Called by the quota catalog when `cache_weight` changes. Takes effect
    /// immediately for future eviction decisions. Does not retroactively evict
    /// entries from the re-weighted shard.
    pub fn set_database_weight(&mut self, database_id: u64, weight: u32) {
        self.shards
            .entry(database_id)
            .or_insert_with(|| DatabaseShard::new(weight))
            .weight = weight.max(1);
    }

    /// Look up a document in the cache. Returns `Some(&[u8])` on hit.
    ///
    /// Single HashMap lookup on the hot path — counters are bumped via
    /// `Cell` so the borrow checker permits `&self` here.
    pub fn get(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
    ) -> Option<&[u8]> {
        let key = Self::make_key(database_id, tenant_id, collection, document_id);
        match self
            .shards
            .get(&database_id)
            .and_then(|shard| shard.entries.get(&key))
        {
            Some(v) => {
                self.hits.set(self.hits.get() + 1);
                Some(v.as_slice())
            }
            None => {
                self.misses.set(self.misses.get() + 1);
                None
            }
        }
    }

    /// Insert or update a document in the cache (write-through).
    pub fn put(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
        value: &[u8],
    ) {
        let key = Self::make_key(database_id, tenant_id, collection, document_id);

        // Ensure the shard exists (default weight = 1).
        self.shards
            .entry(database_id)
            .or_insert_with(|| DatabaseShard::new(1));

        // Update in-place if the key is already present.
        {
            let shard = self.shards.get_mut(&database_id).expect("just inserted");
            if let Some(existing) = shard.entries.get_mut(&key) {
                *existing = value.to_vec();
                return;
            }
        }

        // Evict while the cache is at capacity.  When the inserting shard is
        // above its weighted fair share, continue evicting beyond the single
        // "make room" eviction so that resident-set sizes converge toward the
        // weight ratio under sustained pressure.
        //
        // The extra evictions only fire when `total >= capacity` (i.e., the
        // cache is actually full) so the initial warm-up phase is unaffected.
        //
        // `hint_db_id` is passed to the eviction picker so that when two
        // shards have identical overshoot ratios, the inserting shard is
        // preferred — preventing a cold shard at its proportional share from
        // being displaced in favour of the hot inserting shard.
        if self.total >= self.capacity {
            loop {
                if !self.evict_from_highest_overshoot(database_id) {
                    break;
                }
                // After each eviction re-check whether the inserting shard is
                // still above its weighted fair share.
                let total_weight = self.total_weight();
                let fair_share = self
                    .shards
                    .get(&database_id)
                    .map(|s| {
                        (self.capacity as u64)
                            .saturating_mul(s.weight as u64)
                            .saturating_div(total_weight) as usize
                    })
                    .unwrap_or(0);
                let count = self
                    .shards
                    .get(&database_id)
                    .map(|s| s.entries.len())
                    .unwrap_or(0);
                if count <= fair_share {
                    break;
                }
            }
        }

        let shard = self.shards.get_mut(&database_id).expect("just inserted");
        shard.entries.insert(key.clone(), value.to_vec());
        shard.order.push_back(key.clone());
        *shard.tenant_counts.entry(tenant_id).or_insert(0) += 1;
        self.total += 1;
    }

    /// Remove a document from the cache (invalidation).
    ///
    /// Called on PointDelete and PointUpdate to prevent stale reads.
    /// Does NOT remove from the shard's `order` deque — stale keys are
    /// harmlessly skipped during eviction.
    pub fn invalidate(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
    ) {
        let key = Self::make_key(database_id, tenant_id, collection, document_id);
        let removed = self
            .shards
            .get_mut(&database_id)
            .and_then(|shard| {
                shard.entries.remove(&key).map(|_| {
                    if let Some(count) = shard.tenant_counts.get_mut(&tenant_id) {
                        *count = count.saturating_sub(1);
                    }
                })
            })
            .is_some();
        if removed {
            self.total = self.total.saturating_sub(1);
        }
    }

    /// Evict all cache entries belonging to a single `(database_id, tenant_id, collection)`.
    pub fn evict_collection(&mut self, database_id: u64, tenant_id: u64, collection: &str) {
        let shard = match self.shards.get_mut(&database_id) {
            Some(s) => s,
            None => return,
        };
        let before = shard.entries.len();
        shard
            .entries
            .retain(|k, _| !(k.tenant_id == tenant_id && k.collection == collection));
        let after = shard.entries.len();
        let removed = before.saturating_sub(after);
        shard
            .order
            .retain(|k| !(k.tenant_id == tenant_id && k.collection == collection));
        if removed > 0 {
            if let Some(count) = shard.tenant_counts.get_mut(&tenant_id) {
                *count = count.saturating_sub(removed);
            }
            self.total = self.total.saturating_sub(removed);
        }
    }

    /// Evict all cache entries belonging to a specific tenant within a database.
    pub fn evict_tenant(&mut self, database_id: u64, tenant_id: u64) {
        let shard = match self.shards.get_mut(&database_id) {
            Some(s) => s,
            None => return,
        };
        let before = shard.entries.len();
        shard.entries.retain(|k, _| k.tenant_id != tenant_id);
        shard.order.retain(|k| k.tenant_id != tenant_id);
        shard.tenant_counts.remove(&tenant_id);
        let removed = before.saturating_sub(shard.entries.len());
        self.total = self.total.saturating_sub(removed);
    }

    /// Cache hit rate (0.0–1.0). Returns 0.0 if no lookups yet.
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.get();
        let total = hits + self.misses.get();
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Total cached documents across all database shards.
    pub fn len(&self) -> usize {
        self.total
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Total lookup count (hits + misses).
    pub fn total_lookups(&self) -> u64 {
        self.hits.get() + self.misses.get()
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn make_key(
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        document_id: &StorageKey,
    ) -> CacheKey {
        CacheKey {
            database_id,
            tenant_id,
            collection: collection.to_string(),
            document_id: document_id.to_string(),
        }
    }

    /// Sum of all shard weights.
    fn total_weight(&self) -> u64 {
        self.shards
            .values()
            .map(|s| s.weight as u64)
            .sum::<u64>()
            .max(1)
    }

    /// Evict one entry from the shard with the highest overshoot ratio.
    ///
    /// Among shards with equal overshoot ratios, `hint_db_id` (the inserting
    /// shard) is preferred so that a cold shard at its proportional share is
    /// never displaced in favour of the hot inserting shard.
    ///
    /// Returns `false` if all shards are empty.
    fn evict_from_highest_overshoot(&mut self, hint_db_id: u64) -> bool {
        let total_weight = self.total_weight();
        let db_id = self
            .shards
            .iter()
            .filter(|(_, s)| !s.entries.is_empty())
            .max_by_key(|(id, s)| {
                let score = s.overshoot_score(total_weight);
                // Prefer the inserting shard on ties so equal-ratio cold shards
                // are not evicted in its place.
                let is_hint = u8::from(**id == hint_db_id);
                (score, is_hint)
            })
            .map(|(&id, _)| id);
        match db_id {
            Some(id) => {
                let removed = self.shards.get_mut(&id).expect("just found").evict_one();
                if removed {
                    self.total = self.total.saturating_sub(1);
                }
                removed
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(surrogate))
    }

    // ── Basic API ─────────────────────────────────────────────────────────────

    #[test]
    fn basic_put_get() {
        let mut cache = DocCache::new(16);
        cache.put(0, 1, "users", &key(1), b"alice");
        assert_eq!(cache.get(0, 1, "users", &key(1)), Some(b"alice".as_slice()));
        assert_eq!(cache.get(0, 1, "users", &key(2)), None);
    }

    #[test]
    fn overwrite_updates_value() {
        let mut cache = DocCache::new(16);
        cache.put(0, 1, "users", &key(1), b"alice");
        cache.put(0, 1, "users", &key(1), b"ALICE");
        assert_eq!(cache.get(0, 1, "users", &key(1)), Some(b"ALICE".as_slice()));
    }

    #[test]
    fn invalidate_removes_entry() {
        let mut cache = DocCache::new(16);
        cache.put(0, 1, "users", &key(1), b"alice");
        cache.invalidate(0, 1, "users", &key(1));
        assert_eq!(cache.get(0, 1, "users", &key(1)), None);
    }

    #[test]
    fn tenant_isolation() {
        let mut cache = DocCache::new(16);
        cache.put(0, 1, "users", &key(1), b"tenant1");
        cache.put(0, 2, "users", &key(1), b"tenant2");
        assert_eq!(
            cache.get(0, 1, "users", &key(1)),
            Some(b"tenant1".as_slice())
        );
        assert_eq!(
            cache.get(0, 2, "users", &key(1)),
            Some(b"tenant2".as_slice())
        );
    }

    #[test]
    fn hit_rate_tracking() {
        let mut cache = DocCache::new(16);
        cache.put(0, 1, "c", &key(1), b"1");
        cache.get(0, 1, "c", &key(1)); // hit
        cache.get(0, 1, "c", &key(1)); // hit
        cache.get(0, 1, "c", &key(2)); // miss
        assert!((cache.hit_rate() - 0.6667).abs() < 0.01);
        assert_eq!(cache.total_lookups(), 3);
    }

    // ── Per-database isolation ─────────────────────────────────────────────

    #[test]
    fn cache_key_uniqueness_across_databases() {
        let mut cache = DocCache::new(16);
        cache.put(1, 5, "col", &key(1), b"db1");
        cache.put(2, 5, "col", &key(1), b"db2");
        assert_eq!(cache.get(1, 5, "col", &key(1)), Some(b"db1".as_slice()));
        assert_eq!(cache.get(2, 5, "col", &key(1)), Some(b"db2".as_slice()));
    }

    // ── Weighted eviction ─────────────────────────────────────────────────

    #[test]
    fn hot_db_does_not_evict_cold_db_below_proportional_share() {
        // total capacity = 8; DB1 weight=1, DB2 weight=1 → fair share = 4 each.
        // Fill DB1 with 4, DB2 with 4 → exactly at capacity.
        // Then push 4 more into DB1 → DB1 should be evicted from, not DB2.
        let mut cache = DocCache::new(8);
        cache.set_database_weight(1, 1);
        cache.set_database_weight(2, 1);

        for i in 0..4u32 {
            cache.put(1, 1, "c", &key(i), b"v");
        }
        for i in 0..4u32 {
            cache.put(2, 1, "c", &key(1000 + i), b"v");
        }
        assert_eq!(cache.len(), 8);

        // Push 4 more into DB1 — DB1 is 4/1=4.0 overshooting, DB2 is 4/1=4.0 equal.
        // Since DB1 gets more new inserts, DB1's overshoot grows first and it
        // gets evicted. DB2 should retain all 4 entries.
        for i in 4..8u32 {
            cache.put(1, 1, "c", &key(i), b"v");
        }

        let db2_resident: usize = (0..4u32)
            .filter(|i| cache.get(2, 1, "c", &key(1000 + i)).is_some())
            .count();
        assert_eq!(
            db2_resident, 4,
            "DB2 should retain all 4 entries; resident={db2_resident}"
        );
    }

    #[test]
    fn weight_ratio_affects_resident_sets() {
        // DB1 weight=1, DB2 weight=4 → under pressure DB2 keeps 4x more entries.
        let capacity = 10;
        let mut cache = DocCache::new(capacity);
        cache.set_database_weight(1, 1);
        cache.set_database_weight(2, 4);

        // Fill with 5 DB1 and 5 DB2 entries.
        for i in 0..5u32 {
            cache.put(1, 1, "c", &key(i), b"v");
            cache.put(2, 1, "c", &key(1000 + i), b"v");
        }
        assert_eq!(cache.len(), capacity);

        // Add 5 more DB1 entries to trigger eviction. DB2 (weight=4) should
        // retain more entries than DB1 (weight=1).
        for i in 5..10u32 {
            cache.put(1, 1, "c", &key(i), b"v");
        }

        let db1_count = (0..10u32)
            .filter(|i| cache.get(1, 1, "c", &key(*i)).is_some())
            .count();
        let db2_count = (0..5u32)
            .filter(|i| cache.get(2, 1, "c", &key(1000 + i)).is_some())
            .count();
        assert!(
            db2_count > db1_count,
            "DB2 (weight=4) should have more resident entries than DB1 (weight=1); db2={db2_count} db1={db1_count}"
        );
    }

    #[test]
    fn evict_collection_removes_correct_entries() {
        let mut cache = DocCache::new(16);
        cache.put(1, 1, "col_a", &key(1), b"1");
        cache.put(1, 1, "col_b", &key(1), b"2");
        cache.put(2, 1, "col_a", &key(1), b"3");

        cache.evict_collection(1, 1, "col_a");
        assert_eq!(cache.get(1, 1, "col_a", &key(1)), None);
        assert_eq!(cache.get(1, 1, "col_b", &key(1)), Some(b"2".as_slice()));
        assert_eq!(cache.get(2, 1, "col_a", &key(1)), Some(b"3".as_slice()));
    }

    #[test]
    fn evict_tenant_removes_correct_entries() {
        let mut cache = DocCache::new(16);
        cache.put(1, 1, "col", &key(1), b"t1");
        cache.put(1, 2, "col", &key(1), b"t2");
        cache.put(2, 1, "col", &key(1), b"db2");

        cache.evict_tenant(1, 1);
        assert_eq!(cache.get(1, 1, "col", &key(1)), None);
        assert_eq!(cache.get(1, 2, "col", &key(1)), Some(b"t2".as_slice()));
        assert_eq!(cache.get(2, 1, "col", &key(1)), Some(b"db2".as_slice()));
    }
}
