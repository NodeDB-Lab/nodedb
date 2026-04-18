//! Gateway-level plan cache, keyed on SQL text hash + placeholder types hash
//! + `GatewayVersionSet`.
//!
//! Unlike the per-session `SessionPlanCache` (which caches compiled
//! `Vec<PhysicalTask>` per SQL text for a single connection), the
//! `PlanCache` lives on `SharedState` and is shared across all sessions.
//! It is invalidated precisely on DDL — only entries whose
//! `GatewayVersionSet` references the changed descriptor are evicted.
//!
//! # Capacity
//!
//! Fixed at 1024 entries by default (see `DEFAULT_CAPACITY`). On overflow
//! the oldest entry (insertion order) is evicted — simple FIFO rather than
//! true LRU, sufficient for plan-cache semantics where sequential scans are
//! rare and any eviction just causes a re-plan.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::bridge::physical_plan::PhysicalPlan;

use super::version_set::GatewayVersionSet;

/// Default maximum number of cached plans.
pub const DEFAULT_CAPACITY: usize = 1024;

/// Cache key: SQL hash + placeholder-type hash + descriptor version set.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanCacheKey {
    /// FNV-1a hash of the SQL text.
    pub sql_text_hash: u64,
    /// Hash of the placeholder type list (0 if no placeholders).
    pub placeholder_types_hash: u64,
    /// Descriptor versions the plan was built against.
    pub version_set: GatewayVersionSet,
}

/// Compact key for the version-set side cache: `(sql_text_hash, placeholder_types_hash)`.
///
/// Used by `lookup_version_set` / `insert_version_set` to bridge the gap between
/// "we have SQL text" (at the start of `execute_sql`) and "we have a
/// `DescriptorVersionSet`" (after planning). Without this side cache the plan
/// cache hit rate for the SQL path is literally 0% because the speculative empty
/// version set never matches the actual keyed entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlKey {
    pub sql_text_hash: u64,
    pub placeholder_types_hash: u64,
}

/// An entry in the plan cache.
struct CacheEntry {
    key: PlanCacheKey,
    plan: std::sync::Arc<PhysicalPlan>,
}

/// Thread-safe, bounded plan cache.
///
/// `get` is O(n) in the number of entries with matching SQL/placeholder hash.
/// In practice caches are small (≤1024) and DDL evictions keep them lean.
///
/// ## Two-phase lookup (Gap 5 fix)
///
/// SQL text alone is not enough to build a full `PlanCacheKey` — we need the
/// `GatewayVersionSet`, which requires knowing which collections are touched by
/// the plan. The side cache (`version_set_index`) stores the mapping
/// `(sql_hash, ph_hash) → GatewayVersionSet` so `execute_sql` can perform a
/// two-phase lookup:
///
/// 1. Look up the version set by SQL key.
/// 2. Verify the stored version set is still current (DDL may have bumped it).
/// 3. If current, use it to build the full `PlanCacheKey` and do the plan lookup.
/// 4. On DDL invalidation, also remove the version-set side-cache entry so the
///    next call falls through to re-planning.
pub struct PlanCache {
    inner: Mutex<PlanCacheInner>,
    /// Total number of cache hits since this cache was created.
    hit_count: AtomicU64,
    /// Total number of cache misses (`get` calls that returned `None`)
    /// since this cache was created. Paired with `hit_count` to render
    /// the `gateway_plan_cache_hit_ratio` gauge.
    miss_count: AtomicU64,
}

struct PlanCacheInner {
    entries: VecDeque<CacheEntry>,
    capacity: usize,
    /// Side cache: `(sql_hash, ph_hash)` → last-known `GatewayVersionSet`.
    ///
    /// Bounded implicitly by `capacity`: each plan entry has at most one side-
    /// cache entry; the map is pruned in `invalidate_descriptor` together with
    /// the plan entries it covers.
    version_set_index: HashMap<SqlKey, GatewayVersionSet>,
}

impl PlanCache {
    /// Create a new cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(PlanCacheInner {
                entries: VecDeque::with_capacity(capacity.min(256)),
                capacity,
                version_set_index: HashMap::new(),
            }),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
        }
    }

    /// Create a cache with `DEFAULT_CAPACITY`.
    pub fn default_capacity() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }

    /// Look up a plan by key. Returns `Some(Arc<PhysicalPlan>)` on a hit.
    pub fn get(&self, key: &PlanCacheKey) -> Option<std::sync::Arc<PhysicalPlan>> {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let result = inner
            .entries
            .iter()
            .find(|e| &e.key == key)
            .map(|e| std::sync::Arc::clone(&e.plan));
        if result.is_some() {
            self.hit_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.miss_count.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Total number of cache hits since this cache was created.
    pub fn cache_hit_count(&self) -> u64 {
        self.hit_count.load(Ordering::Relaxed)
    }

    /// Total number of cache misses since this cache was created.
    pub fn cache_miss_count(&self) -> u64 {
        self.miss_count.load(Ordering::Relaxed)
    }

    /// Cache hit ratio in `[0.0, 1.0]`. Returns `0.0` when the cache
    /// has never been consulted so scrapes never see a NaN sample.
    pub fn hit_ratio(&self) -> f64 {
        let h = self.hit_count.load(Ordering::Relaxed);
        let m = self.miss_count.load(Ordering::Relaxed);
        let total = h + m;
        if total == 0 {
            0.0
        } else {
            h as f64 / total as f64
        }
    }

    /// Insert a plan. On capacity overflow, the oldest entry is evicted.
    pub fn insert(&self, key: PlanCacheKey, plan: std::sync::Arc<PhysicalPlan>) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        // Remove any existing entry with the same key first.
        inner.entries.retain(|e| e.key != key);
        if inner.entries.len() >= inner.capacity {
            inner.entries.pop_front();
        }
        inner.entries.push_back(CacheEntry { key, plan });
    }

    /// Evict all plan entries whose `version_set` references `name` at any
    /// version other than `new_version`. Also removes the corresponding
    /// version-set side-cache entries so the next `execute_sql` call re-plans
    /// against the new descriptor rather than hitting a stale two-phase lookup.
    pub fn invalidate_descriptor(&self, name: &str, new_version: u64) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());

        // Collect SQL keys whose stored version set references the changed
        // descriptor so we can evict them from the side cache too.
        let stale_sql_keys: Vec<SqlKey> = inner
            .version_set_index
            .iter()
            .filter(|(_, vs)| vs.contains_collection(name) && !vs.matches(name, new_version))
            .map(|(k, _)| k.clone())
            .collect();
        for sk in &stale_sql_keys {
            inner.version_set_index.remove(sk);
        }

        inner.entries.retain(|e| {
            // Keep entries that don't touch this descriptor at all.
            if !e.key.version_set.contains_collection(name) {
                return true;
            }
            // Keep entries whose version is already current.
            e.key.version_set.matches(name, new_version)
        });
    }

    /// Look up the most recently stored `GatewayVersionSet` for a SQL key.
    ///
    /// Used by `execute_sql` for the two-phase cache lookup: check the side
    /// cache first to recover the version set, then verify it is still current
    /// before doing the full `PlanCacheKey` lookup.
    pub fn lookup_version_set(&self, sql_key: &SqlKey) -> Option<GatewayVersionSet> {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.version_set_index.get(sql_key).cloned()
    }

    /// Store a `GatewayVersionSet` for a SQL key.
    ///
    /// Called by `execute_sql` after a cache miss so the next call can do the
    /// two-phase lookup without re-planning.
    pub fn insert_version_set(&self, sql_key: SqlKey, version_set: GatewayVersionSet) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.version_set_index.insert(sql_key, version_set);
    }

    /// Number of cached plans.
    pub fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Helper: FNV-1a 64-bit hash for SQL text.
pub fn hash_sql(sql: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in sql.as_bytes() {
        h ^= *byte as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Helper: hash a slice of placeholder type names.
pub fn hash_placeholder_types(types: &[&str]) -> u64 {
    if types.is_empty() {
        return 0;
    }
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for ty in types {
        for byte in ty.as_bytes() {
            h ^= *byte as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        // Separate types with a sentinel byte.
        h ^= 0xFF;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::physical_plan::{KvOp, PhysicalPlan};
    use crate::control::gateway::version_set::GatewayVersionSet;

    fn kv_plan(collection: &str) -> Arc<PhysicalPlan> {
        Arc::new(PhysicalPlan::Kv(KvOp::Get {
            collection: collection.into(),
            key: vec![],
            rls_filters: vec![],
        }))
    }

    fn key(sql: &str, collection: &str, version: u64) -> PlanCacheKey {
        PlanCacheKey {
            sql_text_hash: hash_sql(sql),
            placeholder_types_hash: 0,
            version_set: GatewayVersionSet::from_pairs(vec![(collection.into(), version)]),
        }
    }

    #[test]
    fn cache_hit_and_miss() {
        let cache = PlanCache::new(16);
        let k = key("SELECT 1", "users", 1);
        let plan = kv_plan("users");

        assert!(cache.get(&k).is_none());
        cache.insert(k.clone(), Arc::clone(&plan));
        assert!(cache.get(&k).is_some());
    }

    #[test]
    fn version_bump_invalidates_entry() {
        let cache = PlanCache::new(16);
        let k = key("SELECT 1", "users", 1);
        cache.insert(k.clone(), kv_plan("users"));
        assert_eq!(cache.len(), 1);

        // New version bumped — entry at version=1 should be evicted.
        cache.invalidate_descriptor("users", 2);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn invalidate_descriptor_keeps_unrelated_entries() {
        let cache = PlanCache::new(16);
        let k_users = key("q1", "users", 1);
        let k_orders = key("q2", "orders", 5);
        cache.insert(k_users, kv_plan("users"));
        cache.insert(k_orders, kv_plan("orders"));
        assert_eq!(cache.len(), 2);

        // Bump `users` — only the `users` entry should be evicted.
        cache.invalidate_descriptor("users", 2);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn lru_eviction_at_capacity() {
        let cap = 4usize;
        let cache = PlanCache::new(cap);
        for i in 0..=cap {
            let k = key(&format!("q{i}"), &format!("col{i}"), 1);
            cache.insert(k, kv_plan("col"));
        }
        // One entry evicted when capacity exceeded.
        assert_eq!(cache.len(), cap);
    }

    #[test]
    fn current_version_entry_survives_invalidation() {
        let cache = PlanCache::new(16);
        let k = key("q", "users", 3);
        cache.insert(k.clone(), kv_plan("users"));

        // Invalidating with the same version keeps the entry.
        cache.invalidate_descriptor("users", 3);
        assert_eq!(cache.len(), 1);
        assert!(cache.get(&k).is_some());
    }

    #[test]
    fn concurrent_access_no_panic() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(PlanCache::new(256));
        let mut handles = Vec::new();

        for i in 0..8u64 {
            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                let k = PlanCacheKey {
                    sql_text_hash: i,
                    placeholder_types_hash: 0,
                    version_set: GatewayVersionSet::from_pairs(vec![(format!("col{i}"), i)]),
                };
                c.insert(k.clone(), kv_plan("col"));
                let _ = c.get(&k);
                c.invalidate_descriptor(&format!("col{i}"), i + 1);
            }));
        }
        for h in handles {
            h.join().expect("thread panicked");
        }
    }
}
