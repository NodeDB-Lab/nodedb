//! Recent-window cache: keeps the last N hours of data decompressed in memory.
//!
//! Dashboard queries typically hit the last 1-24 hours. Older data stays
//! compressed on disk. This cache holds decompressed column data for the
//! hot window, avoiding decompression overhead on repeated queries.
//!
//! Key: `(tenant_id, collection, min_ts)` — tenant_id ensures cross-tenant
//! isolation. Two tenants with the same collection name get independent
//! cache entries.
//!
//! Config: `hot_window_ms` (e.g., 24 * 3600 * 1000 for 24 hours).
//! Default: 0 (memtable data only, no extra cache).

/// Cache key: (tenant_id, collection, min_ts).
type CacheKey = (u32, String, i64);

/// Decompressed column data for a recently-accessed partition.
#[derive(Debug)]
pub struct CachedPartition {
    /// Partition min timestamp.
    pub min_ts: i64,
    /// Partition max timestamp.
    pub max_ts: i64,
    /// Decompressed timestamps.
    pub timestamps: Vec<i64>,
    /// Decompressed values.
    pub values: Vec<f64>,
    /// Approximate memory usage in bytes.
    pub memory_bytes: usize,
}

/// LRU cache of recently-accessed partition data.
pub struct RecentWindowCache {
    /// Cached partitions, keyed by (tenant_id, collection, min_ts).
    entries: std::collections::HashMap<CacheKey, CachedPartition>,
    /// LRU eviction order.
    order: std::collections::VecDeque<CacheKey>,
    /// Maximum total cached bytes.
    max_bytes: usize,
    /// Current total cached bytes.
    current_bytes: usize,
    /// Hot window duration in milliseconds. Only cache partitions whose
    /// max_ts is within this window of `now`.
    hot_window_ms: i64,
}

impl RecentWindowCache {
    pub fn new(max_bytes: usize, hot_window_ms: i64) -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            order: std::collections::VecDeque::new(),
            max_bytes,
            current_bytes: 0,
            hot_window_ms,
        }
    }

    /// Check if a partition is within the hot window.
    pub fn is_in_hot_window(&self, max_ts: i64, now_ms: i64) -> bool {
        if self.hot_window_ms <= 0 {
            return false;
        }
        max_ts >= now_ms - self.hot_window_ms
    }

    /// Look up cached data for a partition. Requires `tenant_id` for isolation.
    pub fn get(&self, tenant_id: u32, collection: &str, min_ts: i64) -> Option<&CachedPartition> {
        self.entries
            .get(&(tenant_id, collection.to_string(), min_ts))
    }

    /// Insert decompressed partition data into the cache.
    pub fn insert(&mut self, tenant_id: u32, collection: &str, partition: CachedPartition) {
        let key = (tenant_id, collection.to_string(), partition.min_ts);
        let size = partition.memory_bytes;

        if size > self.max_bytes {
            return;
        }

        // Remove existing entry for this key.
        if let Some(old) = self.entries.remove(&key) {
            self.current_bytes -= old.memory_bytes;
            self.order.retain(|k| k != &key);
        }

        // Evict until room.
        while self.current_bytes + size > self.max_bytes {
            if let Some(evict_key) = self.order.pop_front() {
                if let Some(evicted) = self.entries.remove(&evict_key) {
                    self.current_bytes -= evicted.memory_bytes;
                }
            } else {
                break;
            }
        }

        self.current_bytes += size;
        self.order.push_back(key.clone());
        self.entries.insert(key, partition);
    }

    /// Evict partitions that are no longer in the hot window.
    pub fn evict_cold(&mut self, now_ms: i64) {
        let cutoff = now_ms - self.hot_window_ms;
        let cold_keys: Vec<CacheKey> = self
            .entries
            .iter()
            .filter(|(_, p)| p.max_ts < cutoff)
            .map(|(k, _)| k.clone())
            .collect();

        for key in cold_keys {
            if let Some(removed) = self.entries.remove(&key) {
                self.current_bytes -= removed.memory_bytes;
            }
            self.order.retain(|k| k != &key);
        }
    }

    /// Evict all cached entries belonging to a specific tenant.
    ///
    /// Used during tenant purge to ensure zero residual cached data.
    pub fn evict_tenant(&mut self, tenant_id: u32) {
        let keys: Vec<CacheKey> = self
            .entries
            .keys()
            .filter(|k| k.0 == tenant_id)
            .cloned()
            .collect();

        for key in keys {
            if let Some(removed) = self.entries.remove(&key) {
                self.current_bytes -= removed.memory_bytes;
            }
            self.order.retain(|k| k != &key);
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn memory_bytes(&self) -> usize {
        self.current_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T1: u32 = 1;
    const T2: u32 = 2;

    fn make_partition(min_ts: i64, max_ts: i64, rows: usize) -> CachedPartition {
        CachedPartition {
            min_ts,
            max_ts,
            timestamps: vec![min_ts; rows],
            values: vec![0.0; rows],
            memory_bytes: rows * 16,
        }
    }

    #[test]
    fn basic_insert_and_get() {
        let mut cache = RecentWindowCache::new(1_000_000, 3_600_000);
        cache.insert(T1, "metrics", make_partition(1000, 2000, 100));
        assert!(cache.get(T1, "metrics", 1000).is_some());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn tenant_isolation() {
        let mut cache = RecentWindowCache::new(1_000_000, 3_600_000);
        cache.insert(T1, "metrics", make_partition(1000, 2000, 10));
        cache.insert(T2, "metrics", make_partition(1000, 2000, 10));

        // Same collection + min_ts, different tenants — both exist.
        assert!(cache.get(T1, "metrics", 1000).is_some());
        assert!(cache.get(T2, "metrics", 1000).is_some());
        // Cross-tenant lookup must miss.
        assert!(cache.get(T1, "metrics", 9999).is_none());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn hot_window_check() {
        let cache = RecentWindowCache::new(1_000_000, 3_600_000);
        let now = 10_000_000;
        assert!(cache.is_in_hot_window(now - 1_000_000, now));
        assert!(!cache.is_in_hot_window(now - 5_000_000, now));
    }

    #[test]
    fn evict_cold() {
        let mut cache = RecentWindowCache::new(1_000_000, 3_600_000);
        // Old partition: max_ts=2000, will be cold at now=10_000_000.
        cache.insert(T1, "m", make_partition(1000, 2000, 10));
        // Recent partition: max_ts=9_000_000, within hot window at now=10_000_000.
        cache.insert(T1, "m", make_partition(8_000_000, 9_000_000, 10));
        assert_eq!(cache.len(), 2);

        cache.evict_cold(10_000_000);
        assert_eq!(cache.len(), 1);
        assert!(cache.get(T1, "m", 1000).is_none()); // Evicted (cold).
        assert!(cache.get(T1, "m", 8_000_000).is_some()); // Kept (hot).
    }

    #[test]
    fn evict_tenant() {
        let mut cache = RecentWindowCache::new(1_000_000, 3_600_000);
        cache.insert(T1, "m", make_partition(1000, 2000, 10));
        cache.insert(T1, "m", make_partition(3000, 4000, 10));
        cache.insert(T2, "m", make_partition(1000, 2000, 10));
        assert_eq!(cache.len(), 3);

        cache.evict_tenant(T1);
        assert_eq!(cache.len(), 1);
        assert!(cache.get(T1, "m", 1000).is_none());
        assert!(cache.get(T2, "m", 1000).is_some());
    }

    #[test]
    fn lru_eviction() {
        let mut cache = RecentWindowCache::new(500, 3_600_000);
        cache.insert(T1, "m", make_partition(1000, 2000, 20)); // 320 bytes
        cache.insert(T1, "m", make_partition(3000, 4000, 20)); // 320 bytes → evicts first
        assert_eq!(cache.len(), 1);
        assert!(cache.get(T1, "m", 1000).is_none());
        assert!(cache.get(T1, "m", 3000).is_some());
    }
}
