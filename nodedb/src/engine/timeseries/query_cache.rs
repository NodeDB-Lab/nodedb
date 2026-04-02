//! Partition-level query cache for sealed immutable partitions.
//!
//! Sealed partitions never change → cache entries never invalidate.
//! LRU eviction with configurable memory budget.
//!
//! Key: `(tenant_id, partition_id, query_hash)` where tenant_id ensures
//! cross-tenant isolation, partition_id is the partition's min_ts
//! (unique within a collection), and query_hash is a hash of the scan
//! request parameters.

use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};

/// Cache key: (tenant_id, partition_id, query_hash).
type CacheKey = (u32, i64, u64);

/// LRU query cache for sealed partition scan results.
pub struct QueryCache {
    /// (tenant_id, partition_id, query_hash) → cached result bytes.
    entries: HashMap<CacheKey, Vec<u8>>,
    /// LRU eviction order (oldest first).
    order: VecDeque<CacheKey>,
    /// Maximum total cached bytes.
    max_bytes: usize,
    /// Current total cached bytes.
    current_bytes: usize,
}

impl QueryCache {
    /// Create a query cache with the given memory budget.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            max_bytes,
            current_bytes: 0,
        }
    }

    /// Look up a cached result. Requires `tenant_id` to prevent cross-tenant leaks.
    pub fn get(&self, tenant_id: u32, partition_id: i64, query_hash: u64) -> Option<&[u8]> {
        self.entries
            .get(&(tenant_id, partition_id, query_hash))
            .map(|v| v.as_slice())
    }

    /// Insert a result into the cache. Evicts old entries if over budget.
    pub fn insert(&mut self, tenant_id: u32, partition_id: i64, query_hash: u64, result: Vec<u8>) {
        let key = (tenant_id, partition_id, query_hash);

        // Don't cache if the single entry exceeds budget.
        if result.len() > self.max_bytes {
            return;
        }

        // Remove old entry for this key if exists.
        if let Some(old) = self.entries.remove(&key) {
            self.current_bytes -= old.len();
            self.order.retain(|k| k != &key);
        }

        // Evict until we have room.
        while self.current_bytes + result.len() > self.max_bytes {
            if let Some(evict_key) = self.order.pop_front() {
                if let Some(evicted) = self.entries.remove(&evict_key) {
                    self.current_bytes -= evicted.len();
                }
            } else {
                break;
            }
        }

        self.current_bytes += result.len();
        self.order.push_back(key);
        self.entries.insert(key, result);
    }

    /// Invalidate all cached entries for a partition (e.g., if it's modified).
    ///
    /// Sealed partitions are immutable, so this should only be called for
    /// active partitions that are still receiving writes.
    pub fn invalidate_partition(&mut self, tenant_id: u32, partition_id: i64) {
        let keys: Vec<CacheKey> = self
            .entries
            .keys()
            .filter(|&&(tid, pid, _)| tid == tenant_id && pid == partition_id)
            .copied()
            .collect();

        for key in keys {
            if let Some(removed) = self.entries.remove(&key) {
                self.current_bytes -= removed.len();
            }
        }
        self.order
            .retain(|&(tid, pid, _)| !(tid == tenant_id && pid == partition_id));
    }

    /// Evict all cached entries belonging to a specific tenant.
    ///
    /// Used during tenant purge to ensure zero residual cached data.
    pub fn evict_tenant(&mut self, tenant_id: u32) {
        let keys: Vec<CacheKey> = self
            .entries
            .keys()
            .filter(|&&(tid, _, _)| tid == tenant_id)
            .copied()
            .collect();

        for key in keys {
            if let Some(removed) = self.entries.remove(&key) {
                self.current_bytes -= removed.len();
            }
        }
        self.order.retain(|&(tid, _, _)| tid != tenant_id);
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Current memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.current_bytes
    }
}

/// Compute a query hash from scan parameters.
pub fn query_hash(value_column: &str, start_ms: i64, end_ms: i64, bucket_interval_ms: i64) -> u64 {
    let mut hasher = DefaultHasher::new();
    value_column.hash(&mut hasher);
    start_ms.hash(&mut hasher);
    end_ms.hash(&mut hasher);
    bucket_interval_ms.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    const T1: u32 = 1;
    const T2: u32 = 2;

    #[test]
    fn basic_cache_roundtrip() {
        let mut cache = QueryCache::new(1024 * 1024);
        let data = vec![1u8, 2, 3, 4];
        cache.insert(T1, 100, 42, data.clone());
        assert_eq!(cache.get(T1, 100, 42), Some(data.as_slice()));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn cache_miss() {
        let cache = QueryCache::new(1024);
        assert!(cache.get(T1, 100, 42).is_none());
    }

    #[test]
    fn tenant_isolation() {
        let mut cache = QueryCache::new(1024 * 1024);
        let data_t1 = vec![1u8, 2, 3];
        let data_t2 = vec![4u8, 5, 6];

        // Same partition_id and query_hash, different tenants.
        cache.insert(T1, 100, 42, data_t1.clone());
        cache.insert(T2, 100, 42, data_t2.clone());

        assert_eq!(cache.get(T1, 100, 42), Some(data_t1.as_slice()));
        assert_eq!(cache.get(T2, 100, 42), Some(data_t2.as_slice()));
        // Cross-tenant lookup must miss.
        assert!(cache.get(T2, 100, 99).is_none());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn lru_eviction() {
        let mut cache = QueryCache::new(100);
        cache.insert(T1, 1, 0, vec![0u8; 40]);
        cache.insert(T1, 2, 0, vec![0u8; 40]);
        assert_eq!(cache.len(), 2);

        // This should evict partition 1's entry.
        cache.insert(T1, 3, 0, vec![0u8; 40]);
        assert_eq!(cache.len(), 2);
        assert!(cache.get(T1, 1, 0).is_none()); // Evicted.
        assert!(cache.get(T1, 3, 0).is_some());
    }

    #[test]
    fn invalidate_partition() {
        let mut cache = QueryCache::new(1024);
        cache.insert(T1, 100, 1, vec![1]);
        cache.insert(T1, 100, 2, vec![2]);
        cache.insert(T1, 200, 1, vec![3]);
        // Different tenant, same partition_id — must NOT be invalidated.
        cache.insert(T2, 100, 1, vec![4]);
        assert_eq!(cache.len(), 4);

        cache.invalidate_partition(T1, 100);
        assert_eq!(cache.len(), 2);
        assert!(cache.get(T1, 200, 1).is_some());
        assert!(cache.get(T2, 100, 1).is_some()); // Other tenant unaffected.
    }

    #[test]
    fn evict_tenant() {
        let mut cache = QueryCache::new(1024);
        cache.insert(T1, 100, 1, vec![1]);
        cache.insert(T1, 200, 2, vec![2]);
        cache.insert(T2, 100, 1, vec![3]);
        assert_eq!(cache.len(), 3);

        cache.evict_tenant(T1);
        assert_eq!(cache.len(), 1);
        assert!(cache.get(T1, 100, 1).is_none());
        assert!(cache.get(T2, 100, 1).is_some());
    }

    #[test]
    fn query_hash_deterministic() {
        let h1 = query_hash("cpu", 1000, 2000, 60000);
        let h2 = query_hash("cpu", 1000, 2000, 60000);
        assert_eq!(h1, h2);

        let h3 = query_hash("mem", 1000, 2000, 60000);
        assert_ne!(h1, h3);
    }
}
