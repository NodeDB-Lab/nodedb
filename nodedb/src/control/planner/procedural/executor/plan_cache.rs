//! Parsed procedural block cache for triggers and procedures.
//!
//! Avoids re-parsing trigger/procedure bodies on every invocation.
//! Cache key is the body SQL string hash. Entries are evicted via LRU
//! when the cache exceeds `max_entries`.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};

use crate::control::planner::procedural::ast::ProceduralBlock;

/// Thread-safe cache of parsed procedural blocks.
///
/// Used by trigger fire logic to avoid re-parsing the same trigger body
/// on every invocation. Also used by stored procedure CALL to cache
/// the parsed body across invocations.
pub struct ProcedureBlockCache {
    inner: Mutex<CacheInner>,
}

struct CacheInner {
    entries: HashMap<u64, CacheEntry>,
    /// Insertion order for LRU eviction (oldest first).
    order: Vec<u64>,
    max_entries: usize,
}

struct CacheEntry {
    body_sql: String,
    block: Arc<ProceduralBlock>,
}

impl ProcedureBlockCache {
    /// Create a cache with the given maximum entry count.
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(CacheInner {
                entries: HashMap::with_capacity(max_entries),
                order: Vec::with_capacity(max_entries),
                max_entries,
            }),
        }
    }

    /// Get a cached block, or parse and cache it.
    ///
    /// Returns `Arc<ProceduralBlock>` for zero-copy sharing across concurrent
    /// trigger invocations.
    pub fn get_or_parse(
        &self,
        body_sql: &str,
    ) -> Result<Arc<ProceduralBlock>, crate::control::planner::procedural::error::ProceduralError>
    {
        let key = hash_body(body_sql);

        // Hold lock for both check and parse to avoid duplicate work under
        // concurrency. Parsing is fast enough for Control Plane and the lock
        // is uncontended in the cache-hit path (early return).
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());

        // Cache hit — verify the body matches (not just the hash) to guard
        // against 64-bit hash collisions returning the wrong block.
        if let Some(entry) = inner.entries.get(&key) {
            if entry.body_sql == body_sql {
                return Ok(Arc::clone(&entry.block));
            }
            // Hash collision with different body — evict the stale entry
            // and fall through to re-parse.
            inner.entries.remove(&key);
            inner.order.retain(|k| *k != key);
        }

        // Cache miss — parse, cache, return.
        let block = crate::control::planner::procedural::parse_block(body_sql)?;
        let arc = Arc::new(block);

        // Evict oldest if full.
        while inner.entries.len() >= inner.max_entries && !inner.order.is_empty() {
            let evict_key = inner.order.remove(0);
            inner.entries.remove(&evict_key);
        }

        inner.entries.insert(
            key,
            CacheEntry {
                body_sql: body_sql.to_string(),
                block: Arc::clone(&arc),
            },
        );
        inner.order.push(key);

        Ok(arc)
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all cached entries.
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.entries.clear();
        inner.order.clear();
    }
}

fn hash_body(body_sql: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    body_sql.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_hit() {
        let cache = ProcedureBlockCache::new(100);
        let body = "BEGIN RETURN 42; END";
        let b1 = cache.get_or_parse(body).unwrap();
        let b2 = cache.get_or_parse(body).unwrap();
        assert!(Arc::ptr_eq(&b1, &b2)); // Same Arc = cache hit
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn cache_miss_different_body() {
        let cache = ProcedureBlockCache::new(100);
        let b1 = cache.get_or_parse("BEGIN RETURN 1; END").unwrap();
        let b2 = cache.get_or_parse("BEGIN RETURN 2; END").unwrap();
        assert!(!Arc::ptr_eq(&b1, &b2));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn lru_eviction() {
        let cache = ProcedureBlockCache::new(2);
        cache.get_or_parse("BEGIN RETURN 1; END").unwrap();
        cache.get_or_parse("BEGIN RETURN 2; END").unwrap();
        assert_eq!(cache.len(), 2);

        // Third entry evicts the first.
        cache.get_or_parse("BEGIN RETURN 3; END").unwrap();
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn parse_error_not_cached() {
        let cache = ProcedureBlockCache::new(100);
        assert!(cache.get_or_parse("not valid sql").is_err());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn clear_cache() {
        let cache = ProcedureBlockCache::new(100);
        cache.get_or_parse("BEGIN RETURN 1; END").unwrap();
        cache.get_or_parse("BEGIN RETURN 2; END").unwrap();
        cache.clear();
        assert!(cache.is_empty());
    }
}
