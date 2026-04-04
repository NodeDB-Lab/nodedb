//! Last-Value Cache: O(1) lookup of the most recent (timestamp, value) per series.
//!
//! Updated atomically on every write to the timeseries memtable.
//! Same Data Plane core, no cross-core coordination.
//!
//! Memory: ~100 bytes per series × 500K series = ~50 MB (acceptable).

use std::collections::HashMap;

use nodedb_types::timeseries::SeriesId;

/// Per-collection last-value cache.
///
/// Stores the most recent `(timestamp_ms, value)` for each series.
/// Updated in the write path after every memtable insert.
pub struct LastValueCache {
    entries: HashMap<SeriesId, LastValueEntry>,
}

/// A cached last-value entry.
#[derive(Debug, Clone, Copy)]
pub struct LastValueEntry {
    /// Timestamp of the most recent value (milliseconds).
    pub ts: i64,
    /// The most recent metric value.
    pub value: f64,
}

impl LastValueCache {
    /// Create an empty cache.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Create a cache with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
        }
    }

    /// Update the cache for a series. Only updates if `ts >= cached_ts`.
    ///
    /// Returns `true` if the entry was updated (new or newer timestamp).
    pub fn update(&mut self, series_id: SeriesId, ts: i64, value: f64) -> bool {
        match self.entries.get_mut(&series_id) {
            Some(entry) if ts >= entry.ts => {
                entry.ts = ts;
                entry.value = value;
                true
            }
            Some(_) => false, // Older timestamp, skip.
            None => {
                self.entries.insert(series_id, LastValueEntry { ts, value });
                true
            }
        }
    }

    /// O(1) lookup of the last value for a series.
    pub fn get(&self, series_id: SeriesId) -> Option<&LastValueEntry> {
        self.entries.get(&series_id)
    }

    /// Return all cached last values. O(N) where N is distinct series count.
    pub fn all(&self) -> impl Iterator<Item = (SeriesId, &LastValueEntry)> {
        self.entries.iter().map(|(&id, entry)| (id, entry))
    }

    /// Remove a series from the cache (e.g., on TTL expiry or drop).
    pub fn remove(&mut self, series_id: SeriesId) -> bool {
        self.entries.remove(&series_id).is_some()
    }

    /// Evict all entries with timestamps older than `cutoff_ms`.
    /// Returns the number of evicted entries.
    pub fn evict_older_than(&mut self, cutoff_ms: i64) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, entry| entry.ts >= cutoff_ms);
        before - self.entries.len()
    }

    /// Number of cached series.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Approximate memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        // HashMap overhead + per-entry (SeriesId + LastValueEntry).
        self.entries.capacity()
            * (std::mem::size_of::<SeriesId>() + std::mem::size_of::<LastValueEntry>() + 24)
    }

    /// Clear all entries (e.g., on collection drop).
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

impl Default for LastValueCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sid(id: u64) -> SeriesId {
        id
    }

    #[test]
    fn insert_and_get() {
        let mut cache = LastValueCache::new();
        let sid = sid(42);

        assert!(cache.update(sid, 1000, 87.3));
        let entry = cache.get(sid).unwrap();
        assert_eq!(entry.ts, 1000);
        assert!((entry.value - 87.3).abs() < 1e-12);
    }

    #[test]
    fn newer_timestamp_updates() {
        let mut cache = LastValueCache::new();
        let sid = sid(1);

        cache.update(sid, 1000, 10.0);
        assert!(cache.update(sid, 2000, 20.0));

        let entry = cache.get(sid).unwrap();
        assert_eq!(entry.ts, 2000);
        assert!((entry.value - 20.0).abs() < 1e-12);
    }

    #[test]
    fn older_timestamp_skipped() {
        let mut cache = LastValueCache::new();
        let sid = sid(1);

        cache.update(sid, 2000, 20.0);
        assert!(!cache.update(sid, 1000, 10.0)); // Older, should skip.

        let entry = cache.get(sid).unwrap();
        assert_eq!(entry.ts, 2000);
    }

    #[test]
    fn same_timestamp_updates() {
        let mut cache = LastValueCache::new();
        let sid = sid(1);

        cache.update(sid, 1000, 10.0);
        assert!(cache.update(sid, 1000, 99.0)); // Same ts, should update (last-write-wins).

        assert!((cache.get(sid).unwrap().value - 99.0).abs() < 1e-12);
    }

    #[test]
    fn multiple_series() {
        let mut cache = LastValueCache::new();
        cache.update(sid(1), 100, 1.0);
        cache.update(sid(2), 200, 2.0);
        cache.update(sid(3), 300, 3.0);

        assert_eq!(cache.len(), 3);
        assert!((cache.get(sid(2)).unwrap().value - 2.0).abs() < 1e-12);
    }

    #[test]
    fn all_returns_all_entries() {
        let mut cache = LastValueCache::new();
        cache.update(sid(1), 100, 1.0);
        cache.update(sid(2), 200, 2.0);

        let all: Vec<_> = cache.all().collect();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn remove_series() {
        let mut cache = LastValueCache::new();
        let sid = sid(1);
        cache.update(sid, 100, 10.0);
        assert!(cache.remove(sid));
        assert!(cache.get(sid).is_none());
        assert!(!cache.remove(sid)); // Already removed.
    }

    #[test]
    fn clear_cache() {
        let mut cache = LastValueCache::new();
        cache.update(sid(1), 100, 1.0);
        cache.update(sid(2), 200, 2.0);
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn nonexistent_series() {
        let cache = LastValueCache::new();
        assert!(cache.get(sid(999)).is_none());
    }
}
