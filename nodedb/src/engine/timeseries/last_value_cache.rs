// SPDX-License-Identifier: BUSL-1.1

//! Last-Value Cache: O(1) lookup of the most recent (timestamp, value) per series.
//!
//! Updated atomically on every write to the timeseries memtable.
//! Same Data Plane core, no cross-core coordination.
//!
//! Every entry records the system-time ordinal of the write that set it. A
//! value an update replaces stays as a version of its series until no read
//! can reach it: until the cleanup horizon passes the ordinal of the update
//! that replaced it. A read at ordinal `S` then answers with the version
//! that was current at `S`, so a transaction block reads its snapshot.
//!
//! Memory: ~100 bytes per series × 500K series = ~50 MB (acceptable), plus
//! one entry per value replaced inside the history window.

use std::collections::HashMap;

use nodedb_types::timeseries::SeriesId;

/// Per-collection last-value cache.
///
/// Stores the most recent `(timestamp_ms, value)` for each series.
/// Updated in the write path after every memtable insert.
#[derive(Clone)]
pub struct LastValueCache {
    entries: HashMap<SeriesId, LastValueEntry>,
    /// Replaced versions of each series, oldest first. Version `i` was
    /// current from its own ordinal up to the ordinal of version `i + 1`,
    /// or of the current entry for the newest one.
    history: HashMap<SeriesId, Vec<LastValueEntry>>,
    /// The cleanup horizon: no read reaches an ordinal below it.
    horizon: i64,
    /// The newest horizon a version was dropped at. A read below it can
    /// miss the version it needs.
    pruned_below: i64,
    /// The newest ordinal at which retention evicted an entry. A read
    /// below it misses the evicted series.
    evicted_at: i64,
}

/// A cached last-value entry.
#[derive(Debug, Clone, Copy)]
pub struct LastValueEntry {
    /// Timestamp of the most recent value (milliseconds).
    pub ts: i64,
    /// The most recent metric value.
    pub value: f64,
    /// System-time ordinal of the write that set this value.
    pub ordinal: i64,
}

/// Why the cache cannot answer a read at an ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LastValueGap {
    /// The version current at the read was dropped at this horizon.
    Pruned { below: i64 },
    /// Retention evicted series at this ordinal, after the read.
    Evicted { at: i64 },
}

impl LastValueCache {
    /// Create an empty cache.
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Create a cache with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            history: HashMap::new(),
            horizon: 0,
            pruned_below: 0,
            evicted_at: 0,
        }
    }

    /// Mark the cache as built from stored rows at ordinal `at`. It holds
    /// no version older than its build, so a read below `at` is refused.
    pub fn mark_rebuilt(&mut self, at: i64) {
        self.pruned_below = self.pruned_below.max(at);
    }

    /// Set the cleanup horizon. Updates drop the versions no read at or
    /// above it reaches.
    pub fn set_horizon(&mut self, horizon: i64) {
        self.horizon = self.horizon.max(horizon);
    }

    /// Update the cache for a series written at `ordinal`. Only updates if
    /// `ts >= cached_ts`: the newest timestamp wins, and of two equal
    /// timestamps the later write.
    ///
    /// Returns `true` if the entry was updated (new or newer timestamp).
    pub fn update(&mut self, series_id: SeriesId, ts: i64, value: f64, ordinal: i64) -> bool {
        let next = LastValueEntry { ts, value, ordinal };
        match self.entries.get_mut(&series_id) {
            Some(entry) if ts >= entry.ts => {
                let replaced = std::mem::replace(entry, next);
                let versions = self.history.entry(series_id).or_default();
                versions.push(replaced);
                // Version `i` is unreachable once the version after it
                // started at or below the horizon.
                let starts: Vec<i64> = versions
                    .iter()
                    .skip(1)
                    .map(|v| v.ordinal)
                    .chain(std::iter::once(ordinal))
                    .collect();
                let unreachable = starts
                    .iter()
                    .take_while(|start| **start <= self.horizon)
                    .count();
                if unreachable > 0 {
                    versions.drain(..unreachable);
                    self.pruned_below = self.pruned_below.max(self.horizon);
                }
                if versions.is_empty() {
                    self.history.remove(&series_id);
                }
                true
            }
            Some(_) => false, // Older timestamp, skip.
            None => {
                self.entries.insert(series_id, next);
                true
            }
        }
    }

    /// O(1) lookup of the last value for a series.
    pub fn get(&self, series_id: SeriesId) -> Option<&LastValueEntry> {
        self.entries.get(&series_id)
    }

    /// The last value of a series a read at ordinal `at` sees. `Ok(None)`
    /// when the series had no value at `at`.
    pub fn get_at(
        &self,
        series_id: SeriesId,
        at: i64,
    ) -> Result<Option<&LastValueEntry>, LastValueGap> {
        self.check_reachable(at)?;
        Ok(self.version_at(series_id, at))
    }

    /// Every series' last value a read at ordinal `at` sees.
    pub fn all_at(&self, at: i64) -> Result<Vec<(SeriesId, &LastValueEntry)>, LastValueGap> {
        self.check_reachable(at)?;
        Ok(self
            .entries
            .keys()
            .filter_map(|&id| self.version_at(id, at).map(|entry| (id, entry)))
            .collect())
    }

    fn check_reachable(&self, at: i64) -> Result<(), LastValueGap> {
        if at < self.pruned_below {
            return Err(LastValueGap::Pruned {
                below: self.pruned_below,
            });
        }
        if at < self.evicted_at {
            return Err(LastValueGap::Evicted {
                at: self.evicted_at,
            });
        }
        Ok(())
    }

    /// The version of `series_id` current at `at`.
    fn version_at(&self, series_id: SeriesId, at: i64) -> Option<&LastValueEntry> {
        let current = self.entries.get(&series_id)?;
        if current.ordinal <= at {
            return Some(current);
        }
        self.history
            .get(&series_id)?
            .iter()
            .rev()
            .find(|version| version.ordinal <= at)
    }

    /// Return all cached last values. O(N) where N is distinct series count.
    pub fn all(&self) -> impl Iterator<Item = (SeriesId, &LastValueEntry)> {
        self.entries.iter().map(|(&id, entry)| (id, entry))
    }

    /// Remove a series from the cache (e.g., on TTL expiry or drop).
    pub fn remove(&mut self, series_id: SeriesId) -> bool {
        self.history.remove(&series_id);
        self.entries.remove(&series_id).is_some()
    }

    /// Evict all entries with timestamps older than `cutoff_ms`, at system
    /// time `ordinal`. A read below `ordinal` is refused from then on: the
    /// evicted series were visible to it.
    ///
    /// Returns the evicted series IDs so the caller can drop the same series
    /// from the series catalog — otherwise the catalog outlives every series it
    /// ever saw and grows without bound on a high-churn collection.
    pub fn evict_older_than(&mut self, cutoff_ms: i64, ordinal: i64) -> Vec<SeriesId> {
        let evicted: Vec<SeriesId> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.ts < cutoff_ms)
            .map(|(&id, _)| id)
            .collect();
        for id in &evicted {
            self.entries.remove(id);
            self.history.remove(id);
        }
        if !evicted.is_empty() {
            self.evicted_at = self.evicted_at.max(ordinal);
        }
        evicted
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
        let entry = std::mem::size_of::<SeriesId>() + std::mem::size_of::<LastValueEntry>() + 24;
        let versions: usize = self.history.values().map(Vec::capacity).sum();
        self.entries.capacity() * entry
            + self.history.capacity() * entry
            + versions * std::mem::size_of::<LastValueEntry>()
    }

    /// Clear all entries (e.g., on collection drop).
    pub fn clear(&mut self) {
        self.entries.clear();
        self.history.clear();
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

        assert!(cache.update(sid, 1000, 87.3, 1));
        let entry = cache.get(sid).unwrap();
        assert_eq!(entry.ts, 1000);
        assert_eq!(entry.ordinal, 1);
        assert!((entry.value - 87.3).abs() < 1e-12);
    }

    #[test]
    fn newer_timestamp_updates() {
        let mut cache = LastValueCache::new();
        let sid = sid(1);

        cache.update(sid, 1000, 10.0, 1);
        assert!(cache.update(sid, 2000, 20.0, 2));

        let entry = cache.get(sid).unwrap();
        assert_eq!(entry.ts, 2000);
        assert!((entry.value - 20.0).abs() < 1e-12);
    }

    #[test]
    fn older_timestamp_skipped() {
        let mut cache = LastValueCache::new();
        let sid = sid(1);

        cache.update(sid, 2000, 20.0, 1);
        assert!(!cache.update(sid, 1000, 10.0, 2)); // Older, should skip.

        let entry = cache.get(sid).unwrap();
        assert_eq!(entry.ts, 2000);
    }

    #[test]
    fn same_timestamp_updates() {
        let mut cache = LastValueCache::new();
        let sid = sid(1);

        cache.update(sid, 1000, 10.0, 1);
        assert!(cache.update(sid, 1000, 99.0, 2)); // Same ts, should update (last-write-wins).

        assert!((cache.get(sid).unwrap().value - 99.0).abs() < 1e-12);
    }

    #[test]
    fn multiple_series() {
        let mut cache = LastValueCache::new();
        cache.update(sid(1), 100, 1.0, 1);
        cache.update(sid(2), 200, 2.0, 1);
        cache.update(sid(3), 300, 3.0, 1);

        assert_eq!(cache.len(), 3);
        assert!((cache.get(sid(2)).unwrap().value - 2.0).abs() < 1e-12);
    }

    #[test]
    fn all_returns_all_entries() {
        let mut cache = LastValueCache::new();
        cache.update(sid(1), 100, 1.0, 1);
        cache.update(sid(2), 200, 2.0, 1);

        let all: Vec<_> = cache.all().collect();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn remove_series() {
        let mut cache = LastValueCache::new();
        let sid = sid(1);
        cache.update(sid, 100, 10.0, 1);
        assert!(cache.remove(sid));
        assert!(cache.get(sid).is_none());
        assert!(!cache.remove(sid)); // Already removed.
    }

    #[test]
    fn clear_cache() {
        let mut cache = LastValueCache::new();
        cache.update(sid(1), 100, 1.0, 1);
        cache.update(sid(2), 200, 2.0, 1);
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn nonexistent_series() {
        let cache = LastValueCache::new();
        assert!(cache.get(sid(999)).is_none());
    }

    /// A read at ordinal `S` sees the value current at `S`: neither a
    /// value written after `S` nor a series first written after it.
    #[test]
    fn a_read_at_an_ordinal_sees_the_value_current_then() {
        let mut cache = LastValueCache::new();
        cache.update(sid(1), 100, 1.0, 10);
        cache.update(sid(1), 200, 2.0, 20);
        cache.update(sid(1), 300, 3.0, 30);
        cache.update(sid(2), 50, 5.0, 25);

        let value_at = |at: i64| {
            cache
                .get_at(sid(1), at)
                .expect("reachable")
                .map(|entry| entry.value)
        };
        assert_eq!(value_at(9), None);
        assert_eq!(value_at(10), Some(1.0));
        assert_eq!(value_at(29), Some(2.0));
        assert_eq!(value_at(30), Some(3.0));
        let mut at_22: Vec<SeriesId> = cache
            .all_at(22)
            .expect("reachable")
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        at_22.sort_unstable();
        assert_eq!(at_22, vec![sid(1)], "series 2 starts after 22");
    }

    /// Versions no read reaches leave once the horizon passes their
    /// successor, and a read below the horizon is refused.
    #[test]
    fn the_horizon_drops_unreachable_versions() {
        let mut cache = LastValueCache::new();
        cache.update(sid(1), 100, 1.0, 10);
        cache.update(sid(1), 200, 2.0, 20);
        cache.set_horizon(25);
        cache.update(sid(1), 300, 3.0, 30);
        assert_eq!(
            cache
                .get_at(sid(1), 25)
                .expect("reachable")
                .map(|e| e.value),
            Some(2.0)
        );
        assert_eq!(
            cache.get_at(sid(1), 15).err(),
            Some(LastValueGap::Pruned { below: 25 })
        );
    }

    /// A read below an eviction is refused: the evicted series were
    /// visible to it.
    #[test]
    fn a_read_below_an_eviction_is_refused() {
        let mut cache = LastValueCache::new();
        cache.update(sid(1), 100, 1.0, 10);
        cache.update(sid(2), 900, 9.0, 11);
        assert_eq!(cache.evict_older_than(500, 40), vec![sid(1)]);
        assert_eq!(
            cache.all_at(39).err(),
            Some(LastValueGap::Evicted { at: 40 })
        );
        assert_eq!(cache.all_at(40).expect("reachable").len(), 1);
    }
}
