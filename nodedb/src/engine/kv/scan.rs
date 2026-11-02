// SPDX-License-Identifier: BUSL-1.1

//! Cursor-based KV scan with optional glob pattern matching.

use nodedb_types::Surrogate;

use super::entry::KvEntry;
use super::hash_table::KvHashTable;

/// Result of a scan: `(entries, next_cursor_idx)`.
/// Entries are `(key_bytes, value_bytes)` references. `next_cursor_idx = 0` means scan complete.
pub type ScanBatch<'a> = (Vec<(&'a [u8], &'a [u8])>, usize);

/// Scan batch that also surfaces each row's stable surrogate.  Consumed
/// by the clone-delegated read path (snapshot-isolation ceiling filter).
pub type ScanBatchWithSurrogate<'a> = (Vec<(&'a [u8], &'a [u8], Surrogate)>, usize);

/// Parameters for a cursor-based [`super::engine::KvEngine::scan`].
///
/// Bundles all per-scan inputs so the method stays argument-count clean.
/// `surrogate_ceiling` enforces clone snapshot isolation: rows whose
/// surrogate exceeds the ceiling are hidden from the caller.
pub struct KvScanParams<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub cursor: &'a [u8],
    pub count: usize,
    pub now_ms: u64,
    pub match_pattern: Option<&'a str>,
    pub filter_field: Option<&'a str>,
    pub filter_value: Option<&'a [u8]>,
    /// `None` = no ceiling (full visibility). `Some(c)` = hide rows with surrogate > c.
    pub surrogate_ceiling: Option<u32>,
}

impl KvHashTable {
    /// Cursor-based scan: iterate entries starting from `cursor_idx`.
    ///
    /// Returns `(entries, next_cursor)`. `entries` contains up to `count`
    /// non-expired `(key, value)` pairs. `next_cursor` is `0` if the scan
    /// is complete, otherwise the next slot index to resume from.
    ///
    /// Scans the primary table first, then the rehash source (if active).
    /// Optionally filters keys by a glob-like prefix pattern.
    pub fn scan(
        &self,
        cursor_idx: usize,
        count: usize,
        now_ms: u64,
        match_pattern: Option<&str>,
    ) -> ScanBatch<'_> {
        let (rows, next) = self.scan_with_surrogate(cursor_idx, count, now_ms, match_pattern);
        (rows.into_iter().map(|(k, v, _)| (k, v)).collect(), next)
    }

    /// Like [`scan`], but surfaces each row's surrogate alongside its
    /// key/value so a caller can filter by clone snapshot-isolation
    /// ceiling (drop rows bound after the AS-OF point).
    pub fn scan_with_surrogate(
        &self,
        cursor_idx: usize,
        count: usize,
        now_ms: u64,
        match_pattern: Option<&str>,
    ) -> ScanBatchWithSurrogate<'_> {
        // Compute the real slot ceiling first so the capacity hint never
        // exceeds the number of rows that actually exist in the table.
        // `count` may be a large budget-derived ceiling (e.g. ~33 M for a
        // 512 MiB budget); blindly passing it to `with_capacity` would
        // eagerly allocate hundreds of MB even for a tiny collection.
        let total_slots = self.capacity() + self.rehash_source_len();
        let mut results = Vec::with_capacity(count.min(total_slots));

        let mut idx = cursor_idx;
        while results.len() < count && idx < total_slots {
            let entry = self.slot_at(idx);

            if let Some(e) = entry
                && !e.is_expired(now_ms)
                && matches_pattern(&e.key, match_pattern)
            {
                let value = self.read_value(e);
                results.push((e.key.as_slice(), value, e.surrogate));
            }
            idx += 1;
        }

        let next_cursor = if idx >= total_slots { 0 } else { idx };
        (results, next_cursor)
    }

    /// Streaming variant of [`scan_with_surrogate`]: invokes `f(key, value,
    /// surrogate)` for each visited row instead of collecting into a `Vec`.
    ///
    /// Walks the SAME unified slot range (primary table first, then rehash
    /// source) in the SAME order, applies the SAME expiry + pattern filtering,
    /// and respects the SAME `count` cutoff as `scan_with_surrogate`. The rows
    /// passed to `f` (and their order) are therefore identical to the rows the
    /// `Vec` variant would have produced for the same arguments.
    ///
    /// Returns the next cursor index (`0` when the scan is complete), exactly
    /// like `scan_with_surrogate`. If `f` returns `Err`, iteration stops
    /// immediately and the error is propagated — rows are never silently
    /// dropped, and the callback error is never swallowed into `Ok`.
    ///
    /// [`scan_with_surrogate`]: KvHashTable::scan_with_surrogate
    // consumed (via `KvEngine::scan_for_each`) by `scan_collection_for_each`
    pub fn scan_with_surrogate_for_each<F>(
        &self,
        cursor_idx: usize,
        count: usize,
        now_ms: u64,
        match_pattern: Option<&str>,
        mut f: F,
    ) -> crate::Result<usize>
    where
        F: FnMut(&[u8], &[u8], Surrogate) -> crate::Result<()>,
    {
        let total_slots = self.capacity() + self.rehash_source_len();

        let mut emitted = 0usize;
        let mut idx = cursor_idx;
        while emitted < count && idx < total_slots {
            let entry = self.slot_at(idx);

            if let Some(e) = entry
                && !e.is_expired(now_ms)
                && matches_pattern(&e.key, match_pattern)
            {
                let value = self.read_value(e);
                f(e.key.as_slice(), value, e.surrogate)?;
                emitted += 1;
            }
            idx += 1;
        }

        let next_cursor = if idx >= total_slots { 0 } else { idx };
        Ok(next_cursor)
    }

    /// Access a slot by unified index (primary first, then rehash source).
    fn slot_at(&self, idx: usize) -> Option<&KvEntry> {
        let cap = self.capacity();
        if idx < cap {
            self.primary_slot(idx)
        } else {
            self.rehash_slot(idx - cap)
        }
    }
}

/// Public entry point for pattern matching (used by engine.rs index scan path).
pub fn matches_pattern_pub(key: &[u8], pattern: Option<&str>) -> bool {
    matches_pattern(key, pattern)
}

/// Simple glob-style pattern matching for SCAN MATCH.
///
/// Supports `*` (match any sequence) and `?` (match single byte).
/// `None` pattern matches everything.
fn matches_pattern(key: &[u8], pattern: Option<&str>) -> bool {
    let Some(pattern) = pattern else {
        return true;
    };
    glob_match(pattern.as_bytes(), key)
}

/// Glob matching: `*` matches zero or more bytes, `?` matches exactly one byte.
pub(crate) fn glob_match(pattern: &[u8], input: &[u8]) -> bool {
    let mut pi = 0;
    let mut ii = 0;
    let mut star_pi = usize::MAX;
    let mut star_ii = 0;

    while ii < input.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == input[ii]) {
            pi += 1;
            ii += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_ii = ii;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ii += 1;
            ii = star_ii;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::engine::kv::entry::NO_EXPIRY;

    #[test]
    fn glob_basic() {
        assert!(glob_match(b"key_*", b"key_12345"));
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"", b""));
        assert!(!glob_match(b"", b"notempty"));
        assert!(glob_match(b"exact", b"exact"));
        assert!(!glob_match(b"exact", b"wrong"));
    }

    #[test]
    fn glob_question_mark() {
        assert!(glob_match(b"key_??", b"key_ab"));
        assert!(!glob_match(b"key_??", b"key_a"));
        assert!(!glob_match(b"key_??", b"key_abc"));
    }

    #[test]
    fn glob_star_in_middle() {
        assert!(glob_match(b"a*z", b"abcz"));
        assert!(glob_match(b"a*z", b"az"));
        assert!(!glob_match(b"a*z", b"abcx"));
    }

    #[test]
    fn matches_pattern_none_matches_all() {
        assert!(matches_pattern(b"anything", None));
    }

    #[test]
    fn scan_basic() {
        let mut t = KvHashTable::new(16, 0.75, 4, 64);
        for i in 0..5u8 {
            t.put(
                &[i],
                &[i * 10],
                NO_EXPIRY,
                Surrogate::new(u32::from(i) + 1),
                1,
            );
        }

        let (entries, next) = t.scan(0, 100, 0, None);
        assert_eq!(entries.len(), 5);
        assert_eq!(next, 0); // Scan complete.
    }

    #[test]
    fn scan_with_count_limit() {
        let mut t = KvHashTable::new(16, 0.75, 4, 64);
        for i in 0..10u8 {
            t.put(
                &[i],
                &[i * 10],
                NO_EXPIRY,
                Surrogate::new(u32::from(i) + 1),
                1,
            );
        }

        let (entries, next) = t.scan(0, 3, 0, None);
        assert_eq!(entries.len(), 3);
        assert_ne!(next, 0); // More entries available.

        // Continue from cursor.
        let (entries2, _) = t.scan(next, 100, 0, None);
        assert_eq!(entries.len() + entries2.len(), 10);
    }

    #[test]
    fn scan_skips_expired() {
        let mut t = KvHashTable::new(16, 0.75, 4, 64);
        t.put(b"alive", b"v", NO_EXPIRY, Surrogate::new(1), 1);
        t.put(b"dead", b"v", 500, Surrogate::new(2), 1); // Expires at 500.

        let (entries, _) = t.scan(0, 100, 1000, None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, b"alive");
    }
}
