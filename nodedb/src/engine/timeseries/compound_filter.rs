//! Compound tag filter optimization with inverted index for hot tag combinations.
//!
//! For queries like `WHERE host = 'web-1' AND region = 'us-east'`, the current
//! approach is bitmap AND of two symbol bitmap indexes. This works but scans
//! all bitmaps.
//!
//! This module adds an inverted index for frequently-queried tag combinations:
//! `(tag_value_combo) → [row_ids]`. When a compound filter matches < 1% of rows,
//! the inverted index is faster than bitmap AND.
//!
//! Hot combos are auto-detected from query patterns.

use std::collections::HashMap;

/// A compound tag key: sorted vector of (column_index, symbol_id) pairs.
pub type CompoundKey = Vec<(usize, u32)>;

/// Inverted index for compound tag combinations.
///
/// Maps compound keys to sorted vectors of matching row indices.
pub struct CompoundTagIndex {
    /// `compound_key → sorted row indices`.
    index: HashMap<CompoundKey, Vec<u32>>,
    /// Total row count in the indexed partition.
    row_count: u32,
    /// Query frequency tracker: `compound_key → hit_count`.
    query_stats: HashMap<CompoundKey, u64>,
}

impl CompoundTagIndex {
    pub fn new(row_count: u32) -> Self {
        Self {
            index: HashMap::new(),
            row_count,
            query_stats: HashMap::new(),
        }
    }

    /// Build a compound index from multiple symbol columns.
    ///
    /// `columns` is `[(column_index, &[u32])]` — each entry is a symbol column
    /// with its schema index and the u32 symbol IDs for all rows.
    pub fn build(columns: &[(usize, &[u32])], row_count: usize) -> Self {
        let mut index: HashMap<CompoundKey, Vec<u32>> = HashMap::new();

        for row in 0..row_count {
            let key: CompoundKey = columns
                .iter()
                .map(|&(col_idx, values)| (col_idx, values[row]))
                .collect();

            index.entry(key).or_default().push(row as u32);
        }

        Self {
            index,
            row_count: row_count as u32,
            query_stats: HashMap::new(),
        }
    }

    /// Look up rows matching a compound key.
    ///
    /// Returns `Some(row_indices)` if the compound key is indexed,
    /// `None` if not (caller should fall back to bitmap AND).
    pub fn lookup(&mut self, key: &CompoundKey) -> Option<&[u32]> {
        self.query_stats
            .entry(key.clone())
            .and_modify(|c| *c += 1)
            .or_insert(1);

        self.index.get(key).map(|v| v.as_slice())
    }

    /// Check if using the compound index is worthwhile for this key.
    ///
    /// Returns true if the compound key matches < 1% of rows
    /// (high selectivity — inverted index is faster than bitmap scan).
    pub fn should_use_compound(&self, key: &CompoundKey) -> bool {
        match self.index.get(key) {
            Some(rows) => {
                let selectivity = rows.len() as f64 / self.row_count as f64;
                selectivity < 0.01 // < 1% of rows
            }
            None => false, // Key not indexed → fall back.
        }
    }

    /// Get the top N most frequently queried compound keys.
    pub fn hot_combos(&self, n: usize) -> Vec<(CompoundKey, u64)> {
        let mut combos: Vec<(CompoundKey, u64)> = self
            .query_stats
            .iter()
            .map(|(k, &v)| (k.clone(), v))
            .collect();
        combos.sort_by_key(|c| std::cmp::Reverse(c.1));
        combos.truncate(n);
        combos
    }

    /// Number of distinct compound keys indexed.
    pub fn key_count(&self) -> usize {
        self.index.len()
    }

    /// Total rows indexed.
    pub fn total_rows(&self) -> u32 {
        self.row_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_lookup() {
        // 10 rows, 2 tag columns: host (col 1) and region (col 2).
        let hosts: Vec<u32> = vec![0, 0, 1, 1, 0, 0, 1, 1, 0, 1]; // 0=web-1, 1=web-2
        let regions: Vec<u32> = vec![0, 0, 0, 1, 1, 0, 1, 0, 1, 1]; // 0=us-east, 1=eu-west

        let columns = vec![(1, hosts.as_slice()), (2, regions.as_slice())];
        let mut idx = CompoundTagIndex::build(&columns, 10);

        // host=0 AND region=0 → rows [0, 1, 5]
        let key = vec![(1, 0), (2, 0)];
        let rows = idx.lookup(&key).unwrap();
        assert_eq!(rows, &[0, 1, 5]);

        // host=1 AND region=1 → rows [3, 6, 9]
        let key = vec![(1, 1), (2, 1)];
        let rows = idx.lookup(&key).unwrap();
        assert_eq!(rows, &[3, 6, 9]);
    }

    #[test]
    fn selectivity_check() {
        let hosts: Vec<u32> = (0..10_000).map(|i| (i % 100) as u32).collect();
        let regions: Vec<u32> = (0..10_000).map(|i| (i % 10) as u32).collect();

        let columns = vec![(1, hosts.as_slice()), (2, regions.as_slice())];
        let _idx = CompoundTagIndex::build(&columns, 10_000);

        // Use 1000 hosts × 1000 regions = 1M combos. Each combo appears ~0.01 times
        // on average → most combos have 0 or 1 row → high selectivity.
        let hosts2: Vec<u32> = (0..10_000).map(|i| (i % 1000) as u32).collect();
        let regions2: Vec<u32> = (0..10_000).map(|i| (i % 1000) as u32).collect();
        let columns2 = vec![(1, hosts2.as_slice()), (2, regions2.as_slice())];
        let idx2 = CompoundTagIndex::build(&columns2, 10_000);

        // (host=42, region=42): only row 42, 1042, 2042, ... = 10 rows.
        // 10/10000 = 0.1% < 1%.
        let key = vec![(1, 42), (2, 42)];
        assert!(idx2.should_use_compound(&key));
    }

    #[test]
    fn hot_combos_tracking() {
        let hosts: Vec<u32> = vec![0, 1, 0, 1];
        let columns = vec![(1, hosts.as_slice())];
        let mut idx = CompoundTagIndex::build(&columns, 4);

        let key = vec![(1, 0)];
        idx.lookup(&key);
        idx.lookup(&key);
        idx.lookup(&key);

        let hot = idx.hot_combos(1);
        assert_eq!(hot.len(), 1);
        assert_eq!(hot[0].1, 3); // Queried 3 times.
    }

    #[test]
    fn nonexistent_key() {
        let hosts: Vec<u32> = vec![0, 1, 0, 1];
        let columns = vec![(1, hosts.as_slice())];
        let mut idx = CompoundTagIndex::build(&columns, 4);

        let key = vec![(1, 99)]; // Doesn't exist.
        assert!(idx.lookup(&key).is_none());
    }
}
