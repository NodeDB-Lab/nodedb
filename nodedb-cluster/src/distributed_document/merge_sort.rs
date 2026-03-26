//! Distributed ORDER BY + LIMIT merge for document scans.
//!
//! Each shard applies ORDER BY + LIMIT locally and returns its top-N rows.
//! The coordinator performs an N-way merge sort on the (shards × N) rows
//! and returns the global top-N.
//!
//! This is NOT simple concatenation — Shard A's top-10 might all rank
//! below Shard B's top-10 globally.

use serde::{Deserialize, Serialize};

/// A row from a shard, with a sort key for merge-sorting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardRow {
    /// The row payload (JSON bytes or MessagePack).
    pub payload: Vec<u8>,
    /// Sort key extracted from the ORDER BY column(s).
    /// Encoded as comparable bytes (big-endian for numbers, UTF-8 for strings).
    pub sort_key: Vec<u8>,
    /// Which shard produced this row.
    pub shard_id: u16,
}

/// Sort direction for ORDER BY.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// N-way merge-sort merger for distributed ORDER BY + LIMIT.
pub struct OrderByMerger {
    /// All rows from all shards, unsorted.
    rows: Vec<ShardRow>,
    /// Sort direction.
    direction: SortDirection,
}

impl OrderByMerger {
    pub fn new(direction: SortDirection) -> Self {
        Self {
            rows: Vec::new(),
            direction,
        }
    }

    /// Add a shard's locally-sorted, locally-limited rows.
    pub fn add_shard_rows(&mut self, rows: Vec<ShardRow>) {
        self.rows.extend(rows);
    }

    /// Perform the global merge sort and apply the global LIMIT.
    ///
    /// Each shard already applied `ORDER BY + LIMIT` locally, so we have
    /// at most `num_shards × limit` rows. The global sort + limit produces
    /// the correct result.
    pub fn merge(&mut self, global_limit: usize) -> Vec<ShardRow> {
        match self.direction {
            SortDirection::Ascending => {
                self.rows.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
            }
            SortDirection::Descending => {
                self.rows.sort_by(|a, b| b.sort_key.cmp(&a.sort_key));
            }
        }
        self.rows.truncate(global_limit);
        self.rows.clone()
    }

    /// Total rows collected before merge.
    pub fn total_rows(&self) -> usize {
        self.rows.len()
    }
}

/// Encode a sort key from a typed value for byte-comparable ordering.
///
/// Numbers are encoded big-endian with sign flip for correct ordering.
/// Strings are encoded as UTF-8 (natural lexicographic order).
pub fn encode_sort_key_i64(value: i64) -> Vec<u8> {
    // Flip sign bit so negative < positive in unsigned byte ordering.
    let unsigned = (value as u64) ^ (1u64 << 63);
    unsigned.to_be_bytes().to_vec()
}

pub fn encode_sort_key_f64(value: f64) -> Vec<u8> {
    let bits = value.to_bits();
    // IEEE 754 float ordering trick: flip all bits if negative, flip sign bit if positive.
    let ordered = if bits >> 63 == 1 {
        !bits // Negative: flip all bits.
    } else {
        bits | (1u64 << 63) // Positive: flip sign bit.
    };
    ordered.to_be_bytes().to_vec()
}

pub fn encode_sort_key_string(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_sort_ascending() {
        let mut merger = OrderByMerger::new(SortDirection::Ascending);

        // Shard 0: ages [20, 30, 40] (locally sorted, limit 3).
        merger.add_shard_rows(vec![
            ShardRow {
                payload: b"alice".to_vec(),
                sort_key: encode_sort_key_i64(20),
                shard_id: 0,
            },
            ShardRow {
                payload: b"bob".to_vec(),
                sort_key: encode_sort_key_i64(30),
                shard_id: 0,
            },
            ShardRow {
                payload: b"carol".to_vec(),
                sort_key: encode_sort_key_i64(40),
                shard_id: 0,
            },
        ]);

        // Shard 1: ages [15, 25, 35] (locally sorted, limit 3).
        merger.add_shard_rows(vec![
            ShardRow {
                payload: b"dave".to_vec(),
                sort_key: encode_sort_key_i64(15),
                shard_id: 1,
            },
            ShardRow {
                payload: b"eve".to_vec(),
                sort_key: encode_sort_key_i64(25),
                shard_id: 1,
            },
            ShardRow {
                payload: b"frank".to_vec(),
                sort_key: encode_sort_key_i64(35),
                shard_id: 1,
            },
        ]);

        let result = merger.merge(3); // Global LIMIT 3.
        assert_eq!(result.len(), 3);
        // Youngest 3: dave(15), alice(20), eve(25).
        assert_eq!(result[0].payload, b"dave");
        assert_eq!(result[1].payload, b"alice");
        assert_eq!(result[2].payload, b"eve");
    }

    #[test]
    fn merge_sort_descending() {
        let mut merger = OrderByMerger::new(SortDirection::Descending);

        merger.add_shard_rows(vec![
            ShardRow {
                payload: b"a".to_vec(),
                sort_key: encode_sort_key_i64(100),
                shard_id: 0,
            },
            ShardRow {
                payload: b"b".to_vec(),
                sort_key: encode_sort_key_i64(50),
                shard_id: 0,
            },
        ]);
        merger.add_shard_rows(vec![
            ShardRow {
                payload: b"c".to_vec(),
                sort_key: encode_sort_key_i64(90),
                shard_id: 1,
            },
            ShardRow {
                payload: b"d".to_vec(),
                sort_key: encode_sort_key_i64(10),
                shard_id: 1,
            },
        ]);

        let result = merger.merge(2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].payload, b"a"); // 100 (highest)
        assert_eq!(result[1].payload, b"c"); // 90
    }

    #[test]
    fn sort_key_i64_ordering() {
        let neg = encode_sort_key_i64(-100);
        let zero = encode_sort_key_i64(0);
        let pos = encode_sort_key_i64(100);
        assert!(neg < zero);
        assert!(zero < pos);
    }

    #[test]
    fn sort_key_f64_ordering() {
        let neg = encode_sort_key_f64(-1.5);
        let zero = encode_sort_key_f64(0.0);
        let pos = encode_sort_key_f64(1.5);
        assert!(neg < zero);
        assert!(zero < pos);
    }

    #[test]
    fn sort_key_string_ordering() {
        let a = encode_sort_key_string("alice");
        let b = encode_sort_key_string("bob");
        assert!(a < b);
    }
}
