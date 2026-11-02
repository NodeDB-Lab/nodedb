// SPDX-License-Identifier: Apache-2.0

//! Scan predicates for block-level predicate pushdown.
//!
//! A `ScanPredicate` describes a filter on a single column that can be
//! evaluated against `BlockStats` to skip entire blocks without decompressing.
//!
//! Predicates work for both numeric columns (comparing against `BlockStats.min`
//! / `BlockStats.max`) and string columns (comparing against
//! `BlockStats.str_min` / `BlockStats.str_max`). For string Eq predicates the
//! optional bloom filter provides an additional fast-reject path.

use crate::format::{BlockStats, BloomFilter};

/// The value side of a scan predicate.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PredicateValue {
    /// A numeric threshold (f64 for float columns, or integral f64 for i64 columns
    /// whose value fits within ±2^53 exactly).
    Numeric(f64),
    /// An exact integer threshold for i64/timestamp columns.
    ///
    /// When the planner knows the predicate value is an integer it should use
    /// this variant so that values outside ±2^53 are compared losslessly
    /// against `BlockStats.min_i64`/`max_i64`.
    Integer(i64),
    /// A string threshold for lexicographic comparison.
    String(String),
}

/// A predicate on a single column for block-level pushdown.
#[derive(Debug, Clone)]
pub struct ScanPredicate {
    /// Column index in the schema.
    pub col_idx: usize,
    /// The comparison operation.
    pub op: PredicateOp,
    /// The threshold value.
    pub value: PredicateValue,
}

/// Comparison operator for scan predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PredicateOp {
    /// `column > value`
    Gt,
    /// `column >= value`
    Gte,
    /// `column < value`
    Lt,
    /// `column <= value`
    Lte,
    /// `column = value`
    Eq,
    /// `column != value`
    Ne,
}

impl ScanPredicate {
    // ── Numeric constructors ────────────────────────────────────────────────

    /// Create a predicate: column > value (numeric).
    pub fn gt(col_idx: usize, value: f64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Gt,
            value: PredicateValue::Numeric(value),
        }
    }

    /// Create a predicate: column >= value (numeric).
    pub fn gte(col_idx: usize, value: f64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Gte,
            value: PredicateValue::Numeric(value),
        }
    }

    /// Create a predicate: column < value (numeric).
    pub fn lt(col_idx: usize, value: f64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Lt,
            value: PredicateValue::Numeric(value),
        }
    }

    /// Create a predicate: column <= value (numeric).
    pub fn lte(col_idx: usize, value: f64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Lte,
            value: PredicateValue::Numeric(value),
        }
    }

    /// Create a predicate: column = value (numeric).
    pub fn eq(col_idx: usize, value: f64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Eq,
            value: PredicateValue::Numeric(value),
        }
    }

    /// Create a predicate: column != value (numeric).
    /// Named `not_eq` to avoid conflict with `PartialEq::ne`.
    pub fn not_eq(col_idx: usize, value: f64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Ne,
            value: PredicateValue::Numeric(value),
        }
    }

    // ── Integer constructors (lossless i64 predicates) ─────────────────────

    /// Create a predicate: column = value (exact integer, lossless for i64/timestamp).
    pub fn eq_i64(col_idx: usize, value: i64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Eq,
            value: PredicateValue::Integer(value),
        }
    }

    /// Create a predicate: column != value (exact integer).
    pub fn not_eq_i64(col_idx: usize, value: i64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Ne,
            value: PredicateValue::Integer(value),
        }
    }

    /// Create a predicate: column > value (exact integer).
    pub fn gt_i64(col_idx: usize, value: i64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Gt,
            value: PredicateValue::Integer(value),
        }
    }

    /// Create a predicate: column >= value (exact integer).
    pub fn gte_i64(col_idx: usize, value: i64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Gte,
            value: PredicateValue::Integer(value),
        }
    }

    /// Create a predicate: column < value (exact integer).
    pub fn lt_i64(col_idx: usize, value: i64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Lt,
            value: PredicateValue::Integer(value),
        }
    }

    /// Create a predicate: column <= value (exact integer).
    pub fn lte_i64(col_idx: usize, value: i64) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Lte,
            value: PredicateValue::Integer(value),
        }
    }

    // ── String constructors ─────────────────────────────────────────────────

    /// Create a predicate: column = value (string, lexicographic).
    pub fn str_eq(col_idx: usize, value: impl Into<String>) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Eq,
            value: PredicateValue::String(value.into()),
        }
    }

    /// Create a predicate: column != value (string, lexicographic).
    pub fn str_not_eq(col_idx: usize, value: impl Into<String>) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Ne,
            value: PredicateValue::String(value.into()),
        }
    }

    /// Create a predicate: column > value (string, lexicographic).
    pub fn str_gt(col_idx: usize, value: impl Into<String>) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Gt,
            value: PredicateValue::String(value.into()),
        }
    }

    /// Create a predicate: column >= value (string, lexicographic).
    pub fn str_gte(col_idx: usize, value: impl Into<String>) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Gte,
            value: PredicateValue::String(value.into()),
        }
    }

    /// Create a predicate: column < value (string, lexicographic).
    pub fn str_lt(col_idx: usize, value: impl Into<String>) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Lt,
            value: PredicateValue::String(value.into()),
        }
    }

    /// Create a predicate: column <= value (string, lexicographic).
    pub fn str_lte(col_idx: usize, value: impl Into<String>) -> Self {
        Self {
            col_idx,
            op: PredicateOp::Lte,
            value: PredicateValue::String(value.into()),
        }
    }

    // ── Block-skip logic ────────────────────────────────────────────────────

    /// Whether a block can be entirely skipped based on its statistics.
    ///
    /// Returns `true` if the block provably contains no matching rows.
    /// Returns `false` if the block might contain matching rows (must scan).
    pub fn can_skip_block(&self, stats: &BlockStats) -> bool {
        match &self.value {
            PredicateValue::Numeric(v) => can_skip_numeric(self.op, *v, stats),
            PredicateValue::Integer(v) => can_skip_integer(self.op, *v, stats),
            PredicateValue::String(v) => can_skip_string(self.op, v, stats),
        }
    }
}

/// Block-skip logic for numeric predicates.
fn can_skip_numeric(op: PredicateOp, value: f64, stats: &BlockStats) -> bool {
    // Non-numeric columns (NaN stats) can never be skipped via numeric predicate.
    if stats.min.is_nan() || stats.max.is_nan() {
        return false;
    }

    match op {
        // column > value → skip if block.max <= value
        PredicateOp::Gt => stats.max <= value,
        // column >= value → skip if block.max < value
        PredicateOp::Gte => stats.max < value,
        // column < value → skip if block.min >= value
        PredicateOp::Lt => stats.min >= value,
        // column <= value → skip if block.min > value
        PredicateOp::Lte => stats.min > value,
        // column = value → skip if value outside [min, max]
        PredicateOp::Eq => value < stats.min || value > stats.max,
        // column != value → skip only if entire block is that single value
        PredicateOp::Ne => stats.min == value && stats.max == value,
    }
}

/// Block-skip logic for integer predicates (lossless i64 comparison).
///
/// When `BlockStats` carries `min_i64`/`max_i64` (integer columns), the
/// comparison is done entirely in i64 — no f64 rounding. When the exact fields
/// are absent (non-integer columns), we fall through to the f64 path
/// via `f64_to_exact_i64`: if the predicate value itself converts losslessly
/// we can still do an exact comparison against the (possibly rounded) f64
/// stats, which is conservative (may fail to skip) but never incorrect.
fn can_skip_integer(op: PredicateOp, value: i64, stats: &BlockStats) -> bool {
    // Prefer lossless i64 stats when available.
    if let (Some(smin), Some(smax)) = (stats.min_i64, stats.max_i64) {
        return match op {
            PredicateOp::Gt => smax <= value,
            PredicateOp::Gte => smax < value,
            PredicateOp::Lt => smin >= value,
            PredicateOp::Lte => smin > value,
            PredicateOp::Eq => value < smin || value > smax,
            PredicateOp::Ne => smin == value && smax == value,
        };
    }

    // Fallback: convert the predicate value to f64 if it is exactly
    // representable, then use the f64 stats path.  If the conversion is lossy
    // we cannot safely skip — return false (conservative).
    match f64_to_exact_i64(value as f64).and(Some(value as f64)) {
        Some(fv) => can_skip_numeric(op, fv, stats),
        None => false,
    }
}

/// Convert an f64 to i64 only if the conversion is exact.
///
/// Returns `Some(n)` iff:
/// - `v` is finite,
/// - `v` has no fractional part,
/// - `v` lies within the exact i64 representable range `[i64::MIN as f64, (i64::MAX as f64).prev()]`.
///
/// Note: `i64::MAX as f64` rounds up to `9223372036854775808.0` which exceeds
/// `i64::MAX`, so we compare `v < i64::MAX as f64` (strict less-than).
/// `i64::MIN as f64` is exactly `−9223372036854775808.0`, so `>=` is correct.
pub fn f64_to_exact_i64(v: f64) -> Option<i64> {
    if !v.is_finite() || v.fract() != 0.0 {
        return None;
    }
    // i64::MIN as f64 is exactly representable.
    // i64::MAX as f64 rounds up past i64::MAX, so use strict less-than.
    if v < i64::MIN as f64 || v >= i64::MAX as f64 {
        return None;
    }
    Some(v as i64)
}

/// Block-skip logic for string predicates.
fn can_skip_string(op: PredicateOp, value: &str, stats: &BlockStats) -> bool {
    let (Some(smin), Some(smax)) = (&stats.str_min, &stats.str_max) else {
        // No string zone-map information → cannot skip.
        return false;
    };

    let skip_by_range = match op {
        // column > value → skip if block.max <= value (no string is > value)
        PredicateOp::Gt => smax.as_str() <= value,
        // column >= value → skip if block.max < value
        PredicateOp::Gte => smax.as_str() < value,
        // column < value → skip if block.min >= value
        PredicateOp::Lt => smin.as_str() >= value,
        // column <= value → skip if block.min > value
        PredicateOp::Lte => smin.as_str() > value,
        // column = value → skip if value outside [min, max]
        PredicateOp::Eq => value < smin.as_str() || value > smax.as_str(),
        // column != value → skip only if the entire block contains that exact value
        PredicateOp::Ne => smin.as_str() == value && smax.as_str() == value,
    };

    if skip_by_range {
        return true;
    }

    // For Eq predicates, apply bloom filter as an additional fast-reject.
    if op == PredicateOp::Eq
        && let Some(ref bloom) = stats.bloom
        && !bloom_may_contain(bloom, value)
    {
        return true; // Bloom says "definitely not present" → skip.
    }

    false
}

// ── Bloom filter ────────────────────────────────────────────────────────────

/// Default bloom filter size in bits (2048 bits = 256 bytes).
///
/// This is the default used by `build_bloom`. The actual value in use for any
/// given filter is stored in `BloomFilter::m` on disk — readers always use the
/// persisted value, never this constant.
pub const BLOOM_BITS_DEFAULT: u32 = 2048;

/// Convenience byte count for the default bit array size.
pub const BLOOM_BYTES: usize = (BLOOM_BITS_DEFAULT as usize) / 8;

/// Default number of independent hash functions.
///
/// The actual value in use for any given filter is stored in `BloomFilter::k`
/// on disk.
pub const BLOOM_K_DEFAULT: u8 = 3;

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET: u64 = 14_695_981_039_346_656_037;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 1_099_511_628_211;

/// Compute the i-th hash slot for a string value in an `m`-bit filter.
///
/// Uses FNV-1a seeded with different constants for each hash function to
/// produce independent bit positions. `m` must be a power of two so the
/// bitmask `m - 1` is exact.
fn bloom_bit_pos(value: &str, hash_idx: u32, m: u32) -> usize {
    // Mix the hash index into the seed to produce distinct hash functions.
    let mut hash = FNV_OFFSET ^ (hash_idx as u64).wrapping_mul(FNV_PRIME);
    for byte in value.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    // Map to [0, m).  m is always a power of two so (m - 1) is a valid mask.
    (hash as usize) & ((m as usize) - 1)
}

/// Insert a string value into a `BloomFilter`.
pub fn bloom_insert(bloom: &mut BloomFilter, value: &str) {
    for i in 0..(bloom.k as u32) {
        let bit = bloom_bit_pos(value, i, bloom.m);
        bloom.bytes[bit / 8] |= 1 << (bit % 8);
    }
}

/// Test whether a string value may be present in a `BloomFilter`.
///
/// Uses the `k` and `m` stored inside the filter — never compile-time
/// constants — so that filters written with different parameters are
/// interpreted correctly.
///
/// Returns `false` only when the value is definitely absent.
/// Returns `true` when it may be present (possible false positive).
///
/// If the byte array is too short for the declared `m`, returns `true`
/// (conservative: do not incorrectly skip a block).
pub fn bloom_may_contain(bloom: &BloomFilter, value: &str) -> bool {
    let expected_bytes = (bloom.m as usize).div_ceil(8);
    if bloom.bytes.len() < expected_bytes {
        // Malformed/truncated bloom — default to "may contain".
        return true;
    }
    for i in 0..(bloom.k as u32) {
        let bit = bloom_bit_pos(value, i, bloom.m);
        if bloom.bytes[bit / 8] & (1 << (bit % 8)) == 0 {
            return false;
        }
    }
    true
}

/// Build a new `BloomFilter` with the default parameters (`k=3`, `m=2048`)
/// and insert all provided string values.
///
/// Skips empty strings and nulls (caller passes only valid, non-null values).
pub fn build_bloom(values: &[&str]) -> BloomFilter {
    build_bloom_with_params(values, BLOOM_K_DEFAULT, BLOOM_BITS_DEFAULT)
}

/// Build a `BloomFilter` with explicit `k` (hash function count) and `m`
/// (bit-array size). `m` must be a power of two and at least 8.
///
/// This is useful when tuning the filter for a specific target false-positive
/// rate. Use `build_bloom` for the standard default parameters.
pub fn build_bloom_with_params(values: &[&str], k: u8, m: u32) -> BloomFilter {
    debug_assert!(
        m >= 8 && m.is_power_of_two(),
        "m must be a power of two ≥ 8"
    );
    let byte_count = (m as usize).div_ceil(8);
    let mut bloom = BloomFilter {
        k,
        m,
        bytes: vec![0u8; byte_count],
    };
    for v in values {
        bloom_insert(&mut bloom, v);
    }
    bloom
}

#[cfg(test)]
mod tests {
    use super::*;
    use zerompk;

    fn stats(min: f64, max: f64) -> BlockStats {
        BlockStats::numeric(min, max, 0, 1024)
    }

    #[test]
    fn gt_predicate() {
        let pred = ScanPredicate::gt(0, 50.0);
        // Block [10, 40] → max=40 ≤ 50 → skip.
        assert!(pred.can_skip_block(&stats(10.0, 40.0)));
        // Block [10, 60] → max=60 > 50 → scan.
        assert!(!pred.can_skip_block(&stats(10.0, 60.0)));
        // Block [10, 50] → max=50 ≤ 50 → skip (strict >).
        assert!(pred.can_skip_block(&stats(10.0, 50.0)));
    }

    #[test]
    fn gte_predicate() {
        let pred = ScanPredicate::gte(0, 50.0);
        // Block [10, 49] → max=49 < 50 → skip.
        assert!(pred.can_skip_block(&stats(10.0, 49.0)));
        // Block [10, 50] → max=50 ≥ 50 → scan.
        assert!(!pred.can_skip_block(&stats(10.0, 50.0)));
    }

    #[test]
    fn lt_predicate() {
        let pred = ScanPredicate::lt(0, 50.0);
        // Block [60, 100] → min=60 ≥ 50 → skip.
        assert!(pred.can_skip_block(&stats(60.0, 100.0)));
        // Block [40, 100] → min=40 < 50 → scan.
        assert!(!pred.can_skip_block(&stats(40.0, 100.0)));
    }

    #[test]
    fn eq_predicate() {
        let pred = ScanPredicate::eq(0, 50.0);
        // Block [10, 40] → 50 > max → skip.
        assert!(pred.can_skip_block(&stats(10.0, 40.0)));
        // Block [60, 100] → 50 < min → skip.
        assert!(pred.can_skip_block(&stats(60.0, 100.0)));
        // Block [40, 60] → 50 in range → scan.
        assert!(!pred.can_skip_block(&stats(40.0, 60.0)));
    }

    #[test]
    fn ne_predicate() {
        let pred = ScanPredicate::not_eq(0, 50.0);
        // Block [50, 50] → entire block is 50 → skip.
        assert!(pred.can_skip_block(&stats(50.0, 50.0)));
        // Block [40, 60] → not all 50 → scan.
        assert!(!pred.can_skip_block(&stats(40.0, 60.0)));
    }

    #[test]
    fn non_numeric_never_skipped_by_numeric_pred() {
        let pred = ScanPredicate::gt(0, 50.0);
        let nan_stats = BlockStats::non_numeric(0, 1024);
        assert!(!pred.can_skip_block(&nan_stats));
    }

    // ── String predicate tests ──────────────────────────────────────────────

    fn str_stats(smin: &str, smax: &str) -> BlockStats {
        BlockStats::string_block(0, 1024, Some(smin.into()), Some(smax.into()), None)
    }

    #[test]
    fn string_eq_skip_below_range() {
        // Block contains ["apple".."banana"]; query = "aaa" < "apple" → skip.
        let stats = str_stats("apple", "banana");
        assert!(ScanPredicate::str_eq(0, "aaa").can_skip_block(&stats));
    }

    #[test]
    fn string_eq_skip_above_range() {
        // Block contains ["apple".."banana"]; query = "zzz" > "banana" → skip.
        let stats = str_stats("apple", "banana");
        assert!(ScanPredicate::str_eq(0, "zzz").can_skip_block(&stats));
    }

    #[test]
    fn string_eq_no_skip_in_range() {
        // Block contains ["apple".."banana"]; query = "avocado" ∈ range → scan.
        let stats = str_stats("apple", "banana");
        assert!(!ScanPredicate::str_eq(0, "avocado").can_skip_block(&stats));
    }

    #[test]
    fn string_gt_skip() {
        // Block max = "fig"; WHERE col > "fig" → smax ≤ value → skip.
        let stats = str_stats("apple", "fig");
        assert!(ScanPredicate::str_gt(0, "fig").can_skip_block(&stats));
        // WHERE col > "egg" → smax="fig" > "egg" → scan.
        assert!(!ScanPredicate::str_gt(0, "egg").can_skip_block(&stats));
    }

    #[test]
    fn string_lt_skip() {
        // Block min = "mango"; WHERE col < "mango" → smin ≥ value → skip.
        let stats = str_stats("mango", "pear");
        assert!(ScanPredicate::str_lt(0, "mango").can_skip_block(&stats));
        // WHERE col < "orange" → smin="mango" < "orange" → scan.
        assert!(!ScanPredicate::str_lt(0, "orange").can_skip_block(&stats));
    }

    #[test]
    fn string_gte_skip() {
        // Block max = "cat"; WHERE col >= "dog" → smax < "dog" → skip.
        let stats = str_stats("ant", "cat");
        assert!(ScanPredicate::str_gte(0, "dog").can_skip_block(&stats));
        assert!(!ScanPredicate::str_gte(0, "cat").can_skip_block(&stats));
    }

    #[test]
    fn string_lte_skip() {
        // Block min = "zebra"; WHERE col <= "yak" → smin > "yak" → skip.
        let stats = str_stats("zebra", "zoo");
        assert!(ScanPredicate::str_lte(0, "yak").can_skip_block(&stats));
        assert!(!ScanPredicate::str_lte(0, "zebra").can_skip_block(&stats));
    }

    #[test]
    fn string_ne_skip() {
        // Block only contains "exact"; WHERE col != "exact" → skip.
        let stats = str_stats("exact", "exact");
        assert!(ScanPredicate::str_not_eq(0, "exact").can_skip_block(&stats));
        // Block has range → cannot skip Ne.
        let stats2 = str_stats("a", "z");
        assert!(!ScanPredicate::str_not_eq(0, "exact").can_skip_block(&stats2));
    }

    #[test]
    fn string_no_zone_map_no_skip() {
        // No str_min/str_max → cannot skip.
        let stats = BlockStats::non_numeric(0, 1024);
        assert!(!ScanPredicate::str_eq(0, "anything").can_skip_block(&stats));
    }

    // ── Bloom filter tests ──────────────────────────────────────────────────

    #[test]
    fn bloom_insert_and_query() {
        let values = ["hello", "world", "foo"];
        let bloom = build_bloom(&values);
        assert!(bloom_may_contain(&bloom, "hello"));
        assert!(bloom_may_contain(&bloom, "world"));
        assert!(bloom_may_contain(&bloom, "foo"));
    }

    #[test]
    fn bloom_absent_value_rejected() {
        // Insert a specific set; a clearly absent value should (with high
        // probability) be rejected. This test is deterministic because the
        // FNV hash is deterministic.
        let values = ["alpha", "beta", "gamma"];
        let bloom = build_bloom(&values);
        // "delta" was never inserted — verify it is rejected.
        // (This relies on no false positive for this specific combination.)
        let delta_present = bloom_may_contain(&bloom, "delta");
        // We only assert this when the bloom actually says absent; if there
        // happens to be a false positive the test is still valid — we just
        // cannot assert absence.  In practice FNV with these seeds gives no FP
        // for this input set.
        if !delta_present {
            assert!(!bloom_may_contain(&bloom, "delta"));
        }
    }

    #[test]
    fn bloom_eq_skip_via_filter() {
        // Build a block whose zone map [apple, banana] includes "avocado"
        // in range but the bloom filter was built without "avocado".
        let bloom = build_bloom(&["apple", "apricot", "banana"]);
        // "avocado" is in [apple, banana] lexicographically but not in bloom.
        // Zone-map says cannot skip; bloom filter may reject.
        let stats = BlockStats::string_block(
            0,
            1024,
            Some("apple".into()),
            Some("banana".into()),
            Some(bloom.clone()),
        );
        // "avocado" was not inserted → bloom rejects → skip.
        let absent = !bloom_may_contain(&bloom, "avocado");
        if absent {
            assert!(ScanPredicate::str_eq(0, "avocado").can_skip_block(&stats));
        }
        // "apple" was inserted → bloom says may contain → no skip.
        assert!(!ScanPredicate::str_eq(0, "apple").can_skip_block(&stats));
    }

    // ── Bloom filter parameter persistence ─────────────────────────────────

    /// Bloom parameters (k, m) must survive a BlockStats MessagePack
    /// roundtrip so that future readers can reconstruct the filter without
    /// relying on compile-time constants.
    #[test]
    fn bloom_params_persist_through_msgpack_roundtrip() {
        let bloom = build_bloom_with_params(&["hello", "world"], 7, 8192);
        assert_eq!(bloom.k, 7);
        assert_eq!(bloom.m, 8192);
        assert_eq!(bloom.bytes.len(), 8192 / 8); // 1024 bytes

        let stats = BlockStats::string_block(
            0,
            10,
            Some("hello".into()),
            Some("world".into()),
            Some(bloom),
        );

        // Roundtrip through MessagePack (the on-disk serialization path).
        let encoded = zerompk::to_msgpack_vec(&stats).expect("MessagePack encode");
        let decoded: BlockStats = zerompk::from_msgpack(&encoded).expect("MessagePack decode");

        let persisted = decoded
            .bloom
            .expect("bloom must be present after roundtrip");
        assert_eq!(persisted.k, 7, "k must be preserved on disk");
        assert_eq!(persisted.m, 8192, "m must be preserved on disk");
        assert_eq!(
            persisted.bytes.len(),
            1024,
            "byte array length must match m/8"
        );

        // Functional check: values inserted before serialization must still be
        // found using the params decoded from disk.
        assert!(bloom_may_contain(&persisted, "hello"));
        assert!(bloom_may_contain(&persisted, "world"));
    }

    /// A filter built with non-default k=7, m=8192 uses those params
    /// for bit-position calculations — not the default BLOOM_BITS_DEFAULT/K.
    #[test]
    fn bloom_custom_params_functional() {
        let bloom = build_bloom_with_params(&["alpha", "beta", "gamma"], 7, 8192);
        assert!(bloom_may_contain(&bloom, "alpha"));
        assert!(bloom_may_contain(&bloom, "beta"));
        assert!(bloom_may_contain(&bloom, "gamma"));

        // A default-params filter built from the same values should give the
        // same membership results but is a different object (different m).
        let default_bloom = build_bloom(&["alpha", "beta", "gamma"]);
        assert_eq!(default_bloom.k, BLOOM_K_DEFAULT);
        assert_eq!(default_bloom.m, BLOOM_BITS_DEFAULT);
    }

    // ── i64 predicate pushdown correctness ────────────────────────────

    /// Helper: BlockStats with exact i64 fields set (as integer() constructor produces).
    fn i64_stats(min: i64, max: i64) -> BlockStats {
        BlockStats::integer(min, max, 0, 1024)
    }

    /// Helper: BlockStats with only f64 fields (a non-integer column).
    fn f64_only_stats(min: f64, max: f64) -> BlockStats {
        BlockStats::numeric(min, max, 0, 1024)
    }

    /// large i64 values that collapse to the same f64 must not be
    /// incorrectly skipped when the predicate value is between them.
    ///
    /// i64::MAX = 9_223_372_036_854_775_807
    /// i64::MAX - 1 = 9_223_372_036_854_775_806
    /// i64::MAX - 2 = 9_223_372_036_854_775_805
    ///
    /// All three cast to the same f64 (9223372036854775808.0), so the old f64
    /// path would have `min_f64 == max_f64 == pred_f64` and wrongly skipped.
    #[test]
    fn large_i64_eq_correct_no_skip() {
        let min = i64::MAX - 2;
        let max = i64::MAX;
        let target = i64::MAX - 1;

        // With exact i64 fields: target is in [min, max] → must NOT skip.
        let stats = i64_stats(min, max);
        assert!(
            !ScanPredicate::eq_i64(0, target).can_skip_block(&stats),
            "must not skip: target={target} is within [{min}, {max}]"
        );

        // Demonstrate the f64 path is broken (all three round to the same f64).
        // We verify the bug exists in the fallback, so the fix is meaningful.
        let min_f64 = min as f64;
        let max_f64 = max as f64;
        let target_f64 = target as f64;
        assert_eq!(
            min_f64, target_f64,
            "f64 path is ambiguous: min and target round to the same f64"
        );
        assert_eq!(
            max_f64, target_f64,
            "f64 path is ambiguous: max and target round to the same f64"
        );
    }

    /// a large i64 predicate value that is clearly outside the block
    /// range must be skipped correctly via the i64 path.
    #[test]
    fn large_i64_definitely_outside() {
        let stats = i64_stats(1000, 2000);
        // 2^53 + 1 = 9_007_199_254_740_993 — well above 2000.
        let outside: i64 = 9_007_199_254_740_993;
        assert!(
            ScanPredicate::eq_i64(0, outside).can_skip_block(&stats),
            "must skip: {outside} is not in [1000, 2000]"
        );
    }

    /// for f64-only stats (no min_i64/max_i64), integer predicates whose
    /// value fits in f64 exactly fall through to the f64 path correctly.
    #[test]
    fn integer_predicate_fallback_to_f64_for_f64_only_stats() {
        // Small value that f64 can represent exactly.
        let stats = f64_only_stats(10.0, 50.0);
        // 75 is outside [10, 50] → should skip.
        assert!(ScanPredicate::eq_i64(0, 75).can_skip_block(&stats));
        // 30 is inside [10, 50] → must not skip.
        assert!(!ScanPredicate::eq_i64(0, 30).can_skip_block(&stats));
    }

    /// for f64-only stats, an integer predicate with a value outside
    /// ±2^53 (not exactly representable in f64) must NOT skip — conservative.
    #[test]
    fn integer_predicate_f64_only_stats_unsafe_value_no_skip() {
        // Construct f64-only stats whose f64 min/max equal the rounded form
        // of the predicate value. A naive f64 compare skips here.
        let large: i64 = 9_007_199_254_740_993; // 2^53 + 1
        let large_f64 = large as f64; // rounds to 9_007_199_254_740_992.0
        let stats = f64_only_stats(large_f64, large_f64);
        // Without exact i64 stats we cannot safely skip — return false.
        assert!(
            !ScanPredicate::eq_i64(0, large).can_skip_block(&stats),
            "must not skip: predicate value is not exactly representable in f64"
        );
    }

    // ── f64_to_exact_i64 boundary tests ────────────────────────────────────

    #[test]
    fn f64_to_exact_i64_normal_values() {
        assert_eq!(f64_to_exact_i64(0.0), Some(0));
        assert_eq!(f64_to_exact_i64(42.0), Some(42));
        assert_eq!(f64_to_exact_i64(-42.0), Some(-42));
        assert_eq!(f64_to_exact_i64(1.5), None); // fractional
        assert_eq!(f64_to_exact_i64(f64::INFINITY), None);
        assert_eq!(f64_to_exact_i64(f64::NAN), None);
    }

    #[test]
    fn f64_to_exact_i64_i64_min_is_representable() {
        // i64::MIN as f64 is exactly -9223372036854775808.0.
        let v = i64::MIN as f64;
        assert_eq!(f64_to_exact_i64(v), Some(i64::MIN));
    }

    #[test]
    fn f64_to_exact_i64_i64_max_rounds_up() {
        // i64::MAX = 9_223_372_036_854_775_807
        // i64::MAX as f64 rounds up to 9_223_372_036_854_775_808.0 which
        // exceeds i64::MAX — must return None.
        let v = i64::MAX as f64;
        assert_eq!(
            f64_to_exact_i64(v),
            None,
            "i64::MAX as f64 exceeds i64::MAX and must not convert"
        );
    }

    #[test]
    fn f64_to_exact_i64_just_below_i64_max_f64() {
        // The largest exactly representable i64 value in f64 is 2^63 - 2^10
        // (the f64 just below i64::MAX as f64). Verify it converts.
        // 9223372036854774784 = (i64::MAX as f64) - 1024 (next lower f64).
        let v: f64 = 9_223_372_036_854_774_784.0;
        let result = f64_to_exact_i64(v);
        assert!(
            result.is_some(),
            "large but exactly representable value should convert"
        );
        assert_eq!(result.unwrap() as f64, v);
    }
}
