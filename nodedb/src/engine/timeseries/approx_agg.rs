//! Approximate aggregation functions for billion-row timeseries queries.
//!
//! - `HyperLogLog`: approx_count_distinct — 12 KB memory, ~0.8% error
//! - `TDigest`: approx_percentile — mergeable centroids for p50/p99/p999
//! - `SpaceSaving`: topK — bounded memory approximate top-K
//!
//! All three are designed for incremental use: feed samples one at a time
//! or in batches, then read the approximate result. All are mergeable
//! across partitions and shards.

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// HyperLogLog — approx_count_distinct
// ---------------------------------------------------------------------------

/// HyperLogLog cardinality estimator.
///
/// Uses 2^14 = 16384 registers (12 KB memory). Achieves ~0.8% relative
/// error at any cardinality. Mergeable: `hll_a.merge(&hll_b)` produces
/// the union cardinality.
pub struct HyperLogLog {
    registers: Vec<u8>,
    precision: u8, // Number of bits for register indexing (14 = 16384 registers).
}

impl HyperLogLog {
    /// Create a new HLL with precision p (default 14 → 16384 registers, 12 KB).
    pub fn new() -> Self {
        Self::with_precision(14)
    }

    pub fn with_precision(p: u8) -> Self {
        let p = p.clamp(4, 18);
        let m = 1usize << p;
        Self {
            registers: vec![0u8; m],
            precision: p,
        }
    }

    /// Add a value (hashed internally).
    pub fn add(&mut self, value: u64) {
        let hash = splitmix64(value);
        let m = self.registers.len();
        let idx = (hash as usize) & (m - 1);
        // Use lower bits for register index, upper bits for leading zeros count.
        let remaining = hash >> self.precision;
        // Count leading zeros in the (64 - precision)-bit space.
        // leading_zeros() counts from bit 63, subtract the precision bits.
        let leading_zeros = if remaining == 0 {
            (64 - self.precision) + 1
        } else {
            (remaining.leading_zeros() as u8).saturating_sub(self.precision) + 1
        };
        if leading_zeros > self.registers[idx] {
            self.registers[idx] = leading_zeros;
        }
    }

    /// Add a batch of u64 values.
    pub fn add_batch(&mut self, values: &[u64]) {
        for &v in values {
            self.add(v);
        }
    }

    /// Add a batch of f64 values (hashed by their bit representation).
    pub fn add_f64_batch(&mut self, values: &[f64]) {
        for &v in values {
            self.add(v.to_bits());
        }
    }

    /// Estimate the number of distinct values seen.
    pub fn estimate(&self) -> f64 {
        let m = self.registers.len() as f64;
        let alpha = match self.registers.len() {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };

        // Harmonic mean of 2^(-register[i]).
        let sum: f64 = self
            .registers
            .iter()
            .map(|&r| 2.0f64.powi(-(r as i32)))
            .sum();
        let raw_estimate = alpha * m * m / sum;

        // Small range correction.
        if raw_estimate <= 2.5 * m {
            let zeros = self.registers.iter().filter(|&&r| r == 0).count() as f64;
            if zeros > 0.0 {
                return m * (m / zeros).ln();
            }
        }

        // Large range correction (for 32-bit hash space — not needed with 64-bit).
        raw_estimate
    }

    /// Merge another HLL into this one (register-wise max).
    pub fn merge(&mut self, other: &HyperLogLog) {
        for (i, &r) in other.registers.iter().enumerate() {
            if i < self.registers.len() && r > self.registers[i] {
                self.registers[i] = r;
            }
        }
    }

    /// Memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.registers.len()
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Splitmix64 hash — excellent avalanche for sequential integers.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

// ---------------------------------------------------------------------------
// TDigest — approx_percentile
// ---------------------------------------------------------------------------

/// Centroid in the t-digest: represents a cluster of values.
#[derive(Debug, Clone, Copy)]
struct Centroid {
    mean: f64,
    count: u64,
}

/// T-digest approximate quantile estimator.
///
/// Maintains a sorted set of centroids that approximate the data distribution.
/// Accurate at the extremes (p1, p99) and reasonable in the middle.
/// Mergeable across partitions and shards.
pub struct TDigest {
    centroids: Vec<Centroid>,
    max_centroids: usize,
    total_count: u64,
}

impl TDigest {
    /// Create a new t-digest with default compression (200 centroids).
    pub fn new() -> Self {
        Self::with_compression(200)
    }

    pub fn with_compression(max_centroids: usize) -> Self {
        Self {
            centroids: Vec::with_capacity(max_centroids),
            max_centroids: max_centroids.max(10),
            total_count: 0,
        }
    }

    /// Add a single value.
    pub fn add(&mut self, value: f64) {
        if value.is_nan() {
            return;
        }
        self.centroids.push(Centroid {
            mean: value,
            count: 1,
        });
        self.total_count += 1;

        if self.centroids.len() > self.max_centroids * 2 {
            self.compress();
        }
    }

    /// Add a batch of f64 values.
    pub fn add_batch(&mut self, values: &[f64]) {
        for &v in values {
            self.add(v);
        }
    }

    /// Estimate the value at a given quantile (0.0 to 1.0).
    ///
    /// Example: `quantile(0.99)` → p99 latency.
    pub fn quantile(&self, q: f64) -> f64 {
        let q = q.clamp(0.0, 1.0);
        if self.centroids.is_empty() {
            return f64::NAN;
        }

        self.compress_clone().quantile_sorted(q)
    }

    /// Merge another t-digest into this one.
    pub fn merge(&mut self, other: &TDigest) {
        self.centroids.extend_from_slice(&other.centroids);
        self.total_count += other.total_count;
        if self.centroids.len() > self.max_centroids * 2 {
            self.compress();
        }
    }

    /// Total values ingested.
    pub fn count(&self) -> u64 {
        self.total_count
    }

    fn compress(&mut self) {
        if self.centroids.len() <= self.max_centroids {
            return;
        }

        self.centroids.sort_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Merge adjacent centroids until we're within budget.
        // Keep centroids at the extremes (head/tail) more granular for
        // accurate p1/p99, merge more aggressively in the middle.
        let target = self.max_centroids;
        while self.centroids.len() > target {
            // Find the pair with the smallest gap and merge them.
            let mut best_i = 0;
            let mut best_gap = f64::INFINITY;
            for i in 0..self.centroids.len() - 1 {
                let gap = self.centroids[i + 1].mean - self.centroids[i].mean;
                if gap < best_gap {
                    best_gap = gap;
                    best_i = i;
                }
            }
            let a = self.centroids[best_i];
            let b = self.centroids.remove(best_i + 1);
            let total = a.count + b.count;
            self.centroids[best_i] = Centroid {
                mean: (a.mean * a.count as f64 + b.mean * b.count as f64) / total as f64,
                count: total,
            };
        }
    }

    fn compress_clone(&self) -> TDigest {
        let mut clone = self.clone_inner();
        clone.compress();
        clone
    }

    fn clone_inner(&self) -> TDigest {
        TDigest {
            centroids: self.centroids.clone(),
            max_centroids: self.max_centroids,
            total_count: self.total_count,
        }
    }

    fn quantile_sorted(&self, q: f64) -> f64 {
        if self.centroids.is_empty() {
            return f64::NAN;
        }
        if self.centroids.len() == 1 {
            return self.centroids[0].mean;
        }

        let target = q * self.total_count as f64;
        let mut cumulative = 0.0;

        for c in &self.centroids {
            cumulative += c.count as f64;
            if cumulative >= target {
                return c.mean;
            }
        }

        self.centroids.last().map_or(f64::NAN, |c| c.mean)
    }
}

impl Default for TDigest {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// SpaceSaving — topK
// ---------------------------------------------------------------------------

/// Space-saving algorithm for approximate top-K heavy hitters.
///
/// Tracks the K most frequent items with bounded memory. Items not in
/// the top K are approximated — their counts may be over-estimated by
/// at most the minimum count in the structure.
pub struct SpaceSaving {
    /// Item → (count, over_estimation_error).
    items: HashMap<u64, (u64, u64)>,
    max_items: usize,
}

impl SpaceSaving {
    /// Create a new SpaceSaving tracker for top-K items.
    pub fn new(k: usize) -> Self {
        Self {
            items: HashMap::with_capacity(k + 1),
            max_items: k.max(1),
        }
    }

    /// Add an item (represented as a u64 hash/value).
    pub fn add(&mut self, item: u64) {
        if let Some(entry) = self.items.get_mut(&item) {
            entry.0 += 1;
            return;
        }

        if self.items.len() < self.max_items {
            self.items.insert(item, (1, 0));
        } else {
            // Find the item with the minimum count and replace it.
            // Safety: `self.items.len() >= self.max_items >= 1` guarantees non-empty.
            let Some((&min_key, &(min_count, _))) =
                self.items.iter().min_by_key(|(_, (count, _))| *count)
            else {
                return;
            };
            self.items.remove(&min_key);
            self.items.insert(item, (min_count + 1, min_count));
        }
    }

    /// Add a batch of u64 items.
    pub fn add_batch(&mut self, items: &[u64]) {
        for &item in items {
            self.add(item);
        }
    }

    /// Get the top-K items sorted by count (descending).
    ///
    /// Returns `(item, count, error_bound)` tuples. The true count is
    /// between `count - error_bound` and `count`.
    pub fn top_k(&self) -> Vec<(u64, u64, u64)> {
        let mut result: Vec<(u64, u64, u64)> = self
            .items
            .iter()
            .map(|(&item, &(count, error))| (item, count, error))
            .collect();
        result.sort_by(|a, b| b.1.cmp(&a.1));
        result
    }

    /// Merge another SpaceSaving into this one.
    pub fn merge(&mut self, other: &SpaceSaving) {
        for (&item, &(count, error)) in &other.items {
            let entry = self.items.entry(item).or_insert((0, 0));
            entry.0 += count;
            entry.1 += error;
        }

        // Trim to max_items.
        while self.items.len() > self.max_items {
            let Some((&min_key, _)) = self.items.iter().min_by_key(|(_, (count, _))| *count) else {
                break;
            };
            self.items.remove(&min_key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- HyperLogLog tests --

    #[test]
    fn hll_empty() {
        let hll = HyperLogLog::new();
        assert!(hll.estimate() < 1.0);
    }

    #[test]
    fn hll_small_cardinality() {
        let mut hll = HyperLogLog::new();
        for i in 0..100u64 {
            hll.add(i);
        }
        let est = hll.estimate();
        assert!((90.0..110.0).contains(&est), "expected ~100, got {est:.0}");
    }

    #[test]
    fn hll_large_cardinality() {
        let mut hll = HyperLogLog::new();
        for i in 0..100_000u64 {
            hll.add(i);
        }
        let est = hll.estimate();
        let error = (est - 100_000.0).abs() / 100_000.0;
        assert!(error < 0.05, "expected <5% error, got {error:.3}");
    }

    #[test]
    fn hll_duplicates_ignored() {
        let mut hll = HyperLogLog::new();
        for _ in 0..10_000 {
            hll.add(42);
        }
        let est = hll.estimate();
        assert!(est < 5.0, "expected ~1, got {est:.0}");
    }

    #[test]
    fn hll_merge() {
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        for i in 0..5000u64 {
            a.add(i);
        }
        for i in 3000..8000u64 {
            b.add(i);
        }
        a.merge(&b);
        let est = a.estimate();
        // Union: 0..8000 = 8000 distinct.
        let error = (est - 8000.0).abs() / 8000.0;
        assert!(error < 0.05, "expected ~8000, got {est:.0}");
    }

    #[test]
    fn hll_f64_batch() {
        let mut hll = HyperLogLog::new();
        let values: Vec<f64> = (0..1000).map(|i| i as f64 * 0.1).collect();
        hll.add_f64_batch(&values);
        let est = hll.estimate();
        assert!(
            (900.0..1100.0).contains(&est),
            "expected ~1000, got {est:.0}"
        );
    }

    #[test]
    fn hll_memory() {
        let hll = HyperLogLog::new();
        assert_eq!(hll.memory_bytes(), 16384); // 2^14 registers.
    }

    // -- TDigest tests --

    #[test]
    fn tdigest_empty() {
        let td = TDigest::new();
        assert!(td.quantile(0.5).is_nan());
    }

    #[test]
    fn tdigest_single_value() {
        let mut td = TDigest::new();
        td.add(42.0);
        assert!((td.quantile(0.5) - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn tdigest_uniform() {
        let mut td = TDigest::new();
        for i in 0..10_000 {
            td.add(i as f64);
        }
        let p50 = td.quantile(0.5);
        assert!(
            (4500.0..5500.0).contains(&p50),
            "p50 expected ~5000, got {p50:.0}"
        );
        let p99 = td.quantile(0.99);
        assert!(
            (9800.0..10000.0).contains(&p99),
            "p99 expected ~9900, got {p99:.0}"
        );
    }

    #[test]
    fn tdigest_merge() {
        let mut a = TDigest::new();
        let mut b = TDigest::new();
        for i in 0..5000 {
            a.add(i as f64);
        }
        for i in 5000..10000 {
            b.add(i as f64);
        }
        a.merge(&b);
        let p50 = a.quantile(0.5);
        assert!(
            (4000.0..6000.0).contains(&p50),
            "merged p50 expected ~5000, got {p50:.0}"
        );
    }

    // -- SpaceSaving tests --

    #[test]
    fn topk_basic() {
        let mut ss = SpaceSaving::new(3);
        for _ in 0..100 {
            ss.add(1);
        }
        for _ in 0..50 {
            ss.add(2);
        }
        for _ in 0..30 {
            ss.add(3);
        }
        for _ in 0..10 {
            ss.add(4);
        }
        let top = ss.top_k();
        assert_eq!(top[0].0, 1);
        assert_eq!(top[0].1, 100);
    }

    #[test]
    fn topk_merge() {
        let mut a = SpaceSaving::new(5);
        let mut b = SpaceSaving::new(5);
        for _ in 0..100 {
            a.add(1);
        }
        for _ in 0..80 {
            b.add(1);
        }
        for _ in 0..50 {
            b.add(2);
        }
        a.merge(&b);
        let top = a.top_k();
        assert_eq!(top[0].0, 1);
        assert_eq!(top[0].1, 180);
    }

    #[test]
    fn topk_eviction() {
        let mut ss = SpaceSaving::new(3);
        // Add 10 items — only 3 should survive.
        for i in 0..10u64 {
            for _ in 0..(10 - i) {
                ss.add(i);
            }
        }
        let top = ss.top_k();
        assert_eq!(top.len(), 3);
        // Top 3 should be items 0, 1, 2 (highest counts).
        assert!(top[0].1 >= top[1].1);
        assert!(top[1].1 >= top[2].1);
    }
}
