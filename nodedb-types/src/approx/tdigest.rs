// SPDX-License-Identifier: Apache-2.0

//! TDigest — approximate percentile estimation (mergeable centroids).

/// Centroid in the t-digest: represents a cluster of values.
#[derive(Debug, Clone, Copy, zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct Centroid {
    mean: f64,
    count: u64,
}

/// T-digest approximate quantile estimator.
///
/// Maintains a sorted set of centroids that approximate the data distribution.
/// Accurate at the extremes (p1, p99) and reasonable in the middle.
/// Mergeable across partitions and shards.
#[derive(Debug, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct TDigest {
    centroids: Vec<Centroid>,
    max_centroids: usize,
    total_count: u64,
}

impl TDigest {
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

    pub fn add_batch(&mut self, values: &[f64]) {
        for &v in values {
            self.add(v);
        }
    }

    /// Estimate the value at a given quantile (0.0 to 1.0).
    pub fn quantile(&self, q: f64) -> f64 {
        let q = q.clamp(0.0, 1.0);
        if self.centroids.is_empty() {
            return f64::NAN;
        }
        self.compress_clone().quantile_sorted(q)
    }

    pub fn merge(&mut self, other: &TDigest) {
        self.centroids.extend_from_slice(&other.centroids);
        self.total_count += other.total_count;
        if self.centroids.len() > self.max_centroids * 2 {
            self.compress();
        }
    }

    pub fn count(&self) -> u64 {
        self.total_count
    }

    /// Add a pre-aggregated centroid (for merge/deserialization).
    pub fn add_centroid(&mut self, mean: f64, count: u64) {
        self.centroids.push(Centroid { mean, count });
        self.total_count += count;
        if self.centroids.len() > self.max_centroids * 2 {
            self.compress();
        }
    }

    /// Access centroids as (mean, count) pairs for serialization.
    pub fn centroids(&self) -> Vec<(f64, u64)> {
        self.centroids.iter().map(|c| (c.mean, c.count)).collect()
    }

    /// Approximate memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.centroids.capacity() * std::mem::size_of::<Centroid>()
    }

    /// Sort the centroids by mean, then merge the closest pairs down to
    /// `max_centroids`. `quantile_sorted` reads them in sorted order, so the
    /// sort runs even when no merge is needed.
    fn compress(&mut self) {
        self.centroids.sort_by(|a, b| a.mean.total_cmp(&b.mean));
        if self.centroids.len() <= self.max_centroids {
            return;
        }

        let target = self.max_centroids;
        while self.centroids.len() > target {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tdigest_empty() {
        let td = TDigest::new();
        assert!(td.quantile(0.5).is_nan());
    }

    /// Fewer values than the centroid limit are never merged, and their
    /// quantiles still come from the sorted order, not the insertion order.
    #[test]
    fn tdigest_quantile_below_the_centroid_limit_reads_sorted_values() {
        let mut td = TDigest::new();
        for i in 0..100 {
            td.add(f64::from(i % 10));
        }
        let p50 = td.quantile(0.5);
        assert!((4.0..=5.0).contains(&p50), "p50 expected 4..=5, got {p50}");
        assert_eq!(td.quantile(0.0), 0.0);
        assert_eq!(td.quantile(1.0), 9.0);
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

    #[test]
    fn tdigest_msgpack_roundtrip_merge_semantics() {
        let mut a = TDigest::new();
        let mut b = TDigest::new();
        for i in 0..500 {
            a.add(i as f64);
        }
        for i in 500..1000 {
            b.add(i as f64);
        }

        let bytes = zerompk::to_msgpack_vec(&a).expect("serialize TDigest");
        let mut a_prime: TDigest = zerompk::from_msgpack(&bytes).expect("deserialize TDigest");

        a_prime.merge(&b);
        a.merge(&b);

        let p50_orig = a.quantile(0.5);
        let p50_rt = a_prime.quantile(0.5);
        // Both are approximate; require within 10% of each other.
        let diff = (p50_orig - p50_rt).abs() / p50_orig.abs().max(1.0);
        assert!(
            diff < 0.10,
            "roundtrip diverged: orig p50={p50_orig:.0}, roundtrip p50={p50_rt:.0}"
        );
    }
}
