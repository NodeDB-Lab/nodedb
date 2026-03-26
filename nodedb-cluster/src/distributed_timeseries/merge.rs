//! Partial aggregate merge functions for cross-shard aggregation.
//!
//! Each shard computes local partial aggregates. The coordinator merges
//! them using type-specific merge logic:
//! - SUM/COUNT: sum of partials
//! - AVG: sum(sums) / sum(counts)
//! - MIN/MAX: min/max of partials
//! - STDDEV/VARIANCE: Welford's online algorithm (count, mean, M2)
//! - FIRST/LAST: compare timestamps, keep earliest/latest

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A partial aggregate from a single shard for a single time bucket.
///
/// Contains enough state to merge with other shards' partials for the
/// same bucket and produce the final aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialAgg {
    /// Time bucket start timestamp.
    pub bucket_ts: i64,
    /// Row count.
    pub count: u64,
    /// Sum of values.
    pub sum: f64,
    /// Minimum value.
    pub min: f64,
    /// Maximum value.
    pub max: f64,
    /// First value (by timestamp).
    pub first_ts: i64,
    pub first_val: f64,
    /// Last value (by timestamp).
    pub last_ts: i64,
    pub last_val: f64,
    /// Welford's online algorithm state for variance/stddev.
    /// `mean` and `m2` (sum of squared differences from the mean).
    pub welford_mean: f64,
    pub welford_m2: f64,
}

impl PartialAgg {
    /// Create from a single value.
    pub fn from_single(bucket_ts: i64, ts: i64, val: f64) -> Self {
        Self {
            bucket_ts,
            count: 1,
            sum: val,
            min: val,
            max: val,
            first_ts: ts,
            first_val: val,
            last_ts: ts,
            last_val: val,
            welford_mean: val,
            welford_m2: 0.0,
        }
    }

    /// Merge another partial into this one.
    pub fn merge(&mut self, other: &PartialAgg) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other.clone();
            return;
        }

        // SUM, COUNT.
        self.sum += other.sum;
        let new_count = self.count + other.count;

        // MIN, MAX.
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }

        // FIRST (earliest timestamp).
        if other.first_ts < self.first_ts {
            self.first_ts = other.first_ts;
            self.first_val = other.first_val;
        }

        // LAST (latest timestamp).
        if other.last_ts > self.last_ts {
            self.last_ts = other.last_ts;
            self.last_val = other.last_val;
        }

        // Welford's parallel merge for variance/stddev.
        let delta = other.welford_mean - self.welford_mean;
        let combined_mean = (self.welford_mean * self.count as f64
            + other.welford_mean * other.count as f64)
            / new_count as f64;
        let combined_m2 = self.welford_m2
            + other.welford_m2
            + delta * delta * (self.count as f64 * other.count as f64) / new_count as f64;

        self.welford_mean = combined_mean;
        self.welford_m2 = combined_m2;
        self.count = new_count;
    }

    /// Finalize AVG.
    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            f64::NAN
        } else {
            self.sum / self.count as f64
        }
    }

    /// Finalize VARIANCE (population).
    pub fn variance(&self) -> f64 {
        if self.count < 2 {
            0.0
        } else {
            self.welford_m2 / self.count as f64
        }
    }

    /// Finalize STDDEV (population).
    pub fn stddev(&self) -> f64 {
        self.variance().sqrt()
    }
}

/// Merger that collects partial aggregates from multiple shards and
/// produces final merged results per bucket.
pub struct PartialAggMerger {
    /// Bucket → merged partial aggregate.
    buckets: BTreeMap<i64, PartialAgg>,
}

impl PartialAggMerger {
    pub fn new() -> Self {
        Self {
            buckets: BTreeMap::new(),
        }
    }

    /// Add a shard's partial aggregates.
    pub fn add_shard_results(&mut self, partials: &[PartialAgg]) {
        for partial in partials {
            self.buckets
                .entry(partial.bucket_ts)
                .and_modify(|existing| existing.merge(partial))
                .or_insert_with(|| partial.clone());
        }
    }

    /// Get the merged results, sorted by bucket timestamp.
    pub fn finalize(&self) -> Vec<PartialAgg> {
        self.buckets.values().cloned().collect()
    }

    /// Number of distinct buckets.
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }
}

impl Default for PartialAggMerger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_sum_count() {
        let mut a = PartialAgg::from_single(0, 100, 10.0);
        a.merge(&PartialAgg::from_single(0, 200, 20.0));
        assert_eq!(a.count, 2);
        assert_eq!(a.sum, 30.0);
    }

    #[test]
    fn merge_min_max() {
        let mut a = PartialAgg::from_single(0, 100, 50.0);
        let b = PartialAgg {
            min: 10.0,
            max: 90.0,
            ..PartialAgg::from_single(0, 200, 50.0)
        };
        a.merge(&b);
        assert_eq!(a.min, 10.0);
        assert_eq!(a.max, 90.0);
    }

    #[test]
    fn merge_first_last() {
        let mut a = PartialAgg::from_single(0, 200, 20.0);
        let b = PartialAgg::from_single(0, 100, 10.0);
        a.merge(&b);
        assert_eq!(a.first_ts, 100);
        assert_eq!(a.first_val, 10.0);
        assert_eq!(a.last_ts, 200);
        assert_eq!(a.last_val, 20.0);
    }

    #[test]
    fn merge_avg() {
        let mut a = PartialAgg::from_single(0, 100, 10.0);
        a.merge(&PartialAgg::from_single(0, 200, 30.0));
        assert!((a.avg() - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn merge_welford_variance() {
        // Two shards: shard A has [10, 20], shard B has [30, 40].
        // Combined: [10, 20, 30, 40], mean=25, variance=125.
        let mut a = PartialAgg::from_single(0, 1, 10.0);
        a.merge(&PartialAgg::from_single(0, 2, 20.0));

        let mut b = PartialAgg::from_single(0, 3, 30.0);
        b.merge(&PartialAgg::from_single(0, 4, 40.0));

        a.merge(&b);
        assert_eq!(a.count, 4);
        let expected_var = 125.0; // population variance of [10,20,30,40]
        assert!(
            (a.variance() - expected_var).abs() < 1.0,
            "variance {} vs expected {expected_var}",
            a.variance()
        );
    }

    #[test]
    fn merger_multi_shard() {
        let mut merger = PartialAggMerger::new();

        // Shard 0: buckets at 0 and 1000.
        merger.add_shard_results(&[
            PartialAgg {
                count: 100,
                sum: 5000.0,
                ..PartialAgg::from_single(0, 1, 50.0)
            },
            PartialAgg {
                count: 100,
                sum: 6000.0,
                ..PartialAgg::from_single(1000, 1001, 60.0)
            },
        ]);

        // Shard 1: same buckets.
        merger.add_shard_results(&[
            PartialAgg {
                count: 50,
                sum: 2500.0,
                ..PartialAgg::from_single(0, 2, 50.0)
            },
            PartialAgg {
                count: 50,
                sum: 3500.0,
                ..PartialAgg::from_single(1000, 1002, 70.0)
            },
        ]);

        let results = merger.finalize();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].count, 150); // 100 + 50
        assert_eq!(results[0].sum, 7500.0); // 5000 + 2500
        assert_eq!(results[1].count, 150);
    }

    #[test]
    fn merge_empty() {
        let mut a = PartialAgg::from_single(0, 100, 42.0);
        let empty = PartialAgg {
            count: 0,
            ..PartialAgg::from_single(0, 0, 0.0)
        };
        a.merge(&empty);
        assert_eq!(a.count, 1);
        assert_eq!(a.sum, 42.0);
    }
}
