// SPDX-License-Identifier: BUSL-1.1

//! Lock-free histogram for latency distributions.
//!
//! Fixed bucket boundaries with atomic counters. O(1) recording,
//! O(buckets) serialization. Compatible with Prometheus histogram format.

use std::sync::atomic::{AtomicU64, Ordering};

/// WAL fsync latency bucket boundaries in microseconds.
///
/// WAL fsyncs are expected to complete in the sub-millisecond to low-millisecond
/// range on NVMe. The finest granularity starts at 100µs.
pub const WAL_FSYNC_BUCKETS_US: &[u64] = &[
    100,       // 100µs
    500,       // 500µs
    1_000,     // 1ms
    5_000,     // 5ms
    10_000,    // 10ms
    50_000,    // 50ms
    100_000,   // 100ms
    500_000,   // 500ms
    1_000_000, // 1s
];

/// Default latency bucket boundaries in microseconds.
///
/// Covers 10µs to 10s — suitable for database query latency.
pub const DEFAULT_BUCKETS_US: &[u64] = &[
    10,         // 10µs
    50,         // 50µs
    100,        // 100µs
    500,        // 500µs
    1_000,      // 1ms
    5_000,      // 5ms
    10_000,     // 10ms
    50_000,     // 50ms
    100_000,    // 100ms
    500_000,    // 500ms
    1_000_000,  // 1s
    5_000_000,  // 5s
    10_000_000, // 10s
];

/// Atomic histogram with fixed bucket boundaries.
///
/// Each bucket counts observations `≤ boundary`. An `+Inf` overflow
/// bucket is implicit (tracked via `count`).
pub struct AtomicHistogram {
    /// Upper bounds in microseconds.
    boundaries: &'static [u64],
    /// Bucket counters: `buckets[i]` counts observations `≤ boundaries[i]`.
    buckets: Vec<AtomicU64>,
    /// Total observations.
    count: AtomicU64,
    /// Sum of all observed values (microseconds).
    sum: AtomicU64,
}

impl AtomicHistogram {
    /// Create with default latency buckets.
    pub fn new() -> Self {
        Self::with_buckets(DEFAULT_BUCKETS_US)
    }

    /// Create with custom bucket boundaries (must be sorted ascending).
    pub fn with_buckets(boundaries: &'static [u64]) -> Self {
        let buckets = (0..boundaries.len()).map(|_| AtomicU64::new(0)).collect();
        Self {
            boundaries,
            buckets,
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    /// Record an observation in microseconds.
    pub fn observe(&self, value_us: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value_us, Ordering::Relaxed);
        for (i, &boundary) in self.boundaries.iter().enumerate() {
            if value_us <= boundary {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        // Overflow: beyond all buckets (counted in count but not in any bucket).
    }

    /// Total observation count.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Sum of all observations in microseconds.
    pub fn sum_us(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }

    /// Estimate a percentile value in microseconds from bucket boundaries.
    ///
    /// Uses linear interpolation within the bucket that contains the target rank.
    pub fn percentile(&self, p: f64) -> u64 {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return 0;
        }
        let target = (p * total as f64) as u64;
        let mut cumulative = 0u64;
        let mut prev_boundary = 0u64;

        for (i, &boundary) in self.boundaries.iter().enumerate() {
            let bucket_count = self.buckets[i].load(Ordering::Relaxed);
            cumulative += bucket_count;
            if cumulative >= target {
                // Linear interpolation within this bucket.
                let bucket_start = prev_boundary;
                let bucket_width = boundary - bucket_start;
                if bucket_count == 0 {
                    return boundary;
                }
                let fraction = if cumulative > target {
                    (bucket_count - (cumulative - target)) as f64 / bucket_count as f64
                } else {
                    1.0
                };
                return bucket_start + (fraction * bucket_width as f64) as u64;
            }
            prev_boundary = boundary;
        }
        // Beyond all buckets — return last boundary.
        self.boundaries.last().copied().unwrap_or(0)
    }

    /// Write Prometheus histogram format to the output string.
    ///
    /// Produces `_bucket{le="..."}`, `_count`, `_sum` lines.
    pub fn write_prometheus(&self, out: &mut String, name: &str, help: &str) {
        use std::fmt::Write;
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");

        let mut cumulative = 0u64;
        for (i, &boundary) in self.boundaries.iter().enumerate() {
            cumulative += self.buckets[i].load(Ordering::Relaxed);
            // All boundaries stored in microseconds; Prometheus expects seconds.
            let le = format!("{}", boundary as f64 / 1_000_000.0);
            let _ = writeln!(out, "{name}_bucket{{le=\"{le}\"}} {cumulative}");
        }
        let total = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {total}");
        let _ = writeln!(out, "{name}_sum {}", self.sum_us() as f64 / 1_000_000.0);
        let _ = writeln!(out, "{name}_count {total}");
    }

    /// Create a point-in-time snapshot of this histogram as a new
    /// `AtomicHistogram` with the same bucket boundaries and the same
    /// current counts/sum. The snapshot is independent of the original.
    pub fn snapshot(&self) -> Self {
        let snap = Self::with_buckets(self.boundaries);
        for (i, bucket) in self.buckets.iter().enumerate() {
            snap.buckets[i].store(bucket.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        snap.count
            .store(self.count.load(Ordering::Relaxed), Ordering::Relaxed);
        snap.sum
            .store(self.sum.load(Ordering::Relaxed), Ordering::Relaxed);
        snap
    }

    /// Create a point-in-time snapshot of bucket counts for rolling delta calculations.
    pub fn snapshot_counts(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            boundaries: self.boundaries,
            buckets: self
                .buckets
                .iter()
                .map(|b| b.load(Ordering::Relaxed))
                .collect(),
            count: self.count.load(Ordering::Relaxed),
        }
    }

    /// Merge another histogram's counts into this one.
    ///
    /// Both histograms must share the same bucket boundaries — if they do
    /// not, the merge is a no-op for safety.
    pub fn merge(&self, other: &AtomicHistogram) {
        if self.boundaries.len() != other.boundaries.len() {
            return;
        }
        for (dst, src) in self.buckets.iter().zip(other.buckets.iter()) {
            dst.fetch_add(src.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        self.count
            .fetch_add(other.count.load(Ordering::Relaxed), Ordering::Relaxed);
        self.sum
            .fetch_add(other.sum.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

/// Point-in-time snapshot of bucket counts for rolling-window percentile calculations.
#[derive(Debug, Clone, Default)]
pub struct HistogramSnapshot {
    /// Upper bounds in microseconds.
    pub boundaries: &'static [u64],
    /// Bucket counters at snapshot time.
    pub buckets: Vec<u64>,
    /// Total observations at snapshot time.
    pub count: u64,
}

impl HistogramSnapshot {
    /// Compute percentile from the bucket count delta between `self` (earlier) and `newer`.
    ///
    /// `p` is a fraction between `0.0` and `1.0` (e.g. `0.99` for P99).
    /// Returns `0` if no new observations occurred in the interval.
    pub fn delta_percentile(&self, newer: &HistogramSnapshot, p: f64) -> u64 {
        let is_empty_baseline = self.boundaries.is_empty() && self.count == 0;
        if !is_empty_baseline && (self.boundaries != newer.boundaries || newer.count <= self.count)
        {
            return 0;
        }
        if newer.count == 0 {
            return 0;
        }
        let delta_count = if is_empty_baseline {
            newer.count
        } else {
            newer.count - self.count
        };
        if delta_count == 0 {
            return 0;
        }
        let target = (p * delta_count as f64) as u64;
        let mut cumulative = 0u64;
        let mut prev_boundary = 0u64;

        for (i, &boundary) in newer.boundaries.iter().enumerate() {
            let older_bucket = if is_empty_baseline {
                0
            } else {
                self.buckets.get(i).copied().unwrap_or(0)
            };
            let newer_bucket = newer.buckets.get(i).copied().unwrap_or(0);
            let bucket_delta = newer_bucket.saturating_sub(older_bucket);
            cumulative += bucket_delta;
            if cumulative >= target {
                let bucket_start = prev_boundary;
                let bucket_width = boundary - bucket_start;
                if bucket_delta == 0 {
                    return boundary;
                }
                let fraction = if cumulative > target {
                    (bucket_delta - (cumulative - target)) as f64 / bucket_delta as f64
                } else {
                    1.0
                };
                return bucket_start + (fraction * bucket_width as f64) as u64;
            }
            prev_boundary = boundary;
        }
        newer.boundaries.last().copied().unwrap_or(0)
    }
}

impl Default for AtomicHistogram {
    fn default() -> Self {
        Self::new()
    }
}

// Debug impl — don't print all bucket contents.
impl std::fmt::Debug for AtomicHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomicHistogram")
            .field("count", &self.count.load(Ordering::Relaxed))
            .field("sum_us", &self.sum.load(Ordering::Relaxed))
            .field("buckets", &self.boundaries.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_observation() {
        let h = AtomicHistogram::new();
        h.observe(50); // 50µs → falls in ≤50µs bucket
        h.observe(500); // 500µs bucket
        h.observe(5000); // 5ms bucket
        assert_eq!(h.count(), 3);
        assert_eq!(h.sum_us(), 5550);
    }

    #[test]
    fn percentile_estimation() {
        let h = AtomicHistogram::new();
        // All observations in 1ms bucket.
        for _ in 0..100 {
            h.observe(800); // 800µs → ≤1000µs bucket
        }
        let p50 = h.percentile(0.5);
        // Should be somewhere in the 500-1000µs range.
        assert!((500..=1000).contains(&p50), "p50={p50}");
    }

    #[test]
    fn percentile_p99_numerically_correct() {
        // WAL buckets: [100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000]
        let h = AtomicHistogram::with_buckets(WAL_FSYNC_BUCKETS_US);
        // Observe 90 items at 80us (falls in <=100us bucket)
        for _ in 0..90 {
            h.observe(80);
        }
        // Observe 9 items at 400us (falls in <=500us bucket)
        for _ in 0..9 {
            h.observe(400);
        }
        // Observe 1 item at 800us (falls in <=1000us bucket)
        h.observe(800);

        assert_eq!(h.count(), 100);

        // p50 is rank 50 (within first bucket 0..100us)
        let p50 = h.percentile(0.50);
        assert!(p50 <= 100, "expected p50 <= 100, got {p50}");

        // p99 is rank 99 (90 in bucket0 + 9 in bucket1 = 99 -> top of <=500us bucket)
        let p99 = h.percentile(0.99);
        assert!(
            (100..=500).contains(&p99),
            "expected p99 in [100, 500], got {p99}"
        );
        // Passing 99.0 would have returned 1_000_000 (the last boundary).
        assert_ne!(p99, 1_000_000);
    }

    #[test]
    fn rolling_window_delta_percentile() {
        let h = AtomicHistogram::with_buckets(WAL_FSYNC_BUCKETS_US);

        // Window 1: 100 observations at 80us (<=100us)
        for _ in 0..100 {
            h.observe(80);
        }
        let snap1 = h.snapshot_counts();

        // Window 2: 90 observations at 80us, 10 observations at 4000us (<=5000us)
        for _ in 0..90 {
            h.observe(80);
        }
        for _ in 0..10 {
            h.observe(4000);
        }
        let snap2 = h.snapshot_counts();

        // Delta p99 in Window 2 alone (100 new observations: 90 at 80us, 10 at 4000us)
        let delta_p99 = snap1.delta_percentile(&snap2, 0.99);
        assert!(
            (1000..=5000).contains(&delta_p99),
            "expected delta p99 in [1000, 5000], got {delta_p99}"
        );

        // Window 3: No new observations
        let snap3 = h.snapshot_counts();
        let delta_p99_empty = snap2.delta_percentile(&snap3, 0.99);
        assert_eq!(delta_p99_empty, 0);
    }

    #[test]
    fn prometheus_output() {
        let h = AtomicHistogram::new();
        h.observe(100);
        h.observe(5000);
        let mut out = String::new();
        h.write_prometheus(&mut out, "nodedb_query_latency_seconds", "Query latency");
        assert!(out.contains("# TYPE nodedb_query_latency_seconds histogram"));
        assert!(out.contains("nodedb_query_latency_seconds_count 2"));
        assert!(out.contains("le=\"+Inf\""));
    }

    #[test]
    fn overflow_beyond_all_buckets() {
        let h = AtomicHistogram::new();
        h.observe(99_000_000); // 99 seconds — beyond all buckets
        assert_eq!(h.count(), 1);
        // None of the fixed buckets should contain it.
        let mut found_in_bucket = false;
        for i in 0..DEFAULT_BUCKETS_US.len() {
            if h.buckets[i].load(Ordering::Relaxed) > 0 {
                found_in_bucket = true;
            }
        }
        assert!(!found_in_bucket);
    }

    #[test]
    fn empty_histogram() {
        let h = AtomicHistogram::new();
        assert_eq!(h.count(), 0);
        assert_eq!(h.percentile(0.5), 0);
    }
}
