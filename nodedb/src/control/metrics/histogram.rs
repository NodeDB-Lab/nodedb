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
        self.counts_snapshot().percentile(p)
    }

    /// Copy the current bucket counts into an owned [`HistogramSnapshot`].
    pub fn counts_snapshot(&self) -> HistogramSnapshot {
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

/// Plain-value copy of a histogram's bucket counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramSnapshot {
    boundaries: &'static [u64],
    buckets: Vec<u64>,
    count: u64,
}

impl HistogramSnapshot {
    /// Observations recorded between `earlier` and `self`.
    ///
    /// Subtraction saturates per bucket. A snapshot with different bucket
    /// boundaries yields `self` unchanged.
    pub fn since(&self, earlier: &HistogramSnapshot) -> HistogramSnapshot {
        if self.boundaries != earlier.boundaries {
            return self.clone();
        }
        HistogramSnapshot {
            boundaries: self.boundaries,
            buckets: self
                .buckets
                .iter()
                .zip(&earlier.buckets)
                .map(|(now, before)| now.saturating_sub(*before))
                .collect(),
            count: self.count.saturating_sub(earlier.count),
        }
    }

    /// Estimate a percentile in microseconds with linear interpolation inside
    /// the bucket that holds the target rank. Zero when empty.
    pub fn percentile(&self, p: f64) -> u64 {
        let total = self.count;
        if total == 0 {
            return 0;
        }
        let target = (p * total as f64) as u64;
        let mut cumulative = 0u64;
        let mut prev_boundary = 0u64;

        for (&boundary, &bucket_count) in self.boundaries.iter().zip(&self.buckets) {
            cumulative += bucket_count;
            if cumulative >= target {
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
        self.boundaries.last().copied().unwrap_or(0)
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
    fn window_p99_ignores_earlier_slow_samples() {
        let h = AtomicHistogram::new();
        for _ in 0..100 {
            h.observe(800_000);
        }
        let before = h.counts_snapshot();
        for _ in 0..100 {
            h.observe(40);
        }
        let window = h.counts_snapshot().since(&before);
        assert!(
            window.percentile(0.99) <= 50,
            "window={}",
            window.percentile(0.99)
        );
        assert!(h.percentile(0.99) > 500_000);
    }

    #[test]
    fn empty_window_reports_zero() {
        let h = AtomicHistogram::new();
        h.observe(800);
        let snap = h.counts_snapshot();
        assert_eq!(h.counts_snapshot().since(&snap).percentile(0.99), 0);
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
