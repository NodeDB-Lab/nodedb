//! Partial sketch merging for approximate aggregation across shards.
//!
//! Each shard computes a local sketch (HLL, t-digest, topK). The coordinator
//! merges sketches from all shards. The final result has the same error
//! bounds as single-node — sketches are designed to be mergeable.

use nodedb_types::approx::{HyperLogLog, SpaceSaving, TDigest};

/// Merges HyperLogLog sketches from multiple shards.
///
/// Result: union cardinality estimate with same ~0.8% error.
pub fn merge_hll(sketches: &[HyperLogLog]) -> HyperLogLog {
    let mut merged = HyperLogLog::new();
    for sketch in sketches {
        merged.merge(sketch);
    }
    merged
}

/// Merges t-digest sketches from multiple shards.
///
/// Result: approximate quantiles with same accuracy.
pub fn merge_tdigest(digests: &[TDigest]) -> TDigest {
    let mut merged = TDigest::new();
    for digest in digests {
        merged.merge(digest);
    }
    merged
}

/// Merges SpaceSaving summaries from multiple shards.
///
/// Result: approximate top-K with same error bounds.
pub fn merge_topk(summaries: &[SpaceSaving]) -> SpaceSaving {
    let k = summaries
        .first()
        .map_or(10, |s: &SpaceSaving| s.top_k().len().max(10));
    let mut merged = SpaceSaving::new(k);
    for summary in summaries {
        merged.merge(summary);
    }
    merged
}

/// Wrapper for cross-shard sketch merge operations.
pub struct SketchMerger;

impl SketchMerger {
    /// Merge HLL sketches and return estimated cardinality.
    pub fn approx_count_distinct(sketches: &[HyperLogLog]) -> f64 {
        merge_hll(sketches).estimate()
    }

    /// Merge t-digests and return estimated quantile.
    pub fn approx_percentile(digests: &[TDigest], quantile: f64) -> f64 {
        merge_tdigest(digests).quantile(quantile)
    }

    /// Merge topK summaries and return merged top-K items.
    pub fn top_k(summaries: &[SpaceSaving]) -> Vec<(u64, u64, u64)> {
        merge_topk(summaries).top_k()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_hll_across_shards() {
        let mut shard_a = HyperLogLog::new();
        let mut shard_b = HyperLogLog::new();

        for i in 0..5000u64 {
            shard_a.add(i);
        }
        for i in 3000..8000u64 {
            shard_b.add(i);
        }

        let merged = merge_hll(&[shard_a, shard_b]);
        let est = merged.estimate();
        // Union: 0..8000 = 8000 distinct.
        let error = (est - 8000.0).abs() / 8000.0;
        assert!(error < 0.05, "expected ~8000, got {est:.0}");
    }

    #[test]
    fn merge_tdigest_across_shards() {
        let mut shard_a = TDigest::new();
        let mut shard_b = TDigest::new();

        for i in 0..5000 {
            shard_a.add(i as f64);
        }
        for i in 5000..10000 {
            shard_b.add(i as f64);
        }

        let merged = merge_tdigest(&[shard_a, shard_b]);
        let p50 = merged.quantile(0.5);
        assert!(
            (4000.0..6000.0).contains(&p50),
            "merged p50 expected ~5000, got {p50:.0}"
        );
    }

    #[test]
    fn merge_topk_across_shards() {
        let mut shard_a = SpaceSaving::new(5);
        let mut shard_b = SpaceSaving::new(5);

        for _ in 0..100 {
            shard_a.add(1);
        }
        for _ in 0..80 {
            shard_b.add(1);
        }
        for _ in 0..50 {
            shard_b.add(2);
        }

        let merged = merge_topk(&[shard_a, shard_b]);
        let top = merged.top_k();
        assert_eq!(top[0].0, 1);
        assert_eq!(top[0].1, 180); // 100 + 80
    }

    #[test]
    fn sketch_merger_convenience() {
        let mut hll_a = HyperLogLog::new();
        let mut hll_b = HyperLogLog::new();
        for i in 0..1000u64 {
            hll_a.add(i);
        }
        for i in 500..1500u64 {
            hll_b.add(i);
        }
        let est = SketchMerger::approx_count_distinct(&[hll_a, hll_b]);
        assert!(
            (1300.0..1700.0).contains(&est),
            "expected ~1500, got {est:.0}"
        );
    }
}
