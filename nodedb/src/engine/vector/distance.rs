//! Distance metrics for vector similarity search.
//!
//! Re-exports shared scalar implementations from `nodedb-types` and adds
//! SIMD-accelerated dispatch for L2, cosine, and inner product via
//! hand-written AVX-512 / AVX2+FMA / NEON intrinsics in `simd.rs`.

// Re-export scalar functions and DistanceMetric enum from shared crate.
pub use nodedb_types::vector_distance::{
    DistanceMetric, chebyshev, cosine_distance, hamming_f32, jaccard, l2_squared, manhattan,
    neg_inner_product, pearson,
};

/// Compute distance using the specified metric.
///
/// Dispatches L2/cosine/inner-product to the best available SIMD kernel
/// (AVX-512 > AVX2+FMA > NEON > scalar). Other metrics use scalar
/// implementations that the compiler auto-vectorizes.
#[inline]
pub fn distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    let rt = super::simd::runtime();
    match metric {
        DistanceMetric::L2 => (rt.l2_squared)(a, b),
        DistanceMetric::Cosine => (rt.cosine_distance)(a, b),
        DistanceMetric::InnerProduct => (rt.neg_inner_product)(a, b),
        DistanceMetric::Manhattan => manhattan(a, b),
        DistanceMetric::Chebyshev => chebyshev(a, b),
        DistanceMetric::Hamming => hamming_f32(a, b),
        DistanceMetric::Jaccard => jaccard(a, b),
        DistanceMetric::Pearson => pearson(a, b),
    }
}

/// Batch distance computation: compute distances from `query` to each candidate.
///
/// Returns `(index, distance)` pairs sorted by distance ascending, truncated to `top_k`.
pub fn batch_distances(
    query: &[f32],
    candidates: &[&[f32]],
    metric: DistanceMetric,
    top_k: usize,
) -> Vec<(usize, f32)> {
    let mut dists: Vec<(usize, f32)> = candidates
        .iter()
        .enumerate()
        .map(|(i, c)| (i, distance(query, c, metric)))
        .collect();

    // Partial sort for top_k — cheaper than full sort for large candidate sets.
    if top_k < dists.len() {
        dists.select_nth_unstable_by(top_k, |a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
        });
        dists.truncate(top_k);
    }
    dists.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    dists
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2_identical_is_zero() {
        let v = [1.0, 2.0, 3.0];
        assert_eq!(l2_squared(&v, &v), 0.0);
    }

    #[test]
    fn l2_known_distance() {
        let a = [0.0, 0.0];
        let b = [3.0, 4.0];
        assert_eq!(l2_squared(&a, &b), 25.0);
    }

    #[test]
    fn cosine_identical_is_zero() {
        let v = [1.0, 2.0, 3.0];
        assert!(cosine_distance(&v, &v) < 1e-6);
    }

    #[test]
    fn cosine_orthogonal() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        assert!((cosine_distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn neg_ip_basic() {
        let a = [1.0, 2.0];
        let b = [3.0, 4.0];
        assert_eq!(neg_inner_product(&a, &b), -11.0);
    }

    #[test]
    fn manhattan_basic() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 6.0, 3.0];
        assert_eq!(manhattan(&a, &b), 7.0);
    }

    #[test]
    fn chebyshev_basic() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 6.0, 3.0];
        assert_eq!(chebyshev(&a, &b), 4.0);
    }

    #[test]
    fn hamming_basic() {
        let a = [1.0, 0.0, 1.0, 0.0];
        let b = [1.0, 1.0, 0.0, 0.0];
        assert_eq!(hamming_f32(&a, &b), 2.0);
    }

    #[test]
    fn jaccard_basic() {
        let a = [1.0, 0.0, 1.0, 0.0];
        let b = [1.0, 1.0, 0.0, 0.0];
        let j = jaccard(&a, &b);
        assert!((j - (1.0 - 1.0 / 3.0)).abs() < 1e-6);
    }

    #[test]
    fn pearson_identical_is_zero() {
        let v = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert!(pearson(&v, &v) < 1e-6);
    }

    #[test]
    fn pearson_opposite_is_high() {
        let a = [1.0, 2.0, 3.0, 4.0, 5.0];
        let b = [5.0, 4.0, 3.0, 2.0, 1.0];
        assert!(pearson(&a, &b) > 1.5);
    }

    #[test]
    fn batch_basic() {
        let query = [1.0, 0.0];
        let c1 = [1.0, 0.0f32];
        let c2 = [0.0, 1.0f32];
        let c3 = [0.5, 0.5f32];
        let candidates: Vec<&[f32]> = vec![&c1, &c2, &c3];
        let result = batch_distances(&query, &candidates, DistanceMetric::L2, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, 0); // c1 is closest (distance 0)
    }

    #[test]
    fn batch_nan_safe() {
        let query = [1.0, 0.0];
        let c1 = [1.0, 0.0f32];
        let c2 = [0.0, 1.0f32];
        let candidates: Vec<&[f32]> = vec![&c1, &c2];
        // Should not panic even with edge-case inputs.
        let result = batch_distances(&query, &candidates, DistanceMetric::L2, 10);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn high_dimensional() {
        let dim = 768;
        let a: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.001).collect();
        let b: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.002).collect();
        let d = distance(&a, &b, DistanceMetric::L2);
        assert!(d > 0.0);
        let d_cos = distance(&a, &b, DistanceMetric::Cosine);
        assert!((0.0..=2.0).contains(&d_cos));
        let d_man = distance(&a, &b, DistanceMetric::Manhattan);
        assert!(d_man > 0.0);
    }
}
