//! Distance metrics for vector similarity search.

pub mod scalar;

#[cfg(feature = "simd")]
pub mod simd;

pub use scalar::*;

/// Compute distance between two vectors using the specified metric.
///
/// Dispatches to SIMD kernels (AVX-512, AVX2+FMA, NEON) when the `simd`
/// feature is enabled; otherwise uses scalar implementations.
#[inline]
pub fn distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "distance: length mismatch (a.len()={}, b.len()={})",
        a.len(),
        b.len()
    );
    #[cfg(feature = "simd")]
    {
        let rt = simd::runtime();
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
    #[cfg(not(feature = "simd"))]
    {
        scalar::scalar_distance(a, b, metric)
    }
}

/// Batch distance: compute distances from `query` to each candidate.
///
/// Returns `(index, distance)` pairs sorted ascending, truncated to `top_k`.
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

    if top_k < dists.len() {
        dists.select_nth_unstable_by(top_k, |a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
        });
        dists.truncate(top_k);
    }
    dists.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    dists
}
