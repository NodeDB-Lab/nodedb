//! PQ and IVF-PQ codebook training must distribute centroids across the
//! data even when many input vectors are near-duplicates.
//!
//! Spec: k-means initialization selects centroids spread across the data
//! distribution. The current implementation has two compounding bugs:
//!
//!   1. `min_dists[i]` is only updated against `centroids[c - 1]` (the
//!      last centroid), not against the full centroid set. Once two
//!      centroids coincide, `min_dists` stops reflecting "distance to the
//!      nearest centroid," so every subsequent deterministic-argmax pick
//!      lands on the same outlier.
//!   2. The comment says "k-means++" but the selection is deterministic
//!      farthest-point, so outliers dominate rather than being sampled
//!      proportionally to d².
//!
//! Effect: on workloads with repeated prefixes/suffixes (templated chat,
//! shared headers/footers), most of the 256 centroids alias to one or two
//! points and PQ recall collapses.

use nodedb_vector::quantize::pq::PqCodec;

/// Training set of 200 vectors: 190 near-duplicates at the origin plus
/// 10 outliers scattered across a single subspace. A correct k-means++
/// spreads centroids across both clusters; the current farthest-point-
/// with-broken-min-distance-update collapses to ~2 distinct centroids.
fn clustered_with_duplicates() -> Vec<Vec<f32>> {
    let mut vecs: Vec<Vec<f32>> = Vec::with_capacity(200);
    // Cluster A: 190 near-identical vectors near origin.
    for i in 0..190 {
        let eps = (i as f32) * 1e-5;
        vecs.push(vec![eps, -eps, eps * 0.5, -eps * 0.5]);
    }
    // Cluster B: 10 outliers at distinct coordinates.
    for j in 0..10 {
        let x = 100.0 + (j as f32) * 10.0;
        vecs.push(vec![x, -x, x * 0.5, -x * 0.5]);
    }
    vecs
}

fn unique_centroid_count(codec: &PqCodec, vectors: &[Vec<f32>]) -> usize {
    let refs: Vec<&[f32]> = vectors.iter().map(|v| v.as_slice()).collect();
    let codes = codec.encode_batch(&refs);
    let m = codec.m;
    // Per-subspace unique centroid indices used across the batch.
    let mut min_unique = usize::MAX;
    for sub in 0..m {
        let mut seen = std::collections::HashSet::new();
        for row in 0..vectors.len() {
            seen.insert(codes[row * m + sub]);
        }
        if seen.len() < min_unique {
            min_unique = seen.len();
        }
    }
    min_unique
}

#[test]
fn pq_kmeans_produces_diverse_centroids_on_duplicate_heavy_data() {
    let vecs = clustered_with_duplicates();
    let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
    let codec = PqCodec::train(&refs, 4, 2, 16, 20);

    let unique = unique_centroid_count(&codec, &vecs);
    assert!(
        unique >= 4,
        "k-means collapsed to {unique} unique centroids per subspace on \
         duplicate-heavy input; a correct k-means++ should pick at least \
         4 distinct cluster representatives for k=16"
    );
}

#[test]
fn pq_distance_table_separates_duplicates_from_outliers() {
    // Spec test: after training, the PQ distance from a duplicate-cluster
    // query to a duplicate vector must be meaningfully smaller than the
    // distance to an outlier vector. Under the collapse bug, most
    // codebook entries alias to one point so all distances look similar.
    let vecs = clustered_with_duplicates();
    let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
    let codec = PqCodec::train(&refs, 4, 2, 16, 20);

    let query = [0.0f32, 0.0, 0.0, 0.0];
    let table = codec.build_distance_table(&query);

    let dup_code = codec.encode(&vecs[0]); // duplicate cluster
    let outlier_code = codec.encode(&vecs[195]); // outlier cluster

    let dup_dist = codec.asymmetric_distance(&table, &dup_code);
    let outlier_dist = codec.asymmetric_distance(&table, &outlier_code);

    assert!(
        outlier_dist > dup_dist * 10.0,
        "PQ failed to distinguish duplicate (d={dup_dist}) from outlier \
         (d={outlier_dist}) — codebook collapsed and the two codes encode \
         to near-identical table entries"
    );
}

#[cfg(feature = "ivf")]
#[test]
fn ivf_pq_training_does_not_collapse_on_duplicate_heavy_data() {
    use nodedb_vector::DistanceMetric;
    use nodedb_vector::{IvfPqIndex, IvfPqParams};

    let vecs = clustered_with_duplicates();
    let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
    let mut idx = IvfPqIndex::new(
        4,
        IvfPqParams {
            n_cells: 8,
            pq_m: 2,
            pq_k: 16,
            nprobe: 4,
            metric: DistanceMetric::L2,
        },
    );
    idx.train(&refs);
    for v in &vecs {
        idx.add(v);
    }

    // Query at the origin. Correct training assigns near-duplicates to
    // one cell and outliers to another; the nearest result must come
    // from the duplicate cluster (original indices 0..190).
    let results = idx.search(&[0.0, 0.0, 0.0, 0.0], 5);
    assert!(!results.is_empty(), "IVF-PQ returned no results");
    for r in &results {
        assert!(
            r.id < 190,
            "IVF-PQ k-means collapse: query at origin returned outlier id={} \
             instead of a near-duplicate cluster member",
            r.id
        );
    }
}
