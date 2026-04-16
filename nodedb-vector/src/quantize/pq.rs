//! Product Quantization (PQ): 8-16x compression for large datasets.
//!
//! Splits D-dimensional vectors into M subvectors, clusters each subspace
//! with K=256 centroids via k-means. Each vector is encoded as M bytes
//! (one centroid index per subvector).
//!
//! Distance is computed via precomputed lookup tables: for each query,
//! build a `[M][K]` table of distances from the query's subvectors to
//! all centroids. Then the distance to any encoded vector is just M
//! table lookups + additions — O(M) per candidate vs O(D) for FP32.
//!
//! Trade-off: 2-5% recall loss vs SQ8's <1%, but 2-4x more compression
//! (8-16x total vs 4x for SQ8). Best for cost-sensitive large datasets.

use serde::{Deserialize, Serialize};

/// PQ codec with trained codebooks.
#[derive(Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct PqCodec {
    /// Original vector dimensionality.
    pub dim: usize,
    /// Number of subvectors (subspaces).
    pub m: usize,
    /// Centroids per subvector (fixed at 256 for u8 encoding).
    pub k: usize,
    /// Dimensions per subvector: `dim / m`.
    pub sub_dim: usize,
    /// Codebooks: `codebooks[subspace][centroid][sub_dim_component]`.
    /// Total: M × K × sub_dim floats.
    codebooks: Vec<Vec<Vec<f32>>>,
}

impl PqCodec {
    /// Train PQ codebooks from a set of training vectors via k-means.
    ///
    /// `m` = number of subvectors (must divide `dim` evenly).
    /// `k` = centroids per subvector (typically 256).
    /// `max_iter` = k-means iterations (20 is usually sufficient).
    pub fn train(vectors: &[&[f32]], dim: usize, m: usize, k: usize, max_iter: usize) -> Self {
        assert!(!vectors.is_empty());
        assert!(dim > 0 && m > 0 && k > 0);
        assert!(
            dim.is_multiple_of(m),
            "dim ({dim}) must be divisible by m ({m})"
        );

        let sub_dim = dim / m;
        let mut codebooks = Vec::with_capacity(m);

        for sub in 0..m {
            let offset = sub * sub_dim;
            // Extract sub-vectors for this subspace.
            let sub_vectors: Vec<&[f32]> = vectors
                .iter()
                .map(|v| &v[offset..offset + sub_dim])
                .collect();

            let centroids = kmeans(&sub_vectors, sub_dim, k, max_iter);
            codebooks.push(centroids);
        }

        Self {
            dim,
            m,
            k,
            sub_dim,
            codebooks,
        }
    }

    /// Encode a vector: for each subvector, find the nearest centroid index.
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        debug_assert_eq!(vector.len(), self.dim);
        let mut code = Vec::with_capacity(self.m);
        for sub in 0..self.m {
            let offset = sub * self.sub_dim;
            let sub_vec = &vector[offset..offset + self.sub_dim];
            let nearest = self.nearest_centroid(sub, sub_vec);
            code.push(nearest as u8);
        }
        code
    }

    /// Batch encode all vectors into a contiguous byte array.
    pub fn encode_batch(&self, vectors: &[&[f32]]) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.m * vectors.len());
        for v in vectors {
            out.extend(self.encode(v));
        }
        out
    }

    /// Build an asymmetric distance table for a query vector.
    ///
    /// Returns `table[sub][centroid]` = distance from query's sub-vector
    /// to each centroid. Pre-computing this table makes distance evaluation
    /// O(M) per candidate instead of O(D).
    pub fn build_distance_table(&self, query: &[f32]) -> Vec<Vec<f32>> {
        debug_assert_eq!(query.len(), self.dim);
        let mut table = Vec::with_capacity(self.m);
        for sub in 0..self.m {
            let offset = sub * self.sub_dim;
            let sub_query = &query[offset..offset + self.sub_dim];
            let mut dists = Vec::with_capacity(self.k);
            for centroid in &self.codebooks[sub] {
                let d = l2_sub(sub_query, centroid);
                dists.push(d);
            }
            table.push(dists);
        }
        table
    }

    /// Compute asymmetric distance using a precomputed distance table.
    ///
    /// O(M) per candidate — just M table lookups and additions.
    #[inline]
    pub fn asymmetric_distance(&self, table: &[Vec<f32>], code: &[u8]) -> f32 {
        debug_assert_eq!(code.len(), self.m);
        let mut dist = 0.0f32;
        for (sub, &c) in code.iter().enumerate() {
            dist += table[sub][c as usize];
        }
        dist
    }

    /// Decode a PQ code back to an approximate FP32 vector.
    pub fn decode(&self, code: &[u8]) -> Vec<f32> {
        debug_assert_eq!(code.len(), self.m);
        let mut out = Vec::with_capacity(self.dim);
        for (sub, &c) in code.iter().enumerate() {
            out.extend_from_slice(&self.codebooks[sub][c as usize]);
        }
        out
    }

    fn nearest_centroid(&self, subspace: usize, sub_vec: &[f32]) -> usize {
        let mut best_idx = 0;
        let mut best_dist = f32::MAX;
        for (i, centroid) in self.codebooks[subspace].iter().enumerate() {
            let d = l2_sub(sub_vec, centroid);
            if d < best_dist {
                best_dist = d;
                best_idx = i;
            }
        }
        best_idx
    }
}

/// L2 squared distance for sub-vectors (used in k-means and encoding).
#[inline]
fn l2_sub(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

/// Simple k-means clustering for PQ codebook training.
///
/// Uses proper k-means++ initialization (weighted d² sampling) with a
/// deterministic seed so training is reproducible across runs.
fn kmeans(data: &[&[f32]], dim: usize, k: usize, max_iter: usize) -> Vec<Vec<f32>> {
    let n = data.len();
    if n == 0 || k == 0 {
        return Vec::new();
    }
    let k = k.min(n); // Can't have more centroids than data points.

    // K-means++ initialization with deterministic xorshift.
    let mut rng = crate::hnsw::Xorshift64::new(0xC0FF_EEDE_ADBE_EF42);

    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(k);
    centroids.push(data[0].to_vec());

    let mut min_dists = vec![f32::MAX; n];
    // Update against the first centroid.
    for (i, point) in data.iter().enumerate() {
        let d = l2_sub(point, &centroids[0]);
        if d < min_dists[i] {
            min_dists[i] = d;
        }
    }

    for _ in 1..k {
        let total: f64 = min_dists.iter().map(|&d| d as f64).sum();
        let next_idx = if total < f64::EPSILON {
            // All points coincide with existing centroids.
            0
        } else {
            let target = rng.next_f64() * total;
            let mut acc = 0.0f64;
            let mut chosen = n - 1;
            for (i, &d) in min_dists.iter().enumerate() {
                acc += d as f64;
                if acc >= target {
                    chosen = i;
                    break;
                }
            }
            chosen
        };
        centroids.push(data[next_idx].to_vec());
        // Incrementally update min_dists against the new centroid.
        let last = centroids.last().expect("just pushed");
        for (i, point) in data.iter().enumerate() {
            let d = l2_sub(point, last);
            if d < min_dists[i] {
                min_dists[i] = d;
            }
        }
    }

    // K-means iterations.
    let mut assignments = vec![0usize; n];
    for _ in 0..max_iter {
        // Assignment step.
        let mut changed = false;
        for (i, point) in data.iter().enumerate() {
            let mut best = 0;
            let mut best_d = f32::MAX;
            for (c, centroid) in centroids.iter().enumerate() {
                let d = l2_sub(point, centroid);
                if d < best_d {
                    best_d = d;
                    best = c;
                }
            }
            if assignments[i] != best {
                assignments[i] = best;
                changed = true;
            }
        }
        if !changed {
            break;
        }

        // Update step: recompute centroids as means.
        let mut sums = vec![vec![0.0f32; dim]; k];
        let mut counts = vec![0usize; k];
        for (i, point) in data.iter().enumerate() {
            let c = assignments[i];
            counts[c] += 1;
            for d in 0..dim {
                sums[c][d] += point[d];
            }
        }
        for c in 0..k {
            if counts[c] > 0 {
                for d in 0..dim {
                    centroids[c][d] = sums[c][d] / counts[c] as f32;
                }
            }
        }
    }

    centroids
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_clustered_data() -> Vec<Vec<f32>> {
        // 4 clusters in 4D space, 50 points each.
        let mut vecs = Vec::new();
        for cluster in 0..4 {
            let center = cluster as f32 * 10.0;
            for i in 0..50 {
                vecs.push(vec![
                    center + (i as f32) * 0.1,
                    center + (i as f32) * 0.05,
                    center - (i as f32) * 0.1,
                    center + (i as f32) * 0.02,
                ]);
            }
        }
        vecs
    }

    #[test]
    fn encode_decode_roundtrip() {
        let vecs = make_clustered_data();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = PqCodec::train(&refs, 4, 2, 16, 10);

        for v in &vecs {
            let code = codec.encode(v);
            assert_eq!(code.len(), 2); // M=2 bytes
            let decoded = codec.decode(&code);
            assert_eq!(decoded.len(), 4);
        }
    }

    #[test]
    fn distance_table_gives_correct_ordering() {
        let vecs = make_clustered_data();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = PqCodec::train(&refs, 4, 2, 16, 10);

        let codes: Vec<Vec<u8>> = vecs.iter().map(|v| codec.encode(v)).collect();
        let query = &[5.0, 5.0, 5.0, 5.0];
        let table = codec.build_distance_table(query);

        // Find nearest via PQ distance.
        let mut pq_dists: Vec<(usize, f32)> = codes
            .iter()
            .enumerate()
            .map(|(i, c)| (i, codec.asymmetric_distance(&table, c)))
            .collect();
        pq_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // Find nearest via exact L2.
        let mut exact_dists: Vec<(usize, f32)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (i, l2_sub(query, v)))
            .collect();
        exact_dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // Top-5 from PQ should have significant overlap with exact top-10.
        let pq_top: std::collections::HashSet<usize> = pq_dists[..5].iter().map(|x| x.0).collect();
        let exact_top: std::collections::HashSet<usize> =
            exact_dists[..10].iter().map(|x| x.0).collect();
        let overlap = pq_top.intersection(&exact_top).count();
        assert!(overlap >= 3, "PQ recall too low: {overlap}/5 in top-10");
    }

    #[test]
    fn batch_encode() {
        let vecs = make_clustered_data();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = PqCodec::train(&refs, 4, 2, 16, 10);

        let batch = codec.encode_batch(&refs);
        assert_eq!(batch.len(), 2 * 200); // M=2, N=200
    }
}
