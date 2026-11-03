// SPDX-License-Identifier: Apache-2.0

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

use std::mem::size_of;

use nodedb_mem::{ReservationToken, ScopedMemory};
use nodedb_types::decode_bounds::checked_decode_capacity;

use crate::error::{VectorError, check_dim};

use super::pq_kmeans::{kmeans, l2_sub};

/// Hard ceiling for a decoded PQ vector. This bounds corrupted persisted
/// configuration even when the codec has no scoped memory handle attached.
const MAX_PQ_DECODE_DIM: usize = 1_048_576;
const MAX_PQ_CODEBOOK_BYTES: usize = 64 * 1024 * 1024;
// MessagePack stores each f32 as a marker plus four payload bytes and also
// carries nested-array headers. A 96 MiB envelope covers every codec whose
// complete decoded allocation (floats plus Vec headers) fits the 64 MiB cap.
const MAX_PQ_SERIALIZED_BYTES: usize = 96 * 1024 * 1024;

/// Centroids per PQ subspace: one byte per code.
pub const PQ_CENTROIDS: usize = 256;

/// Largest OPQ rotation matrix, in bytes (`dim × dim` f32): dim 4096.
const MAX_OPQ_ROTATION_BYTES: usize = 64 * 1024 * 1024;

/// Check that a PQ codec of `m` subspaces over `dim` components fits the
/// codec limits at `PQ_CENTROIDS` centroids. CREATE and ALTER VECTOR INDEX
/// call this, so a shape the codec cannot train is refused at DDL.
pub fn check_pq_shape(dim: usize, m: usize) -> Result<(), VectorError> {
    let invalid = |detail: String| Err(VectorError::InvalidInput { detail });
    if dim == 0 || dim > MAX_PQ_DECODE_DIM {
        return invalid(format!(
            "PQ dimension {dim} is outside 1..={MAX_PQ_DECODE_DIM}"
        ));
    }
    if m == 0 || !dim.is_multiple_of(m) {
        return invalid(format!("pq_m ({m}) must divide dim ({dim}) evenly"));
    }
    let sub_dim = dim / m;
    if pq_codebook_allocation_bytes(m, PQ_CENTROIDS, sub_dim)
        .is_none_or(|bytes| bytes > MAX_PQ_CODEBOOK_BYTES)
    {
        return invalid(format!(
            "PQ codebook for pq_m={m}, sub-dimension {sub_dim} exceeds \
             {MAX_PQ_CODEBOOK_BYTES} bytes"
        ));
    }
    Ok(())
}

/// [`check_pq_shape`] plus the `dim × dim` rotation an OPQ codec holds.
pub fn check_opq_shape(dim: usize, m: usize) -> Result<(), VectorError> {
    check_pq_shape(dim, m)?;
    if dim
        .checked_mul(dim)
        .and_then(|n| n.checked_mul(size_of::<f32>()))
        .is_none_or(|bytes| bytes > MAX_OPQ_ROTATION_BYTES)
    {
        return Err(VectorError::InvalidInput {
            detail: format!("OPQ rotation for dim {dim} exceeds {MAX_OPQ_ROTATION_BYTES} bytes"),
        });
    }
    Ok(())
}

fn pq_codebook_allocation_bytes(m: usize, k: usize, sub_dim: usize) -> Option<usize> {
    let outer_headers = m.checked_mul(size_of::<Vec<Vec<f32>>>())?;
    let centroid_count = m.checked_mul(k)?;
    let centroid_headers = centroid_count.checked_mul(size_of::<Vec<f32>>())?;
    let float_bytes = centroid_count
        .checked_mul(sub_dim)?
        .checked_mul(size_of::<f32>())?;
    outer_headers
        .checked_add(centroid_headers)?
        .checked_add(float_bytes)
}

/// Reserve `bytes` from `memory`. The returned token must be kept alive for
/// the duration of the allocation it covers.
#[inline]
fn try_reserve_or_skip(
    memory: &ScopedMemory,
    bytes: usize,
) -> Result<ReservationToken, VectorError> {
    Ok(memory.reserve(bytes)?)
}

/// The on-disk shape of a [`PqCodec`] — everything except the runtime
/// memory scope, which is never serialized.
#[derive(Clone, Debug, zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct PqCodecData {
    dim: usize,
    m: usize,
    k: usize,
    sub_dim: usize,
    codebooks: Vec<Vec<Vec<f32>>>,
}

/// PQ codec with trained codebooks.
#[derive(Clone, Debug)]
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

    /// Charges heap-significant operations (`train`, `encode_batch`,
    /// `build_distance_table`, `decode`, `to_bytes`) against the bound
    /// database, tenant, and engine budget before allocating, releasing
    /// the reservation when the returned value drops (RAII). A runtime
    /// concern only — it is never serialized.
    memory: ScopedMemory,
}

impl PqCodec {
    /// Train PQ codebooks from a set of training vectors via k-means.
    ///
    /// `m` = number of subvectors (must divide `dim` evenly).
    /// `k` = centroids per subvector (typically 256).
    /// `max_iter` = k-means iterations (20 is usually sufficient).
    pub fn train(
        vectors: &[&[f32]],
        dim: usize,
        m: usize,
        k: usize,
        max_iter: usize,
        memory: ScopedMemory,
    ) -> Result<Self, VectorError> {
        let invalid = |detail: String| Err(VectorError::InvalidInput { detail });
        if vectors.is_empty() {
            return invalid("PQ training needs at least one vector".into());
        }
        if dim == 0 || dim > MAX_PQ_DECODE_DIM {
            return invalid(format!(
                "PQ dimension {dim} is outside 1..={MAX_PQ_DECODE_DIM}"
            ));
        }
        if m == 0 || !dim.is_multiple_of(m) {
            return invalid(format!("PQ dimension {dim} must be divisible by m ({m})"));
        }
        if k == 0 || k > usize::from(u8::MAX) + 1 || k > vectors.len() {
            return invalid(format!(
                "PQ centroid count {k} must be in 1..=256 and at most the {} training vectors",
                vectors.len()
            ));
        }
        let sub_dim = dim / m;
        if pq_codebook_allocation_bytes(m, k, sub_dim)
            .is_none_or(|bytes| bytes > MAX_PQ_CODEBOOK_BYTES)
        {
            return invalid(format!(
                "PQ codebook for m={m}, k={k}, sub-dimension {sub_dim} exceeds \
                 {MAX_PQ_CODEBOOK_BYTES} bytes"
            ));
        }
        // Parameters are checked before any vector is read.
        for v in vectors {
            check_dim(dim, v.len())?;
        }

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

        Ok(Self {
            dim,
            m,
            k,
            sub_dim,
            codebooks,
            memory,
        })
    }

    /// Encode a vector: for each subvector, find the nearest centroid index.
    ///
    /// This is a per-vector hot-path operation.  Governor charging is
    /// intentionally skipped here to avoid atomic overhead on every candidate
    /// during search; use [`encode_batch`] for bulk encoding with budget
    /// enforcement.
    ///
    /// Precondition: `vector.len() == self.dim`. This per-candidate hot path
    /// does not re-check it; every caller checks the dimension first
    /// (`encode_batch`, the IVF-PQ `add`, and the codec-index entry points).
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
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
    ///
    /// Charges `m * vectors.len()` bytes to the bound budget (if set)
    /// before allocating the output buffer.  The guard is released at
    /// the end of this call — the buffer itself remains alive.
    pub fn encode_batch(&self, vectors: &[&[f32]]) -> Result<Vec<u8>, VectorError> {
        for v in vectors {
            check_dim(self.dim, v.len())?;
        }
        let capacity = self.m * vectors.len();
        let _g = try_reserve_or_skip(&self.memory, capacity * size_of::<u8>())?;
        let mut out = Vec::with_capacity(capacity);
        for v in vectors {
            out.extend(self.encode(v));
        }
        Ok(out)
    }

    /// Build an asymmetric distance table for a query vector.
    ///
    /// Returns `table[sub][centroid]` = distance from query's sub-vector
    /// to each centroid. Pre-computing this table makes distance evaluation
    /// O(M) per candidate instead of O(D).
    ///
    /// Charges `m * k * size_of::<f32>()` bytes to the bound budget (if set)
    /// before allocating the table.
    pub fn build_distance_table(&self, query: &[f32]) -> Result<Vec<Vec<f32>>, VectorError> {
        check_dim(self.dim, query.len())?;
        let total_bytes = self.m * self.k * size_of::<f32>();
        let _g = try_reserve_or_skip(&self.memory, total_bytes)?;
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
        Ok(table)
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
    ///
    /// Charges `dim * size_of::<f32>()` bytes to the bound budget (if set)
    /// before allocating the output buffer.
    pub fn decode(&self, code: &[u8]) -> Result<Vec<f32>, VectorError> {
        self.validate_shape()?;
        let decoded_dim = self
            .m
            .checked_mul(self.sub_dim)
            .filter(|&value| value == self.dim && value <= MAX_PQ_DECODE_DIM)
            .ok_or(VectorError::StoredDimensionMismatch {
                expected: self.dim,
                got: 0,
            })?;
        if code.len() != self.m {
            return Err(VectorError::StoredDimensionMismatch {
                expected: self.m,
                got: code.len(),
            });
        }
        if code.iter().any(|&index| usize::from(index) >= self.k) {
            return Err(VectorError::DeserializationFailed(
                "PQ code contains an out-of-range centroid index".to_string(),
            ));
        }
        let output_capacity = checked_decode_capacity(
            decoded_dim,
            size_of::<f32>(),
            0,
            0,
            MAX_PQ_DECODE_DIM,
            MAX_PQ_DECODE_DIM * size_of::<f32>(),
        )
        .ok_or(VectorError::StoredDimensionMismatch {
            expected: self.dim,
            got: 0,
        })?;
        let allocation_bytes = output_capacity * size_of::<f32>();
        let _g = try_reserve_or_skip(&self.memory, allocation_bytes)?;
        let mut out = Vec::with_capacity(output_capacity);
        for (sub, &c) in code.iter().enumerate() {
            out.extend_from_slice(&self.codebooks[sub][c as usize]);
        }
        Ok(out)
    }

    /// Serialize the codec to bytes with a versioned magic header.
    ///
    /// Format: `[NDPQ\0\0 (6 bytes)][version: u8 = 1][msgpack payload]`
    ///
    /// Charges the estimated serialized size to the bound budget (if set) before
    /// allocating the output buffer.  The estimate is conservative:
    /// `m * k * sub_dim * size_of::<f32>() + 64` (header + framing overhead).
    pub fn to_bytes(&self) -> Result<Vec<u8>, VectorError> {
        self.validate_shape()?;
        const MAGIC: &[u8; 6] = b"NDPQ\0\0";
        const VERSION: u8 = 1;
        let estimated = self.m * self.k * self.sub_dim * size_of::<f32>() + 64;
        let _g = try_reserve_or_skip(&self.memory, estimated)?;
        let data = PqCodecData {
            dim: self.dim,
            m: self.m,
            k: self.k,
            sub_dim: self.sub_dim,
            codebooks: self.codebooks.clone(),
        };
        let payload = zerompk::to_msgpack_vec(&data).map_err(|e| {
            VectorError::CheckpointSerializationError {
                detail: format!("PQ codec encode: {e}"),
            }
        })?;
        let mut out = Vec::with_capacity(7 + payload.len());
        out.extend_from_slice(MAGIC);
        out.push(VERSION);
        out.extend_from_slice(&payload);
        Ok(out)
    }

    /// Deserialize the codec from bytes produced by [`Self::to_bytes`].
    ///
    /// Returns `VectorError::InvalidMagic` if the header does not match
    /// `NDPQ\0\0`, and `VectorError::UnsupportedVersion` for unknown versions.
    pub fn from_bytes(bytes: &[u8], memory: ScopedMemory) -> Result<Self, VectorError> {
        const MAGIC: &[u8; 6] = b"NDPQ\0\0";
        const PQ_FORMAT_VERSION: u8 = 1;

        if bytes.len() < 7 || &bytes[0..6] != MAGIC {
            return Err(VectorError::InvalidMagic);
        }
        let version = bytes[6];
        if version != PQ_FORMAT_VERSION {
            return Err(VectorError::UnsupportedVersion {
                found: version,
                expected: PQ_FORMAT_VERSION,
            });
        }
        let payload = &bytes[7..];
        if payload.len() > MAX_PQ_SERIALIZED_BYTES {
            return Err(VectorError::DeserializationFailed(
                "PQ codec payload exceeds the decode limit".to_string(),
            ));
        }
        super::pq_decode::preflight_pq_payload(payload, MAX_PQ_DECODE_DIM, MAX_PQ_CODEBOOK_BYTES)?;
        let data = zerompk::from_msgpack::<PqCodecData>(payload)
            .map_err(|e| VectorError::DeserializationFailed(e.to_string()))?;
        let codec = Self {
            dim: data.dim,
            m: data.m,
            k: data.k,
            sub_dim: data.sub_dim,
            codebooks: data.codebooks,
            memory,
        };
        codec.validate_shape()?;
        Ok(codec)
    }

    fn validate_shape(&self) -> Result<(), VectorError> {
        let valid_scalar_shape = self.dim > 0
            && self.m > 0
            && self.k > 0
            && self.k <= usize::from(u8::MAX) + 1
            && self.sub_dim > 0
            && self
                .m
                .checked_mul(self.sub_dim)
                .is_some_and(|dim| dim == self.dim && dim <= MAX_PQ_DECODE_DIM);
        let codebook_bytes = pq_codebook_allocation_bytes(self.m, self.k, self.sub_dim);
        let valid_codebooks = self.codebooks.len() == self.m
            && self.codebooks.iter().all(|book| {
                book.len() == self.k && book.iter().all(|centroid| centroid.len() == self.sub_dim)
            });
        if !valid_scalar_shape
            || codebook_bytes.is_none_or(|bytes| bytes > MAX_PQ_CODEBOOK_BYTES)
            || !valid_codebooks
        {
            return Err(VectorError::DeserializationFailed(
                "invalid PQ codec shape".to_string(),
            ));
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_memory;

    fn assert_invalid(result: Result<PqCodec, VectorError>) {
        assert!(
            matches!(result, Err(VectorError::InvalidInput { .. })),
            "expected InvalidInput"
        );
    }

    #[test]
    fn train_rejects_a_vector_of_the_wrong_dimension() {
        let good = [0.0_f32, 1.0];
        let short = [0.0_f32];
        let result = PqCodec::train(&[&good, &short], 2, 1, 1, 1, test_memory());
        assert!(matches!(
            result,
            Err(VectorError::DimensionMismatch {
                expected: 2,
                got: 1
            })
        ));
    }

    #[test]
    fn distance_table_rejects_a_query_of_the_wrong_dimension() {
        let a = [0.0_f32, 1.0];
        let b = [1.0_f32, 0.0];
        let codec = PqCodec::train(&[&a, &b], 2, 1, 2, 1, test_memory()).unwrap();
        assert!(matches!(
            codec.build_distance_table(&[1.0]),
            Err(VectorError::DimensionMismatch {
                expected: 2,
                got: 1
            })
        ));
        assert!(matches!(
            codec.encode_batch(&[&[1.0]]),
            Err(VectorError::DimensionMismatch {
                expected: 2,
                got: 1
            })
        ));
    }

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
    fn train_rejects_dimension_above_decode_limit() {
        let vector = [0.0];
        assert_invalid(PqCodec::train(
            &[&vector],
            MAX_PQ_DECODE_DIM + 1,
            1,
            1,
            1,
            test_memory(),
        ));
    }

    #[test]
    fn train_rejects_raw_64_mib_codebook_once_container_overhead_is_counted() {
        let vector = [0.0];
        let vectors = vec![vector.as_slice(); 256];
        assert_invalid(PqCodec::train(&vectors, 65_536, 1, 256, 1, test_memory()));
    }

    #[test]
    fn train_rejects_codebook_above_decode_limit() {
        let vector = [0.0];
        let vectors = vec![vector.as_slice(); 17];
        assert_invalid(PqCodec::train(
            &vectors,
            MAX_PQ_DECODE_DIM,
            1,
            17,
            1,
            test_memory(),
        ));
    }

    #[test]
    fn train_rejects_more_centroids_than_training_vectors() {
        let vector = [0.0];
        assert_invalid(PqCodec::train(&[&vector], 1, 1, 2, 1, test_memory()));
    }

    #[test]
    fn train_rejects_centroid_count_above_u8_encoding_range() {
        let vector = [0.0];
        assert_invalid(PqCodec::train(&[&vector], 1, 1, 257, 1, test_memory()));
    }

    #[test]
    fn decode_rejects_unbounded_configured_dimension_before_allocation() {
        let codec = PqCodec {
            dim: MAX_PQ_DECODE_DIM + 1,
            m: 1,
            k: 1,
            sub_dim: MAX_PQ_DECODE_DIM + 1,
            codebooks: vec![vec![vec![]]],
            memory: test_memory(),
        };
        assert!(codec.decode(&[0]).is_err());
    }

    #[test]
    fn from_bytes_rejects_oversized_outer_array_before_allocation() {
        // PqCodec's zerompk representation is a five-element array:
        // dim, m, k, sub_dim, codebooks.
        let mut payload = vec![0x95, 1, 1, 1, 1, 0xdd];
        payload.extend_from_slice(&u32::try_from(MAX_PQ_DECODE_DIM + 1).unwrap().to_be_bytes());
        let mut bytes = b"NDPQ\0\0\x01".to_vec();
        bytes.extend_from_slice(&payload);
        assert!(matches!(
            PqCodec::from_bytes(&bytes, test_memory()),
            Err(VectorError::DeserializationFailed(_))
        ));
    }

    #[test]
    fn from_bytes_rejects_malformed_codebook_shape() {
        let malformed = PqCodecData {
            dim: 4,
            m: 2,
            k: 1,
            sub_dim: 2,
            codebooks: vec![vec![vec![0.0, 0.0]]],
        };
        let payload = zerompk::to_msgpack_vec(&malformed).unwrap();
        let mut bytes = b"NDPQ\0\0\x01".to_vec();
        bytes.extend_from_slice(&payload);
        assert!(matches!(
            PqCodec::from_bytes(&bytes, test_memory()),
            Err(VectorError::DeserializationFailed(_))
        ));
    }

    #[test]
    fn decode_rejects_out_of_range_centroid_index() {
        let codec = PqCodec {
            dim: 1,
            m: 1,
            k: 1,
            sub_dim: 1,
            codebooks: vec![vec![vec![0.0]]],
            memory: test_memory(),
        };
        assert!(matches!(
            codec.decode(&[1]),
            Err(VectorError::DeserializationFailed(_))
        ));
    }

    #[test]
    fn encode_decode_roundtrip() {
        let vecs = make_clustered_data();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = PqCodec::train(&refs, 4, 2, 16, 10, test_memory()).unwrap();

        for v in &vecs {
            let code = codec.encode(v);
            assert_eq!(code.len(), 2); // M=2 bytes
            let decoded = codec.decode(&code).unwrap();
            assert_eq!(decoded.len(), 4);
        }
    }

    #[test]
    fn distance_table_gives_correct_ordering() {
        let vecs = make_clustered_data();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = PqCodec::train(&refs, 4, 2, 16, 10, test_memory()).unwrap();

        let codes: Vec<Vec<u8>> = vecs.iter().map(|v| codec.encode(v)).collect();
        let query = &[5.0, 5.0, 5.0, 5.0];
        let table = codec.build_distance_table(query).unwrap();

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
        let codec = PqCodec::train(&refs, 4, 2, 16, 10, test_memory()).unwrap();

        let batch = codec.encode_batch(&refs).unwrap();
        assert_eq!(batch.len(), 2 * 200); // M=2, N=200
    }

    // golden format test — verifies the on-disk layout is stable.
    #[test]
    fn pq_codec_golden_format() {
        let vecs = make_clustered_data();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let codec = PqCodec::train(&refs, 4, 2, 16, 10, test_memory()).unwrap();

        let bytes = codec.to_bytes().unwrap();

        // Magic header.
        assert_eq!(&bytes[0..6], b"NDPQ\0\0", "magic mismatch");
        // Version byte.
        assert_eq!(bytes[6], 1u8, "version must be 1");
        // The complete persisted envelope must pass bounded preflight and decode.
        let restored =
            PqCodec::from_bytes(&bytes, test_memory()).expect("persisted PQ codec must decode");
        assert_eq!(restored.dim, codec.dim);
        assert_eq!(restored.m, codec.m);
    }

    #[test]
    fn pq_version_mismatch_returns_error() {
        // Craft a header with magic correct but version = 0 (unsupported).
        let mut crafted = b"NDPQ\0\0".to_vec();
        crafted.push(0u8); // wrong version
        crafted.extend_from_slice(b"\x80"); // minimal valid msgpack map

        let err = PqCodec::from_bytes(&crafted, test_memory()).unwrap_err();
        assert!(
            matches!(
                err,
                VectorError::UnsupportedVersion {
                    found: 0,
                    expected: 1
                }
            ),
            "expected UnsupportedVersion, got: {err:?}"
        );
    }

    #[test]
    fn pq_invalid_magic_returns_error() {
        let bad: &[u8] = b"JUNK\0\0\x01some-payload";
        let err = PqCodec::from_bytes(bad, test_memory()).unwrap_err();
        assert!(
            matches!(err, VectorError::InvalidMagic),
            "expected InvalidMagic, got: {err:?}"
        );
    }

    #[test]
    fn pq_shape_check_refuses_what_the_codec_cannot_train() {
        assert!(check_pq_shape(128, 8).is_ok());
        for (dim, m) in [(0, 1), (12, 8), (16, 0), (MAX_PQ_DECODE_DIM + 1, 1)] {
            assert!(
                matches!(
                    check_pq_shape(dim, m),
                    Err(VectorError::InvalidInput { .. })
                ),
                "dim {dim}, m {m}"
            );
        }
        // 256 centroids of 65536 / 8 floats each overflow the codebook cap.
        assert!(check_pq_shape(65_536, 8).is_err());
    }

    #[test]
    fn opq_shape_check_adds_the_rotation_limit() {
        assert!(check_opq_shape(1024, 8).is_ok());
        assert!(
            check_opq_shape(8192, 8).is_err(),
            "8192² f32 exceeds 64 MiB"
        );
        assert!(check_opq_shape(12, 8).is_err());
    }
}
