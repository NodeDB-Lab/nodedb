//! `VectorCodec` implementation for `PqCodec`.
//!
//! Wraps the existing Product Quantization codec as a dual-phase codec.
//! The `Quantized` newtype holds a `UnifiedQuantizedVector` with
//! `QuantMode::Pq` and M code bytes in the packed-bits region.
//! The `Query` type carries both the precomputed per-subspace distance table
//! (`Vec<Vec<f32>>`) and the original FP32 query (for symmetric dequantization).

#[cfg(feature = "ivf")]
use nodedb_codec::vector_quant::{
    codec::{AdcLut, VectorCodec},
    layout::{QuantHeader, QuantMode, UnifiedQuantizedVector},
};

#[cfg(feature = "ivf")]
use crate::quantize::pq::PqCodec;

// ── Newtype ───────────────────────────────────────────────────────────────────

/// Thin newtype wrapping `UnifiedQuantizedVector` for PQ-encoded vectors.
#[cfg(feature = "ivf")]
pub struct PqQuantized(pub UnifiedQuantizedVector);

#[cfg(feature = "ivf")]
impl AsRef<UnifiedQuantizedVector> for PqQuantized {
    #[inline]
    fn as_ref(&self) -> &UnifiedQuantizedVector {
        &self.0
    }
}

// ── Query type ────────────────────────────────────────────────────────────────

/// Prepared query for PQ asymmetric distance computation.
///
/// `distance_table[sub][centroid]` holds the precomputed L2 squared distance
/// from the query's sub-vector in `sub` to each of the K centroids.
/// `raw` is the original FP32 query, retained for symmetric dequantization.
#[cfg(feature = "ivf")]
pub struct PqQuery {
    /// Per-subspace distance table: `distance_table[sub][centroid] -> f32`.
    pub distance_table: Vec<Vec<f32>>,
    /// Original FP32 query (used for symmetric distance dequantization).
    pub raw: Vec<f32>,
}

// ── Helper ────────────────────────────────────────────────────────────────────

#[cfg(feature = "ivf")]
#[inline]
fn packed_bits_of(q: &PqQuantized) -> &[u8] {
    q.0.packed_bits()
}

// ── VectorCodec impl ──────────────────────────────────────────────────────────

#[cfg(feature = "ivf")]
impl VectorCodec for PqCodec {
    type Quantized = PqQuantized;
    /// Prepared query: precomputed distance table + original FP32 query.
    type Query = PqQuery;

    /// Encode an FP32 vector: one centroid index byte per subspace.
    ///
    /// # Panics
    ///
    /// `UnifiedQuantizedVector::new` fails only when the outlier bitmask does
    /// not match the provided outlier slice. With `outlier_bitmask = 0` and an
    /// empty slice this can never happen. The `expect` is therefore unreachable
    /// in practice.
    fn encode(&self, v: &[f32]) -> Self::Quantized {
        let codes = self.encode(v);
        let header = QuantHeader {
            quant_mode: QuantMode::Pq as u16,
            dim: self.dim as u16,
            global_scale: 0.0,
            residual_norm: 0.0,
            dot_quantized: 0.0,
            outlier_bitmask: 0,
            reserved: [0; 8],
        };
        let uqv = UnifiedQuantizedVector::new(header, &codes, &[])
            .expect("PqCodec::encode: layout construction is infallible (no outliers)");
        PqQuantized(uqv)
    }

    /// Prepare the query by precomputing the M×K asymmetric distance table.
    fn prepare_query(&self, q: &[f32]) -> Self::Query {
        let distance_table = self.build_distance_table(q);
        PqQuery {
            distance_table,
            raw: q.to_vec(),
        }
    }

    /// Build the `AdcLut` from the precomputed distance table for use by
    /// SIMD rerank kernels (`pshufb` / `vpermb`).
    fn adc_lut(&self, q: &Self::Query) -> Option<AdcLut> {
        let m = self.m as u16;
        let k = self.k as u16;
        let mut lut = AdcLut::new(m, k);
        for (sub, sub_table) in q.distance_table.iter().enumerate() {
            for (centroid, &dist) in sub_table.iter().enumerate() {
                let idx = sub * self.k + centroid;
                lut.table[idx] = dist;
            }
        }
        Some(lut)
    }

    /// Symmetric distance between two PQ-encoded vectors.
    ///
    /// Both codes are decoded to approximate FP32 vectors via the codebook,
    /// then the squared L2 difference is accumulated. This is the correct
    /// definition of symmetric PQ distance: each vector is approximated by
    /// its nearest centroids, and the distance is computed in FP32.
    #[inline]
    fn fast_symmetric_distance(&self, q: &Self::Quantized, v: &Self::Quantized) -> f32 {
        let dq_a = self.decode(packed_bits_of(q));
        let dq_b = self.decode(packed_bits_of(v));
        dq_a.iter()
            .zip(dq_b.iter())
            .map(|(&a, &b)| {
                let d = a - b;
                d * d
            })
            .sum()
    }

    /// Asymmetric ADC distance: precomputed distance table vs stored code.
    ///
    /// O(M) per candidate — delegates to `PqCodec::asymmetric_distance`.
    #[inline]
    fn exact_asymmetric_distance(&self, q: &Self::Query, v: &Self::Quantized) -> f32 {
        self.asymmetric_distance(&q.distance_table, packed_bits_of(v))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "ivf"))]
mod tests {
    use super::*;

    fn make_codec() -> PqCodec {
        let vecs: Vec<Vec<f32>> = (0..80)
            .map(|i| {
                let c = (i / 20) as f32 * 5.0;
                vec![
                    c + (i as f32) * 0.1,
                    c - (i as f32) * 0.05,
                    c + 1.0,
                    c - 1.0,
                ]
            })
            .collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        PqCodec::train(&refs, 4, 2, 8, 10)
    }

    /// `encode` round-trip: packed_bits in the UQV must match the raw
    /// `PqCodec::encode` output.
    #[test]
    fn encode_packed_bits_matches_raw_encode() {
        let codec = make_codec();
        let v = vec![2.0f32, 1.0, 3.0, -1.0];
        let raw = codec.encode(&v);
        let quantized = <PqCodec as VectorCodec>::encode(&codec, &v);
        assert_eq!(quantized.as_ref().packed_bits(), raw.as_slice());
    }

    /// `fast_symmetric_distance` returns a non-negative finite value.
    #[test]
    fn fast_symmetric_distance_is_non_negative_finite() {
        let codec = make_codec();
        let a = <PqCodec as VectorCodec>::encode(&codec, &[0.5, 0.1, 1.0, -0.5]);
        let b = <PqCodec as VectorCodec>::encode(&codec, &[5.0, 4.0, 6.0, 4.5]);
        let d = codec.fast_symmetric_distance(&a, &b);
        assert!(d.is_finite(), "expected finite distance, got {d}");
        assert!(d >= 0.0, "expected non-negative distance, got {d}");
    }

    /// `exact_asymmetric_distance` returns a non-negative finite value.
    #[test]
    fn exact_asymmetric_distance_is_non_negative_finite() {
        let codec = make_codec();
        let q = codec.prepare_query(&[0.5, 0.1, 1.0, -0.5]);
        let v = <PqCodec as VectorCodec>::encode(&codec, &[5.0, 4.0, 6.0, 4.5]);
        let d = codec.exact_asymmetric_distance(&q, &v);
        assert!(d.is_finite(), "expected finite distance, got {d}");
        assert!(d >= 0.0, "expected non-negative distance, got {d}");
    }

    /// `adc_lut` produces a table with the correct shape.
    #[test]
    fn adc_lut_has_correct_shape() {
        let codec = make_codec();
        let q = codec.prepare_query(&[0.5, 0.1, 1.0, -0.5]);
        let lut =
            <PqCodec as VectorCodec>::adc_lut(&codec, &q).expect("PqCodec must produce an AdcLut");
        assert_eq!(lut.subspace_count, codec.m as u16);
        assert_eq!(lut.centroids_per_subspace, codec.k as u16);
        assert_eq!(lut.table.len(), codec.m * codec.k);
        assert!(lut.table.iter().all(|v| v.is_finite()));
    }

    /// Verify the trait impl compiles via a generic function.
    fn use_vector_codec<C: VectorCodec>(c: &C, q: &[f32], v: &[f32]) -> f32 {
        let qv = c.encode(v);
        let qq = c.prepare_query(q);
        c.fast_symmetric_distance(&qv, &qv) + c.exact_asymmetric_distance(&qq, &qv)
    }

    #[test]
    fn trait_bounds_compile() {
        let codec = make_codec();
        let result = use_vector_codec(&codec, &[0.5, 0.1, 1.0, -0.5], &[5.0, 4.0, 6.0, 4.5]);
        assert!(result.is_finite());
    }
}
