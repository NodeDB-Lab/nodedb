// SPDX-License-Identifier: Apache-2.0

//! `RerankCodec` wrapper for BBQ (Better Binary Quantization).
//!
//! BBQ is a training-based codec: `train()` calibrates a centroid from a
//! sample of vectors. Until training is complete, `encode` and `prepare_query`
//! return `RerankError::NotTrained`.
//!
//! Distance uses the asymmetric path from the BBQ paper: the query is kept in
//! centered FP32; the stored vector is reconstructed from its 1-bit sign pack
//! and `residual_norm` (≈ ±norm/√dim per dimension). The L2 distance between
//! the exact centered query and the reconstructed candidate is returned.
//!
//! The prepared form is `PreparedQuery::Bytes` with the layout:
//!   [0..4]         alpha (query_norm) as f32 little-endian
//!   [4..4+dim*4]   centered f32 values, each as f32 little-endian

use nodedb_codec::vector_quant::bbq::BbqCodec;
use nodedb_codec::vector_quant::codec::VectorCodec as _;
use nodedb_codec::vector_quant::layout::UnifiedQuantizedVectorRef;

use crate::{
    rerank::codec::{CodecName, PreparedQuery, RerankCodec},
    rerank::types::RerankError,
};

// ── Payload helpers ───────────────────────────────────────────────────────────

fn encode_payload(query_norm: f32, centered: &[f32]) -> Vec<u8> {
    // Layout: 4 bytes alpha (query_norm f32 LE) || dim * 4 bytes centered f32 LE
    let mut buf = Vec::with_capacity(4 + centered.len() * 4);
    buf.extend_from_slice(&query_norm.to_le_bytes());
    for &x in centered {
        buf.extend_from_slice(&x.to_le_bytes());
    }
    buf
}

/// Byte length of a prepared BBQ payload for `dim`: alpha + centered f32s.
fn payload_len(dim: usize) -> usize {
    4 + dim * 4
}

// ── BbqRerank ─────────────────────────────────────────────────────────────────

/// Default oversample multiplier used when the caller does not specify one.
pub const DEFAULT_OVERSAMPLE: u8 = 4;

/// Object-safe `RerankCodec` wrapper around `BbqCodec`.
///
/// The codec starts untrained. `encode` and `prepare_query` return
/// `RerankError::NotTrained` until `train()` has been called with a
/// representative sample of vectors.
///
/// `from_codec` accepts a pre-calibrated `BbqCodec` (used when restoring
/// from a snapshot).
pub struct BbqRerank {
    codec: Option<BbqCodec>,
    dim: usize,
    oversample: u8,
}

impl BbqRerank {
    /// Construct an untrained wrapper.
    ///
    /// `encode` / `distance_prepared` return `RerankError::NotTrained` until
    /// `train()` is called.
    pub fn new(dim: usize, oversample: u8) -> Self {
        Self {
            codec: None,
            dim,
            oversample,
        }
    }

    /// Construct from a pre-calibrated codec (used when restoring from snapshot).
    pub fn from_codec(codec: BbqCodec) -> Self {
        let dim = codec.dim;
        Self {
            codec: Some(codec),
            dim,
            oversample: DEFAULT_OVERSAMPLE,
        }
    }
}

impl RerankCodec for BbqRerank {
    /// Encode a full-precision vector to BBQ 1-bit bytes.
    ///
    /// The serialised form is the raw `UnifiedQuantizedVector` buffer
    /// (`as_bytes()`): 32-byte `QuantHeader` followed by `dim.div_ceil(8)`
    /// sign-packed bits plus 14 bytes of corrective factors in the header.
    fn encode(&self, v: &[f32]) -> Result<Vec<u8>, RerankError> {
        if v.len() != self.dim {
            return Err(RerankError::BadInput(format!(
                "bbq encode: vector len {} != codec dim {}",
                v.len(),
                self.dim
            )));
        }
        let codec = self.codec.as_ref().ok_or_else(|| {
            RerankError::NotTrained(
                "bbq: codec must be trained before encoding (call train() with a sample of vectors)"
                    .to_string(),
            )
        })?;
        let quantized = codec.encode(v);
        Ok(quantized.as_ref().as_bytes().to_vec())
    }

    /// Prepare the query by centering it and serialising the exact FP32 centered
    /// vector alongside the query norm.
    ///
    /// The prepared form is `PreparedQuery::Bytes` with the layout:
    ///   4 bytes query_norm (f32 LE) || dim × 4 bytes centered f32 LE.
    fn prepare_query(&self, q: &[f32]) -> Result<PreparedQuery, RerankError> {
        if q.len() != self.dim {
            return Err(RerankError::BadInput(format!(
                "bbq prepare_query: query len {} != codec dim {}",
                q.len(),
                self.dim
            )));
        }
        let codec = self.codec.as_ref().ok_or_else(|| {
            RerankError::NotTrained(
                "bbq: codec must be trained before prepare_query (call train() with a sample of vectors)"
                    .to_string(),
            )
        })?;
        let query = codec.prepare_query(q);
        Ok(PreparedQuery::Bytes(encode_payload(
            query.query_norm,
            &query.centered,
        )))
    }

    /// Compute asymmetric L2 distance from a prepared query to a BBQ-encoded
    /// candidate.
    ///
    /// The query is the exact centered FP32 vector. The stored candidate is
    /// reconstructed from its sign bits and `residual_norm` (each dim ≈
    /// ±norm/√dim). Returns L2 distance between them.
    ///
    /// Expects `PreparedQuery::Bytes` produced by `prepare_query`.
    fn distance_prepared(
        &self,
        prepared: &PreparedQuery,
        encoded: &[u8],
    ) -> Result<f32, RerankError> {
        let payload = match prepared {
            PreparedQuery::Bytes(b) => b.as_slice(),
            _ => {
                return Err(RerankError::BadInput(
                    "bbq distance: prepared query is not Bytes".to_string(),
                ));
            }
        };

        let expected = payload_len(self.dim);
        if payload.len() != expected {
            return Err(RerankError::BadInput(format!(
                "bbq distance: payload len {} != expected {} for dim {}",
                payload.len(),
                expected,
                self.dim
            )));
        }

        let packed_len = self.dim.div_ceil(8);
        let uqv_ref = UnifiedQuantizedVectorRef::from_bytes(encoded, packed_len).map_err(|e| {
            RerankError::BadInput(format!("bbq distance: failed to parse encoded bytes: {e}"))
        })?;

        let header = uqv_ref.header();
        // Fused and allocation-free: the kernel reads the centered query
        // straight from the prepared payload bytes, so a rerank pass pays one
        // pass and no allocation per candidate (nor per query).
        Ok((crate::distance::simd::runtime().l2_bbq)(
            &payload[4..],
            uqv_ref.packed_bits(),
            header.residual_norm,
            self.dim,
        ))
    }

    fn name(&self) -> CodecName {
        CodecName::Bbq
    }

    fn to_bytes(&self) -> Result<Vec<u8>, RerankError> {
        let codec = self.codec.as_ref().ok_or_else(|| {
            RerankError::NotTrained("bbq sidecar serialize: codec not trained".to_string())
        })?;
        codec
            .to_bytes()
            .map_err(|e| RerankError::BadInput(format!("bbq to_bytes: {e}")))
    }

    /// Calibrate from a sample of vectors.
    ///
    /// Validates that:
    /// - `samples` is non-empty.
    /// - Every sample has length `self.dim`.
    ///
    /// On success, stores the calibrated codec; subsequent `encode` /
    /// `distance_prepared` calls will succeed.
    fn train(&mut self, samples: &[&[f32]]) -> Result<(), RerankError> {
        if samples.is_empty() {
            return Err(RerankError::BadInput(
                "bbq train: empty sample set".to_string(),
            ));
        }
        for s in samples {
            if s.len() != self.dim {
                return Err(RerankError::BadInput(format!(
                    "bbq train: sample has len {} but codec dim is {}",
                    s.len(),
                    self.dim
                )));
            }
        }
        let codec = BbqCodec::calibrate(samples, self.dim, self.oversample);
        self.codec = Some(codec);
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const DIM: usize = 16;
    const N: usize = 64;

    fn det_vec(i: usize, dim: usize) -> Vec<f32> {
        (0..dim)
            .map(|j| ((i * 31 + j) % 100) as f32 / 100.0)
            .collect()
    }

    fn trained() -> BbqRerank {
        let vecs: Vec<Vec<f32>> = (0..N).map(|i| det_vec(i, DIM)).collect();
        let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let mut codec = BbqRerank::new(DIM, DEFAULT_OVERSAMPLE);
        codec.train(&refs).expect("train must succeed");
        codec
    }

    #[test]
    fn train_then_encode_roundtrip() {
        let codec = trained();
        let v = det_vec(0, DIM);
        let enc = codec.encode(&v).expect("encode");
        let prep = codec.prepare_query(&v).expect("prepare_query");
        let dist = codec.distance_prepared(&prep, &enc).expect("distance");
        assert!(dist.is_finite(), "distance must be finite, got {dist}");
        assert!(dist >= 0.0, "distance must be non-negative, got {dist}");
    }

    #[test]
    fn encode_before_train_returns_not_trained() {
        let codec = BbqRerank::new(DIM, DEFAULT_OVERSAMPLE);
        let v = det_vec(0, DIM);
        let err = codec.encode(&v).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("not trained") || msg.contains("trained"),
            "expected 'trained' in error, got: {msg}"
        );
    }

    #[test]
    fn train_with_empty_samples_fails() {
        let mut codec = BbqRerank::new(DIM, DEFAULT_OVERSAMPLE);
        let err = codec.train(&[]).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("bad input") || msg.contains("empty"),
            "expected bad input error, got: {msg}"
        );
    }

    #[test]
    fn train_with_dim_mismatch_fails() {
        let vecs: Vec<Vec<f32>> = (0..N).map(|i| det_vec(i, DIM)).collect();
        let mut refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
        let bad = det_vec(0, DIM + 4);
        refs.push(bad.as_slice());
        let mut codec = BbqRerank::new(DIM, DEFAULT_OVERSAMPLE);
        let err = codec.train(&refs).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("bad input") || msg.contains("dim"),
            "expected bad input error, got: {msg}"
        );
    }

    #[test]
    fn prepare_query_wrong_dim_fails() {
        let codec = trained();
        let bad = det_vec(0, DIM + 2);
        match codec.prepare_query(&bad) {
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    msg.contains("bad input") || msg.contains("dim"),
                    "expected bad input error, got: {msg}"
                );
            }
            Ok(_) => panic!("expected an error for wrong dim"),
        }
    }

    #[test]
    fn distance_prepared_wrong_variant_fails() {
        let codec = trained();
        let v = det_vec(0, DIM);
        let enc = codec.encode(&v).expect("encode");
        let bad_prepared = PreparedQuery::Raw(vec![0.0f32; DIM]);
        let err = codec.distance_prepared(&bad_prepared, &enc).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("Bytes") || msg.contains("not Bytes"),
            "error message should mention Bytes variant, got: {msg}"
        );
    }

    #[test]
    fn name_is_expected() {
        let codec = BbqRerank::new(DIM, DEFAULT_OVERSAMPLE);
        assert_eq!(codec.name(), CodecName::Bbq);
    }
}
