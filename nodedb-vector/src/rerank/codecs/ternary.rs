// SPDX-License-Identifier: Apache-2.0

//! `RerankCodec` wrapper for BitNet 1.58 ternary quantization.
//!
//! Bridges `TernaryCodec` (which implements `VectorCodec` with associated
//! types) into the object-safe `RerankCodec` trait used by the rerank sidecar.
//!
//! Ternary has no learned state: each vector carries its own absmean scale,
//! so `train()` is the default no-op.
//!
//! The prepared form is `PreparedQuery::Bytes` with the layout:
//!   [0..4]   query scale as f32 little-endian
//!   [4..]    hot-packed query trits (length = dim.div_ceil(4))

use nodedb_codec::vector_quant::codec::VectorCodec as _;
use nodedb_codec::vector_quant::layout::{QuantMode, UnifiedQuantizedVectorRef};
use nodedb_codec::vector_quant::ternary::{TernaryCodec, cold_to_hot, ternary_dot, unpack_hot};

use crate::{
    rerank::codec::{CodecName, PreparedQuery, RerankCodec},
    rerank::types::RerankError,
};

/// Length of the serialized-state magic.
const MAGIC_LEN: usize = 6;
/// Serialized-state magic: `NDTRN\0`, then a version byte and `dim` as u32 LE.
const MAGIC: &[u8; MAGIC_LEN] = b"NDTRN\0";
/// Serialized-state format version.
const FORMAT_VERSION: u8 = 1;
/// Length of the serialized state.
const STATE_LEN: usize = MAGIC_LEN + 1 + 4;

/// Hot-packed trit bytes for `dim` components: 4 trits per byte.
#[inline]
fn hot_len(dim: usize) -> usize {
    dim.div_ceil(4)
}

/// Squared norm of hot-packed trits scaled by `scale`.
fn scaled_norm_sq(hot: &[u8], dim: usize, scale: f32) -> f32 {
    let nonzero = unpack_hot(hot, dim).iter().filter(|&&t| t != 0).count();
    scale * scale * nonzero as f32
}

/// Object-safe `RerankCodec` wrapper around `TernaryCodec`.
pub struct TernaryRerank {
    codec: TernaryCodec,
    dim: usize,
}

impl TernaryRerank {
    /// Create a ternary rerank codec for vectors of length `dim`.
    pub fn new(dim: usize) -> Self {
        Self {
            codec: TernaryCodec::new(dim),
            dim,
        }
    }

    /// Restore from the bytes [`RerankCodec::to_bytes`] wrote.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, RerankError> {
        if bytes.len() != STATE_LEN {
            return Err(RerankError::BadInput(format!(
                "ternary from_bytes: {} bytes, expected {STATE_LEN}",
                bytes.len()
            )));
        }
        if &bytes[..MAGIC_LEN] != MAGIC {
            return Err(RerankError::BadInput(
                "ternary from_bytes: bad magic".into(),
            ));
        }
        if bytes[MAGIC_LEN] != FORMAT_VERSION {
            return Err(RerankError::BadInput(format!(
                "ternary from_bytes: unsupported version {}",
                bytes[MAGIC_LEN]
            )));
        }
        let mut dim_bytes = [0u8; 4];
        dim_bytes.copy_from_slice(&bytes[MAGIC_LEN + 1..]);
        Ok(Self::new(u32::from_le_bytes(dim_bytes) as usize))
    }
}

impl RerankCodec for TernaryRerank {
    /// Encode a full-precision vector to hot-packed trits. The serialized
    /// form is the raw `UnifiedQuantizedVector` buffer: header, then trits.
    fn encode(&self, v: &[f32]) -> Result<Vec<u8>, RerankError> {
        if v.len() != self.dim {
            return Err(RerankError::BadInput(format!(
                "ternary encode: vector len {} != codec dim {}",
                v.len(),
                self.dim
            )));
        }
        Ok(self.codec.encode(v).as_ref().as_bytes().to_vec())
    }

    fn prepare_query(&self, q: &[f32]) -> Result<PreparedQuery, RerankError> {
        if q.len() != self.dim {
            return Err(RerankError::BadInput(format!(
                "ternary prepare_query: query len {} != codec dim {}",
                q.len(),
                self.dim
            )));
        }
        let query = self.codec.prepare_query(q);
        let mut payload = Vec::with_capacity(4 + query.trits_hot.len());
        payload.extend_from_slice(&query.scale.to_le_bytes());
        payload.extend_from_slice(&query.trits_hot);
        Ok(PreparedQuery::Bytes(payload))
    }

    /// Approximate squared L2 between the scaled query and candidate trits.
    fn distance_prepared(
        &self,
        prepared: &PreparedQuery,
        encoded: &[u8],
    ) -> Result<f32, RerankError> {
        let PreparedQuery::Bytes(payload) = prepared else {
            return Err(RerankError::BadInput(
                "ternary distance: expected PreparedQuery::Bytes".to_string(),
            ));
        };
        let packed_len = hot_len(self.dim);
        if payload.len() != 4 + packed_len {
            return Err(RerankError::BadInput(format!(
                "ternary distance: payload len {} != expected {} for dim {}",
                payload.len(),
                4 + packed_len,
                self.dim
            )));
        }
        let mut scale_bytes = [0u8; 4];
        scale_bytes.copy_from_slice(&payload[..4]);
        let q_scale = f32::from_le_bytes(scale_bytes);
        let q_hot = &payload[4..];

        let uqv = UnifiedQuantizedVectorRef::from_bytes(encoded, packed_len).map_err(|e| {
            RerankError::BadInput(format!(
                "ternary distance: failed to parse encoded bytes: {e}"
            ))
        })?;
        let header = uqv.header();
        let v_hot = if header.quant_mode == QuantMode::TernarySimd as u16 {
            uqv.packed_bits().to_vec()
        } else if header.quant_mode == QuantMode::TernaryPacked as u16 {
            cold_to_hot(uqv.packed_bits(), self.dim)
        } else {
            return Err(RerankError::BadInput(format!(
                "ternary distance: candidate quant mode {} is not ternary",
                header.quant_mode
            )));
        };
        let v_scale = header.global_scale;
        let dot = ternary_dot(q_hot, &v_hot, self.dim);
        let norm_q = scaled_norm_sq(q_hot, self.dim, q_scale);
        let norm_v = scaled_norm_sq(&v_hot, self.dim, v_scale);
        Ok((norm_q + norm_v - 2.0 * q_scale * v_scale * dot as f32).max(0.0))
    }

    fn name(&self) -> CodecName {
        CodecName::Ternary
    }

    /// Format: `[NDTRN\0][version u8][dim u32 LE]`.
    fn to_bytes(&self) -> Result<Vec<u8>, RerankError> {
        let dim = u32::try_from(self.dim).map_err(|_| {
            RerankError::BadInput(format!("ternary to_bytes: dim {} exceeds u32", self.dim))
        })?;
        let mut buf = Vec::with_capacity(STATE_LEN);
        buf.extend_from_slice(MAGIC);
        buf.push(FORMAT_VERSION);
        buf.extend_from_slice(&dim.to_le_bytes());
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIM: usize = 16;

    fn vector(sign: f32) -> Vec<f32> {
        (0..DIM)
            .map(|i| if i % 3 == 0 { 0.0 } else { sign })
            .collect()
    }

    #[test]
    fn identical_vectors_have_zero_distance() {
        let codec = TernaryRerank::new(DIM);
        let v = vector(1.0);
        let enc = codec.encode(&v).expect("encode");
        let prepared = codec.prepare_query(&v).expect("prepare");
        let d = codec.distance_prepared(&prepared, &enc).expect("distance");
        assert!(d.abs() < 1e-4, "self distance {d}");
    }

    #[test]
    fn opposite_vectors_are_farther_than_identical() {
        let codec = TernaryRerank::new(DIM);
        let pos = vector(1.0);
        let neg = vector(-1.0);
        let prepared = codec.prepare_query(&pos).expect("prepare");
        let near = codec
            .distance_prepared(&prepared, &codec.encode(&pos).expect("encode"))
            .expect("distance");
        let far = codec
            .distance_prepared(&prepared, &codec.encode(&neg).expect("encode"))
            .expect("distance");
        assert!(far > near, "far {far} near {near}");
    }

    #[test]
    fn bytes_roundtrip_keeps_dim() {
        let codec = TernaryRerank::new(DIM);
        let bytes = codec.to_bytes().expect("to_bytes");
        let restored = TernaryRerank::from_bytes(&bytes).expect("from_bytes");
        assert_eq!(restored.dim, DIM);
        assert_eq!(restored.name(), CodecName::Ternary);
        assert!(TernaryRerank::from_bytes(&bytes[..5]).is_err());
    }

    #[test]
    fn wrong_dim_is_bad_input() {
        let codec = TernaryRerank::new(DIM);
        assert!(codec.encode(&[0.0; DIM + 1]).is_err());
        assert!(codec.prepare_query(&[0.0; DIM - 1]).is_err());
    }
}
