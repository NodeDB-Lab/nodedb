// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use super::types::RerankError;

/// Identity tag for a rerank codec — used to detect mismatch when a search
/// requests a different codec than the sidecar was built with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecName {
    Sq8,
    Pq,
    Binary,
    RaBitQ,
    Bbq,
    Ternary,
    Opq,
}

impl CodecName {
    pub fn as_str(self) -> &'static str {
        match self {
            CodecName::Sq8 => "sq8",
            CodecName::Pq => "pq",
            CodecName::Binary => "binary",
            CodecName::RaBitQ => "rabitq",
            CodecName::Bbq => "bbq",
            CodecName::Ternary => "ternary",
            CodecName::Opq => "opq",
        }
    }
}

/// Prepared query form — opaque payload held by the caller between
/// `prepare_query` and `distance_prepared` calls. New variants will be added
/// as specific codec impls land in later sub-tasks.
pub enum PreparedQuery {
    /// Raw full-precision query, used by codecs whose prepared form is just
    /// the input vector (e.g. RaBitQ rotation applied later, Binary).
    Raw(Vec<f32>),
    /// Per-subspace lookup table, used by ADC-style codecs (PQ, OPQ).
    Lut(Vec<Vec<f32>>),
    /// Codec-specific opaque bytes — for codecs that don't fit the above two
    /// shapes (e.g. BBQ carries a centroid + alpha).
    Bytes(Vec<u8>),
}

/// Object-safe trait for asymmetric rerank codecs. Each impl wraps an existing
/// `nodedb-codec::VectorCodec` and exposes a uniform shape so the sidecar can
/// hold `Arc<dyn RerankCodec>` regardless of the underlying associated-type
/// machinery.
pub trait RerankCodec: Send + Sync {
    /// Encode a full-precision vector. Returns fixed-width bytes for this codec.
    fn encode(&self, v: &[f32]) -> Result<Vec<u8>, RerankError>;

    /// Prepare a query once before repeated distance calls.
    fn prepare_query(&self, q: &[f32]) -> Result<PreparedQuery, RerankError>;

    /// Compute asymmetric distance from a prepared query to an encoded vector.
    fn distance_prepared(
        &self,
        prepared: &PreparedQuery,
        encoded: &[u8],
    ) -> Result<f32, RerankError>;

    /// Identity tag for mismatch detection.
    fn name(&self) -> CodecName;

    /// Train from a sample of vectors. Default no-op for codecs that don't need
    /// training (e.g. Binary). Specific codec impls override this when needed.
    fn train(&mut self, _samples: &[&[f32]]) -> Result<(), RerankError> {
        Ok(())
    }

    /// Serialize trained state to bytes. Each codec uses its own magic header
    /// (NDSQ / NDBIN / NDPQ / NDRBQ / NDBBQ / NDTRN / NDOPQ). The bytes are codec-specific;
    /// `rerank_codec_from_bytes` is used for restore, paired with `name()`.
    fn to_bytes(&self) -> Result<Vec<u8>, RerankError>;
}

/// Reconstruct a `RerankCodec` from its byte form. The `name` tag tells us
/// which wrapper to dispatch into; the bytes are the codec's own format.
pub fn rerank_codec_from_bytes(
    name: CodecName,
    bytes: &[u8],
    memory: nodedb_mem::ScopedMemory,
) -> Result<Arc<dyn RerankCodec>, RerankError> {
    use crate::quantize::pq::PqCodec;
    use crate::quantize::sq8::Sq8Codec;
    use crate::rerank::codecs::{
        BbqRerank, BinaryRerank, OpqRerank, PqRerank, RaBitQRerank, Sq8Rerank, TernaryRerank,
    };
    use nodedb_codec::vector_quant::bbq::BbqCodec;
    use nodedb_codec::vector_quant::opq::OpqCodec;
    use nodedb_codec::vector_quant::rabitq::RaBitQCodec;

    match name {
        CodecName::Sq8 => {
            let inner = Sq8Codec::from_bytes(bytes)
                .map_err(|e| RerankError::BadInput(format!("sq8 from_bytes: {e}")))?;
            Ok(Arc::new(Sq8Rerank::from_codec(inner)))
        }
        CodecName::Binary => {
            // Format: [NDBIN\0 (6 bytes)][version u8 = 1][dim u32 LE (4 bytes)] — 11 bytes total.
            if bytes.len() < 11 {
                return Err(RerankError::BadInput("binary from_bytes: too short".into()));
            }
            if &bytes[..6] != b"NDBIN\0" {
                return Err(RerankError::BadInput("binary from_bytes: bad magic".into()));
            }
            // bytes[6] is version, bytes[7..11] is dim as u32 LE.
            let dim = u32::from_le_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]) as usize;
            Ok(Arc::new(BinaryRerank::new(dim)))
        }
        CodecName::Pq => {
            let inner = PqCodec::from_bytes(bytes, memory.clone())
                .map_err(|e| RerankError::BadInput(format!("pq from_bytes: {e}")))?;
            Ok(Arc::new(PqRerank::from_codec(inner, memory)))
        }
        CodecName::RaBitQ => {
            let inner = RaBitQCodec::from_bytes(bytes)
                .map_err(|e| RerankError::BadInput(format!("rabitq from_bytes: {e}")))?;
            Ok(Arc::new(RaBitQRerank::from_codec(inner)))
        }
        CodecName::Bbq => {
            let inner = BbqCodec::from_bytes(bytes)
                .map_err(|e| RerankError::BadInput(format!("bbq from_bytes: {e}")))?;
            Ok(Arc::new(BbqRerank::from_codec(inner)))
        }
        CodecName::Ternary => Ok(Arc::new(TernaryRerank::from_bytes(bytes)?)),
        CodecName::Opq => {
            let inner = OpqCodec::from_bytes(bytes)
                .map_err(|e| RerankError::BadInput(format!("opq from_bytes: {e}")))?;
            Ok(Arc::new(OpqRerank::from_codec(inner)))
        }
    }
}
