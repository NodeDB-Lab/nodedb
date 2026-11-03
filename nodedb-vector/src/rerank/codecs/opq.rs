// SPDX-License-Identifier: Apache-2.0

//! `RerankCodec` wrapper for Optimized Product Quantization (OPQ).
//!
//! OPQ is a training-based codec: `train()` learns the rotation and the PQ
//! codebooks. Until training is complete, `encode`, `prepare_query` and
//! `to_bytes` return `RerankError::NotTrained`.
//!
//! The encoded form is the `m` code bytes, one per subspace. The prepared
//! form is `PreparedQuery::Lut`: per subspace, the squared L2 from the rotated
//! query sub-vector to every centroid.

use nodedb_codec::vector_quant::codec::VectorCodec as _;
use nodedb_codec::vector_quant::opq::OpqCodec;

use crate::{
    rerank::codec::{CodecName, PreparedQuery, RerankCodec},
    rerank::types::RerankError,
};

/// Most centroids per subspace: one byte per code.
const MAX_CENTROIDS: usize = 256;
/// Alternating rotation and codebook passes of a training run.
const OPQ_ITERS: usize = 4;
/// k-means iterations per pass.
const KMEANS_ITERS: usize = 10;

/// Object-safe `RerankCodec` wrapper around `OpqCodec`.
pub struct OpqRerank {
    codec: Option<OpqCodec>,
    dim: usize,
    m: usize,
}

impl OpqRerank {
    /// An untrained wrapper for vectors of length `dim` split into `m`
    /// subspaces.
    pub fn new(dim: usize, m: usize) -> Self {
        Self {
            codec: None,
            dim,
            m,
        }
    }

    /// Wrap a trained codec (used when restoring from a snapshot).
    pub fn from_codec(codec: OpqCodec) -> Self {
        let (dim, m) = (codec.dim, codec.m);
        Self {
            codec: Some(codec),
            dim,
            m,
        }
    }

    fn trained(&self, op: &str) -> Result<&OpqCodec, RerankError> {
        self.codec.as_ref().ok_or_else(|| {
            RerankError::NotTrained(format!(
                "opq: codec must be trained before {op} (call train() with a sample of vectors)"
            ))
        })
    }

    fn check_len(&self, op: &str, len: usize) -> Result<(), RerankError> {
        if len == self.dim {
            Ok(())
        } else {
            Err(RerankError::BadInput(format!(
                "opq {op}: vector len {len} != codec dim {}",
                self.dim
            )))
        }
    }
}

impl RerankCodec for OpqRerank {
    fn encode(&self, v: &[f32]) -> Result<Vec<u8>, RerankError> {
        self.check_len("encode", v.len())?;
        let codec = self.trained("encoding")?;
        Ok(codec.encode(v).as_ref().packed_bits().to_vec())
    }

    fn prepare_query(&self, q: &[f32]) -> Result<PreparedQuery, RerankError> {
        self.check_len("prepare_query", q.len())?;
        let codec = self.trained("prepare_query")?;
        let query = codec.prepare_query(q);
        let lut = query
            .distance_table
            .chunks(codec.k)
            .map(<[f32]>::to_vec)
            .collect();
        Ok(PreparedQuery::Lut(lut))
    }

    /// Sum of one table entry per subspace: the ADC squared L2.
    fn distance_prepared(
        &self,
        prepared: &PreparedQuery,
        encoded: &[u8],
    ) -> Result<f32, RerankError> {
        let PreparedQuery::Lut(lut) = prepared else {
            return Err(RerankError::BadInput(
                "opq distance: expected PreparedQuery::Lut".to_string(),
            ));
        };
        if encoded.len() != self.m || lut.len() != self.m {
            return Err(RerankError::BadInput(format!(
                "opq distance: {} codes and {} table rows, expected {}",
                encoded.len(),
                lut.len(),
                self.m
            )));
        }
        let mut sum = 0.0f32;
        for (row, &code) in lut.iter().zip(encoded) {
            sum += row.get(usize::from(code)).copied().ok_or_else(|| {
                RerankError::BadInput(format!(
                    "opq distance: code {code} outside the {} centroids",
                    row.len()
                ))
            })?;
        }
        Ok(sum)
    }

    fn name(&self) -> CodecName {
        CodecName::Opq
    }

    fn to_bytes(&self) -> Result<Vec<u8>, RerankError> {
        self.trained("serialize")?
            .to_bytes()
            .map_err(|e| RerankError::BadInput(format!("opq to_bytes: {e}")))
    }

    /// Train the rotation and codebooks on `samples`, with up to 256
    /// centroids per subspace.
    fn train(&mut self, samples: &[&[f32]]) -> Result<(), RerankError> {
        let codec = OpqCodec::train(
            samples,
            self.dim,
            self.m,
            MAX_CENTROIDS,
            OPQ_ITERS,
            KMEANS_ITERS,
        )
        .map_err(|e| RerankError::Train(format!("opq train: {e}")))?;
        self.codec = Some(codec);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIM: usize = 8;

    fn vectors() -> Vec<Vec<f32>> {
        (0..40)
            .map(|i| (0..DIM).map(|j| ((i * 7 + j * 3) % 17) as f32).collect())
            .collect()
    }

    fn trained() -> OpqRerank {
        let vecs = vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(Vec::as_slice).collect();
        let mut codec = OpqRerank::new(DIM, 2);
        codec.train(&refs).expect("train");
        codec
    }

    #[test]
    fn untrained_codec_refuses_to_encode() {
        let codec = OpqRerank::new(DIM, 2);
        assert!(matches!(
            codec.encode(&[0.0; DIM]),
            Err(RerankError::NotTrained(_))
        ));
    }

    #[test]
    fn own_code_ranks_in_the_nearer_half() {
        let codec = trained();
        let vecs = vectors();
        let prepared = codec.prepare_query(&vecs[5]).expect("prepare");
        let mut all: Vec<f32> = vecs
            .iter()
            .map(|v| {
                codec
                    .distance_prepared(&prepared, &codec.encode(v).expect("encode"))
                    .expect("distance")
            })
            .collect();
        let own = all[5];
        all.sort_by(f32::total_cmp);
        assert!(
            own <= all[all.len() / 2],
            "own {own}, median {}",
            all[all.len() / 2]
        );
    }

    #[test]
    fn bytes_roundtrip_restores_the_codec() {
        let codec = trained();
        let bytes = codec.to_bytes().expect("to_bytes");
        let inner = OpqCodec::from_bytes(&bytes).expect("from_bytes");
        let restored = OpqRerank::from_codec(inner);
        let v = vectors().remove(3);
        assert_eq!(
            codec.encode(&v).expect("encode"),
            restored.encode(&v).expect("encode")
        );
    }

    #[test]
    fn train_rejects_m_not_dividing_dim() {
        let vecs = vectors();
        let refs: Vec<&[f32]> = vecs.iter().map(Vec::as_slice).collect();
        let mut codec = OpqRerank::new(DIM, 3);
        assert!(matches!(codec.train(&refs), Err(RerankError::Train(_))));
    }
}
