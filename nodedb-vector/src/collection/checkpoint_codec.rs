// SPDX-License-Identifier: Apache-2.0

//! Persisting the codec of a sealed segment in a checkpoint.
//!
//! PQ and SQ8 codecs persist their parameters and codes. A graph codec is
//! never persisted: the restored segment carries none, and the owning core
//! queues its rebuild.

use crate::collection::checkpoint::SealedSnapshot;
use crate::collection::segment::SealedSegment;
use crate::collection::segment_codec::SegmentCodec;
use crate::error::VectorError;
use crate::quantize::pq::PqCodec;
use crate::quantize::sq8::Sq8Codec;

/// The checkpoint snapshot of sealed segment `seg`: its graph and its PQ or
/// SQ8 codec.
pub(super) fn sealed_snapshot(seg: &SealedSegment) -> Result<SealedSnapshot, VectorError> {
    let (pq_bytes, pq_codes, sq8_bytes, sq8_codes) = match &seg.codec {
        SegmentCodec::Pq { codec, codes } => (
            Some(
                codec
                    .to_bytes()
                    .map_err(|e| VectorError::CheckpointSerializationError {
                        detail: format!("PQ codec encode: {e}"),
                    })?,
            ),
            Some(codes.clone()),
            None,
            None,
        ),
        SegmentCodec::Sq8 { codec, codes } => {
            (None, None, Some(codec.to_bytes()), Some(codes.clone()))
        }
        SegmentCodec::None | SegmentCodec::Graph(_) => (None, None, None, None),
    };
    Ok(SealedSnapshot {
        base_id: seg.base_id,
        hnsw_bytes: seg.index.checkpoint_to_bytes()?,
        pq_bytes,
        pq_codes,
        sq8_bytes,
        sq8_codes,
    })
}

/// The codec a sealed snapshot persisted: PQ, SQ8, or none. The codes must
/// hold one row per node of the segment's `nodes` nodes: `m` bytes per row
/// for PQ, `dim` bytes for SQ8.
pub(super) fn restore_codec(
    ss: &SealedSnapshot,
    nodes: usize,
    memory: &nodedb_mem::ScopedMemory,
) -> Result<SegmentCodec, VectorError> {
    match (&ss.pq_bytes, &ss.pq_codes, &ss.sq8_bytes, &ss.sq8_codes) {
        (Some(bytes), Some(codes), None, None) => {
            let codec = PqCodec::from_bytes(bytes, memory.clone()).map_err(|e| {
                VectorError::CheckpointDeserializationError {
                    detail: format!("PQ codec decode: {e}"),
                }
            })?;
            check_code_len(ss.base_id, "PQ", codes.len(), nodes, codec.m)?;
            Ok(SegmentCodec::Pq {
                codec,
                codes: codes.clone(),
            })
        }
        // Restore SQ8 from persisted bytes, never recompute on load.
        (None, None, Some(bytes), Some(codes)) => {
            let codec = Sq8Codec::from_bytes(bytes).map_err(|e| {
                VectorError::CheckpointDeserializationError {
                    detail: format!("SQ8 codec decode: {e}"),
                }
            })?;
            check_code_len(ss.base_id, "SQ8", codes.len(), nodes, codec.dim())?;
            Ok(SegmentCodec::Sq8 {
                codec,
                codes: codes.clone(),
            })
        }
        (None, None, None, None) => Ok(SegmentCodec::None),
        _ => Err(VectorError::CheckpointDeserializationError {
            detail: format!(
                "sealed segment at {} carries an incomplete or mixed codec",
                ss.base_id
            ),
        }),
    }
}

/// Check that `len` code bytes hold one `width`-byte row per node.
fn check_code_len(
    base_id: u32,
    codec: &str,
    len: usize,
    nodes: usize,
    width: usize,
) -> Result<(), VectorError> {
    if nodes.checked_mul(width) == Some(len) {
        return Ok(());
    }
    Err(VectorError::CheckpointDeserializationError {
        detail: format!(
            "sealed segment at {base_id} holds {len} {codec} code bytes; \
             {nodes} nodes of {width} bytes need {}",
            nodes.saturating_mul(width)
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::lifecycle::VectorCollection;
    use crate::distance::DistanceMetric;
    use crate::hnsw::HnswParams;
    use crate::test_support::{build_now, test_memory};

    /// A snapshot of one sealed segment of 64 vectors under `coll`'s plan.
    fn sealed(mut coll: VectorCollection) -> (SealedSnapshot, usize) {
        coll.set_seal_threshold(64);
        for i in 0..64u32 {
            coll.insert(vec![i as f32, (i % 5) as f32, (i % 7) as f32, 1.0])
                .unwrap();
        }
        let req = coll.seal("k", test_memory()).unwrap();
        assert!(
            coll.complete_build(req.segment_id, build_now(&req))
                .installed()
        );
        let seg = &coll.sealed[0];
        (sealed_snapshot(seg).unwrap(), seg.index.len())
    }

    fn l2() -> HnswParams {
        HnswParams {
            metric: DistanceMetric::L2,
            ..HnswParams::default()
        }
    }

    #[test]
    fn truncated_sq8_codes_fail_the_restore() {
        let (mut ss, nodes) = sealed(VectorCollection::new(4, l2()));
        assert!(restore_codec(&ss, nodes, &test_memory()).is_ok());
        if let Some(codes) = ss.sq8_codes.as_mut() {
            codes.pop();
        }
        let err = restore_codec(&ss, nodes, &test_memory()).err();
        assert!(
            matches!(
                err,
                Some(VectorError::CheckpointDeserializationError { .. })
            ),
            "{err:?}"
        );
    }

    #[test]
    fn truncated_pq_codes_fail_the_restore() {
        let (mut ss, nodes) = sealed(VectorCollection::with_pq_config(4, l2(), 2));
        assert!(restore_codec(&ss, nodes, &test_memory()).is_ok());
        if let Some(codes) = ss.pq_codes.as_mut() {
            codes.truncate(codes.len() - 2);
        }
        let err = restore_codec(&ss, nodes, &test_memory()).err();
        assert!(
            matches!(
                err,
                Some(VectorError::CheckpointDeserializationError { .. })
            ),
            "{err:?}"
        );
    }
}
