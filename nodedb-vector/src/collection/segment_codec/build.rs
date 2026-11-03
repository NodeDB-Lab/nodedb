// SPDX-License-Identifier: Apache-2.0

//! Building a segment codec over a built HNSW graph, on the builder thread.
//!
//! Every allocation is reserved from the request's memory scope before it
//! is made. The vector table and training scratch release their
//! reservations when they drop. The built codec's reservations travel with
//! it in a [`CodecCharge`].

use std::borrow::Cow;
use std::mem::size_of;

use nodedb_codec::vector_quant::bbq::BbqCodec;
use nodedb_codec::vector_quant::codec::VectorCodec;
use nodedb_codec::vector_quant::opq::OpqCodec;
use nodedb_codec::vector_quant::rabitq::RaBitQCodec;
use nodedb_codec::vector_quant::ternary::TernaryCodec;
use nodedb_mem::ScopedMemory;

use crate::codec_index::HnswCodecIndex;
use crate::error::VectorError;
use crate::hnsw::{HnswIndex, HnswParams};
use crate::quantize::BinaryCodec;
use crate::quantize::pq::{PQ_CENTROIDS, PqCodec};
use crate::quantize::sq8::Sq8Codec;

use super::charge::{
    CodecCharge, graph_bytes, opq_training_bytes, pq_training_bytes, scratch, vector_table_bytes,
};
use super::codec::SegmentCodec;
use super::graph::CodecGraph;
use super::plan::{CodecPlan, GraphCodecKind, check_graph_codec_shape};

/// k-means iterations of a PQ codebook.
const PQ_TRAIN_ITERS: usize = 20;
/// Alternating rotation and codebook passes of an OPQ codec.
const OPQ_ITERS: usize = 4;
/// k-means iterations per OPQ pass.
const OPQ_KMEANS_ITERS: usize = 10;
/// Most vectors a learned codec trains on, taken at an even stride.
const MAX_TRAINING_SAMPLES: usize = 10_000;
/// BBQ calibration oversample.
const BBQ_CALIBRATION_OVERSAMPLE: u8 = 3;
/// SQ8 parameters per dimension: min, max, scale and inverse scale.
const SQ8_PARAMS_PER_DIM: usize = 4;

/// The inputs of one graph codec build.
struct GraphBuild<'a> {
    all: &'a [&'a [f32]],
    sample: &'a [&'a [f32]],
    dim: usize,
    params: &'a HnswParams,
    seed: u64,
    memory: &'a ScopedMemory,
}

impl SegmentCodec {
    /// Build the codec `plan` names over every node of `index`, soft-deleted
    /// nodes included: codes and graph nodes are indexed by local node id.
    /// Vectors are read in place when the index stores f32, and decoded
    /// otherwise. SQ8 calibrates on every vector. The learned codecs train
    /// on at most `MAX_TRAINING_SAMPLES` vectors.
    ///
    /// Returns the codec and the reservations that cover it. Fails with
    /// [`VectorError::BudgetExhausted`] when `memory` refuses a reservation,
    /// and with the codec's own error when the shape does not fit.
    pub fn build(
        plan: CodecPlan,
        index: &HnswIndex,
        memory: ScopedMemory,
        seed: u64,
    ) -> Result<(Self, CodecCharge), VectorError> {
        if plan == CodecPlan::None {
            return Ok((Self::None, CodecCharge::default()));
        }
        let dim = index.dim();
        let nodes = index.len();
        if nodes == 0 {
            return Err(VectorError::InvalidInput {
                detail: "a vector codec needs at least one vector".into(),
            });
        }
        if let CodecPlan::Graph(_) = plan {
            check_graph_codec_shape(dim)?;
        }
        let stride = nodes.div_ceil(MAX_TRAINING_SAMPLES);
        let samples = nodes.div_ceil(stride);
        let owned = (0..nodes as u32)
            .filter(|&id| !index.borrows_f32(id))
            .count();
        let _table = scratch(&memory, vector_table_bytes(nodes, samples, owned, dim))?;
        let vectors: Vec<Cow<'_, [f32]>> = (0..nodes as u32)
            .map(|id| index.vector_f32(id))
            .collect::<Result<_, _>>()?;
        let all: Vec<&[f32]> = vectors.iter().map(|v| &**v).collect();
        let sample: Vec<&[f32]> = all.iter().step_by(stride).copied().collect();

        let mut held = CodecCharge::default();
        let codec = match plan {
            CodecPlan::None => Self::None,
            CodecPlan::Sq8 => {
                let params = dim * SQ8_PARAMS_PER_DIM * size_of::<f32>();
                held.reserve(&memory, params.saturating_add(nodes.saturating_mul(dim)))?;
                let codec = Sq8Codec::calibrate(&all, dim)?;
                let codes = codec.quantize_batch(&all);
                Self::Sq8 { codec, codes }
            }
            CodecPlan::Pq { m } => {
                let k = PQ_CENTROIDS.min(sample.len());
                let codebook = k.saturating_mul(dim).saturating_mul(size_of::<f32>());
                held.reserve(&memory, codebook.saturating_add(nodes.saturating_mul(m)))?;
                let codec = {
                    let _training = scratch(&memory, pq_training_bytes(sample.len(), k, dim))?;
                    PqCodec::train(&sample, dim, m, k, PQ_TRAIN_ITERS, memory.clone())?
                };
                let codes = codec.encode_batch(&all)?;
                Self::Pq { codec, codes }
            }
            CodecPlan::Graph(kind) => {
                let build = GraphBuild {
                    all: &all,
                    sample: &sample,
                    dim,
                    params: &index.params,
                    seed,
                    memory: &memory,
                };
                Self::Graph(CodecGraph::build(kind, &build, &mut held)?)
            }
        };
        // An estimate below the codec's real size is topped up here.
        held.cover(&memory, codec.resident_bytes())?;
        Ok((codec, held))
    }
}

impl CodecGraph {
    /// Train the codec `kind` names on `build.sample` and insert every
    /// vector of `build.all` under its local node id. The graph's
    /// reservation goes into `held`.
    fn build(
        kind: GraphCodecKind,
        build: &GraphBuild<'_>,
        held: &mut CodecCharge,
    ) -> Result<Self, VectorError> {
        let (dim, sample, seed) = (build.dim, build.sample, build.seed);
        let f32_bytes = size_of::<f32>();
        Ok(match kind {
            GraphCodecKind::RaBitQ => {
                // The centroid holds one f32 per dimension.
                held.reserve(build.memory, dim.saturating_mul(f32_bytes))?;
                let codec = RaBitQCodec::calibrate(sample, dim, seed);
                Self::RaBitQ(fill(codec, build, held)?)
            }
            GraphCodecKind::Bbq => {
                held.reserve(build.memory, dim.saturating_mul(f32_bytes))?;
                let codec = BbqCodec::calibrate(sample, dim, BBQ_CALIBRATION_OVERSAMPLE);
                Self::Bbq(fill(codec, build, held)?)
            }
            GraphCodecKind::Binary => Self::Binary(fill(BinaryCodec { dim }, build, held)?),
            GraphCodecKind::Ternary => Self::Ternary(fill(TernaryCodec::new(dim), build, held)?),
            GraphCodecKind::Opq { m: pq_m } => {
                let k = PQ_CENTROIDS.min(sample.len());
                // The rotation is `dim × dim` and the codebooks `k × dim`.
                let rotation = dim.saturating_mul(dim);
                let codebooks = k.saturating_mul(dim);
                held.reserve(
                    build.memory,
                    rotation.saturating_add(codebooks).saturating_mul(f32_bytes),
                )?;
                let codec = {
                    let _training =
                        scratch(build.memory, opq_training_bytes(sample.len(), k, dim))?;
                    OpqCodec::train(sample, dim, pq_m, k, OPQ_ITERS, OPQ_KMEANS_ITERS)?
                };
                Self::Opq(fill(codec, build, held)?)
            }
        })
    }
}

/// A codec graph over `codec` holding every vector of `build.all` under its
/// local node id. The graph's estimated bytes are reserved into `held`
/// before the first insert.
fn fill<C: VectorCodec>(
    codec: C,
    build: &GraphBuild<'_>,
    held: &mut CodecCharge,
) -> Result<HnswCodecIndex<C>, VectorError> {
    let quantized = build
        .all
        .first()
        .map_or(0, |v| codec.encode(v).as_ref().as_bytes().len());
    let (m, efc) = (build.params.m, build.params.ef_construction);
    held.reserve(
        build.memory,
        graph_bytes::<C>(build.all.len(), quantized, m),
    )?;
    let mut index = HnswCodecIndex::new(build.dim, m, efc, codec, build.seed);
    for (local, v) in build.all.iter().enumerate() {
        index.insert(local as u32, v)?;
    }
    Ok(index)
}

#[cfg(test)]
mod tests {
    use nodedb_types::vector_dtype::VectorStorageDtype;

    use super::*;
    use crate::distance::DistanceMetric;
    use crate::test_support::{seeded_index, seeded_vectors, test_memory};

    fn index() -> HnswIndex {
        let vectors = seeded_vectors(64, 4, 3);
        seeded_index(&vectors, DistanceMetric::L2, VectorStorageDtype::F32)
    }

    #[test]
    fn a_built_codec_holds_a_charge_that_covers_it() {
        let plans = [
            CodecPlan::Sq8,
            CodecPlan::Pq { m: 2 },
            CodecPlan::Graph(GraphCodecKind::Binary),
            CodecPlan::Graph(GraphCodecKind::Opq { m: 2 }),
        ];
        for plan in plans {
            let (codec, charge) = SegmentCodec::build(plan, &index(), test_memory(), 7).unwrap();
            assert!(
                charge.bytes() >= codec.resident_bytes(),
                "{plan:?}: {} < {}",
                charge.bytes(),
                codec.resident_bytes()
            );
        }
    }

    #[test]
    fn a_codec_build_past_the_budget_is_refused() {
        let governor = std::sync::Arc::new(
            nodedb_mem::MemoryGovernor::new(nodedb_mem::GovernorConfig {
                global_ceiling: 64 * nodedb_mem::EngineId::ALL.len(),
                engine_limits: nodedb_mem::EngineLimits::uniform(64),
            })
            .expect("tight governor"),
        );
        let memory = ScopedMemory::new(
            governor,
            nodedb_types::DatabaseId::DEFAULT,
            nodedb_types::TenantId::new(0),
            nodedb_mem::EngineId::Vector,
        );
        let err = SegmentCodec::build(CodecPlan::Sq8, &index(), memory, 7).err();
        assert!(
            matches!(err, Some(VectorError::BudgetExhausted(_))),
            "{err:?}"
        );
    }
}
