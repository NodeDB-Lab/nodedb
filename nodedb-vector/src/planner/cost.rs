// SPDX-License-Identifier: Apache-2.0

//! Multidimensional cost model for the vector query planner.
//!
//! All values are **heuristic estimates** derived from typical hardware
//! characteristics; they are not the result of micro-benchmarks.  The purpose
//! is to give the planner a consistent basis for comparing execution strategies
//! across quantization tiers and index types — not to produce sub-microsecond
//! accurate predictions.

use super::query_options::{IndexType, QuantizationKind};

/// All cost dimensions produced by `estimate_cost`.
#[derive(Debug, Clone, Copy)]
pub struct VectorCost {
    /// Estimated HNSW / Vamana graph traversal cost in microseconds.
    pub graph_traversal_us: f32,

    /// Estimated codec decode cost **per vector touched** in nanoseconds.
    pub codec_decode_ns: f32,

    /// Estimated rerank cost in microseconds.
    ///
    /// This is the full-precision re-scoring phase that follows compressed
    /// traversal.  Zero when `quantization == None`.
    pub rerank_us: f32,

    /// Filter selectivity in `[0.0, 1.0]`.  Copied from `CostModelInputs`.
    pub filter_selectivity: f32,

    /// Expected candidate set size after filtering.  Copied from inputs.
    pub candidate_set_size: usize,

    /// Predicted recall in `[0.0, 1.0]` for the chosen index × quantization
    /// combination.
    pub predicted_recall: f32,
}

/// Inputs to the cost model.
pub struct CostModelInputs {
    /// Total number of vectors in the collection.
    pub n_vectors: usize,

    /// Embedding dimension.
    pub dim: usize,

    /// Which graph index will be used.
    pub index_type: IndexType,

    /// Which quantization codec will be used during traversal.
    pub quantization: QuantizationKind,

    /// Beam width.
    pub ef_search: usize,

    /// Global filter selectivity in `[0.0, 1.0]`.
    /// `1.0` means no filter (all vectors are candidates).
    pub global_selectivity: f32,

    /// Expected candidate set size after applying filters.
    /// Typically `(n_vectors as f32 * global_selectivity) as usize`.
    pub candidate_set_size: usize,
}

// ── Internal constants ────────────────────────────────────────────────────────

/// Decode cost per vector in nanoseconds, by quantization kind.
fn codec_decode_ns_for(quantization: QuantizationKind) -> f32 {
    match quantization {
        QuantizationKind::None => 16.0,
        QuantizationKind::Sq8 => 4.0,
        QuantizationKind::Pq | QuantizationKind::Opq => 0.5,
        QuantizationKind::RaBitQ | QuantizationKind::Bbq | QuantizationKind::Binary => 0.3,
        QuantizationKind::Ternary => 0.4,
    }
}

/// Predicted recall, by index × quantization combination.
fn predicted_recall_for(index_type: IndexType, quantization: QuantizationKind) -> f32 {
    match (index_type, quantization) {
        (IndexType::Hnsw, QuantizationKind::None) => 0.99,
        (IndexType::Hnsw, QuantizationKind::Sq8) => 0.97,
        (IndexType::Hnsw, QuantizationKind::Pq) | (IndexType::Hnsw, QuantizationKind::Opq) => 0.92,
        (IndexType::Hnsw, QuantizationKind::RaBitQ) | (IndexType::Hnsw, QuantizationKind::Bbq) => {
            0.95
        }
        (IndexType::Hnsw, QuantizationKind::Binary) => 0.85,
        (IndexType::Hnsw, QuantizationKind::Ternary) => 0.90,
        // Vamana + RaBitQ with default oversample-3 rerank.
        (IndexType::Vamana, QuantizationKind::RaBitQ) => 0.96,
        (IndexType::Vamana, QuantizationKind::Bbq) => 0.96,
        (IndexType::Vamana, QuantizationKind::None) => 0.99,
        (IndexType::Vamana, QuantizationKind::Sq8) => 0.97,
        (IndexType::Vamana, _) => 0.92,
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Compute a multidimensional cost estimate for executing a vector query with
/// the given parameters.
///
/// # Heuristic model
///
/// - **Graph traversal** — scales with `log2(n_vectors) * ef_search` for HNSW
///   (hierarchical graph) and with `ef_search` alone for Vamana (flat beam
///   search over SSD-resident graph, I/O cost folded in heuristically).
///
/// - **Codec decode** — fixed ns/vector constant by quantization kind,
///   multiplied by `ef_search` (number of vectors scored during traversal).
///
/// - **Rerank** — only applies when a compressed codec is used.  Proportional
///   to `oversample * ef_search` at a fixed 0.01 µs/candidate.
///
/// - **Predicted recall** — table lookup over `(index_type, quantization)`.
pub fn estimate_cost(inputs: &CostModelInputs) -> VectorCost {
    let n = inputs.n_vectors.max(1) as f32;

    // Graph traversal cost.
    let graph_traversal_us = match inputs.index_type {
        IndexType::Hnsw => n.log2() * inputs.ef_search as f32 * 0.001,
        IndexType::Vamana => inputs.ef_search as f32 * 0.002,
    };

    // Codec decode cost (ns per vector × ef_search vectors).
    let codec_decode_ns = codec_decode_ns_for(inputs.quantization);

    // Rerank cost.
    let rerank_us = if inputs.quantization == QuantizationKind::None {
        0.0
    } else {
        f32::from(crate::rerank::DEFAULT_OVERSAMPLE) * inputs.ef_search as f32 * 0.01
    };

    let predicted_recall = predicted_recall_for(inputs.index_type, inputs.quantization);

    VectorCost {
        graph_traversal_us,
        codec_decode_ns,
        rerank_us,
        filter_selectivity: inputs.global_selectivity,
        candidate_set_size: inputs.candidate_set_size,
        predicted_recall,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_inputs() -> CostModelInputs {
        CostModelInputs {
            n_vectors: 1_000_000,
            dim: 768,
            index_type: IndexType::Hnsw,
            quantization: QuantizationKind::Sq8,
            ef_search: 64,
            global_selectivity: 1.0,
            candidate_set_size: 1_000_000,
        }
    }

    #[test]
    fn estimate_returns_finite_positive_values() {
        let cost = estimate_cost(&base_inputs());
        assert!(cost.graph_traversal_us.is_finite() && cost.graph_traversal_us > 0.0);
        assert!(cost.codec_decode_ns.is_finite() && cost.codec_decode_ns > 0.0);
        assert!(cost.rerank_us.is_finite() && cost.rerank_us >= 0.0);
        assert!(cost.predicted_recall > 0.0 && cost.predicted_recall <= 1.0);
        assert!(cost.filter_selectivity >= 0.0 && cost.filter_selectivity <= 1.0);
    }

    #[test]
    fn hnsw_graph_traversal_scales_with_log_n() {
        // HNSW traversal ~ log2(n) * ef; larger n → more traversal cost.
        let small = estimate_cost(&CostModelInputs {
            n_vectors: 1_000,
            ..base_inputs()
        });
        let large = estimate_cost(&CostModelInputs {
            n_vectors: 1_000_000,
            ..base_inputs()
        });
        assert!(
            large.graph_traversal_us > small.graph_traversal_us,
            "HNSW traversal should grow with n_vectors"
        );
    }

    #[test]
    fn vamana_traversal_is_flat_across_n() {
        // Vamana cost is ef_search-only, not log(n)-dependent.
        let small = estimate_cost(&CostModelInputs {
            n_vectors: 1_000,
            index_type: IndexType::Vamana,
            quantization: QuantizationKind::RaBitQ,
            ..base_inputs()
        });
        let large = estimate_cost(&CostModelInputs {
            n_vectors: 1_000_000_000,
            index_type: IndexType::Vamana,
            quantization: QuantizationKind::RaBitQ,
            ..base_inputs()
        });
        assert!(
            (small.graph_traversal_us - large.graph_traversal_us).abs() < f32::EPSILON,
            "Vamana traversal cost must not depend on n_vectors"
        );
    }

    #[test]
    fn sq8_decode_more_expensive_than_rabitq() {
        let sq8 = estimate_cost(&base_inputs());
        let rabitq = estimate_cost(&CostModelInputs {
            quantization: QuantizationKind::RaBitQ,
            ..base_inputs()
        });
        assert!(
            sq8.codec_decode_ns > rabitq.codec_decode_ns,
            "SQ8 decode ({}) should cost more ns/vector than RaBitQ ({})",
            sq8.codec_decode_ns,
            rabitq.codec_decode_ns
        );
    }

    #[test]
    fn none_quantization_has_zero_rerank() {
        let cost = estimate_cost(&CostModelInputs {
            quantization: QuantizationKind::None,
            ..base_inputs()
        });
        assert_eq!(cost.rerank_us, 0.0, "FP32 traversal needs no rerank");
    }

    #[test]
    fn compressed_quantization_has_nonzero_rerank() {
        let cost = estimate_cost(&CostModelInputs {
            quantization: QuantizationKind::Bbq,
            ..base_inputs()
        });
        assert!(
            cost.rerank_us > 0.0,
            "BBQ traversal should incur rerank cost"
        );
    }

    #[test]
    fn predicted_recall_within_bounds() {
        for quant in [
            QuantizationKind::None,
            QuantizationKind::Sq8,
            QuantizationKind::Pq,
            QuantizationKind::RaBitQ,
            QuantizationKind::Bbq,
            QuantizationKind::Binary,
            QuantizationKind::Ternary,
        ] {
            for idx in [IndexType::Hnsw, IndexType::Vamana] {
                let cost = estimate_cost(&CostModelInputs {
                    index_type: idx,
                    quantization: quant,
                    ..base_inputs()
                });
                assert!(
                    cost.predicted_recall >= 0.0 && cost.predicted_recall <= 1.0,
                    "recall out of bounds for {idx:?} + {quant:?}: {}",
                    cost.predicted_recall
                );
            }
        }
    }

    #[test]
    fn candidate_set_size_and_selectivity_passthrough() {
        let inputs = CostModelInputs {
            global_selectivity: 0.05,
            candidate_set_size: 50_000,
            ..base_inputs()
        };
        let cost = estimate_cost(&inputs);
        assert!((cost.filter_selectivity - 0.05).abs() < f32::EPSILON);
        assert_eq!(cost.candidate_set_size, 50_000);
    }
}
