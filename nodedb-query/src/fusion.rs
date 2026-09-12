// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::hash::Hash;

/// Reciprocal Rank Fusion (RRF) for combining ranked results from multiple engines.
///
/// RRF is used when a query hits multiple engines (e.g., vector similarity +
/// metadata filter + BM25 text search). Each engine returns a ranked list;
/// RRF combines them into a single ranked list.
///
/// Formula: RRF_score(d) = Σ 1 / (k + rank_i(d))
/// where k is a smoothing constant (default 60).
/// RRF smoothing constant. Standard value from Cormack et al. (2009).
pub const DEFAULT_RRF_K: f64 = 60.0;

/// A scored result from a single engine.
///
/// `K` is the key the engine ranks under. It defaults to `String` for
/// callers whose key is opaque text.
#[derive(Debug, Clone)]
pub struct RankedResult<K = String> {
    /// Document identifier (engine-specific).
    pub document_id: K,
    /// Rank within the engine's result list (0-based).
    pub rank: usize,
    /// Original score from the engine (for diagnostics).
    pub score: f32,
    /// Source engine identifier.
    pub source: &'static str,
}

/// A fused result after RRF combination.
#[derive(Debug, Clone)]
pub struct FusedResult<K = String> {
    pub document_id: K,
    pub rrf_score: f64,
    /// Per-engine contributions for explainability.
    pub contributions: Vec<(&'static str, f64)>,
}

/// Sum each key's contributions, sort by RRF score, and cut to `top_k`.
///
/// The tie-break on `document_id` is what makes the output deterministic:
/// RRF produces many equal scores and the score map iterates in arbitrary
/// order, so a unique secondary key gives a total order.
fn finish_fusion<K: Clone + Eq + Hash + Ord>(
    scores: HashMap<K, Vec<(&'static str, f64)>>,
    top_k: usize,
) -> Vec<FusedResult<K>> {
    let mut fused: Vec<FusedResult<K>> = scores
        .into_iter()
        .map(|(doc_id, contributions)| {
            let rrf_score = contributions.iter().map(|(_, s)| s).sum();
            FusedResult {
                document_id: doc_id,
                rrf_score,
                contributions,
            }
        })
        .collect();

    fused.sort_unstable_by(|a, b| {
        b.rrf_score
            .partial_cmp(&a.rrf_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.document_id.cmp(&b.document_id))
    });
    fused.truncate(top_k);
    fused
}

/// Fuse multiple ranked result lists using Reciprocal Rank Fusion.
///
/// Each inner Vec is a ranked list from one engine (ordered by relevance).
/// Returns the top_k fused results sorted by RRF score (descending).
pub fn reciprocal_rank_fusion<K: Clone + Eq + Hash + Ord>(
    ranked_lists: &[Vec<RankedResult<K>>],
    k: Option<f64>,
    top_k: usize,
) -> Vec<FusedResult<K>> {
    let k = k.unwrap_or(DEFAULT_RRF_K);

    let mut scores: HashMap<K, Vec<(&'static str, f64)>> = HashMap::new();

    for list in ranked_lists {
        for result in list {
            let contribution = 1.0 / (k + result.rank as f64 + 1.0);
            scores
                .entry(result.document_id.clone())
                .or_default()
                .push((result.source, contribution));
        }
    }

    finish_fusion(scores, top_k)
}

/// Fuse ranked lists with per-list **linear weights**.
///
/// Each list's reciprocal-rank contribution is scaled by its weight, so a
/// more-trusted source can dominate: `contribution = weight_i / (k + rank + 1)`.
/// Unlike [`reciprocal_rank_fusion_weighted`] (which varies the `k` decay
/// constant per list), this scales contribution magnitude directly, which is
/// the right lever when one source (e.g. BM25) is far more reliable than another
/// (e.g. a weak dense index) — equal-weight RRF would let the weak source drag
/// down the strong source's ranking. `weights.len()` must equal
/// `ranked_lists.len()`.
///
/// # Panics
///
/// Panics if `weights.len() != ranked_lists.len()`.
pub fn reciprocal_rank_fusion_linear<K: Clone + Eq + Hash + Ord>(
    ranked_lists: &[Vec<RankedResult<K>>],
    k: Option<f64>,
    weights: &[f64],
    top_k: usize,
) -> Vec<FusedResult<K>> {
    assert_eq!(
        ranked_lists.len(),
        weights.len(),
        "weights length must match ranked_lists length"
    );
    let k = k.unwrap_or(DEFAULT_RRF_K);

    let mut scores: HashMap<K, Vec<(&'static str, f64)>> = HashMap::new();

    for (list_idx, list) in ranked_lists.iter().enumerate() {
        let w = weights[list_idx];
        for result in list {
            let contribution = w / (k + result.rank as f64 + 1.0);
            scores
                .entry(result.document_id.clone())
                .or_default()
                .push((result.source, contribution));
        }
    }

    finish_fusion(scores, top_k)
}

/// Fuse ranked lists with per-list k-constants for weighted influence.
///
/// Each list gets its own k value: lower k → steeper rank discount → more
/// influence. Typical usage: `k_i = base_k / weight_i`.
///
/// # Panics
///
/// Panics if `k_per_list.len() != ranked_lists.len()`.
pub fn reciprocal_rank_fusion_weighted<K: Clone + Eq + Hash + Ord>(
    ranked_lists: &[Vec<RankedResult<K>>],
    k_per_list: &[f64],
    top_k: usize,
) -> Vec<FusedResult<K>> {
    assert_eq!(
        ranked_lists.len(),
        k_per_list.len(),
        "k_per_list length must match ranked_lists length"
    );

    let mut scores: HashMap<K, Vec<(&'static str, f64)>> = HashMap::new();

    for (list_idx, list) in ranked_lists.iter().enumerate() {
        let k = k_per_list[list_idx];
        for result in list {
            let contribution = 1.0 / (k + result.rank as f64 + 1.0);
            scores
                .entry(result.document_id.clone())
                .or_default()
                .push((result.source, contribution));
        }
    }

    finish_fusion(scores, top_k)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ranked(doc_ids: &[&str], source: &'static str) -> Vec<RankedResult> {
        doc_ids
            .iter()
            .enumerate()
            .map(|(rank, &id)| RankedResult {
                document_id: id.to_string(),
                rank,
                score: 1.0 - (rank as f32 * 0.1),
                source,
            })
            .collect()
    }

    #[test]
    fn single_list_preserves_order() {
        let list = make_ranked(&["d1", "d2", "d3"], "vector");
        let fused = reciprocal_rank_fusion(&[list], None, 10);
        assert_eq!(fused.len(), 3);
        assert_eq!(fused[0].document_id, "d1");
    }

    #[test]
    fn overlapping_lists_boost_common_docs() {
        let vector = make_ranked(&["d1", "d2", "d3"], "vector");
        let sparse = make_ranked(&["d2", "d1", "d4"], "sparse");
        let fused = reciprocal_rank_fusion(&[vector, sparse], None, 10);
        let top2_ids: Vec<&str> = fused[..2].iter().map(|f| f.document_id.as_str()).collect();
        assert!(top2_ids.contains(&"d1"));
        assert!(top2_ids.contains(&"d2"));
    }

    /// A non-`String` key fuses the same way: the key type is the caller's.
    #[test]
    fn typed_key_fuses_and_tie_breaks_on_ord() {
        let make = |ids: &[u32], source: &'static str| -> Vec<RankedResult<u32>> {
            ids.iter()
                .enumerate()
                .map(|(rank, &id)| RankedResult {
                    document_id: id,
                    rank,
                    score: 0.0,
                    source,
                })
                .collect()
        };
        let a = make(&[7, 3], "a");
        let b = make(&[3, 9], "b");
        let fused = reciprocal_rank_fusion(&[a, b], None, 10);
        assert_eq!(fused[0].document_id, 3);
        // 7 and 9 each rank #1 in one list, so they tie; `Ord` breaks it.
        assert_eq!(fused[1].document_id, 7);
        assert_eq!(fused[2].document_id, 9);
    }

    #[test]
    fn weighted_rrf() {
        let list_a = make_ranked(&["a1", "a2"], "vector");
        let list_b = make_ranked(&["b1", "a1"], "text");
        let fused = reciprocal_rank_fusion_weighted(&[list_a, list_b], &[30.0, 120.0], 10);
        let a1 = fused.iter().find(|f| f.document_id == "a1").unwrap();
        assert_eq!(a1.contributions.len(), 2);
    }

    #[test]
    fn linear_weight_lets_strong_source_dominate() {
        // `a1` is ranked #1 by the strong source and #2 by the weak source;
        // `b1` is ranked #1 only by the weak source. With a heavy weight on
        // the strong source, `a1` must outrank `b1` — equal-weight RRF would
        // tie/flip them on the weak source's #1.
        let strong = make_ranked(&["a1", "a2"], "strong");
        let weak = make_ranked(&["b1", "a1"], "weak");
        let fused = reciprocal_rank_fusion_linear(&[strong, weak], None, &[4.0, 0.25], 10);

        let a1 = fused.iter().position(|f| f.document_id == "a1").unwrap();
        let b1 = fused.iter().position(|f| f.document_id == "b1").unwrap();
        assert!(a1 < b1, "a1 (rank {a1}) should outrank b1 (rank {b1})");

        let a1_res = &fused[a1];
        assert_eq!(a1_res.contributions.len(), 2);
        // Contribution magnitude scales linearly with the per-list weight.
        let strong_contrib = a1_res
            .contributions
            .iter()
            .find(|(src, _)| *src == "strong")
            .map(|(_, s)| *s)
            .unwrap();
        let expected = 4.0 / (DEFAULT_RRF_K + 0.0 + 1.0);
        assert!((strong_contrib - expected).abs() < 1e-12);
    }

    #[test]
    #[should_panic(expected = "weights length must match ranked_lists length")]
    fn linear_mismatched_weights_panics() {
        let list = make_ranked(&["d1"], "vector");
        let _ = reciprocal_rank_fusion_linear(&[list], None, &[1.0, 2.0], 10);
    }

    #[test]
    fn empty() {
        assert!(reciprocal_rank_fusion::<String>(&[], None, 10).is_empty());
        assert!(reciprocal_rank_fusion_linear::<String>(&[], None, &[], 10).is_empty());
        assert!(reciprocal_rank_fusion_weighted::<String>(&[], &[], 10).is_empty());
    }
}
