//! Filter routing decision tree for the vector query planner.
//!
//! Given information about the query's filter predicates, this module decides
//! which execution path to use: brute-force scan, a pre-built SIEVE subindex,
//! Compass IVF+B+-tree, or NaviX adaptive-local on the global graph.

use super::query_options::QuantizationKind;

// ── Filter routing ────────────────────────────────────────────────────────────

/// The execution path chosen by the filter router.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterRoute {
    /// Brute-force scan over the filtered set.
    ///
    /// Selected when `global_selectivity < brute_force_threshold`.  At very
    /// low selectivity the filtered candidate set is so small that a linear
    /// scan over it is cheaper than HNSW traversal.
    BruteForce,

    /// A pre-built SIEVE subindex matched the query's predicate signature.
    ///
    /// The planner routes directly to the dedicated HNSW subindex rather than
    /// the global graph, avoiding wasted traversal into non-matching partitions.
    SieveSubindex {
        /// Predicate signature string that identified the matching subindex
        /// (e.g. `"tenant_id=42"`).
        signature: String,
    },

    /// Run NaviX adaptive-local filtered traversal on the global HNSW graph.
    ///
    /// Default path when no subindex matches and the candidate set is not
    /// small enough for brute-force.
    NavixGlobal,

    /// Compass IVF + per-cluster B+-tree filtered search.
    ///
    /// Returned only when `compass_enabled == true` and the query has three
    /// or more numeric range predicates.  Gated because Compass (VLDB 2025)
    /// requires a separate index build step.
    Compass,
}

/// Inputs to the filter routing decision tree.
pub struct FilterRouteInputs<'a> {
    /// Fraction of the total collection that passes the filter `[0.0, 1.0]`.
    pub global_selectivity: f32,

    /// Selectivity below which brute-force is preferred over HNSW traversal.
    ///
    /// A reasonable default is `0.001` (0.1%).
    pub brute_force_threshold: f32,

    /// Predicate signature of a SIEVE subindex that matched this query, if any.
    pub matched_sieve_signature: Option<&'a str>,

    /// Whether the Compass index is available for this collection.
    pub compass_enabled: bool,

    /// Number of numeric range predicates in the query.
    ///
    /// Compass is only advantageous when three or more numeric predicates
    /// (e.g. `price BETWEEN 10 AND 100`, `ts > X`, `lat < Y`) are intersected.
    pub numeric_predicate_count: u8,
}

/// Decide which execution path to use for this filtered vector query.
///
/// # Decision tree
///
/// 1. `global_selectivity < brute_force_threshold` → [`FilterRoute::BruteForce`]
/// 2. `matched_sieve_signature.is_some()` → [`FilterRoute::SieveSubindex`]
/// 3. `compass_enabled && numeric_predicate_count >= 3` → [`FilterRoute::Compass`]
/// 4. otherwise → [`FilterRoute::NavixGlobal`]
pub fn route_filter(inputs: &FilterRouteInputs<'_>) -> FilterRoute {
    if inputs.global_selectivity < inputs.brute_force_threshold {
        return FilterRoute::BruteForce;
    }

    if let Some(sig) = inputs.matched_sieve_signature {
        return FilterRoute::SieveSubindex {
            signature: sig.to_owned(),
        };
    }

    if inputs.compass_enabled && inputs.numeric_predicate_count >= 3 {
        return FilterRoute::Compass;
    }

    FilterRoute::NavixGlobal
}

// ── Quantization tier picker ──────────────────────────────────────────────────

/// Pick the best quantization tier for a given candidate set size and target
/// recall requirement.
///
/// # Heuristic
///
/// - Small sets (< 1 000): FP32 — the set fits in cache; no compression gain.
/// - Medium sets (1 000 – 99 999):
///   - target recall ≥ 0.97 → SQ8 (high fidelity, 4× compression)
///   - otherwise → PQ (8× compression, slightly lower recall)
/// - Large sets (≥ 100 000):
///   - target recall ≥ 0.95 → BBQ (centroid-centered 1-bit + rerank)
///   - otherwise → RaBitQ (raw 1-bit, fastest scan)
pub fn pick_quantization(candidate_set_size: usize, target_recall: f32) -> QuantizationKind {
    if candidate_set_size < 1_000 {
        QuantizationKind::None
    } else if candidate_set_size < 100_000 {
        if target_recall >= 0.97 {
            QuantizationKind::Sq8
        } else {
            QuantizationKind::Pq
        }
    } else if target_recall >= 0.95 {
        QuantizationKind::Bbq
    } else {
        QuantizationKind::RaBitQ
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── route_filter tests ────────────────────────────────────────────────────

    fn default_inputs<'a>() -> FilterRouteInputs<'a> {
        FilterRouteInputs {
            global_selectivity: 0.5,
            brute_force_threshold: 0.001,
            matched_sieve_signature: None,
            compass_enabled: false,
            numeric_predicate_count: 0,
        }
    }

    #[test]
    fn brute_force_fires_below_threshold() {
        let inputs = FilterRouteInputs {
            global_selectivity: 0.0005,
            ..default_inputs()
        };
        assert_eq!(route_filter(&inputs), FilterRoute::BruteForce);
    }

    #[test]
    fn brute_force_fires_exactly_at_threshold_minus_epsilon() {
        let threshold = 0.001_f32;
        let inputs = FilterRouteInputs {
            global_selectivity: threshold - f32::EPSILON,
            brute_force_threshold: threshold,
            ..default_inputs()
        };
        assert_eq!(route_filter(&inputs), FilterRoute::BruteForce);
    }

    #[test]
    fn sieve_subindex_fires_when_signature_matched() {
        let inputs = FilterRouteInputs {
            global_selectivity: 0.5,
            matched_sieve_signature: Some("tenant_id=42"),
            ..default_inputs()
        };
        assert_eq!(
            route_filter(&inputs),
            FilterRoute::SieveSubindex {
                signature: "tenant_id=42".to_owned()
            }
        );
    }

    #[test]
    fn sieve_takes_priority_over_compass() {
        // Both SIEVE match and Compass conditions satisfied — SIEVE wins (step 2
        // before step 3 in the decision tree).
        let inputs = FilterRouteInputs {
            global_selectivity: 0.5,
            matched_sieve_signature: Some("lang=en"),
            compass_enabled: true,
            numeric_predicate_count: 5,
            ..default_inputs()
        };
        assert_eq!(
            route_filter(&inputs),
            FilterRoute::SieveSubindex {
                signature: "lang=en".to_owned()
            }
        );
    }

    #[test]
    fn compass_fires_when_enabled_and_enough_predicates() {
        let inputs = FilterRouteInputs {
            global_selectivity: 0.5,
            matched_sieve_signature: None,
            compass_enabled: true,
            numeric_predicate_count: 3,
            ..default_inputs()
        };
        assert_eq!(route_filter(&inputs), FilterRoute::Compass);
    }

    #[test]
    fn compass_does_not_fire_with_fewer_than_3_predicates() {
        let inputs = FilterRouteInputs {
            global_selectivity: 0.5,
            matched_sieve_signature: None,
            compass_enabled: true,
            numeric_predicate_count: 2,
            ..default_inputs()
        };
        assert_eq!(route_filter(&inputs), FilterRoute::NavixGlobal);
    }

    #[test]
    fn compass_does_not_fire_when_disabled() {
        let inputs = FilterRouteInputs {
            global_selectivity: 0.5,
            matched_sieve_signature: None,
            compass_enabled: false,
            numeric_predicate_count: 10,
            ..default_inputs()
        };
        assert_eq!(route_filter(&inputs), FilterRoute::NavixGlobal);
    }

    #[test]
    fn navix_global_is_default_fallback() {
        let inputs = default_inputs();
        assert_eq!(route_filter(&inputs), FilterRoute::NavixGlobal);
    }

    // ── pick_quantization tests ───────────────────────────────────────────────

    #[test]
    fn small_set_returns_none() {
        assert_eq!(pick_quantization(0, 0.99), QuantizationKind::None);
        assert_eq!(pick_quantization(999, 0.99), QuantizationKind::None);
    }

    #[test]
    fn medium_set_high_recall_returns_sq8() {
        assert_eq!(pick_quantization(1_000, 0.97), QuantizationKind::Sq8);
        assert_eq!(pick_quantization(50_000, 0.99), QuantizationKind::Sq8);
        assert_eq!(pick_quantization(99_999, 1.0), QuantizationKind::Sq8);
    }

    #[test]
    fn medium_set_lower_recall_returns_pq() {
        assert_eq!(pick_quantization(1_000, 0.96), QuantizationKind::Pq);
        assert_eq!(pick_quantization(50_000, 0.90), QuantizationKind::Pq);
    }

    #[test]
    fn large_set_high_recall_returns_bbq() {
        assert_eq!(pick_quantization(100_000, 0.95), QuantizationKind::Bbq);
        assert_eq!(pick_quantization(1_000_000, 0.99), QuantizationKind::Bbq);
    }

    #[test]
    fn large_set_lower_recall_returns_rabitq() {
        assert_eq!(pick_quantization(100_000, 0.94), QuantizationKind::RaBitQ);
        assert_eq!(
            pick_quantization(10_000_000, 0.80),
            QuantizationKind::RaBitQ
        );
    }

    #[test]
    fn boundary_1000_uses_medium_path() {
        // Exactly 1_000 crosses into medium tier.
        assert_ne!(pick_quantization(1_000, 0.99), QuantizationKind::None);
    }

    #[test]
    fn boundary_100000_uses_large_path() {
        // Exactly 100_000 crosses into large tier.
        let result = pick_quantization(100_000, 0.95);
        assert!(
            result == QuantizationKind::Bbq || result == QuantizationKind::RaBitQ,
            "expected BBQ or RaBitQ at boundary, got {result:?}"
        );
    }
}
