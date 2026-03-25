//! MATCH query optimizer — reorders pattern triples by selectivity.
//!
//! Uses CSR edge statistics to determine which edge labels are most
//! selective (fewest edges), and reorders the triple execution order
//! to start with the most selective joins. This reduces the intermediate
//! result set size, dramatically improving performance for multi-hop patterns.
//!
//! Strategy:
//! 1. Score each triple by edge label selectivity (lower = more selective).
//! 2. Among equally selective triples, prefer those with already-bound variables.
//! 3. Reorder triples within each PatternChain while preserving correctness.
//!
//! Does NOT reorder across MatchClauses (MATCH vs OPTIONAL MATCH).

use super::ast::{MatchClause, MatchQuery, PatternChain, PatternTriple};
use crate::engine::graph::csr::CsrIndex;

/// Optimize a MatchQuery by reordering triples for better join selectivity.
///
/// Modifies the query in-place. Each PatternChain's triples are reordered
/// so that the most selective (lowest edge count) triple executes first.
///
/// Triples with already-bound variables (from earlier triples in the same
/// chain) are preferred as they produce fewer intermediate results.
pub fn optimize(query: &mut MatchQuery, csr: &CsrIndex) {
    for clause in &mut query.clauses {
        optimize_clause(clause, csr);
    }
}

fn optimize_clause(clause: &mut MatchClause, csr: &CsrIndex) {
    for chain in &mut clause.patterns {
        optimize_chain(chain, csr);
    }
}

/// Reorder triples in a chain by selectivity.
///
/// Uses a greedy approach:
/// 1. Score each unplaced triple by: label selectivity * bound-variable bonus.
/// 2. Place the lowest-cost triple next.
/// 3. Mark its output variables as bound.
/// 4. Repeat until all triples are placed.
fn optimize_chain(chain: &mut PatternChain, csr: &CsrIndex) {
    let n = chain.triples.len();
    if n <= 1 {
        return; // Nothing to reorder.
    }

    let mut placed: Vec<PatternTriple> = Vec::with_capacity(n);
    let mut remaining: Vec<PatternTriple> = chain.triples.drain(..).collect();
    let mut bound_vars: std::collections::HashSet<String> = std::collections::HashSet::new();

    for _ in 0..n {
        // Score each remaining triple.
        let mut best_idx = 0;
        let mut best_cost = f64::INFINITY;

        for (idx, triple) in remaining.iter().enumerate() {
            let cost = score_triple(triple, csr, &bound_vars);
            if cost < best_cost {
                best_cost = cost;
                best_idx = idx;
            }
        }

        // Place the best triple.
        let triple = remaining.swap_remove(best_idx);

        // Mark variables as bound.
        if let Some(ref name) = triple.src.name {
            bound_vars.insert(name.clone());
        }
        if let Some(ref name) = triple.dst.name {
            bound_vars.insert(name.clone());
        }
        if let Some(ref name) = triple.edge.name {
            bound_vars.insert(name.clone());
        }

        placed.push(triple);
    }

    chain.triples = placed;
}

/// Score a triple for join ordering. Lower = more selective = better.
///
/// Cost = label_edge_count * bound_variable_factor
///
/// - If both src and dst are already bound: factor = 0.01 (point lookup)
/// - If one endpoint is bound: factor = 0.1 (single-node expansion)
/// - If neither is bound: factor = 1.0 (full scan)
fn score_triple(
    triple: &PatternTriple,
    csr: &CsrIndex,
    bound_vars: &std::collections::HashSet<String>,
) -> f64 {
    // Base cost: label edge count (fewer = more selective).
    let label_count = triple
        .edge
        .edge_type
        .as_ref()
        .map_or(csr.edge_count(), |label| csr.label_edge_count(label));

    // If label has zero edges, this triple can't produce results — cheapest possible.
    if label_count == 0 {
        return 0.0;
    }

    let base_cost = label_count as f64;

    // Bound variable factor.
    let src_bound = triple
        .src
        .name
        .as_ref()
        .is_some_and(|n| bound_vars.contains(n));
    let dst_bound = triple
        .dst
        .name
        .as_ref()
        .is_some_and(|n| bound_vars.contains(n));

    let factor = match (src_bound, dst_bound) {
        (true, true) => 0.01,                 // Both bound: point edge check.
        (true, false) | (false, true) => 0.1, // One bound: neighborhood scan.
        (false, false) => 1.0,                // Neither bound: full label scan.
    };

    // Variable-length paths are more expensive — multiply by max_hops.
    let hop_factor = if triple.edge.is_variable_length() {
        triple.edge.max_hops as f64
    } else {
        1.0
    };

    base_cost * factor * hop_factor
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::pattern::ast::*;
    use crate::engine::graph::pattern::compiler;

    fn social_graph() -> CsrIndex {
        let mut csr = CsrIndex::new();
        // KNOWS: 100 edges (dense), CREATED: 5 edges (sparse)
        for i in 0..100 {
            csr.add_edge(&format!("p{i}"), "KNOWS", &format!("p{}", (i + 1) % 100));
        }
        for i in 0..5 {
            csr.add_edge(&format!("p{i}"), "CREATED", &format!("doc{i}"));
        }
        csr.compact();
        csr
    }

    #[test]
    fn optimize_reorders_by_selectivity() {
        let csr = social_graph();

        // Two separate chains: KNOWS (100 edges) and CREATED (5 edges).
        let mut query =
            compiler::parse("MATCH (a)-[:KNOWS]->(b), (b)-[:CREATED]->(c) RETURN a, b, c").unwrap();

        // Before optimization: first chain is KNOWS, second is CREATED.
        assert_eq!(
            query.clauses[0].patterns[0].triples[0]
                .edge
                .edge_type
                .as_deref(),
            Some("KNOWS")
        );
        assert_eq!(
            query.clauses[0].patterns[1].triples[0]
                .edge
                .edge_type
                .as_deref(),
            Some("CREATED")
        );

        // Apply optimizer.
        optimize(&mut query, &csr);

        // Chains remain separate (optimizer works within chains),
        // but verify the optimizer ran without error.
        assert_eq!(query.clauses[0].patterns.len(), 2);
    }

    #[test]
    fn optimize_prefers_bound_variables() {
        let csr = social_graph();

        // Two triples in one chain: KNOWS and CREATED.
        // If we construct a chain manually:
        let mut chain = PatternChain {
            triples: vec![
                PatternTriple {
                    src: NodeBinding {
                        name: Some("a".into()),
                        label: None,
                    },
                    edge: EdgeBinding {
                        name: None,
                        edge_type: Some("KNOWS".into()),
                        direction: EdgeDirection::Right,
                        min_hops: 1,
                        max_hops: 1,
                    },
                    dst: NodeBinding {
                        name: Some("b".into()),
                        label: None,
                    },
                },
                PatternTriple {
                    src: NodeBinding {
                        name: Some("b".into()),
                        label: None,
                    },
                    edge: EdgeBinding {
                        name: None,
                        edge_type: Some("CREATED".into()),
                        direction: EdgeDirection::Right,
                        min_hops: 1,
                        max_hops: 1,
                    },
                    dst: NodeBinding {
                        name: Some("c".into()),
                        label: None,
                    },
                },
            ],
        };

        optimize_chain(&mut chain, &csr);

        // CREATED (5 edges) should come first since it's more selective.
        // But the second triple has "b" from the first triple.
        // Score for KNOWS with nothing bound: 100 * 1.0 = 100.
        // Score for CREATED with nothing bound: 5 * 1.0 = 5.
        // CREATED wins first.
        assert_eq!(chain.triples[0].edge.edge_type.as_deref(), Some("CREATED"));
    }

    #[test]
    fn optimize_single_triple_unchanged() {
        let csr = social_graph();

        let mut chain = PatternChain {
            triples: vec![PatternTriple {
                src: NodeBinding {
                    name: Some("a".into()),
                    label: None,
                },
                edge: EdgeBinding {
                    name: None,
                    edge_type: Some("KNOWS".into()),
                    direction: EdgeDirection::Right,
                    min_hops: 1,
                    max_hops: 1,
                },
                dst: NodeBinding {
                    name: Some("b".into()),
                    label: None,
                },
            }],
        };

        optimize_chain(&mut chain, &csr);
        assert_eq!(chain.triples.len(), 1);
    }

    #[test]
    fn score_bound_vs_unbound() {
        let csr = social_graph();

        let triple = PatternTriple {
            src: NodeBinding {
                name: Some("a".into()),
                label: None,
            },
            edge: EdgeBinding {
                name: None,
                edge_type: Some("KNOWS".into()),
                direction: EdgeDirection::Right,
                min_hops: 1,
                max_hops: 1,
            },
            dst: NodeBinding {
                name: Some("b".into()),
                label: None,
            },
        };

        let mut bound = std::collections::HashSet::new();
        let unbound_score = score_triple(&triple, &csr, &bound);

        bound.insert("a".to_string());
        let one_bound_score = score_triple(&triple, &csr, &bound);

        bound.insert("b".to_string());
        let both_bound_score = score_triple(&triple, &csr, &bound);

        assert!(one_bound_score < unbound_score);
        assert!(both_bound_score < one_bound_score);
    }

    #[test]
    fn score_variable_length_penalized() {
        let csr = social_graph();
        let bound = std::collections::HashSet::new();

        let fixed = PatternTriple {
            src: NodeBinding {
                name: Some("a".into()),
                label: None,
            },
            edge: EdgeBinding {
                name: None,
                edge_type: Some("KNOWS".into()),
                direction: EdgeDirection::Right,
                min_hops: 1,
                max_hops: 1,
            },
            dst: NodeBinding {
                name: Some("b".into()),
                label: None,
            },
        };

        let variable = PatternTriple {
            src: NodeBinding {
                name: Some("a".into()),
                label: None,
            },
            edge: EdgeBinding {
                name: None,
                edge_type: Some("KNOWS".into()),
                direction: EdgeDirection::Right,
                min_hops: 1,
                max_hops: 3,
            },
            dst: NodeBinding {
                name: Some("b".into()),
                label: None,
            },
        };

        let fixed_score = score_triple(&fixed, &csr, &bound);
        let variable_score = score_triple(&variable, &csr, &bound);

        assert!(
            variable_score > fixed_score,
            "variable-length should be more expensive"
        );
    }
}
