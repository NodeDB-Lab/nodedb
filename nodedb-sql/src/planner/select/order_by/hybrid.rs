// SPDX-License-Identifier: Apache-2.0

//! Hybrid-search plan construction from `rrf_score(...)` calls.
//!
//! Two-source form:
//!   `rrf_score(vector_distance(...), bm25_score(...), k1?, k2?)`
//!   → `SqlPlan::HybridSearch`
//!
//! Three-source form:
//!   `rrf_score(vector_distance(...), bm25_score(...), graph_score(...), k1?, k2?, k3?)`
//!   → `SqlPlan::HybridSearchTriple`
//!
//! The third argument is detected by checking whether it is a function call
//! (graph_score) rather than a numeric literal. `k1`/`k2`/`k3` (RRF constants)
//! default to 60.0 each. `score_alias` carries the SELECT alias the response
//! should use for the RRF score column — without it, the executor falls back
//! to the fixed internal name `rrf_score`.
//!
//! Validation:
//! - Fewer than 2 source args: typed error.
//! - Exactly 4 or more than 6 args where arg[2] is numeric: typed error
//!   (3 sources require arg[2] to be graph_score(...), not a k-constant).
//! - 3 sources + 2 k constants: typed error (inconsistent arity).
//! - 3 sources + 3 k constants: valid triple-source form.

use crate::types::{HybridSearchPlan, HybridSearchTriplePlan};
use sqlparser::ast;

use super::super::helpers::{
    extract_float, extract_float_array, extract_func_args, extract_string_literal,
    source_projection,
};
use crate::error::{Result, SqlError};
use crate::types::{Projection, SqlPlan};

/// Build a `SqlPlan::HybridSearch` or `SqlPlan::HybridSearchTriple` from a
/// `rrf_score(...)` call depending on argument arity.
pub(super) fn plan_hybrid_from_sort(
    args: &[ast::Expr],
    collection: &str,
    plan: &SqlPlan,
    score_alias: Option<&str>,
) -> Result<Option<SqlPlan>> {
    if args.len() < 2 {
        return Err(no_args_rrf_score_error());
    }

    let limit = match plan {
        SqlPlan::Scan { limit, .. } => limit.unwrap_or(10),
        _ => 10,
    };
    let projection = source_projection(plan);

    // Determine whether args[2] (if present) is a function call (graph source)
    // or a numeric literal (k-constant for the two-source form).
    let third_is_graph_score = args.get(2).is_some_and(is_function_call);

    if third_is_graph_score {
        plan_hybrid_triple(args, collection, limit, score_alias, projection)
    } else {
        plan_hybrid_two_source(args, collection, limit, score_alias, projection)
    }
}

/// Two-source: `rrf_score(vector_distance(...), bm25_score(...), k1?, k2?)`.
fn plan_hybrid_two_source(
    args: &[ast::Expr],
    collection: &str,
    limit: usize,
    score_alias: Option<&str>,
    projection: Vec<Projection>,
) -> Result<Option<SqlPlan>> {
    // args[2] and args[3] are optional k-constants. If there are more than 4
    // args in the two-source form, something is wrong.
    if args.len() > 4 {
        return Err(SqlError::InvalidFunction {
            detail: format!(
                "rrf_score() two-source form accepts at most 4 arguments \
                 (rank1, rank2, k1?, k2?); got {}. \
                 For three-source fusion use rrf_score(vector_distance(...), \
                 bm25_score(...), graph_score(...), k1?, k2?, k3?).",
                args.len()
            ),
        });
    }

    let vector = extract_vector_arg(&args[0])?;
    let text = extract_text_arg(&args[1])?;
    let k1 = args
        .get(2)
        .and_then(|e| extract_float(e).ok())
        .unwrap_or(60.0);
    let k2 = args
        .get(3)
        .and_then(|e| extract_float(e).ok())
        .unwrap_or(60.0);

    let vector_weight = k2 as f32 / (k1 as f32 + k2 as f32);

    Ok(Some(SqlPlan::HybridSearch(HybridSearchPlan {
        collection: collection.into(),
        query_vector: vector,
        query_text: text,
        top_k: limit,
        ef_search: limit * 2,
        vector_weight,
        fuzzy: true,
        score_alias: score_alias.map(|s| s.to_string()),
        projection,
    })))
}

/// Three-source: `rrf_score(vector_distance(...), bm25_score(...), graph_score(...), k1?, k2?, k3?)`.
fn plan_hybrid_triple(
    args: &[ast::Expr],
    collection: &str,
    limit: usize,
    score_alias: Option<&str>,
    projection: Vec<Projection>,
) -> Result<Option<SqlPlan>> {
    // After the three source functions, we accept 0 or 3 k-constants.
    // Anything else (e.g. 1 or 2 k-constants) is an inconsistent arity.
    let k_count = args.len().saturating_sub(3);
    if k_count == 1 || k_count == 2 {
        return Err(SqlError::InvalidFunction {
            detail: format!(
                "rrf_score() three-source form requires 0 or 3 k-constants \
                 after the three source arguments, not {k_count}. \
                 Use rrf_score(v, t, g) or rrf_score(v, t, g, k1, k2, k3)."
            ),
        });
    }
    if args.len() > 6 {
        return Err(SqlError::InvalidFunction {
            detail: format!(
                "rrf_score() accepts at most 6 arguments in the three-source form \
                 (rank1, rank2, rank3, k1?, k2?, k3?); got {}.",
                args.len()
            ),
        });
    }

    let vector = extract_vector_arg(&args[0])?;
    let text = extract_text_arg(&args[1])?;
    let (graph_seed_id, graph_depth, graph_edge_label) = extract_graph_score_args(&args[2])?;

    let k1 = args
        .get(3)
        .and_then(|e| extract_float(e).ok())
        .unwrap_or(60.0);
    let k2 = args
        .get(4)
        .and_then(|e| extract_float(e).ok())
        .unwrap_or(60.0);
    let k3 = args
        .get(5)
        .and_then(|e| extract_float(e).ok())
        .unwrap_or(60.0);

    Ok(Some(SqlPlan::HybridSearchTriple(HybridSearchTriplePlan {
        collection: collection.into(),
        query_vector: vector,
        query_text: text,
        graph_seed_id,
        graph_depth,
        graph_edge_label,
        top_k: limit,
        ef_search: limit * 2,
        fuzzy: true,
        rrf_k: (k1, k2, k3),
        score_alias: score_alias.map(|s| s.to_string()),
        projection,
    })))
}

/// Extract the float-array from a `vector_distance(col, ARRAY[...])` expression.
fn extract_vector_arg(expr: &ast::Expr) -> Result<Vec<f32>> {
    Ok(match expr {
        ast::Expr::Function(f) => {
            let inner_args = extract_func_args(f)?;
            if inner_args.len() >= 2 {
                extract_float_array(&inner_args[1]).unwrap_or_default()
            } else {
                Vec::new()
            }
        }
        _ => Vec::new(),
    })
}

/// Extract the query string from a `bm25_score(col, 'query')` expression.
fn extract_text_arg(expr: &ast::Expr) -> Result<String> {
    Ok(match expr {
        ast::Expr::Function(f) => {
            let inner_args = extract_func_args(f)?;
            if inner_args.len() >= 2 {
                extract_string_literal(&inner_args[1]).unwrap_or_default()
            } else {
                String::new()
            }
        }
        _ => String::new(),
    })
}

/// Extract `(seed_id, depth, edge_label)` from a
/// `graph_score(node_id_col, seed_id, depth => N, label => 'edge')` expression.
///
/// Named args (`depth => N`, `label => 'e'`) are represented in sqlparser as
/// `Expr::Named { name, arg }`. Positional arg[0] is the node_id column
/// (ignored — the executor resolves surrogates from the collection), arg[1]
/// is the seed node id string.
fn extract_graph_score_args(expr: &ast::Expr) -> Result<(String, usize, Option<String>)> {
    let ast::Expr::Function(f) = expr else {
        return Ok((String::new(), 1, None));
    };
    let inner_args = extract_func_args(f)?;

    // arg[1] is the seed node id.
    let seed_id = inner_args
        .get(1)
        .and_then(|e| extract_string_literal(e).ok())
        .unwrap_or_default();

    // Remaining args may be named: `depth => N` and `label => 'edge'`.
    let mut depth: usize = 1;
    let mut edge_label: Option<String> = None;

    for arg in inner_args.iter().skip(2) {
        if let ast::Expr::Named { name, expr } = arg {
            let key = name.value.to_ascii_lowercase();
            match key.as_str() {
                "depth" => {
                    if let Ok(d) = extract_float(expr) {
                        depth = d as usize;
                    }
                }
                "label" => {
                    edge_label = extract_string_literal(expr).ok();
                }
                _ => {}
            }
        }
    }

    Ok((seed_id, depth, edge_label))
}

/// Returns true when `expr` is a `Function` call (rather than a numeric literal).
fn is_function_call(expr: &ast::Expr) -> bool {
    matches!(expr, ast::Expr::Function(_))
}

/// Construct the typed error returned for `rrf_score()` with no arguments.
pub(super) fn no_args_rrf_score_error() -> SqlError {
    SqlError::InvalidFunction {
        detail: "rrf_score() requires at least vector_distance(...) and bm25_score(...) \
                 arguments; e.g. rrf_score(vector_distance(emb, ARRAY[...]), \
                 bm25_score(content, 'query'))"
            .into(),
    }
}
