//! Search pattern detection helpers for the plan converter.
//! Handles both vector similarity and BM25 full-text search patterns.
//!
//! These are standalone functions extracted from `PlanConverter`. They handle the
//! `ORDER BY vector_distance(...) LIMIT k` pattern detection and related
//! utilities.

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;

/// Detect `ORDER BY vector_distance(field, ARRAY[...]) LIMIT k` pattern.
///
/// Returns `(collection, query_vector, top_k)` if the sort expression
/// contains a `vector_distance()` function call over a table scan.
pub(super) fn try_extract_vector_search(
    sort_exprs: &[datafusion::logical_expr::expr::Sort],
    input: &LogicalPlan,
    fetch: Option<usize>,
) -> crate::Result<Option<(String, Vec<f32>, usize)>> {
    // Check if the first sort expression is a ScalarFunction named "vector_distance".
    let first = match sort_exprs.first() {
        Some(s) => s,
        None => return Ok(None),
    };

    let func_name = match &first.expr {
        Expr::ScalarFunction(func) => func.name(),
        _ => return Ok(None),
    };

    if func_name != "vector_distance" {
        return Ok(None);
    }

    let func = match &first.expr {
        Expr::ScalarFunction(f) => f,
        _ => return Ok(None),
    };

    // Extract query vector from the second argument (ARRAY literal).
    let query_vector = if func.args.len() >= 2 {
        extract_float_array(&func.args[1])?
    } else {
        return Ok(None);
    };

    if query_vector.is_empty() {
        return Ok(None);
    }

    // Extract collection from the input plan (should be a TableScan).
    let collection = match extract_table_name(input) {
        Some(c) => c,
        None => return Ok(None),
    };

    // top_k from LIMIT, default 10.
    let top_k = fetch.unwrap_or(10);

    Ok(Some((collection, query_vector, top_k)))
}

/// Extract a float array from an Expr (for vector_distance query vector).
pub(super) fn extract_float_array(expr: &Expr) -> crate::Result<Vec<f32>> {
    match expr {
        Expr::Literal(lit, _) => {
            let s = lit.to_string();
            // Try to parse as comma-separated floats: [0.1, 0.2, 0.3]
            let trimmed = s.trim_matches(|c| c == '[' || c == ']');
            let values: Vec<f32> = trimmed
                .split(',')
                .filter_map(|v| v.trim().parse::<f32>().ok())
                .collect();
            Ok(values)
        }
        Expr::ScalarFunction(func) if func.name() == "make_array" => {
            // DataFusion represents ARRAY[1,2,3] as make_array(1,2,3).
            let mut values = Vec::with_capacity(func.args.len());
            for arg in &func.args {
                if let Expr::Literal(lit, _) = arg {
                    let s = lit.to_string();
                    if let Ok(f) = s.parse::<f32>() {
                        values.push(f);
                    }
                }
            }
            Ok(values)
        }
        _ => Ok(Vec::new()),
    }
}

/// Extract the table name from a logical plan (traversing projections/filters).
///
/// Returns the name normalized to lowercase, following the PostgreSQL convention
/// that unquoted identifiers are folded to lowercase.
pub(super) fn extract_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::TableScan(scan) => Some(scan.table_name.to_string().to_lowercase()),
        LogicalPlan::Projection(proj) => extract_table_name(&proj.input),
        LogicalPlan::Filter(filter) => extract_table_name(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => extract_table_name(&alias.input),
        _ => None,
    }
}

/// Hybrid search params: (collection, query_vector, query_text, top_k, vector_k, text_k).
type HybridParams = (String, Vec<f32>, String, usize, f64, f64);

/// Detect `ORDER BY rrf_score(vector_distance(...), bm25_score(...)) LIMIT k`.
///
/// Returns `(collection, query_vector, query_text, top_k)` for hybrid search.
pub(super) fn try_extract_hybrid_search(
    sort_exprs: &[datafusion::logical_expr::SortExpr],
    input: &LogicalPlan,
    fetch: Option<usize>,
) -> crate::Result<Option<HybridParams>> {
    let sort_expr = match sort_exprs.first() {
        Some(s) => s,
        None => return Ok(None),
    };
    let Expr::ScalarFunction(rrf_func) = &sort_expr.expr else {
        return Ok(None);
    };
    if rrf_func.name() != "rrf_score" || rrf_func.args.len() < 2 {
        return Ok(None);
    }

    // Extract vector_distance(...) from first arg.
    let mut query_vector = Vec::new();
    if let Expr::ScalarFunction(vd) = &rrf_func.args[0]
        && vd.name() == "vector_distance"
        && vd.args.len() >= 2
    {
        query_vector = extract_float_array(&vd.args[1])?;
    }
    if query_vector.is_empty() {
        return Ok(None);
    }

    // Extract bm25_score(...) from second arg.
    let mut query_text = String::new();
    if let Expr::ScalarFunction(bm) = &rrf_func.args[1]
        && bm.name() == "bm25_score"
        && bm.args.len() >= 2
        && let Expr::Literal(lit, _) = &bm.args[1]
    {
        query_text = lit.to_string().trim_matches('\'').to_string();
    }
    if query_text.is_empty() {
        return Ok(None);
    }

    let collection = match extract_table_name(input) {
        Some(c) => c,
        None => return Ok(None),
    };
    let top_k = fetch.unwrap_or(10);

    // Extract optional per-source RRF k-constants from 3rd/4th args.
    // Syntax: rrf_score(vector_distance(...), bm25_score(...), 60, 40)
    //   → vector_k = 60, text_k = 40
    // Defaults: 60.0 for both (standard RRF k).
    let vector_k = rrf_func
        .args
        .get(2)
        .and_then(|e| match e {
            Expr::Literal(lit, _) => lit.to_string().parse::<f64>().ok(),
            _ => None,
        })
        .unwrap_or(60.0);
    let text_k = rrf_func
        .args
        .get(3)
        .and_then(|e| match e {
            Expr::Literal(lit, _) => lit.to_string().parse::<f64>().ok(),
            _ => None,
        })
        .unwrap_or(60.0);

    Ok(Some((
        collection,
        query_vector,
        query_text,
        top_k,
        vector_k,
        text_k,
    )))
}

/// Detect `WHERE text_match(field, 'query')` in a filter predicate.
///
/// Returns `(collection, query_text)` if the predicate (or any AND-branch)
/// contains a `text_match()` call. Walks through AND conjunctions recursively.
pub(super) fn try_extract_text_match_predicate(
    predicate: &Expr,
    input: &LogicalPlan,
) -> Option<(String, String)> {
    match predicate {
        Expr::ScalarFunction(func) if func.name() == "text_match" => {
            if func.args.len() < 2 {
                return None;
            }
            let query_text = match &func.args[1] {
                Expr::Literal(lit, _) => lit
                    .to_string()
                    .trim_matches('\'')
                    .trim_matches('"')
                    .to_string(),
                _ => return None,
            };
            let collection = extract_table_name(input)?;
            Some((collection, query_text))
        }
        Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::And => {
            // Check both branches.
            try_extract_text_match_predicate(&binary.left, input)
                .or_else(|| try_extract_text_match_predicate(&binary.right, input))
        }
        _ => None,
    }
}

/// Detect `ORDER BY bm25_score(field, 'query') LIMIT k` and extract search parameters.
///
/// Returns `(collection, query_text, top_k)` if the pattern matches.
pub(super) fn try_extract_text_search(
    sort_exprs: &[datafusion::logical_expr::SortExpr],
    input: &LogicalPlan,
    fetch: Option<usize>,
) -> Option<(String, String, usize)> {
    let sort_expr = sort_exprs.first()?;
    let Expr::ScalarFunction(func) = &sort_expr.expr else {
        return None;
    };
    if func.name() != "bm25_score" || func.args.len() != 2 {
        return None;
    }
    let (Expr::Literal(_field_lit, _), Expr::Literal(query_lit, _)) =
        (&func.args[0], &func.args[1])
    else {
        return None;
    };
    let query_text = query_lit.to_string().trim_matches('\'').to_string();
    let collection = extract_table_name(input)?;
    let top_k = fetch.unwrap_or(10);
    Some((collection, query_text, top_k))
}
