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
//! The fused search returns its best `top_k` rows. `top_k` is the query's
//! `LIMIT + OFFSET`: the rows any tail above the search can return. A query
//! with no LIMIT is a typed error, since the fusion ranks a bounded list.
//!
//! Validation:
//! - Fewer than 2 source args: typed error.
//! - Exactly 4 or more than 6 args where arg[2] is numeric: typed error
//!   (3 sources require arg[2] to be graph_score(...), not a k-constant).
//! - 3 sources + 2 k constants: typed error (inconsistent arity).
//! - 3 sources + 3 k constants: valid triple-source form.
//! - A leg that is not `vector_distance(...)`, `bm25_score(...)`, or
//!   `graph_score(...)` in its position: typed error.
//! - A k-constant that is not a positive finite number: typed error.

use crate::types::{HybridSearchPlan, HybridSearchTriplePlan};
use sqlparser::ast;

use super::super::helpers::{
    extract_float, extract_float_array, extract_func_args, source_projection,
};
use super::super::text_call::{TextCall, resolve_table_column, resolve_text_call};
use super::aliases::function_call_name;
use super::graph_score::parse_graph_score_args;
use super::text_score::refuse_scan_clauses;
use crate::error::{Result, SqlError};
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::resolver::columns::ResolvedTable;
use crate::types::{Filter, Projection, SqlPlan};

/// The RRF constant of a leg that names none.
const DEFAULT_RRF_K: f64 = 60.0;

/// What every hybrid plan takes from the plan it replaces.
struct HybridBase<'a> {
    table: &'a ResolvedTable,
    functions: &'a FunctionRegistry,
    top_k: usize,
    filters: Vec<Filter>,
    score_alias: Option<&'a str>,
    projection: Vec<Projection>,
    temporal: crate::temporal::TemporalScope,
}

/// Build a `SqlPlan::HybridSearch` or `SqlPlan::HybridSearchTriple` from a
/// `rrf_score(...)` call depending on argument arity. `None` when `plan` is
/// no `Scan`: no other plan becomes a hybrid search.
pub(super) fn plan_hybrid_from_sort(
    args: &[ast::Expr],
    table: &ResolvedTable,
    plan: &SqlPlan,
    score_alias: Option<&str>,
    functions: &FunctionRegistry,
) -> Result<Option<SqlPlan>> {
    if args.len() < 2 {
        return Err(no_args_rrf_score_error());
    }
    let SqlPlan::Scan {
        limit,
        offset,
        filters,
        temporal,
        ..
    } = plan
    else {
        return Ok(None);
    };
    refuse_scan_clauses(plan, "rrf_score()")?;
    let Some(limit) = limit else {
        return Err(missing_limit_error());
    };
    let base = HybridBase {
        table,
        functions,
        top_k: limit.saturating_add(*offset),
        filters: filters.clone(),
        score_alias,
        projection: source_projection(plan),
        temporal: *temporal,
    };

    // Determine whether args[2] (if present) is a function call (graph source)
    // or a numeric literal (k-constant for the two-source form).
    let third_is_graph_score = args.get(2).is_some_and(is_function_call);

    if third_is_graph_score {
        plan_hybrid_triple(args, base)
    } else {
        plan_hybrid_two_source(args, base)
    }
}

/// Two-source: `rrf_score(vector_distance(...), bm25_score(...), k1?, k2?)`.
fn plan_hybrid_two_source(args: &[ast::Expr], base: HybridBase<'_>) -> Result<Option<SqlPlan>> {
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

    let (vector_field, vector) = extract_vector_arg(&args[0], &base)?;
    let text = extract_text_arg(&args[1], &base)?;
    let k1 = rrf_k(args, 2, "k1")?;
    let k2 = rrf_k(args, 3, "k2")?;

    let vector_weight = k2 as f32 / (k1 as f32 + k2 as f32);

    Ok(Some(SqlPlan::HybridSearch(HybridSearchPlan {
        collection: base.table.name.clone(),
        vector_field,
        query_vector: vector,
        text_field: text.field,
        query_text: text.query,
        filters: base.filters,
        top_k: base.top_k,
        ef_search: base.top_k.saturating_mul(2),
        vector_weight,
        mode: text.options.params.mode,
        fuzzy: text.options.params.fuzzy,
        score_alias: base.score_alias.map(|s| s.to_string()),
        projection: base.projection,
        temporal: base.temporal,
    })))
}

/// Three-source: `rrf_score(vector_distance(...), bm25_score(...), graph_score(...), k1?, k2?, k3?)`.
fn plan_hybrid_triple(args: &[ast::Expr], base: HybridBase<'_>) -> Result<Option<SqlPlan>> {
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

    let (vector_field, vector) = extract_vector_arg(&args[0], &base)?;
    let text = extract_text_arg(&args[1], &base)?;
    let graph = extract_graph_arg(&args[2], &base)?;

    let k1 = rrf_k(args, 3, "k1")?;
    let k2 = rrf_k(args, 4, "k2")?;
    let k3 = rrf_k(args, 5, "k3")?;

    Ok(Some(SqlPlan::HybridSearchTriple(HybridSearchTriplePlan {
        collection: base.table.name.clone(),
        vector_field,
        query_vector: vector,
        text_field: text.field,
        query_text: text.query,
        filters: base.filters,
        graph_seed_id: graph.seed_id,
        graph_depth: graph.depth,
        graph_edge_label: graph.edge_label,
        top_k: base.top_k,
        ef_search: base.top_k.saturating_mul(2),
        mode: text.options.params.mode,
        fuzzy: text.options.params.fuzzy,
        rrf_k: (k1, k2, k3),
        score_alias: base.score_alias.map(|s| s.to_string()),
        projection: base.projection,
        temporal: base.temporal,
    })))
}

/// The function call of the leg in argument `position` of `rrf_score(...)`,
/// when its search trigger is `trigger`. Any other expression is a typed
/// error naming `shape`, the call the position takes.
fn leg_call<'e>(
    expr: &'e ast::Expr,
    base: &HybridBase<'_>,
    trigger: SearchTrigger,
    position: &str,
    shape: &str,
) -> Result<(&'e ast::Function, String)> {
    let leg_error = || SqlError::InvalidFunction {
        detail: format!("rrf_score(): the {position} argument must be {shape}; got {expr}"),
    };
    let ast::Expr::Function(f) = expr else {
        return Err(leg_error());
    };
    let name = function_call_name(expr).ok_or_else(leg_error)?;
    if base.functions.search_trigger(&name) != trigger {
        return Err(leg_error());
    }
    Ok((f, name))
}

/// The column and query vector of the `vector_distance(column, [...])` leg.
fn extract_vector_arg(expr: &ast::Expr, base: &HybridBase<'_>) -> Result<(String, Vec<f32>)> {
    const SHAPE: &str = "vector_distance(column, [...])";
    let (f, _) = leg_call(expr, base, SearchTrigger::VectorSearch, "first", SHAPE)?;
    let leg_error = || SqlError::InvalidFunction {
        detail: format!("rrf_score(): the first argument must be {SHAPE}; got {expr}"),
    };
    let inner_args = extract_func_args(f)?;
    let [column, query, ..] = inner_args.as_slice() else {
        return Err(leg_error());
    };
    let field = resolve_table_column(column, base.table)?.ok_or_else(leg_error)?;
    Ok((field, extract_float_array(query)?))
}

/// The column, query string, and options of the
/// `bm25_score(column, 'query', ...)` leg. `None` column for
/// `bm25_score(*, 'query')`: the whole-document index.
fn extract_text_arg(expr: &ast::Expr, base: &HybridBase<'_>) -> Result<TextCall> {
    let (f, name) = leg_call(
        expr,
        base,
        SearchTrigger::TextSearch,
        "second",
        "bm25_score(column, 'query')",
    )?;
    resolve_text_call(&name, f, base.table)
}

/// The BFS spec of the `graph_score(node_id_col, 'seed', ...)` leg.
fn extract_graph_arg(
    expr: &ast::Expr,
    base: &HybridBase<'_>,
) -> Result<super::graph_score::GraphScoreArgs> {
    let (f, _) = leg_call(
        expr,
        base,
        SearchTrigger::GraphSearch,
        "third",
        "graph_score(node_id_col, 'seed', ...)",
    )?;
    parse_graph_score_args(f)
}

/// The RRF constant `name` at argument `index`, or [`DEFAULT_RRF_K`] when
/// the call names none. It must be a positive finite number.
fn rrf_k(args: &[ast::Expr], index: usize, name: &str) -> Result<f64> {
    let Some(expr) = args.get(index) else {
        return Ok(DEFAULT_RRF_K);
    };
    let k = extract_float(expr).map_err(|_| SqlError::InvalidFunction {
        detail: format!("rrf_score(): {name} must be a numeric constant; got {expr}"),
    })?;
    if !k.is_finite() || k <= 0.0 {
        return Err(SqlError::InvalidFunction {
            detail: format!("rrf_score(): {name} must be a positive number; got {expr}"),
        });
    }
    Ok(k)
}

/// The typed error for a `rrf_score(...)` query with no LIMIT.
fn missing_limit_error() -> SqlError {
    SqlError::InvalidFunction {
        detail: "rrf_score() fuses a ranked list of LIMIT rows; add a LIMIT clause \
                 (e.g. ... ORDER BY score DESC LIMIT 10)"
            .into(),
    }
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
