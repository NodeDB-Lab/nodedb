// SPDX-License-Identifier: Apache-2.0

//! Search-trigger detection in WHERE clauses.
//!
//! Recognises every `SearchTrigger` shape that has a docs-advertised WHERE
//! form: `TextMatch`, the spatial predicates, `VectorSearch`, and
//! `MultiVectorSearch`. The match on `SearchTrigger` is exhaustive so the
//! compiler refuses to forget a new trigger here when one is added.
//!
//! AND-recursion: when one side of an `AND` is a search trigger, the other
//! side is carried through as a scan filter — without this, the docs form
//! `WHERE tenant = 't1' AND embedding <-> $q` would silently drop the
//! tenant predicate after the vector trigger fires.

use sqlparser::ast;

use super::entry_ann::parse_ann_options;
use super::helpers::{
    convert_where_to_filters, extract_column_name, extract_float, extract_float_array,
    extract_func_args, extract_string_literal, extract_text_field, metric_from_func_name,
};
use crate::error::{Result, SqlError};
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::parser::normalize::normalize_ident;
use crate::types::*;

/// Default `top_k` placeholder used when a WHERE-derived search plan has no
/// surrounding `LIMIT`. `apply_limit` in `entry.rs` overwrites `top_k` and
/// `ef_search` from the user's `LIMIT N` after planning; this default only
/// survives when the query has no LIMIT, in which case the canonical pgvector
/// shape returns the 10 nearest rows.
const DEFAULT_TOP_K: usize = 10;

/// `2 * top_k` is the standard HNSW beam-width heuristic when the user has
/// not specified `ef_search` explicitly via `vector_distance(... ef_search => N)`.
const DEFAULT_EF_SEARCH_MULTIPLIER: usize = 2;

/// Try to detect search-triggering patterns in a WHERE clause.
pub(super) fn try_extract_where_search(
    expr: &ast::Expr,
    table: &crate::resolver::columns::ResolvedTable,
    functions: &FunctionRegistry,
    projection: &[Projection],
) -> Result<Option<SqlPlan>> {
    try_extract_with_extra_filters(expr, table, functions, None, projection)
}

/// Internal entry that threads an optional sibling-AND filter through to the
/// concrete trigger handler. The public entry calls this with `None`; the
/// AND recursion branch calls it with the *other* side of the AND so a
/// vector / spatial / text trigger can carry the sibling predicate as a
/// scan filter instead of silently dropping it.
fn try_extract_with_extra_filters(
    expr: &ast::Expr,
    table: &crate::resolver::columns::ResolvedTable,
    functions: &FunctionRegistry,
    extra_filter: Option<&ast::Expr>,
    projection: &[Projection],
) -> Result<Option<SqlPlan>> {
    match expr {
        ast::Expr::Function(func) => {
            let name = function_name(func);
            dispatch_trigger(&name, func, table, functions, extra_filter, projection)
        }
        // AND: recurse on each side, carrying the other side as a scan filter.
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            // Try left as the trigger, with right as the carried filter.
            if let Some(plan) = try_extract_with_extra_filters(
                left,
                table,
                functions,
                Some(right.as_ref()),
                projection,
            )? {
                return Ok(Some(plan));
            }
            // Try right as the trigger, with left as the carried filter.
            if let Some(plan) = try_extract_with_extra_filters(
                right,
                table,
                functions,
                Some(left.as_ref()),
                projection,
            )? {
                return Ok(Some(plan));
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

fn function_name(func: &ast::Function) -> String {
    func.name
        .0
        .iter()
        .map(|p| match p {
            ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn dispatch_trigger(
    name: &str,
    func: &ast::Function,
    table: &crate::resolver::columns::ResolvedTable,
    functions: &FunctionRegistry,
    extra_filter: Option<&ast::Expr>,
    projection: &[Projection],
) -> Result<Option<SqlPlan>> {
    // Exhaustive match on `SearchTrigger`: when a new trigger is added,
    // this fails to compile until a WHERE-clause routing decision is made
    // for it. This is the structural fix for the original bug class —
    // silent fall-through on unhandled triggers.
    match functions.search_trigger(name) {
        SearchTrigger::TextMatch => plan_text_from_where(func, table, extra_filter, projection),
        SearchTrigger::SpatialDWithin
        | SearchTrigger::SpatialContains
        | SearchTrigger::SpatialIntersects
        | SearchTrigger::SpatialWithin => {
            plan_spatial_from_where(name, func, table, extra_filter, projection)
        }
        SearchTrigger::VectorSearch => {
            plan_vector_from_where(name, func, table, extra_filter, projection)
        }
        SearchTrigger::MultiVectorSearch => {
            plan_multi_vector_from_where(func, table, extra_filter, projection)
        }
        // The remaining triggers either have no WHERE-clause shape advertised
        // anywhere in the docs (`HybridSearch`, `TextSearch`, the array TVFs,
        // `TimeBucket`) or are not search triggers at all (`None`). We fall
        // back to scalar evaluation, which matches the existing contract for
        // these surfaces. The match is exhaustive so a new trigger added to
        // the enum will fail this file at compile time, forcing a routing
        // decision rather than another silent fall-through.
        // `sparse_score(...)` is an ORDER BY similarity surface (canonical form
        // `ORDER BY sparse_score(field, '{...}') DESC LIMIT k`); it has no
        // WHERE-clause shape, so fall through to scalar evaluation like the
        // other non-WHERE search triggers.
        SearchTrigger::SparseSearch
        | SearchTrigger::HybridSearch
        | SearchTrigger::TextSearch
        | SearchTrigger::TimeBucket
        | SearchTrigger::ArraySlice
        | SearchTrigger::ArrayProject
        | SearchTrigger::ArrayAgg
        | SearchTrigger::ArrayElementwise
        | SearchTrigger::ArrayFlush
        | SearchTrigger::ArrayCompact
        // graph_score() is a planner-intercepted marker inside rrf_score(...);
        // it has no standalone WHERE-clause shape — fall through to scalar eval.
        | SearchTrigger::GraphSearch
        | SearchTrigger::None => Ok(None),
    }
}

fn extra_filter_to_filters(
    extra: Option<&ast::Expr>,
    table: &crate::resolver::columns::ResolvedTable,
) -> Result<Vec<Filter>> {
    match extra {
        Some(e) => {
            let scope = crate::resolver::columns::TableScope::single(table.clone())?;
            convert_where_to_filters(e, &scope)
        }
        None => Ok(Vec::new()),
    }
}

fn plan_text_from_where(
    func: &ast::Function,
    table: &crate::resolver::columns::ResolvedTable,
    extra_filter: Option<&ast::Expr>,
    projection: &[Projection],
) -> Result<Option<SqlPlan>> {
    use crate::fts_types::FtsQuery;

    let args = extract_func_args(func)?;
    if args.len() < 2 {
        return Ok(None);
    }
    let field = extract_text_field(&args[0])?;
    let query_text = extract_string_literal(&args[1])?;

    // Detect a phrase query: query_text surrounded by double-quotes.
    // SQL form: `text_match(body, '"quick brown fox"')`.
    // The outer SQL single-quotes are stripped by the SQL parser; the
    // inner double-quotes are literal characters in the string value.
    let fts_query =
        if query_text.starts_with('"') && query_text.ends_with('"') && query_text.len() > 2 {
            let inner = &query_text[1..query_text.len() - 1];
            let terms: Vec<String> = inner.split_whitespace().map(|s| s.to_string()).collect();
            if terms.len() > 1 {
                FtsQuery::Phrase(terms)
            } else {
                FtsQuery::Plain {
                    text: inner.to_string(),
                    fuzzy: true,
                }
            }
        } else {
            FtsQuery::Plain {
                text: query_text,
                fuzzy: true,
            }
        };

    Ok(Some(SqlPlan::TextSearch {
        collection: table.name.clone(),
        field,
        query: fts_query,
        top_k: 1000,
        filters: extra_filter_to_filters(extra_filter, table)?,
        score_alias: None,
        projection: projection.to_vec(),
    }))
}

fn plan_vector_from_where(
    name: &str,
    func: &ast::Function,
    table: &crate::resolver::columns::ResolvedTable,
    extra_filter: Option<&ast::Expr>,
    projection: &[Projection],
) -> Result<Option<SqlPlan>> {
    let args = extract_func_args(func)?;
    if args.len() < 2 {
        return Ok(None);
    }
    let field = extract_column_name(&args[0])?;
    let query_vector = extract_float_array(&args[1])?;

    let raw_func_args: &[ast::FunctionArg] = match &func.args {
        ast::FunctionArguments::List(list) => &list.args,
        _ => &[],
    };
    let ann_options = parse_ann_options(raw_func_args)?;
    let ef_search = ann_options
        .ef_search_override
        .unwrap_or(DEFAULT_TOP_K * DEFAULT_EF_SEARCH_MULTIPLIER);

    Ok(Some(SqlPlan::VectorSearch {
        collection: table.name.clone(),
        field,
        query_vector,
        top_k: DEFAULT_TOP_K,
        ef_search,
        metric: metric_from_func_name(name),
        filters: extra_filter_to_filters(extra_filter, table)?,
        array_prefilter: None,
        ann_options,
        // Vector-primary skip-payload-fetch and payload-filter peeling are
        // applied uniformly by the post-pass in `entry::plan_query` after
        // planning returns — see the `if let SqlPlan::VectorSearch ...`
        // block there. WHERE-derived plans flow through the same post-pass
        // and need no special handling here.
        skip_payload_fetch: false,
        payload_filters: Vec::new(),
        projection: projection.to_vec(),
    }))
}

fn plan_multi_vector_from_where(
    func: &ast::Function,
    table: &crate::resolver::columns::ResolvedTable,
    extra_filter: Option<&ast::Expr>,
    projection: &[Projection],
) -> Result<Option<SqlPlan>> {
    let args = extract_func_args(func)?;
    if args.len() < 2 {
        return Ok(None);
    }
    // multi_vector_distance(field, query_vector) — same shape as VectorSearch
    // but a separate plan variant. The sibling-AND filter would attach here
    // once the executor accepts filters on MultiVectorSearch; for now we keep
    // parity with the existing variant fields and raise on a sibling filter
    // so the user gets a clear "not yet supported here" instead of silent drop.
    if extra_filter.is_some() {
        return Err(SqlError::Unsupported {
            detail:
                "AND-combined predicates with multi_vector_distance(...) in WHERE are not supported; \
                 use a subquery or rewrite as ORDER BY"
                    .into(),
        });
    }
    let _field = extract_column_name(&args[0])?;
    let query_vector = extract_float_array(&args[1])?;
    Ok(Some(SqlPlan::MultiVectorSearch {
        collection: table.name.clone(),
        query_vector,
        top_k: DEFAULT_TOP_K,
        ef_search: DEFAULT_TOP_K * DEFAULT_EF_SEARCH_MULTIPLIER,
        projection: projection.to_vec(),
    }))
}

fn plan_spatial_from_where(
    name: &str,
    func: &ast::Function,
    table: &crate::resolver::columns::ResolvedTable,
    extra_filter: Option<&ast::Expr>,
    projection: &[Projection],
) -> Result<Option<SqlPlan>> {
    let predicate = match name {
        "st_dwithin" => SpatialPredicate::DWithin,
        "st_contains" => SpatialPredicate::Contains,
        "st_intersects" => SpatialPredicate::Intersects,
        "st_within" => SpatialPredicate::Within,
        _ => return Ok(None),
    };
    let args = extract_func_args(func)?;
    if args.is_empty() {
        return Err(SqlError::MissingField {
            field: "geometry column".into(),
            context: name.into(),
        });
    }
    let field = extract_column_name(&args[0])?;
    let geom_arg = args.get(1).ok_or_else(|| SqlError::MissingField {
        field: "query geometry".into(),
        context: name.into(),
    })?;
    let geometry = crate::planner::geometry_expr::resolve_geometry_expr(geom_arg).map_err(|e| {
        SqlError::InvalidFunction {
            detail: format!("invalid geometry in {name}: {e}"),
        }
    })?;
    let issues = nodedb_spatial::validate::validate_geometry(&geometry);
    if !issues.is_empty() {
        return Err(SqlError::InvalidFunction {
            detail: format!("invalid geometry in {name}: {}", issues.join("; ")),
        });
    }
    let distance = if args.len() >= 3 {
        extract_float(&args[2]).unwrap_or(0.0)
    } else {
        0.0
    };
    Ok(Some(SqlPlan::SpatialScan {
        collection: table.name.clone(),
        field,
        predicate,
        query_geometry: geometry,
        distance_meters: distance,
        attribute_filters: extra_filter_to_filters(extra_filter, table)?,
        limit: 1000,
        projection: projection.to_vec(),
    }))
}
