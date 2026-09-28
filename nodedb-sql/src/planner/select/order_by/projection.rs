// SPDX-License-Identifier: Apache-2.0

//! SELECT-projection fallback for hybrid-search and text-search trigger detection.
//!
//! When `apply_order_by` left the plan as a `Scan` (no ORDER BY, or an
//! ORDER BY that did not match any search trigger), the `rrf_score(...)` or
//! `bm25_score(...)` call may still appear directly in the SELECT projection.
//! The canonical shape `SELECT id, rrf_score(...) AS score FROM c WHERE ... LIMIT N`
//! requires this entry path because there is no ORDER BY clause to inspect.
//! A score call no search plan serves is refused at plan time
//! (`planner::search_scope`): it has no per-row value.
//!
//! Text-search shape: `SELECT id, bm25_score(field, term) FROM c ORDER BY id`.
//! The plan stays a Scan after ORDER BY (non-search sort key). This pass
//! detects `bm25_score` or `text_match` in the SELECT list and promotes the
//! plan to `SqlPlan::TextSearch` with `score_alias` set. The converter then
//! emits `TextOp::BM25ScoreScan` — a full-collection scan where each row
//! receives the BM25 score for the query term (null if the term does not occur).

use sqlparser::ast;

use super::super::helpers::{extract_func_args, extract_string_literal, extract_text_field};
use super::aliases::function_call_name;
use super::hybrid::{no_args_rrf_score_error, plan_hybrid_from_sort};
use crate::error::Result;
use crate::fts_types::FtsQuery;
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::parser::normalize::normalize_ident;
use crate::types::SqlPlan;

/// Try to fire a hybrid-search trigger from the SELECT projection alone.
///
/// Also handles `bm25_score(field, term)` and `text_match(field, term)` in
/// the SELECT list when no ORDER BY search trigger fired. When detected, the
/// Scan (or existing TextSearch) is promoted to a `SqlPlan::TextSearch` with
/// `score_alias` set so the converter can emit `TextOp::BM25ScoreScan`.
pub(in crate::planner::select) fn try_hybrid_from_projection(
    plan: &SqlPlan,
    select_items: &[ast::SelectItem],
    functions: &FunctionRegistry,
) -> Result<Option<SqlPlan>> {
    // Try hybrid first (rrf_score).
    let collection = match plan {
        SqlPlan::Scan { collection, .. } => collection.clone(),
        SqlPlan::TextSearch { collection, .. } => collection.clone(),
        _ => return Ok(None),
    };

    for item in select_items {
        let (expr, alias) = match item {
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, Some(normalize_ident(alias))),
            ast::SelectItem::UnnamedExpr(expr) => (expr, None),
            _ => continue,
        };
        let ast::Expr::Function(func) = expr else {
            continue;
        };
        let name = function_call_name(expr).unwrap_or_default();

        match functions.search_trigger(&name) {
            SearchTrigger::HybridSearch => {
                // Only fire hybrid trigger for Scan plans.
                if !matches!(plan, SqlPlan::Scan { .. }) {
                    continue;
                }
                let args = extract_func_args(func)?;
                if args.is_empty() {
                    return Err(no_args_rrf_score_error());
                }
                return plan_hybrid_from_sort(&args, &collection, plan, alias.as_deref());
            }
            SearchTrigger::TextSearch => {
                // bm25_score(field, term) in SELECT — promote to BM25ScoreScan.
                let args = extract_func_args(func)?;
                if args.len() < 2 {
                    continue;
                }
                let query_text = extract_string_literal(&args[1]).unwrap_or_default();
                if query_text.is_empty() {
                    continue;
                }
                let field = extract_text_field(&args[0])?;
                // Use the explicit AS alias when present; otherwise use the
                // stringified expression so the injected row field key matches
                // the lookup key the pgwire projection layer derives from
                // `UnnamedExpr.to_string()`.
                let score_alias = alias.clone().unwrap_or_else(|| expr.to_string());
                return Ok(Some(build_text_search_score_scan(
                    plan,
                    &collection,
                    field,
                    query_text,
                    score_alias,
                )));
            }
            SearchTrigger::TextMatch => {
                // text_match(field, term) in SELECT — same as bm25_score.
                let args = extract_func_args(func)?;
                if args.len() < 2 {
                    continue;
                }
                let query_text = extract_string_literal(&args[1]).unwrap_or_default();
                if query_text.is_empty() {
                    continue;
                }
                let field = extract_text_field(&args[0])?;
                let score_alias = alias.clone().unwrap_or_else(|| expr.to_string());
                return Ok(Some(build_text_search_score_scan(
                    plan,
                    &collection,
                    field,
                    query_text,
                    score_alias,
                )));
            }
            _ => {}
        }
    }
    Ok(None)
}

/// Build a `SqlPlan::TextSearch` with `score_alias` set for a BM25ScoreScan.
///
/// Carries forward filters from an existing `Scan` or `TextSearch` plan.
/// When the input is already a `TextSearch` (from a WHERE `text_match(...)`),
/// the existing field, query, and filters are preserved and only the
/// `score_alias` is attached. Otherwise `field` names the scored column.
fn build_text_search_score_scan(
    plan: &SqlPlan,
    collection: &str,
    field: Option<String>,
    query_text: String,
    score_alias: String,
) -> SqlPlan {
    match plan {
        SqlPlan::TextSearch {
            field: existing_field,
            query,
            top_k,
            filters,
            projection,
            ..
        } => SqlPlan::TextSearch {
            collection: collection.to_string(),
            field: existing_field.clone(),
            query: query.clone(),
            top_k: *top_k,
            filters: filters.clone(),
            score_alias: Some(score_alias),
            projection: projection.clone(),
        },
        SqlPlan::Scan {
            filters,
            limit,
            projection,
            ..
        } => SqlPlan::TextSearch {
            collection: collection.to_string(),
            field,
            query: FtsQuery::Plain {
                text: query_text,
                fuzzy: true,
            },
            top_k: limit.unwrap_or(10_000),
            filters: filters.clone(),
            score_alias: Some(score_alias),
            projection: projection.clone(),
        },
        _ => SqlPlan::TextSearch {
            collection: collection.to_string(),
            field,
            query: FtsQuery::Plain {
                text: query_text,
                fuzzy: true,
            },
            top_k: 10_000,
            filters: Vec::new(),
            score_alias: Some(score_alias),
            projection: Vec::new(),
        },
    }
}
