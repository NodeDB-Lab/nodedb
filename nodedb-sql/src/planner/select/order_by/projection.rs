// SPDX-License-Identifier: Apache-2.0

//! SELECT-projection pass for hybrid-search and text-score columns.
//!
//! When `apply_order_by` left the plan as a `Scan` (no ORDER BY, or an
//! ORDER BY that did not match any search trigger), the `rrf_score(...)` or
//! `bm25_score(...)` call may still appear directly in the SELECT projection.
//! The canonical shape `SELECT id, rrf_score(...) AS score FROM c WHERE ... LIMIT N`
//! requires this entry path because there is no ORDER BY clause to inspect.
//! A score call no search plan serves is refused at plan time
//! (`planner::search_scope`): it has no per-row value.
//!
//! Every `bm25_score(column, q)` / `text_match(column, q)` in the SELECT list
//! becomes one score column. On a `TextSearch` plan the columns join the plan
//! and its shape stays as it is. A `Scan` becomes a score scan: every row its
//! filters admit, each with its score. A row the scoped index holds but the
//! query does not match scores `0.0`. A row the index does not hold scores
//! `null`.

use sqlparser::ast;

use super::super::helpers::extract_func_args;
use super::super::text_call::resolve_text_call;
use super::aliases::function_call_name;
use super::hybrid::{no_args_rrf_score_error, plan_hybrid_from_sort};
use super::text_score::{ScanSort, attach_scores};
use crate::error::Result;
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::parser::normalize::normalize_ident;
use crate::planner::select::post_process::post_process;
use crate::resolver::columns::ResolvedTable;
use crate::types::{SqlPlan, TextScoreColumn};

/// Fire a hybrid search or attach text-score columns from the SELECT list.
///
/// `Ok(None)` when the SELECT list holds neither, or `plan` is neither a
/// `Scan` nor a `TextSearch`: no other plan becomes a search here.
pub(in crate::planner::select) fn try_hybrid_from_projection(
    plan: &SqlPlan,
    select_items: &[ast::SelectItem],
    functions: &FunctionRegistry,
    table: &ResolvedTable,
) -> Result<Option<SqlPlan>> {
    if !matches!(plan, SqlPlan::Scan { .. } | SqlPlan::TextSearch(_)) {
        return Ok(None);
    }
    let mut scores = Vec::new();
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
                // Only a Scan becomes a hybrid search.
                let SqlPlan::Scan { sort_keys, .. } = plan else {
                    continue;
                };
                let args = extract_func_args(func)?;
                if args.is_empty() {
                    return Err(no_args_rrf_score_error());
                }
                let Some(hybrid) = plan_hybrid_from_sort(&args, table, plan, alias.as_deref())?
                else {
                    return Ok(None);
                };
                // An ORDER BY that named no search trigger still sorts the
                // fused rows.
                if sort_keys.is_empty() {
                    return Ok(Some(hybrid));
                }
                return post_process(hybrid, sort_keys.clone(), None, 0).map(Some);
            }
            SearchTrigger::TextSearch | SearchTrigger::TextMatch => {
                let call = resolve_text_call(&name, func, table)?;
                // The explicit AS alias, else the stringified expression, the
                // key the pgwire projection layer derives from
                // `UnnamedExpr.to_string()`.
                scores.push(TextScoreColumn {
                    field: call.field,
                    query: call.query,
                    mode: call.options.params.mode,
                    fuzzy: call.options.params.fuzzy,
                    alias: alias.unwrap_or_else(|| expr.to_string()),
                });
            }
            _ => {}
        }
    }
    if scores.is_empty() {
        return Ok(None);
    }
    attach_scores(plan, scores, ScanSort::Keep)
}
