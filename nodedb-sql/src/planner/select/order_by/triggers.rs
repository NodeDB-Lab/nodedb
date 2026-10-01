// SPDX-License-Identifier: Apache-2.0

//! Search-trigger detection on ORDER BY expressions.
//!
//! Maps a `SearchTrigger`-tagged function call (e.g. `vector_distance(...)`,
//! `text_match(...)`, `rrf_score(...)`) into the corresponding `SqlPlan`
//! search variant, pulling collection / filters / limit context from the
//! current plan.

use sqlparser::ast::{self, FunctionArg, FunctionArguments};

use super::super::entry_ann::parse_ann_options;
use super::super::helpers::{
    extract_column_name, extract_float_array, extract_func_args, extract_string_literal,
    extract_text_field, metric_from_func_name, source_projection,
};
use super::aliases::function_call_name;
use super::hybrid::{no_args_rrf_score_error, plan_hybrid_from_sort};
use super::vector_join::extract_vector_join_target;
use crate::error::{Result, SqlError};
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::types::*;

/// Default `ef_search` multiplier applied when the user has not supplied
/// `ef_search_override` in the `vector_distance` options. `2 * top_k` is
/// the standard HNSW heuristic.
const DEFAULT_EF_SEARCH_MULTIPLIER: usize = 2;

/// Try to detect a search-triggering function call.
///
/// `score_alias` is propagated only into hybrid-search plans — vector and
/// text searches return a fixed-shape response.
pub(super) fn try_extract_sort_search(
    expr: &ast::Expr,
    plan: &SqlPlan,
    functions: &FunctionRegistry,
    score_alias: Option<&str>,
) -> Result<Option<SqlPlan>> {
    let ast::Expr::Function(func) = expr else {
        return Ok(None);
    };
    let name = function_call_name(expr).unwrap_or_default();
    let (collection, array_prefilter) = match plan {
        SqlPlan::Scan { collection, .. } => (collection.clone(), None),
        SqlPlan::Join { left, right, .. } => match extract_vector_join_target(left, right) {
            Some(t) => (t.vector_collection, t.array_prefilter),
            None => return Ok(None),
        },
        _ => return Ok(None),
    };
    let args = extract_func_args(func)?;
    let raw_func_args: &[FunctionArg] = match &func.args {
        FunctionArguments::List(list) => &list.args,
        _ => &[],
    };

    match functions.search_trigger(&name) {
        SearchTrigger::VectorSearch => {
            if args.len() < 2 {
                return Ok(None);
            }
            let field = extract_column_name(&args[0])?;
            let vector = extract_float_array(&args[1])?;
            let ann_options = parse_ann_options(raw_func_args)?;
            let limit = match plan {
                SqlPlan::Scan { limit, .. } => limit.unwrap_or(10),
                // A no-LIMIT join (`None`) falls back to the same default
                // top-k as a no-LIMIT scan; an explicit `LIMIT n` is honored.
                SqlPlan::Join { limit, .. } => limit.unwrap_or(10),
                _ => 10,
            };
            let ef_search = ann_options
                .ef_search_override
                .unwrap_or(limit * DEFAULT_EF_SEARCH_MULTIPLIER);
            let metric = metric_from_func_name(&name);
            Ok(Some(SqlPlan::VectorSearch {
                collection,
                field,
                query_vector: vector,
                top_k: limit,
                ef_search,
                metric,
                filters: match plan {
                    SqlPlan::Scan { filters, .. } => filters.clone(),
                    _ => Vec::new(),
                },
                array_prefilter,
                ann_options,
                // Projection analysis and payload-filter peeling require
                // catalog access; the caller (`plan_query`) fills these
                // fields after `apply_order_by` returns.
                skip_payload_fetch: false,
                payload_filters: Vec::new(),
                projection: source_projection(plan),
            }))
        }
        SearchTrigger::SparseSearch => {
            if args.len() < 2 {
                return Ok(None);
            }
            let field = extract_column_name(&args[0])?;
            let literal = extract_string_literal(&args[1])?;
            // Parse `'{dim: weight, ...}'` into sorted `(dimension, weight)`
            // entries. `top_k` is applied later by `apply_limit` from the
            // query's `LIMIT` clause (mirrors VectorSearch — plan_select seeds
            // `Scan::limit` as None, so this default is only a fallback).
            let query_entries = nodedb_types::SparseVector::parse_literal(&literal)
                .map_err(|e| SqlError::InvalidFunction {
                    detail: format!("invalid sparse query vector in sparse_score(...): {e}"),
                })?
                .entries()
                .to_vec();
            let top_k = match plan {
                SqlPlan::Scan { limit, .. } => limit.unwrap_or(10),
                SqlPlan::Join { limit, .. } => limit.unwrap_or(10),
                _ => 10,
            };
            Ok(Some(SqlPlan::SparseSearch {
                collection,
                field,
                query_entries,
                top_k,
                projection: source_projection(plan),
            }))
        }
        SearchTrigger::TextSearch if args.len() >= 2 => {
            let field = extract_text_field(&args[0])?;
            let query_text = extract_string_literal(&args[1])?;
            let limit = match plan {
                SqlPlan::Scan { limit, .. } => limit.unwrap_or(10),
                _ => 10,
            };
            Ok(Some(SqlPlan::TextSearch {
                collection,
                field,
                query: crate::fts_types::FtsQuery::Plain {
                    text: query_text,
                    fuzzy: true,
                },
                top_k: limit,
                filters: match plan {
                    SqlPlan::Scan { filters, .. } => filters.clone(),
                    _ => Vec::new(),
                },
                score_alias: score_alias.map(|s| s.to_string()),
                projection: source_projection(plan),
            }))
        }
        SearchTrigger::TextSearch => Ok(None),
        SearchTrigger::HybridSearch => {
            if args.is_empty() {
                return Err(no_args_rrf_score_error());
            }
            plan_hybrid_from_sort(&args, &collection, plan, score_alias)
        }
        _ => Ok(None),
    }
}
