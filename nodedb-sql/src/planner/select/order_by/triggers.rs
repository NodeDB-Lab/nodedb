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
    metric_from_func_name, source_projection,
};
use super::super::text_call::{resolve_table_column, resolve_text_call};
use super::aliases::function_call_name;
use super::hybrid::{no_args_rrf_score_error, plan_hybrid_from_sort};
use super::text_score::{ScanSort, attach_scores};
use super::vector_join::extract_vector_join_target;
use crate::error::{Result, SqlError};
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::resolver::columns::ResolvedTable;
use crate::types::*;

/// Default `ef_search` multiplier applied when the user has not supplied
/// `ef_search_override` in the `vector_distance` options. `2 * top_k` is
/// the standard HNSW heuristic.
const DEFAULT_EF_SEARCH_MULTIPLIER: usize = 2;

/// The vector column `function(query)` searches when it names none: the
/// vector-primary field, else the one declared vector column, else the
/// collection-level index (`""`). Several declared vector columns are
/// ambiguous, a typed error naming them.
fn default_vector_field(function: &str, table: Option<&ResolvedTable>) -> Result<String> {
    let Some(table) = table else {
        return Ok(String::new());
    };
    if let Some(vp) = &table.info.vector_primary {
        return Ok(vp.vector_field.clone());
    }
    let vector_columns: Vec<&str> = table
        .info
        .columns
        .iter()
        .filter(|c| matches!(c.data_type, SqlDataType::Vector(_)))
        .map(|c| c.name.as_str())
        .collect();
    match vector_columns.as_slice() {
        [] => Ok(String::new()),
        [only] => Ok((*only).to_owned()),
        several => Err(SqlError::Unsupported {
            detail: format!(
                "{function}(query) names no column and '{}' has several vector columns \
                 ({}); name one: {function}(column, query)",
                table.name,
                several.join(", ")
            ),
        }),
    }
}

/// `function(column)`: the call names the searched column but no query
/// vector. The one-argument signature takes a query vector, so no signature
/// matches: PostgreSQL reports this as `undefined_function` (`42883`).
fn no_query_vector_error(function: &str) -> SqlError {
    SqlError::UndefinedFunction {
        name: function.to_owned(),
    }
}

/// A search plan an ORDER BY trigger produced.
pub(super) enum SortSearch {
    /// The plan returns its rows in the order the ORDER BY asked for.
    Ranked(SqlPlan),
    /// A text plan that carries the score under `alias`. Its rows are
    /// sorted by that column.
    Scored { plan: SqlPlan, alias: String },
}

/// Try to detect a search-triggering function call.
///
/// `score_alias` names the hybrid score column, and the score column of a
/// `bm25_score(...)` sort. `table` is the single relation in scope: the
/// text and hybrid triggers fire only over one.
pub(super) fn try_extract_sort_search(
    expr: &ast::Expr,
    plan: &SqlPlan,
    functions: &FunctionRegistry,
    score_alias: Option<&str>,
    table: Option<&ResolvedTable>,
) -> Result<Option<SortSearch>> {
    let ast::Expr::Function(func) = expr else {
        return Ok(None);
    };
    let name = function_call_name(expr).unwrap_or_default();
    match functions.search_trigger(&name) {
        // ORDER BY bm25_score(column, q): the plan carries the score column
        // and the rows sort by it.
        SearchTrigger::TextSearch => {
            let Some(table) = table else {
                return Ok(None);
            };
            let call = resolve_text_call(&name, func, table)?;
            let alias = score_alias.map_or_else(|| expr.to_string(), str::to_owned);
            let column = TextScoreColumn {
                field: call.field,
                query: call.query,
                mode: call.options.params.mode,
                fuzzy: call.options.params.fuzzy,
                alias: alias.clone(),
            };
            return Ok(attach_scores(plan, vec![column], ScanSort::Consumed)?
                .map(|plan| SortSearch::Scored { plan, alias }));
        }
        SearchTrigger::HybridSearch => {
            let (Some(table), SqlPlan::Scan { .. }) = (table, plan) else {
                return Ok(None);
            };
            let args = extract_func_args(func)?;
            if args.is_empty() {
                return Err(no_args_rrf_score_error());
            }
            return Ok(
                plan_hybrid_from_sort(&args, table, plan, score_alias)?.map(SortSearch::Ranked)
            );
        }
        _ => {}
    }
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
            let (field, query_arg) = match args.as_slice() {
                [] => return Ok(None),
                // A lone column is the searched field with no query vector:
                // no signature takes it, as PostgreSQL reports `42883`.
                [ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_)] => {
                    return Err(no_query_vector_error(&name));
                }
                // `vector_distance(query)` (the `SEARCH c USING VECTOR(q, k)`
                // form) searches the collection's default vector column.
                [query] => (default_vector_field(&name, table)?, query),
                // Over one relation, its qualifier is redundant: `t.embedding`
                // names the `embedding` column.
                [column, query, ..] => {
                    let field = match table {
                        Some(table) => resolve_table_column(column, table)?.ok_or_else(|| {
                            SqlError::Unsupported {
                                detail: format!("expected column name, got: {column}"),
                            }
                        })?,
                        None => extract_column_name(column)?,
                    };
                    (field, query)
                }
            };
            let vector = extract_float_array(query_arg)?;
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
            Ok(Some(SortSearch::Ranked(SqlPlan::VectorSearch {
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
                pk_prefilter: None,
                projection: source_projection(plan),
            })))
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
            Ok(Some(SortSearch::Ranked(SqlPlan::SparseSearch {
                collection,
                field,
                query_entries,
                top_k,
                projection: source_projection(plan),
            })))
        }
        _ => Ok(None),
    }
}
