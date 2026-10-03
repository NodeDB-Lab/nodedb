// SPDX-License-Identifier: Apache-2.0

//! ORDER BY entry point.
//!
//! Maps an ORDER BY clause to either sort keys on the existing scan plan or
//! a search-shaped plan (`VectorSearch`, `TextSearch`, `HybridSearch`) when
//! the leading sort expression matches a registered `SearchTrigger`.

use sqlparser::ast;

use super::aliases::{resolve_order_by_target, select_output_aliases};
use super::triggers::{SortSearch, try_extract_sort_search};
use crate::error::Result;
use crate::functions::registry::FunctionRegistry;
use crate::planner::agg_bind::{BindName, bind_aggregate_calls};
use crate::planner::select::post_process::post_process;
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// Apply ORDER BY, detecting search-triggering sort expressions.
///
/// `select_items` is the raw SELECT list from the AST. It is required so
/// that an ORDER BY referencing an alias (`ORDER BY score DESC` where the
/// SELECT carries `rrf_score(...) AS score`) can be resolved back to the
/// underlying function call before the search-trigger check runs. Without
/// this resolution the search trigger would only fire when the literal
/// function call appears in ORDER BY — a shape no SQL author would write
/// when the same expression is also being projected.
pub(in crate::planner::select) fn apply_order_by(
    plan: &SqlPlan,
    order_by: &ast::OrderBy,
    functions: &FunctionRegistry,
    select_items: &[ast::SelectItem],
    scope: &TableScope,
) -> Result<SqlPlan> {
    let exprs = match &order_by.kind {
        ast::OrderByKind::Expressions(exprs) => exprs,
        ast::OrderByKind::All(_) => return Ok(plan.clone()),
    };

    if exprs.is_empty() {
        return Ok(plan.clone());
    }

    // Two resolution rules apply before the trigger check:
    //   (a) Bare-identifier ORDER BY → look up the alias in the SELECT
    //       projection and substitute the underlying expression.
    //   (b) Literal function-call ORDER BY → also check the SELECT for the
    //       same call under an alias, and propagate that alias.
    let first = &exprs[0];
    let (resolved_expr, score_alias) = resolve_order_by_target(&first.expr, select_items);
    match try_extract_sort_search(
        resolved_expr,
        plan,
        functions,
        score_alias.as_deref(),
        scope.single_table(),
    )? {
        Some(SortSearch::Ranked(search_plan)) => return Ok(search_plan),
        // The leading key sorts by the score column the plan carries. The
        // rest convert as plain sort keys.
        //
        // A row the index does not hold scores `null`. Its key takes the
        // NULL placement of every other ORDER BY key: last ascending, first
        // descending, unless the clause names `NULLS FIRST` / `NULLS LAST`.
        Some(SortSearch::Scored {
            plan: scored,
            alias,
        }) => {
            let sort_scope = scope.with_output_names(select_output_aliases(select_items));
            let mut keys = vec![SortKey {
                expr: SqlExpr::Column {
                    table: None,
                    name: alias,
                },
                ascending: first.options.asc.unwrap_or(true),
                nulls_first: first
                    .options
                    .nulls_first
                    .unwrap_or(!first.options.asc.unwrap_or(true)),
            }];
            for o in &exprs[1..] {
                keys.push(SortKey {
                    expr: convert_expr(&o.expr, &ColumnScope::Relations(&sort_scope))?,
                    ascending: o.options.asc.unwrap_or(true),
                    nulls_first: o
                        .options
                        .nulls_first
                        .unwrap_or(!o.options.asc.unwrap_or(true)),
                });
            }
            return post_process(scored, keys, None, 0);
        }
        None => {}
    }

    // After GROUP BY, an ORDER BY term may name an aggregate — projected or
    // not — or compute over one. Those values exist only once the groups are
    // finalized, so each call is bound to the column it lands in and any
    // aggregate the sort alone introduces joins the computed list.
    let mut bound_aggregates: Option<Vec<AggregateExpr>> = None;
    let sort_keys: Vec<SortKey> = if let SqlPlan::Aggregate { aggregates, .. } = plan {
        let mut extended = aggregates.clone();
        // ORDER BY sorts after aggregates are renamed to their user aliases,
        // so each key must address the output name. Binding runs to
        // completion first: a sort-only aggregate appends to `extended`, and
        // the name it lands in has to be in scope when the key converts.
        let bound: Vec<ast::Expr> = exprs
            .iter()
            .map(|o| {
                bind_aggregate_calls(
                    &o.expr,
                    select_items,
                    &mut extended,
                    functions,
                    BindName::Output,
                    scope,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let sort_scope = scope.with_output_names(
            extended
                .iter()
                .map(|a| a.alias.clone())
                .chain(select_output_aliases(select_items)),
        );
        let keys = exprs
            .iter()
            .zip(&bound)
            .map(|(o, expr)| {
                Ok(SortKey {
                    expr: convert_expr(expr, &ColumnScope::Relations(&sort_scope))?,
                    ascending: o.options.asc.unwrap_or(true),
                    nulls_first: o
                        .options
                        .nulls_first
                        .unwrap_or(!o.options.asc.unwrap_or(true)),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        bound_aggregates = Some(extended);
        keys
    } else {
        let sort_scope = scope.with_output_names(select_output_aliases(select_items));
        exprs
            .iter()
            .map(|o| {
                Ok(SortKey {
                    expr: convert_expr(&o.expr, &ColumnScope::Relations(&sort_scope))?,
                    ascending: o.options.asc.unwrap_or(true),
                    nulls_first: o
                        .options
                        .nulls_first
                        .unwrap_or(!o.options.asc.unwrap_or(true)),
                })
            })
            .collect::<Result<_>>()?
    };

    match plan {
        SqlPlan::Scan {
            collection,
            alias,
            engine,
            filters,
            projection,
            limit,
            offset,
            distinct,
            window_functions,
            temporal,
            ..
        } => Ok(SqlPlan::Scan {
            collection: collection.clone(),
            alias: alias.clone(),
            engine: *engine,
            filters: filters.clone(),
            projection: projection.clone(),
            sort_keys,
            limit: *limit,
            offset: *offset,
            distinct: *distinct,
            window_functions: window_functions.clone(),
            temporal: *temporal,
        }),
        // ORDER BY applied to a GROUP BY result: stash the sort keys
        // on the Aggregate plan; the executor sorts the finalized
        // group rows before returning. Without this branch the sort
        // is silently dropped — every `… GROUP BY x ORDER BY x` query
        // comes back in hash-map iteration order, which is a
        // data-correctness bug for any downstream consumer.
        SqlPlan::Aggregate {
            input,
            group_by,
            group_by_aliases,
            output_order,
            aggregates,
            having,
            limit,
            grouping_sets,
            ..
        } => Ok(SqlPlan::Aggregate {
            input: input.clone(),
            group_by: group_by.clone(),
            group_by_aliases: group_by_aliases.clone(),
            output_order: output_order.clone(),
            aggregates: bound_aggregates.unwrap_or_else(|| aggregates.clone()),
            having: having.clone(),
            limit: *limit,
            grouping_sets: grouping_sets.clone(),
            sort_keys,
        }),
        // A timeseries scan carries its own sort keys: the engine returns
        // rows in the order it finds them (memtable, then partitions), which
        // is not the order the client asked for. Dropping the sort here would
        // silently answer `ORDER BY ts DESC` with ascending rows.
        SqlPlan::TimeseriesScan(TimeseriesScanPlan {
            collection,
            time_range,
            bucket_interval_ms,
            group_by,
            aggregates,
            filters,
            projection,
            gap_fill,
            limit,
            tiered,
            temporal,
            ..
        }) => Ok(SqlPlan::TimeseriesScan(TimeseriesScanPlan {
            collection: collection.clone(),
            time_range: *time_range,
            bucket_interval_ms: *bucket_interval_ms,
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
            filters: filters.clone(),
            projection: projection.clone(),
            gap_fill: gap_fill.clone(),
            limit: *limit,
            sort_keys,
            tiered: *tiered,
            temporal: *temporal,
        })),
        // Cte wraps an inner outer plan; push ORDER BY into that outer
        // so derived-table queries (`SELECT … FROM (…) AS t ORDER BY …`)
        // honour the sort. inline_cte downstream merges the outer Scan
        // with the inner subquery plan; the sort_keys ride along.
        SqlPlan::Cte(CtePlan { definitions, outer }) => Ok(SqlPlan::Cte(CtePlan {
            definitions: definitions.clone(),
            outer: Box::new(apply_order_by(
                outer,
                order_by,
                functions,
                select_items,
                scope,
            )?),
        })),
        // The clause is non-empty here (`exprs` was checked above), so these
        // keys were asked for. A variant with no slot to hold them must not
        // pass through unchanged — that answers the query in whatever order
        // the engine happened to produce. Sort its rows in a post-processing
        // tail instead.
        other => post_process(other.clone(), sort_keys, None, 0),
    }
}
