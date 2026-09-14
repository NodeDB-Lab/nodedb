// SPDX-License-Identifier: Apache-2.0

//! GROUP BY and aggregate planning.

use sqlparser::ast::{self, GroupByExpr};

use crate::engine_rules::{self, AggregateParams};
use crate::error::Result;
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::parser::normalize::normalize_ident;
use crate::planner::group_by::{convert_group_by_with_projection, group_by_output_aliases};
use crate::planner::grouping_sets::expand_group_by;
use crate::resolver::ColumnScope;
use crate::resolver::columns::{ResolvedTable, TableScope};
use crate::resolver::expr::convert_expr;
use crate::temporal::TemporalScope;
use crate::types::*;

/// Plan an aggregate query (GROUP BY + aggregate functions).
pub fn plan_aggregate(
    select: &ast::Select,
    table: &ResolvedTable,
    filters: &[Filter],
    scope: &TableScope,
    functions: &FunctionRegistry,
    temporal: &TemporalScope,
) -> Result<SqlPlan> {
    // Detect ROLLUP / CUBE / GROUPING SETS before falling through to plain convert.
    let grouping_expansion = expand_group_by(&select.group_by, scope)?;

    let (group_by_exprs, grouping_sets) = if let Some(exp) = grouping_expansion {
        (exp.canonical_keys, Some(exp.grouping_sets))
    } else {
        (
            convert_group_by_with_projection(
                &select.group_by,
                &select.projection,
                &table.info.columns,
                scope,
            )?,
            None,
        )
    };

    let mut aggregates = extract_aggregates_from_projection(&select.projection, functions, scope)?;
    // HAVING is bound to the aggregates' computed output columns, and any
    // aggregate it alone introduces is added to `aggregates` so it is actually
    // computed.
    let having = match &select.having {
        Some(expr) => {
            super::having::plan_having(expr, &select.projection, &mut aggregates, functions, scope)?
        }
        None => Vec::new(),
    };

    // When grouping sets are present, detect GROUPING(col) in the projection and
    // synthesize AggregateExpr entries so the executor can compute them per-set.
    if grouping_sets.is_some() {
        let grouping_aggs = extract_grouping_calls(&select.projection, &group_by_exprs, scope)?;
        aggregates.extend(grouping_aggs);
    }

    // Records the SELECT-list interleaving of GROUP BY keys and aggregates so
    // output columns follow the user's SELECT order. Computed after the full
    // aggregate list (including any GROUPING pseudo-aggregates) is assembled so
    // grouping slots index correctly.
    let output_order = super::aggregate_order::compute_output_order(
        &select.projection,
        &group_by_exprs,
        functions,
        scope,
    )?;

    // Extract timeseries-specific params (bucket interval, group columns) if applicable.
    let (bucket_interval_ms, group_columns) =
        extract_timeseries_params(&select.group_by, &select.projection, functions)?;

    let rules = engine_rules::resolve_engine_rules(table.info.engine);
    let mut base_plan = rules.plan_aggregate(AggregateParams {
        collection: table.name.clone(),
        alias: table.alias.clone(),
        filters: filters.to_vec(),
        group_by: group_by_exprs.clone(),
        aggregates: aggregates.clone(),
        having: having.clone(),
        limit: 10000,
        bucket_interval_ms,
        group_columns,
        has_auto_tier: table.info.has_auto_tier,
        bitemporal: table.info.bitemporal,
        temporal: *temporal,
    })?;

    // The engine rules build the Aggregate node without a projection in scope,
    // so the SELECT-list output aliases for the GROUP BY keys are attached here
    // (Postgres: `SELECT k AS label ... GROUP BY k` yields output column
    // `label`, not `k`).
    let group_by_aliases = group_by_output_aliases(&select.projection, &group_by_exprs);
    if let SqlPlan::Aggregate {
        group_by_aliases: alias_slot,
        output_order: order_slot,
        ..
    } = &mut base_plan
    {
        *alias_slot = group_by_aliases.clone();
        *order_slot = output_order.clone();
    }

    // Wrap the plan to attach grouping sets if present.
    if let Some(sets) = grouping_sets {
        return Ok(attach_grouping_sets(
            base_plan,
            group_by_exprs,
            group_by_aliases,
            output_order,
            aggregates,
            having,
            sets,
        ));
    }

    Ok(base_plan)
}

/// Attach `grouping_sets` to an existing `SqlPlan::Aggregate` node, or wrap it
/// in a new one when the engine rules returned a non-Aggregate plan.
fn attach_grouping_sets(
    base_plan: SqlPlan,
    group_by: Vec<SqlExpr>,
    group_by_aliases: Vec<Option<String>>,
    output_order: Vec<AggOutputSlot>,
    aggregates: Vec<AggregateExpr>,
    having: Vec<Filter>,
    grouping_sets: Vec<Vec<usize>>,
) -> SqlPlan {
    match base_plan {
        SqlPlan::Aggregate {
            input,
            limit,
            grouping_sets: _,
            sort_keys,
            ..
        } => SqlPlan::Aggregate {
            input,
            group_by,
            group_by_aliases,
            output_order,
            aggregates,
            having,
            limit,
            grouping_sets: Some(grouping_sets),
            sort_keys,
        },
        other => {
            // Engine returned something other than Aggregate (e.g. TimeseriesIngest).
            // Wrap it so grouping sets are not silently dropped.
            SqlPlan::Aggregate {
                input: Box::new(other),
                group_by,
                group_by_aliases,
                output_order,
                aggregates,
                having,
                limit: 10000,
                grouping_sets: Some(grouping_sets),
                sort_keys: Vec::new(),
            }
        }
    }
}

/// Extract timeseries-specific parameters from GROUP BY (bucket interval, group columns).
///
/// Returns `(Some(interval_ms), group_columns)` if a `time_bucket()` call is found,
/// or `(None, empty)` otherwise. Non-timeseries engines ignore these values.
fn extract_timeseries_params(
    raw_group_by: &GroupByExpr,
    select_items: &[ast::SelectItem],
    functions: &FunctionRegistry,
) -> Result<(Option<i64>, Vec<String>)> {
    let mut bucket_interval_ms: Option<i64> = None;
    let mut group_columns = Vec::new();

    if let GroupByExpr::Expressions(exprs, _) = raw_group_by {
        for expr in exprs {
            let resolved = resolve_group_by_expr(expr, select_items);
            let check_expr = resolved.unwrap_or(expr);

            if let Some(interval) = try_extract_time_bucket(check_expr, functions)? {
                bucket_interval_ms = Some(interval);
                continue;
            }

            if let ast::Expr::Identifier(ident) = expr {
                group_columns.push(normalize_ident(ident));
            }
        }
    }

    Ok((bucket_interval_ms, group_columns))
}

/// Check if an expression is a time_bucket() call and extract the interval.
fn try_extract_time_bucket(expr: &ast::Expr, functions: &FunctionRegistry) -> Result<Option<i64>> {
    if let ast::Expr::Function(func) = expr {
        let name = func
            .name
            .0
            .iter()
            .map(|p| match p {
                ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
                _ => String::new(),
            })
            .collect::<Vec<_>>()
            .join(".");
        if functions.search_trigger(&name) == SearchTrigger::TimeBucket {
            return Ok(Some(extract_bucket_interval(func)?));
        }
    }
    Ok(None)
}

/// Resolve a GROUP BY expression that references a SELECT alias or ordinal.
///
/// `GROUP BY b` where `b` is an alias → returns the aliased expression.
/// `GROUP BY 1` → returns the 1st SELECT expression (0-indexed).
fn resolve_group_by_expr<'a>(
    expr: &ast::Expr,
    select_items: &'a [ast::SelectItem],
) -> Option<&'a ast::Expr> {
    match expr {
        ast::Expr::Identifier(ident) => {
            let alias_name = normalize_ident(ident);
            select_items.iter().find_map(|item| {
                if let ast::SelectItem::ExprWithAlias { expr, alias } = item
                    && normalize_ident(alias) == alias_name
                {
                    Some(expr)
                } else {
                    None
                }
            })
        }
        ast::Expr::Value(v) => {
            if let ast::Value::Number(n, _) = &v.value
                && let Ok(idx) = n.parse::<usize>()
                && idx >= 1
                && idx <= select_items.len()
            {
                match &select_items[idx - 1] {
                    ast::SelectItem::UnnamedExpr(e) => Some(e),
                    ast::SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract the bucket interval from a time_bucket() call.
///
/// Handles both argument orders:
/// - `time_bucket('1 hour', timestamp)` — interval first
/// - `time_bucket(timestamp, '1 hour')` — timestamp first
/// - `time_bucket(3600, timestamp)` — integer seconds
fn extract_bucket_interval(func: &ast::Function) -> Result<i64> {
    let args = match &func.args {
        ast::FunctionArguments::List(args) => &args.args,
        _ => return Ok(0),
    };
    // Try each argument position for the interval literal.
    for arg in args {
        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Value(v))) = arg {
            match &v.value {
                ast::Value::SingleQuotedString(s) => {
                    let ms = parse_interval_to_ms(s);
                    if ms > 0 {
                        return Ok(ms);
                    }
                }
                ast::Value::Number(n, _) => {
                    if let Ok(secs) = n.parse::<i64>()
                        && secs > 0
                    {
                        return Ok(secs * 1000);
                    }
                }
                _ => {}
            }
        }
    }
    Ok(0)
}

/// Parse an interval string to milliseconds.
///
/// Delegates to the canonical `nodedb_types::kv_parsing::parse_interval_to_ms`.
fn parse_interval_to_ms(s: &str) -> i64 {
    nodedb_types::kv_parsing::parse_interval_to_ms(s)
        .map(|ms| ms as i64)
        .unwrap_or(0)
}

/// Scan the SELECT projection for `GROUPING(col)` calls and return synthetic
/// `AggregateExpr` entries so the executor can compute the per-set bitmask value.
///
/// For `GROUPING(col)`, the canonical index of `col` in `canonical_keys` is
/// encoded into the `field` of the resulting `AggregateExpr` (as a decimal string).
/// The executor reads this index and checks the grouping-set bitmask at run time.
fn extract_grouping_calls(
    items: &[ast::SelectItem],
    canonical_keys: &[SqlExpr],
    scope: &TableScope,
) -> Result<Vec<AggregateExpr>> {
    let mut out = Vec::new();
    for item in items {
        let (expr, alias): (&ast::Expr, String) = match item {
            ast::SelectItem::UnnamedExpr(expr) => (expr, format!("{expr}")),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, normalize_ident(alias)),
            _ => continue,
        };
        collect_grouping_from_expr(expr, &alias, canonical_keys, &mut out, scope)?;
    }
    Ok(out)
}

/// Recursively collect `GROUPING(col)` calls from an expression.
fn collect_grouping_from_expr(
    expr: &ast::Expr,
    alias: &str,
    canonical_keys: &[SqlExpr],
    out: &mut Vec<AggregateExpr>,
    scope: &TableScope,
) -> Result<()> {
    match expr {
        ast::Expr::Function(f) => {
            let name = normalize_function_name(f);
            if name.eq_ignore_ascii_case("grouping") {
                // Extract the column argument(s).
                let args = function_args_exprs(f);
                for col_expr in &args {
                    let canonical_idx = crate::planner::grouping_sets::resolve_grouping_col(
                        col_expr,
                        canonical_keys,
                    )?;
                    // Encode index in the field name; alias is user-visible output name.
                    out.push(AggregateExpr {
                        function: "grouping".into(),
                        args: vec![convert_expr(col_expr, &ColumnScope::Relations(scope))?],
                        alias: alias.to_string(),
                        distinct: false,
                        grouping_col_index: Some(canonical_idx),
                    });
                }
            }
        }
        // Recurse into binary ops and other wrappers.
        ast::Expr::BinaryOp { left, right, .. } => {
            collect_grouping_from_expr(left, alias, canonical_keys, out, scope)?;
            collect_grouping_from_expr(right, alias, canonical_keys, out, scope)?;
        }
        _ => {}
    }
    Ok(())
}

/// Extract the positional expression arguments from a function call.
pub(super) fn function_args_exprs(f: &ast::Function) -> Vec<&ast::Expr> {
    match &f.args {
        ast::FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Return the simple lowercase name of a function (unqualified part only).
pub(super) fn normalize_function_name(f: &ast::Function) -> String {
    f.name
        .0
        .last()
        .map(|p| match p {
            ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .unwrap_or_default()
}

/// Extract aggregate expressions from SELECT projection.
///
/// `scope` gates each aggregate argument, so a column no relation declares
/// raises an undefined-column error instead of planning.
pub fn extract_aggregates_from_projection(
    items: &[ast::SelectItem],
    functions: &FunctionRegistry,
    scope: &TableScope,
) -> Result<Vec<AggregateExpr>> {
    let mut aggregates = Vec::new();
    for item in items {
        let (expr, alias): (&ast::Expr, String) = match item {
            // Lowercase the unparsed expression text so the resulting
            // alias (which becomes the JSON row key after the
            // `apply_user_aliases_to_rows` rename) matches the pgwire
            // lookup key built by `expr_column_names`, which also
            // lowercases. Without this match the row description
            // column `count(distinct user_id)` would not resolve to
            // the JSON value stored under `COUNT(DISTINCT user_id)`,
            // and the client would see NULL. The lowercase rule lives in
            // `unaliased_projection_alias` so no site can derive it
            // differently.
            ast::SelectItem::UnnamedExpr(expr) => (
                expr,
                crate::planner::ast_helpers::unaliased_projection_alias(expr),
            ),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, normalize_ident(alias)),
            _ => continue,
        };
        let mut extracted =
            crate::aggregate_walk::extract_aggregates(expr, &alias, functions, scope)?;
        aggregates.append(&mut extracted);
    }
    Ok(aggregates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_intervals() {
        assert_eq!(parse_interval_to_ms("1h"), 3_600_000);
        assert_eq!(parse_interval_to_ms("15m"), 900_000);
        assert_eq!(parse_interval_to_ms("30s"), 30_000);
        assert_eq!(parse_interval_to_ms("7d"), 604_800_000);
        // Word-form intervals.
        assert_eq!(parse_interval_to_ms("1 hour"), 3_600_000);
        assert_eq!(parse_interval_to_ms("2 hours"), 7_200_000);
        assert_eq!(parse_interval_to_ms("15 minutes"), 900_000);
        assert_eq!(parse_interval_to_ms("30 seconds"), 30_000);
        assert_eq!(parse_interval_to_ms("1 day"), 86_400_000);
        assert_eq!(parse_interval_to_ms("5 min"), 300_000);
    }

    /// Helper: parse a SQL SELECT and return the select body + projection.
    fn parse_select(sql: &str) -> ast::Select {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            ast::Statement::Query(q) => match *q.body {
                ast::SetExpr::Select(s) => *s,
                _ => panic!("expected SELECT"),
            },
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn resolve_group_by_alias_to_time_bucket() {
        let select = parse_select(
            "SELECT time_bucket('1 hour', timestamp) AS b, COUNT(*) FROM t GROUP BY b",
        );
        let functions = FunctionRegistry::new();

        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
            let resolved = resolve_group_by_expr(&exprs[0], &select.projection);
            assert!(resolved.is_some(), "alias 'b' should resolve");
            let interval = try_extract_time_bucket(resolved.unwrap(), &functions).unwrap();
            assert_eq!(interval, Some(3_600_000));
        } else {
            panic!("expected GROUP BY expressions");
        }
    }

    #[test]
    fn resolve_group_by_ordinal_to_time_bucket() {
        let select =
            parse_select("SELECT time_bucket('5 minutes', timestamp), COUNT(*) FROM t GROUP BY 1");
        let functions = FunctionRegistry::new();

        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
            let resolved = resolve_group_by_expr(&exprs[0], &select.projection);
            assert!(resolved.is_some(), "ordinal 1 should resolve");
            let interval = try_extract_time_bucket(resolved.unwrap(), &functions).unwrap();
            assert_eq!(interval, Some(300_000));
        } else {
            panic!("expected GROUP BY expressions");
        }
    }

    #[test]
    fn resolve_group_by_plain_column_not_time_bucket() {
        let select = parse_select("SELECT qtype, COUNT(*) FROM t GROUP BY qtype");
        let functions = FunctionRegistry::new();

        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
            let resolved = resolve_group_by_expr(&exprs[0], &select.projection);
            // 'qtype' is not an alias in SELECT, so resolve returns None.
            assert!(resolved.is_none());
            let interval = try_extract_time_bucket(&exprs[0], &functions).unwrap();
            assert_eq!(interval, None);
        } else {
            panic!("expected GROUP BY expressions");
        }
    }
}
