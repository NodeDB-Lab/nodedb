//! GROUP BY and aggregate planning.

use sqlparser::ast::{self, GroupByExpr};

use crate::error::Result;
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::parser::normalize::normalize_ident;
use crate::resolver::columns::ResolvedTable;
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// Plan an aggregate query (GROUP BY + aggregate functions).
pub fn plan_aggregate(
    select: &ast::Select,
    table: &ResolvedTable,
    filters: &[Filter],
    _scope: &crate::resolver::columns::TableScope,
    functions: &FunctionRegistry,
) -> Result<SqlPlan> {
    let group_by_exprs = convert_group_by(&select.group_by)?;
    let aggregates = extract_aggregates_from_projection(&select.projection, functions)?;
    let having = match &select.having {
        Some(expr) => super::select::convert_where_to_filters(expr)?,
        None => Vec::new(),
    };

    // Check for timeseries routing: GROUP BY time_bucket(...) on timeseries collection.
    if table.info.engine == EngineType::Timeseries {
        return plan_timeseries_aggregate(
            table,
            &group_by_exprs,
            &aggregates,
            filters,
            &select.group_by,
            &select.projection,
            functions,
        );
    }

    let base_scan = SqlPlan::Scan {
        collection: table.name.clone(),
        alias: table.alias.clone(),
        engine: table.info.engine,
        filters: filters.to_vec(),
        projection: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
        window_functions: Vec::new(),
    };

    Ok(SqlPlan::Aggregate {
        input: Box::new(base_scan),
        group_by: group_by_exprs,
        aggregates,
        having,
        limit: 10000,
    })
}

/// Plan a timeseries aggregate with optional time_bucket.
fn plan_timeseries_aggregate(
    table: &ResolvedTable,
    _group_by: &[SqlExpr],
    aggregates: &[AggregateExpr],
    filters: &[Filter],
    raw_group_by: &GroupByExpr,
    select_items: &[ast::SelectItem],
    functions: &FunctionRegistry,
) -> Result<SqlPlan> {
    let mut bucket_interval_ms: i64 = 0;
    let mut group_columns = Vec::new();

    // Check for time_bucket in GROUP BY.
    if let GroupByExpr::Expressions(exprs, _) = raw_group_by {
        for expr in exprs {
            // Resolve the expression: GROUP BY alias or GROUP BY ordinal (1-based)
            // should resolve to the corresponding SELECT item expression.
            let resolved = resolve_group_by_expr(expr, select_items);
            let check_expr = resolved.unwrap_or(expr);

            if let Some(interval) = try_extract_time_bucket(check_expr, functions)? {
                bucket_interval_ms = interval;
                continue;
            }

            // Non-time_bucket GROUP BY columns.
            if let ast::Expr::Identifier(ident) = expr {
                group_columns.push(normalize_ident(ident));
            }
        }
    }

    // Extract time range from filters.
    let time_range = extract_time_range(filters);

    Ok(SqlPlan::TimeseriesScan {
        collection: table.name.clone(),
        time_range,
        bucket_interval_ms,
        group_by: group_columns,
        aggregates: aggregates.to_vec(),
        filters: filters.to_vec(),
        projection: Vec::new(),
        gap_fill: String::new(),
        limit: 10000,
        tiered: table.info.has_auto_tier,
    })
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
            for item in select_items {
                if let ast::SelectItem::ExprWithAlias { expr, alias } = item {
                    if normalize_ident(alias) == alias_name {
                        return Some(expr);
                    }
                }
            }
            None
        }
        ast::Expr::Value(v) => {
            if let ast::Value::Number(n, _) = &v.value {
                if let Ok(idx) = n.parse::<usize>() {
                    if idx >= 1 && idx <= select_items.len() {
                        let item = &select_items[idx - 1];
                        match item {
                            ast::SelectItem::UnnamedExpr(e) => return Some(e),
                            ast::SelectItem::ExprWithAlias { expr, .. } => return Some(expr),
                            _ => {}
                        }
                    }
                }
            }
            None
        }
        _ => None,
    }
}

/// Extract the bucket interval from a time_bucket() call.
fn extract_bucket_interval(func: &ast::Function) -> Result<i64> {
    let args = match &func.args {
        ast::FunctionArguments::List(args) => &args.args,
        _ => return Ok(0),
    };
    if args.is_empty() {
        return Ok(0);
    }
    let interval_str = match &args[0] {
        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Value(v))) => {
            match &v.value {
                ast::Value::SingleQuotedString(s) => s.clone(),
                _ => return Ok(0),
            }
        }
        _ => return Ok(0),
    };
    Ok(parse_interval_to_ms(&interval_str))
}

/// Parse an interval string to milliseconds.
///
/// Accepted forms: `"1h"`, `"15m"`, `"30s"`, `"1d"`, `"1 hour"`, `"15 minutes"`,
/// `"30 seconds"`, `"7 days"`. Plural and singular word forms both work.
fn parse_interval_to_ms(s: &str) -> i64 {
    let s = s.trim();
    if s.is_empty() {
        return 0;
    }

    // Split into numeric part and unit part (handles both "1h" and "1 hour").
    let num_end = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let num: i64 = s[..num_end].trim().parse().unwrap_or(0);
    let unit = s[num_end..].trim();

    match unit {
        "s" | "sec" | "second" | "seconds" => num * 1_000,
        "m" | "min" | "minute" | "minutes" => num * 60_000,
        "h" | "hr" | "hour" | "hours" => num * 3_600_000,
        "d" | "day" | "days" => num * 86_400_000,
        "" => {
            // Bare number — treat as seconds.
            num * 1_000
        }
        _ => 0,
    }
}

/// Extract time range from filters (timestamp >= X AND timestamp <= Y).
fn extract_time_range(_filters: &[Filter]) -> (i64, i64) {
    // Default: unbounded.
    (i64::MIN, i64::MAX)
}

/// Convert GROUP BY clause to SqlExpr list.
pub fn convert_group_by(group_by: &GroupByExpr) -> Result<Vec<SqlExpr>> {
    match group_by {
        GroupByExpr::All(_) => Ok(Vec::new()),
        GroupByExpr::Expressions(exprs, _) => exprs.iter().map(convert_expr).collect(),
    }
}

/// Extract aggregate expressions from SELECT projection.
pub fn extract_aggregates_from_projection(
    items: &[ast::SelectItem],
    functions: &FunctionRegistry,
) -> Result<Vec<AggregateExpr>> {
    let mut aggregates = Vec::new();
    for item in items {
        match item {
            ast::SelectItem::UnnamedExpr(expr) => {
                extract_aggregates_from_expr(expr, &format!("{expr}"), functions, &mut aggregates)?;
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                extract_aggregates_from_expr(
                    expr,
                    &normalize_ident(alias),
                    functions,
                    &mut aggregates,
                )?;
            }
            _ => {}
        }
    }
    Ok(aggregates)
}

fn extract_aggregates_from_expr(
    expr: &ast::Expr,
    alias: &str,
    functions: &FunctionRegistry,
    out: &mut Vec<AggregateExpr>,
) -> Result<()> {
    match expr {
        ast::Expr::Function(func) => {
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
            if functions.is_aggregate(&name) {
                let args = match &func.args {
                    ast::FunctionArguments::List(args) => args
                        .args
                        .iter()
                        .filter_map(|a| match a {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                convert_expr(e).ok()
                            }
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                                Some(SqlExpr::Wildcard)
                            }
                            _ => None,
                        })
                        .collect(),
                    _ => Vec::new(),
                };
                let distinct = matches!(&func.args,
                    ast::FunctionArguments::List(args) if matches!(args.duplicate_treatment, Some(ast::DuplicateTreatment::Distinct))
                );
                out.push(AggregateExpr {
                    function: name,
                    args,
                    alias: alias.into(),
                    distinct,
                });
            }
        }
        ast::Expr::BinaryOp { left, right, .. } => {
            extract_aggregates_from_expr(left, alias, functions, out)?;
            extract_aggregates_from_expr(right, alias, functions, out)?;
        }
        ast::Expr::Nested(inner) => {
            extract_aggregates_from_expr(inner, alias, functions, out)?;
        }
        _ => {}
    }
    Ok(())
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
