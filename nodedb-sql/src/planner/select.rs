//! SELECT query planning: FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT.
//!
//! This is the main entry point for SELECT statement conversion. It detects
//! search patterns (vector, text, hybrid, spatial) directly from the AST
//! instead of reverse-engineering an optimizer's output.

use sqlparser::ast::{self, Query, Select, SetExpr};

use crate::error::{Result, SqlError};
use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::parser::normalize::normalize_ident;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// Plan a SELECT query.
pub fn plan_query(
    query: &Query,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> Result<SqlPlan> {
    // Handle CTEs (WITH clause).
    if let Some(with) = &query.with
        && with.recursive
    {
        return super::cte::plan_recursive_cte(query, catalog, functions);
    }
    // Non-recursive CTEs: plan as subquery (inline expansion).
    // For now, fall through to main SELECT — CTE references resolve via catalog.

    // Handle UNION.
    match &*query.body {
        SetExpr::Select(select) => {
            let mut plan = plan_select(select, catalog, functions)?;
            if let Some(order_by) = &query.order_by {
                plan = apply_order_by(&plan, order_by, functions)?;
            }
            plan = apply_limit(plan, &query.limit_clause);
            Ok(plan)
        }
        SetExpr::SetOperation {
            op,
            left,
            right,
            set_quantifier,
        } => super::union::plan_set_operation(op, left, right, set_quantifier, catalog, functions),
        _ => Err(SqlError::Unsupported {
            detail: format!("query body type: {}", query.body),
        }),
    }
}

/// Plan a single SELECT statement (no UNION, no CTE wrapper).
fn plan_select(
    select: &Select,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> Result<SqlPlan> {
    // 1. Resolve FROM tables.
    let scope = TableScope::resolve_from(catalog, &select.from)?;

    // 2. Check for JOINs.
    if let Some(plan) = try_plan_join(select, &scope, catalog, functions)? {
        return Ok(plan);
    }

    // 3. Single-table query.
    let table = scope.single_table().ok_or_else(|| SqlError::Unsupported {
        detail: "multi-table FROM without JOIN".into(),
    })?;

    // 4. Convert WHERE filters.
    let filters = match &select.selection {
        Some(expr) => {
            // Check for search-triggering functions in WHERE.
            if let Some(plan) = try_extract_where_search(expr, table, functions)? {
                return Ok(plan);
            }
            convert_where_to_filters(expr)?
        }
        None => Vec::new(),
    };

    // 5. Check for GROUP BY / aggregation.
    if has_aggregation(select, functions) {
        return super::aggregate::plan_aggregate(select, table, &filters, &scope, functions);
    }

    // 6. Convert projection.
    let projection = convert_projection(&select.projection)?;

    // 7. Convert window functions (SELECT with OVER).
    let window_functions = super::window::extract_window_functions(&select.projection, functions)?;

    // 8. Build base scan plan.
    Ok(SqlPlan::Scan {
        collection: table.name.clone(),
        engine: table.info.engine,
        filters,
        projection,
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: select.distinct.is_some(),
        window_functions,
    })
}

/// Check if a SELECT has aggregation (GROUP BY or aggregate functions in projection).
fn has_aggregation(select: &Select, functions: &FunctionRegistry) -> bool {
    let group_by_non_empty = match &select.group_by {
        ast::GroupByExpr::All(_) => true,
        ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
    };
    if group_by_non_empty {
        return true;
    }
    for item in &select.projection {
        if let ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } =
            item
            && expr_contains_aggregate(expr, functions)
        {
            return true;
        }
    }
    false
}

/// Check if an expression contains an aggregate function call.
fn expr_contains_aggregate(expr: &ast::Expr, functions: &FunctionRegistry) -> bool {
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
                return true;
            }
            // Check args recursively.
            if let ast::FunctionArguments::List(args) = &func.args {
                for arg in &args.args {
                    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = arg
                        && expr_contains_aggregate(e, functions)
                    {
                        return true;
                    }
                }
            }
            false
        }
        ast::Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left, functions) || expr_contains_aggregate(right, functions)
        }
        ast::Expr::Nested(inner) => expr_contains_aggregate(inner, functions),
        _ => false,
    }
}

/// Try to detect search-triggering patterns in WHERE clause.
fn try_extract_where_search(
    expr: &ast::Expr,
    table: &crate::resolver::columns::ResolvedTable,
    functions: &FunctionRegistry,
) -> Result<Option<SqlPlan>> {
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
            match functions.search_trigger(&name) {
                SearchTrigger::TextMatch => {
                    let args = extract_func_args(func)?;
                    if args.len() >= 2 {
                        let query_text = extract_string_literal(&args[1])?;
                        return Ok(Some(SqlPlan::TextSearch {
                            collection: table.name.clone(),
                            query: query_text,
                            top_k: 1000,
                            fuzzy: true,
                            filters: Vec::new(),
                        }));
                    }
                }
                SearchTrigger::SpatialDWithin
                | SearchTrigger::SpatialContains
                | SearchTrigger::SpatialIntersects
                | SearchTrigger::SpatialWithin => {
                    return plan_spatial_from_where(&name, func, table);
                }
                _ => {}
            }
        }
        // AND: check left and right for search triggers, combine non-search as filters.
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            if let Some(plan) = try_extract_where_search(left, table, functions)? {
                return Ok(Some(plan));
            }
            if let Some(plan) = try_extract_where_search(right, table, functions)? {
                return Ok(Some(plan));
            }
        }
        _ => {}
    }
    Ok(None)
}

fn plan_spatial_from_where(
    name: &str,
    func: &ast::Function,
    table: &crate::resolver::columns::ResolvedTable,
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
    let geom_str = extract_string_literal(geom_arg).unwrap_or_default();
    let distance = if args.len() >= 3 {
        extract_float(&args[2]).unwrap_or(0.0)
    } else {
        0.0
    };
    Ok(Some(SqlPlan::SpatialScan {
        collection: table.name.clone(),
        field,
        predicate,
        query_geometry: geom_str.into_bytes(),
        distance_meters: distance,
        attribute_filters: Vec::new(),
        limit: 1000,
        projection: Vec::new(),
    }))
}

/// Apply ORDER BY, detecting search-triggering sort expressions.
fn apply_order_by(
    plan: &SqlPlan,
    order_by: &ast::OrderBy,
    functions: &FunctionRegistry,
) -> Result<SqlPlan> {
    let exprs = match &order_by.kind {
        ast::OrderByKind::Expressions(exprs) => exprs,
        ast::OrderByKind::All(_) => return Ok(plan.clone()),
    };

    if exprs.is_empty() {
        return Ok(plan.clone());
    }

    // Check first ORDER BY expression for search triggers.
    let first = &exprs[0];
    if let Some(search_plan) = try_extract_sort_search(&first.expr, plan, functions)? {
        return Ok(search_plan);
    }

    // Normal sort keys.
    let sort_keys: Vec<SortKey> = exprs
        .iter()
        .map(|o| {
            Ok(SortKey {
                expr: convert_expr(&o.expr)?,
                ascending: o.options.asc.unwrap_or(true),
                nulls_first: o.options.nulls_first.unwrap_or(false),
            })
        })
        .collect::<Result<_>>()?;

    match plan {
        SqlPlan::Scan {
            collection,
            engine,
            filters,
            projection,
            limit,
            offset,
            distinct,
            window_functions,
            ..
        } => Ok(SqlPlan::Scan {
            collection: collection.clone(),
            engine: *engine,
            filters: filters.clone(),
            projection: projection.clone(),
            sort_keys,
            limit: *limit,
            offset: *offset,
            distinct: *distinct,
            window_functions: window_functions.clone(),
        }),
        _ => Ok(plan.clone()),
    }
}

/// Try to detect search-triggering ORDER BY expressions.
fn try_extract_sort_search(
    expr: &ast::Expr,
    plan: &SqlPlan,
    functions: &FunctionRegistry,
) -> Result<Option<SqlPlan>> {
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
        let collection = match plan {
            SqlPlan::Scan { collection, .. } => collection.clone(),
            _ => return Ok(None),
        };
        let args = extract_func_args(func)?;

        match functions.search_trigger(&name) {
            SearchTrigger::VectorSearch => {
                if args.len() < 2 {
                    return Ok(None);
                }
                let field = extract_column_name(&args[0])?;
                let vector = extract_float_array(&args[1])?;
                let limit = match plan {
                    SqlPlan::Scan { limit, .. } => limit.unwrap_or(10),
                    _ => 10,
                };
                return Ok(Some(SqlPlan::VectorSearch {
                    collection,
                    field,
                    query_vector: vector,
                    top_k: limit,
                    ef_search: limit * 2,
                    filters: match plan {
                        SqlPlan::Scan { filters, .. } => filters.clone(),
                        _ => Vec::new(),
                    },
                }));
            }
            SearchTrigger::TextSearch => {
                if args.len() >= 2 {
                    let query_text = extract_string_literal(&args[1])?;
                    let limit = match plan {
                        SqlPlan::Scan { limit, .. } => limit.unwrap_or(10),
                        _ => 10,
                    };
                    return Ok(Some(SqlPlan::TextSearch {
                        collection,
                        query: query_text,
                        top_k: limit,
                        fuzzy: true,
                        filters: match plan {
                            SqlPlan::Scan { filters, .. } => filters.clone(),
                            _ => Vec::new(),
                        },
                    }));
                }
            }
            SearchTrigger::HybridSearch => {
                return plan_hybrid_from_sort(&args, &collection, plan, functions);
            }
            _ => {}
        }
    }
    Ok(None)
}

fn plan_hybrid_from_sort(
    args: &[ast::Expr],
    collection: &str,
    plan: &SqlPlan,
    _functions: &FunctionRegistry,
) -> Result<Option<SqlPlan>> {
    // rrf_score(vector_distance(...), bm25_score(...), k1, k2)
    if args.len() < 2 {
        return Ok(None);
    }
    let vector = match &args[0] {
        ast::Expr::Function(f) => {
            let inner_args = extract_func_args(f)?;
            if inner_args.len() >= 2 {
                extract_float_array(&inner_args[1]).unwrap_or_default()
            } else {
                Vec::new()
            }
        }
        _ => Vec::new(),
    };
    let text = match &args[1] {
        ast::Expr::Function(f) => {
            let inner_args = extract_func_args(f)?;
            if inner_args.len() >= 2 {
                extract_string_literal(&inner_args[1]).unwrap_or_default()
            } else {
                String::new()
            }
        }
        _ => String::new(),
    };
    let k1 = args
        .get(2)
        .and_then(|e| extract_float(e).ok())
        .unwrap_or(60.0);
    let k2 = args
        .get(3)
        .and_then(|e| extract_float(e).ok())
        .unwrap_or(60.0);
    let limit = match plan {
        SqlPlan::Scan { limit, .. } => limit.unwrap_or(10),
        _ => 10,
    };
    let vector_weight = k2 as f32 / (k1 as f32 + k2 as f32);

    Ok(Some(SqlPlan::HybridSearch {
        collection: collection.into(),
        query_vector: vector,
        query_text: text,
        top_k: limit,
        ef_search: limit * 2,
        vector_weight,
        fuzzy: true,
    }))
}

/// Apply LIMIT and OFFSET to a plan.
fn apply_limit(mut plan: SqlPlan, limit_clause: &Option<ast::LimitClause>) -> SqlPlan {
    let (limit_val, offset_val) = match limit_clause {
        None => (None, 0usize),
        Some(ast::LimitClause::LimitOffset { limit, offset, .. }) => {
            let lv = limit.as_ref().and_then(|e| match e {
                ast::Expr::Value(v) => match &v.value {
                    ast::Value::Number(n, _) => n.parse::<usize>().ok(),
                    _ => None,
                },
                _ => None,
            });
            let ov = offset
                .as_ref()
                .and_then(|o| match &o.value {
                    ast::Expr::Value(v) => match &v.value {
                        ast::Value::Number(n, _) => n.parse::<usize>().ok(),
                        _ => None,
                    },
                    _ => None,
                })
                .unwrap_or(0);
            (lv, ov)
        }
        Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => {
            let lv = match limit {
                ast::Expr::Value(v) => match &v.value {
                    ast::Value::Number(n, _) => n.parse::<usize>().ok(),
                    _ => None,
                },
                _ => None,
            };
            let ov = match offset {
                ast::Expr::Value(v) => match &v.value {
                    ast::Value::Number(n, _) => n.parse::<usize>().ok(),
                    _ => None,
                },
                _ => None,
            }
            .unwrap_or(0);
            (lv, ov)
        }
    };

    match plan {
        SqlPlan::Scan {
            ref mut limit,
            ref mut offset,
            ..
        } => {
            *limit = limit_val;
            *offset = offset_val;
        }
        SqlPlan::Aggregate {
            limit: ref mut l, ..
        } => {
            if let Some(lv) = limit_val {
                *l = lv;
            }
        }
        _ => {}
    }
    plan
}

// ── Helpers ──

/// Convert SELECT projection items.
pub fn convert_projection(items: &[ast::SelectItem]) -> Result<Vec<Projection>> {
    let mut result = Vec::new();
    for item in items {
        match item {
            ast::SelectItem::UnnamedExpr(expr) => {
                let sql_expr = convert_expr(expr)?;
                match &sql_expr {
                    SqlExpr::Column { name, .. } => {
                        result.push(Projection::Column(name.clone()));
                    }
                    SqlExpr::Wildcard => {
                        result.push(Projection::Star);
                    }
                    _ => {
                        result.push(Projection::Computed {
                            expr: sql_expr,
                            alias: format!("{expr}"),
                        });
                    }
                }
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                let sql_expr = convert_expr(expr)?;
                result.push(Projection::Computed {
                    expr: sql_expr,
                    alias: normalize_ident(alias),
                });
            }
            ast::SelectItem::Wildcard(_) => {
                result.push(Projection::Star);
            }
            ast::SelectItem::QualifiedWildcard(kind, _) => {
                let table_name = match kind {
                    ast::SelectItemQualifiedWildcardKind::ObjectName(name) => {
                        crate::parser::normalize::normalize_object_name(name)
                    }
                    _ => String::new(),
                };
                result.push(Projection::QualifiedStar(table_name));
            }
        }
    }
    Ok(result)
}

/// Convert a WHERE expression into a list of Filter.
pub fn convert_where_to_filters(expr: &ast::Expr) -> Result<Vec<Filter>> {
    let sql_expr = convert_expr(expr)?;
    Ok(vec![Filter {
        expr: FilterExpr::Expr(sql_expr),
    }])
}

fn extract_func_args(func: &ast::Function) -> Result<Vec<ast::Expr>> {
    match &func.args {
        ast::FunctionArguments::List(args) => Ok(args
            .args
            .iter()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e.clone()),
                _ => None,
            })
            .collect()),
        _ => Ok(Vec::new()),
    }
}

fn extract_column_name(expr: &ast::Expr) -> Result<String> {
    match expr {
        ast::Expr::Identifier(ident) => Ok(normalize_ident(ident)),
        ast::Expr::CompoundIdentifier(parts) => Ok(parts
            .iter()
            .map(normalize_ident)
            .collect::<Vec<_>>()
            .join(".")),
        _ => Err(SqlError::Unsupported {
            detail: format!("expected column name, got: {expr}"),
        }),
    }
}

fn extract_string_literal(expr: &ast::Expr) -> Result<String> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => Ok(s.clone()),
            _ => Err(SqlError::Unsupported {
                detail: format!("expected string literal, got: {expr}"),
            }),
        },
        _ => Err(SqlError::Unsupported {
            detail: format!("expected string literal, got: {expr}"),
        }),
    }
}

fn extract_float(expr: &ast::Expr) -> Result<f64> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.parse::<f64>().map_err(|_| SqlError::TypeMismatch {
                detail: format!("expected number: {n}"),
            }),
            _ => Err(SqlError::TypeMismatch {
                detail: format!("expected number, got: {expr}"),
            }),
        },
        _ => Err(SqlError::TypeMismatch {
            detail: format!("expected number, got: {expr}"),
        }),
    }
}

/// Extract a float array from ARRAY[...] or make_array(...) expression.
fn extract_float_array(expr: &ast::Expr) -> Result<Vec<f32>> {
    match expr {
        ast::Expr::Array(ast::Array { elem, .. }) => elem
            .iter()
            .map(|e| extract_float(e).map(|f| f as f32))
            .collect(),
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
            if name == "make_array" || name == "array" {
                let args = extract_func_args(func)?;
                args.iter()
                    .map(|e| extract_float(e).map(|f| f as f32))
                    .collect()
            } else {
                Err(SqlError::Unsupported {
                    detail: format!("expected array, got function: {name}"),
                })
            }
        }
        _ => Err(SqlError::Unsupported {
            detail: format!("expected array literal, got: {expr}"),
        }),
    }
}

/// Check if a SELECT has the DISTINCT keyword.
fn try_plan_join(
    select: &Select,
    scope: &TableScope,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
) -> Result<Option<SqlPlan>> {
    if select.from.len() != 1 {
        return Ok(None);
    }
    let from = &select.from[0];
    if from.joins.is_empty() {
        return Ok(None);
    }
    super::join::plan_join_from_select(select, scope, catalog, functions)
}
