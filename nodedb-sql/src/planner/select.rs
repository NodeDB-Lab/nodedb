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
    // Non-recursive CTEs: plan each CTE subquery and the outer query.
    if let Some(with) = &query.with
        && !with.cte_tables.is_empty()
    {
        let inner_query = Query {
            with: None,
            body: query.body.clone(),
            order_by: query.order_by.clone(),
            limit_clause: query.limit_clause.clone(),
            fetch: query.fetch.clone(),
            locks: query.locks.clone(),
            for_clause: query.for_clause.clone(),
            settings: query.settings.clone(),
            format_clause: query.format_clause.clone(),
            pipe_operators: query.pipe_operators.clone(),
        };

        // Plan each CTE subquery.
        let mut definitions = Vec::new();
        let mut cte_names = Vec::new();
        for cte in &with.cte_tables {
            let name = normalize_ident(&cte.alias.name);
            let cte_plan = plan_query(&cte.query, catalog, functions)?;
            definitions.push((name.clone(), cte_plan));
            cte_names.push(name);
        }

        // Build CTE-aware catalog so the outer query can reference CTE names.
        let cte_catalog = CteCatalog {
            inner: catalog,
            cte_names,
        };
        let outer = plan_query(&inner_query, &cte_catalog, functions)?;

        return Ok(SqlPlan::Cte {
            definitions,
            outer: Box::new(outer),
        });
    }

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

    // 2. Handle constant queries (no FROM clause): SELECT 1, SELECT 'hello', etc.
    if select.from.is_empty() {
        let projection = convert_projection(&select.projection)?;
        let mut columns = Vec::new();
        let mut values = Vec::new();
        for (i, proj) in projection.iter().enumerate() {
            match proj {
                Projection::Computed { expr, alias } => {
                    columns.push(alias.clone());
                    values.push(eval_constant_expr(expr, functions));
                }
                Projection::Column(name) => {
                    columns.push(name.clone());
                    values.push(SqlValue::Null);
                }
                _ => {
                    columns.push(format!("col{i}"));
                    values.push(SqlValue::Null);
                }
            }
        }
        return Ok(SqlPlan::ConstantResult { columns, values });
    }

    // 3. Check for JOINs.
    if let Some(plan) = try_plan_join(select, &scope, catalog, functions)? {
        return Ok(plan);
    }

    // 4. Single-table query.
    let table = scope.single_table().ok_or_else(|| SqlError::Unsupported {
        detail: "multi-table FROM without JOIN".into(),
    })?;

    // 4. Extract subqueries from WHERE and rewrite as semi/anti joins.
    let (subquery_joins, effective_where) = if let Some(expr) = &select.selection {
        let extraction = super::subquery::extract_subqueries(expr, catalog, functions)?;
        (extraction.joins, extraction.remaining_where)
    } else {
        (Vec::new(), None)
    };

    // 5. Convert remaining WHERE filters.
    let filters = match &effective_where {
        Some(expr) => {
            // Check for search-triggering functions in WHERE.
            if let Some(plan) = try_extract_where_search(expr, table, functions)? {
                return Ok(plan);
            }
            convert_where_to_filters(expr)?
        }
        None => Vec::new(),
    };

    // 6. Check for GROUP BY / aggregation.
    if has_aggregation(select, functions) {
        let mut plan =
            super::aggregate::plan_aggregate(select, table, &filters, &scope, functions)?;

        // Semi/anti subquery joins belong below the aggregate so they filter
        // the input rows before grouping. Scalar subqueries remain above the
        // aggregate because their column-vs-column comparison is evaluated
        // after the cross join materializes the scalar result row.
        if let SqlPlan::Aggregate { input, .. } = &mut plan {
            let mut base_input = std::mem::replace(
                input,
                Box::new(SqlPlan::ConstantResult {
                    columns: Vec::new(),
                    values: Vec::new(),
                }),
            );
            for sq in subquery_joins
                .iter()
                .filter(|sq| sq.join_type != JoinType::Cross)
            {
                base_input = Box::new(SqlPlan::Join {
                    left: base_input,
                    right: Box::new(sq.inner_plan.clone()),
                    on: vec![(sq.outer_column.clone(), sq.inner_column.clone())],
                    join_type: sq.join_type,
                    condition: None,
                    limit: 10000,
                    projection: Vec::new(),
                    filters: Vec::new(),
                });
            }
            *input = base_input;
        }

        for sq in subquery_joins
            .into_iter()
            .filter(|sq| sq.join_type == JoinType::Cross)
        {
            plan = SqlPlan::Join {
                left: Box::new(plan),
                right: Box::new(sq.inner_plan),
                on: vec![(sq.outer_column, sq.inner_column)],
                join_type: sq.join_type,
                condition: None,
                limit: 10000,
                projection: Vec::new(),
                filters: Vec::new(),
            };
        }
        return Ok(plan);
    }

    // 7. Convert projection.
    let projection = convert_projection(&select.projection)?;

    // 8. Convert window functions (SELECT with OVER).
    let window_functions = super::window::extract_window_functions(&select.projection, functions)?;

    // 9. Build base scan plan.
    let scan_projection = if subquery_joins.is_empty() {
        projection.clone()
    } else {
        Vec::new()
    };

    let rules = crate::engine_rules::resolve_engine_rules(table.info.engine);
    let mut plan = rules.plan_scan(crate::engine_rules::ScanParams {
        collection: table.name.clone(),
        alias: table.alias.clone(),
        filters,
        projection: scan_projection,
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: select.distinct.is_some(),
        window_functions,
    })?;

    // 10. Wrap with subquery joins (semi/anti/cross) if any.
    for sq in subquery_joins {
        // For cross-joins (scalar subqueries), move column-referencing filters
        // from the base scan to the join's post-filters. The filter compares
        // a field from the base scan with a field from the subquery result,
        // so it can only be evaluated after the join merges both sides.
        let join_filters = if sq.join_type == JoinType::Cross {
            if let SqlPlan::Scan {
                ref mut filters, ..
            } = plan
            {
                // Move filters that reference the scalar result column to the join.
                let mut moved = Vec::new();
                filters.retain(|f| {
                    if has_column_ref_filter(&f.expr) {
                        moved.push(f.clone());
                        false
                    } else {
                        true
                    }
                });
                moved
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        plan = SqlPlan::Join {
            left: Box::new(plan),
            right: Box::new(sq.inner_plan),
            on: vec![(sq.outer_column, sq.inner_column)],
            join_type: sq.join_type,
            condition: None,
            limit: 10000,
            projection: Vec::new(),
            filters: join_filters,
        };
    }

    if let SqlPlan::Join {
        projection: ref mut join_projection,
        ..
    } = plan
    {
        *join_projection = projection;
    }

    Ok(plan)
}

/// Check if a filter expression contains a column-vs-column comparison
/// (from scalar subquery rewriting). These filters must be evaluated
/// post-join, not pre-join, since one column comes from the subquery result.
fn has_column_ref_filter(expr: &FilterExpr) -> bool {
    match expr {
        FilterExpr::Expr(sql_expr) => has_column_comparison(sql_expr),
        FilterExpr::And(filters) => filters.iter().any(|f| has_column_ref_filter(&f.expr)),
        FilterExpr::Or(filters) => filters.iter().any(|f| has_column_ref_filter(&f.expr)),
        _ => false,
    }
}

fn has_column_comparison(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::BinaryOp { left, right, .. } => {
            let left_is_col = matches!(left.as_ref(), SqlExpr::Column { .. });
            let right_is_col = matches!(right.as_ref(), SqlExpr::Column { .. });
            if left_is_col && right_is_col {
                return true;
            }
            has_column_comparison(left) || has_column_comparison(right)
        }
        _ => false,
    }
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
    let geom_str = extract_geometry_arg(geom_arg)?;
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
            alias,
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
            alias: alias.clone(),
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
                    SqlExpr::Column { table, name } => {
                        result.push(Projection::Column(qualified_name(table.as_deref(), name)));
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

/// Build a qualified column reference (`table.name` or just `name`).
pub fn qualified_name(table: Option<&str>, name: &str) -> String {
    table.map_or_else(|| name.to_string(), |table| format!("{table}.{name}"))
}

/// Convert a WHERE expression into a list of Filter.
pub fn convert_where_to_filters(expr: &ast::Expr) -> Result<Vec<Filter>> {
    let sql_expr = convert_expr(expr)?;
    Ok(vec![Filter {
        expr: FilterExpr::Expr(sql_expr),
    }])
}

pub(crate) fn extract_func_args(func: &ast::Function) -> Result<Vec<ast::Expr>> {
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

/// Evaluate a constant SqlExpr to a SqlValue. Delegates to the shared
/// `const_fold::fold_constant` helper so that zero-arg scalar functions
/// like `now()` and `current_timestamp` go through the same evaluator
/// as the runtime expression path.
fn eval_constant_expr(expr: &SqlExpr, functions: &FunctionRegistry) -> SqlValue {
    super::const_fold::fold_constant(expr, functions).unwrap_or(SqlValue::Null)
}

/// Extract a geometry argument: handles ST_Point(lon, lat), ST_GeomFromGeoJSON('...'),
/// or a raw string literal containing GeoJSON.
fn extract_geometry_arg(expr: &ast::Expr) -> Result<String> {
    match expr {
        // ST_Point(lon, lat) → GeoJSON Point
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
            let args = extract_func_args(func)?;
            match name.as_str() {
                "st_point" if args.len() >= 2 => {
                    let lon = extract_float(&args[0])?;
                    let lat = extract_float(&args[1])?;
                    Ok(format!(r#"{{"type":"Point","coordinates":[{lon},{lat}]}}"#))
                }
                "st_geomfromgeojson" if !args.is_empty() => extract_string_literal(&args[0]),
                _ => Ok(format!("{expr}")),
            }
        }
        // Raw string literal: assumed to be GeoJSON.
        _ => extract_string_literal(expr).or_else(|_| Ok(format!("{expr}"))),
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

pub(crate) fn extract_string_literal(expr: &ast::Expr) -> Result<String> {
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

pub(crate) fn extract_float(expr: &ast::Expr) -> Result<f64> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.parse::<f64>().map_err(|_| SqlError::TypeMismatch {
                detail: format!("expected number: {n}"),
            }),
            _ => Err(SqlError::TypeMismatch {
                detail: format!("expected number, got: {expr}"),
            }),
        },
        // Handle negative numbers: -73.9855 is parsed as UnaryOp { Minus, 73.9855 }
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr: inner,
        } => extract_float(inner).map(|f| -f),
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

/// Catalog wrapper that resolves CTE names as schemaless document collections.
struct CteCatalog<'a> {
    inner: &'a dyn SqlCatalog,
    cte_names: Vec<String>,
}

impl SqlCatalog for CteCatalog<'_> {
    fn get_collection(
        &self,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        // Check CTE names first.
        if self.cte_names.iter().any(|n| n == name) {
            return Ok(Some(CollectionInfo {
                name: name.into(),
                engine: EngineType::DocumentSchemaless,
                columns: Vec::new(),
                primary_key: Some("id".into()),
                has_auto_tier: false,
            }));
        }
        self.inner.get_collection(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::registry::FunctionRegistry;
    use crate::parser::statement::parse_sql;
    use sqlparser::ast::Statement;

    struct TestCatalog;

    impl SqlCatalog for TestCatalog {
        fn get_collection(
            &self,
            name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            let info = match name {
                "products" => Some(CollectionInfo {
                    name: "products".into(),
                    engine: EngineType::DocumentSchemaless,
                    columns: Vec::new(),
                    primary_key: Some("id".into()),
                    has_auto_tier: false,
                }),
                "users" => Some(CollectionInfo {
                    name: "users".into(),
                    engine: EngineType::DocumentSchemaless,
                    columns: Vec::new(),
                    primary_key: Some("id".into()),
                    has_auto_tier: false,
                }),
                "orders" => Some(CollectionInfo {
                    name: "orders".into(),
                    engine: EngineType::DocumentSchemaless,
                    columns: Vec::new(),
                    primary_key: Some("id".into()),
                    has_auto_tier: false,
                }),
                "docs" => Some(CollectionInfo {
                    name: "docs".into(),
                    engine: EngineType::DocumentSchemaless,
                    columns: Vec::new(),
                    primary_key: Some("id".into()),
                    has_auto_tier: false,
                }),
                "tags" => Some(CollectionInfo {
                    name: "tags".into(),
                    engine: EngineType::DocumentSchemaless,
                    columns: Vec::new(),
                    primary_key: Some("id".into()),
                    has_auto_tier: false,
                }),
                "user_prefs" => Some(CollectionInfo {
                    name: "user_prefs".into(),
                    engine: EngineType::KeyValue,
                    columns: Vec::new(),
                    primary_key: Some("key".into()),
                    has_auto_tier: false,
                }),
                _ => None,
            };
            Ok(info)
        }
    }

    fn plan_select_sql(sql: &str) -> SqlPlan {
        let statements = parse_sql(sql).unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query statement");
        };
        plan_query(query, &TestCatalog, &FunctionRegistry::new()).unwrap()
    }

    #[test]
    fn aggregate_subquery_join_filters_input_before_aggregation() {
        let plan = plan_select_sql(
            "SELECT AVG(price) FROM products WHERE category IN (SELECT DISTINCT category FROM products WHERE qty > 100)",
        );

        let SqlPlan::Aggregate { input, .. } = plan else {
            panic!("expected aggregate plan");
        };

        let SqlPlan::Join {
            left,
            join_type,
            on,
            ..
        } = *input
        else {
            panic!("expected semi-join below aggregate");
        };

        assert_eq!(join_type, JoinType::Semi);
        assert_eq!(on, vec![("category".into(), "category".into())]);
        assert!(matches!(*left, SqlPlan::Scan { .. }));
    }

    #[test]
    fn scalar_subquery_defers_projection_until_after_join_filter() {
        let plan = plan_select_sql(
            "SELECT user_id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)",
        );

        let SqlPlan::Join {
            left,
            projection,
            filters,
            ..
        } = plan
        else {
            panic!("expected join plan");
        };

        let SqlPlan::Scan {
            projection: scan_projection,
            ..
        } = *left
        else {
            panic!("expected scan on join left");
        };

        assert!(scan_projection.is_empty(), "scan projected too early");
        assert_eq!(projection.len(), 1);
        match &projection[0] {
            Projection::Column(name) => assert_eq!(name, "user_id"),
            other => panic!("expected user_id projection, got {other:?}"),
        }
        assert!(
            !filters.is_empty(),
            "scalar comparison should stay post-join"
        );
    }

    #[test]
    fn chained_join_preserves_qualified_on_keys() {
        let plan = plan_select_sql(
            "SELECT d.name, t.tag, p.theme \
             FROM docs d \
             LEFT JOIN tags t ON d.id = t.doc_id \
             INNER JOIN user_prefs p ON d.id = p.key",
        );

        let SqlPlan::Join { left, on, .. } = plan else {
            panic!("expected outer join plan");
        };
        assert_eq!(on, vec![("d.id".into(), "p.key".into())]);

        let SqlPlan::Join { on: inner_on, .. } = *left else {
            panic!("expected nested left join");
        };
        assert_eq!(inner_on, vec![("d.id".into(), "t.doc_id".into())]);
    }
}
