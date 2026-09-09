// SPDX-License-Identifier: Apache-2.0

//! Single SELECT statement planning (no UNION, no CTE wrapper).

use sqlparser::ast::{self, Select};

use super::comma_lateral::try_plan_comma_lateral;
use super::derived_from::try_plan_derived_from;
use super::helpers::{convert_projection, convert_where_to_filters};
use super::query_tail::QueryTail;
use super::where_search::try_extract_where_search;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::planner::ast_helpers::strip_single_table_qualifiers;
use crate::resolver::columns::TableScope;
use crate::temporal::TemporalScope;
use crate::types::*;

/// A planned SELECT body and the column namespace it resolved against.
pub(in crate::planner::select) struct PlannedSelect {
    pub plan: SqlPlan,
    pub scope: TableScope,
}

/// Plan a single SELECT statement (no UNION, no CTE wrapper).
///
/// `tail` carries the enclosing query's ORDER BY / LIMIT so the base scan can
/// be built with them in place — see [`QueryTail`] for why the engine rules
/// need them before `plan_scan` runs.
pub(super) fn plan_select(
    select: &Select,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
    tail: &QueryTail<'_>,
) -> Result<PlannedSelect> {
    // 0. Intercept array table-valued functions before catalog resolution
    //    so a name like `ARRAY_SLICE` is not looked up as a collection.
    if let Some(plan) =
        crate::planner::array_fn::try_plan_array_table_fn(&select.from, catalog, temporal)?
    {
        // `resolve_from` synthesizes a relation from the array's dims and
        // attrs, so ORDER BY and the tail clauses resolve its columns.
        return Ok(PlannedSelect {
            plan,
            scope: TableScope::resolve_from(catalog, &select.from)?,
        });
    }

    // 0.5. Derived FROM subquery: `FROM (SELECT ...) AS t`.
    //
    // Plan the inner subquery first, then desugar into a synthetic CTE
    // so the outer SELECT — which may reference `t` like any other
    // relation — plans against a catalog that resolves the alias to a
    // schemaless source. Until this branch existed the resolver
    // dropped non-LATERAL derived factors silently, the scope ended
    // up empty, and the planner errored with "multi-table FROM
    // without JOIN".
    if let Some(planned) = try_plan_derived_from(select, catalog, functions, temporal, tail)? {
        return Ok(planned);
    }

    // 1. Resolve FROM tables.
    let scope = TableScope::resolve_from(catalog, &select.from)?;

    // 2. Handle constant queries (no FROM clause): SELECT 1, SELECT 'hello', etc.
    if select.from.is_empty() {
        // Intercept maintenance functions (ARRAY_FLUSH / ARRAY_COMPACT)
        // before falling through to constant evaluation.
        if let Some(plan) =
            crate::planner::array_fn::try_plan_array_maint_fn(&select.projection, catalog)?
        {
            return Ok(PlannedSelect { plan, scope });
        }
        let projection = convert_projection(&select.projection, &scope)?;
        let mut columns = Vec::new();
        let mut values = Vec::new();
        let mut volatile = false;
        for (i, proj) in projection.iter().enumerate() {
            match proj {
                Projection::Computed { expr, alias } => {
                    columns.push(alias.clone());
                    volatile |= crate::types::plan::expr_is_volatile(expr);
                    values.push(crate::planner::catalog_expr_fold::eval_catalog_constant(
                        expr, catalog, functions,
                    )?);
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
        return Ok(PlannedSelect {
            plan: SqlPlan::ConstantResult {
                columns,
                values,
                volatile,
            },
            scope,
        });
    }

    // 3. Check for JOINs (including LATERAL).
    if let Some(plan) = try_plan_join(select, &scope, catalog, functions, temporal)? {
        return Ok(PlannedSelect { plan, scope });
    }

    // 3b. Comma-LATERAL syntax: `FROM t, LATERAL (SELECT ...) x`.
    if let Some(plan) = try_plan_comma_lateral(select, &scope, catalog, temporal)? {
        return Ok(PlannedSelect { plan, scope });
    }

    // 4. Single-table query.
    // Cloned rather than borrowed: `scope` moves into the returned
    // `PlannedSelect` while this relation is still in use.
    let single = scope
        .single_table()
        .cloned()
        .ok_or_else(|| SqlError::Unsupported {
            detail: "multi-table FROM without JOIN".into(),
        })?;
    let table = &single;

    // For a single table the column qualifier (`t.` or its alias) is always
    // redundant, so strip it from the projection, WHERE, and GROUP BY here —
    // before it can become a literal `"t.col"` field-lookup string that
    // silently matches zero rows / projects an empty value. A qualifier that
    // matches neither the table name nor its alias becomes a typed error. This
    // is scoped to the single-table branch only: the JOIN path (handled above
    // by `try_plan_join`) deliberately keeps qualifiers for merged-document
    // evaluation and is never reached here.
    let valid_qualifiers: Vec<&str> = {
        let ref_name = table.ref_name();
        if ref_name == table.name {
            vec![table.name.as_str()]
        } else {
            vec![table.name.as_str(), ref_name]
        }
    };
    let normalized_select = strip_single_table_qualifiers(select, &valid_qualifiers)?;
    let select = &normalized_select;

    // 4. Extract subqueries from WHERE and rewrite as semi/anti joins.
    let (subquery_joins, effective_where) = if let Some(expr) = &select.selection {
        let extraction = crate::planner::subquery::extract_subqueries(
            expr, &scope, catalog, functions, temporal,
        )?;
        (extraction.joins, extraction.remaining_where)
    } else {
        (Vec::new(), None)
    };

    // 5. Convert remaining WHERE filters. When a WHERE clause is present the
    // projection is converted here (needed for WHERE-search detection) and
    // cached so step 7 doesn't redo the same conversion.
    // Qualifiers were already stripped from `select` (and thus from
    // `effective_where`) by `strip_single_table_qualifiers` above, so the
    // WHERE expr here carries only bare column names.
    let mut cached_projection = None;
    let filters = match &effective_where {
        Some(expr) => {
            // Check for search-triggering functions in WHERE. The resolved
            // SELECT target list is threaded through so the search plan
            // self-describes its output columns.
            let where_projection = convert_projection(&select.projection, &scope)?;
            if let Some(plan) = try_extract_where_search(expr, table, functions, &where_projection)?
            {
                return Ok(PlannedSelect { plan, scope });
            }
            cached_projection = Some(where_projection);
            convert_where_to_filters(expr, &scope)?
        }
        None => Vec::new(),
    };

    // 6. Check for GROUP BY / aggregation.
    if has_aggregation(select, functions) {
        let mut plan = crate::planner::aggregate::plan_aggregate(
            select, table, &filters, &scope, functions, &temporal,
        )?;

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
                    volatile: false,
                }),
            );
            for sq in subquery_joins
                .iter()
                .filter(|sq| sq.join_type != JoinType::Cross)
            {
                base_input = Box::new(SqlPlan::Join {
                    left: base_input,
                    right: Box::new(sq.inner_plan.clone()),
                    on: sq.on.clone(),
                    join_type: sq.join_type,
                    condition: None,
                    limit: None,
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
                on: sq.on,
                join_type: sq.join_type,
                condition: None,
                limit: None,
                projection: Vec::new(),
                filters: Vec::new(),
            };
        }
        return Ok(PlannedSelect { plan, scope });
    }

    // 7. Convert projection (reuse the WHERE-search conversion if we already
    // did it in step 5, to avoid converting the same projection twice).
    let projection = match cached_projection {
        Some(p) => p,
        None => convert_projection(&select.projection, &scope)?,
    };

    // 8. Convert window functions (SELECT with OVER).
    let window_functions =
        crate::planner::window::extract_window_functions(select, functions, &scope)?;

    // 9. Build base scan plan.
    let scan_projection = if subquery_joins.is_empty() {
        projection.clone()
    } else {
        Vec::new()
    };

    // The enclosing query's ORDER BY / LIMIT are resolved here, before
    // `plan_scan`, because the engine rules decide the access path from them:
    // the document index-lookup rewrite must decline when a sort is requested,
    // and must carry the real row bound rather than a default. `apply_order_by`
    // and `apply_limit` re-derive the same values on the finished plan and
    // overwrite these fields with an identical result.
    //
    // A scan that is about to be wrapped in subquery joins takes neither:
    // ordering and bounding the join's input instead of its output would
    // answer the query from the wrong rows. Those clauses land on the join
    // itself downstream — the same reason `scan_projection` is empty here.
    let (sort_keys, limit, offset) = if subquery_joins.is_empty() {
        let (limit, offset) = tail.limit_offset()?;
        // ORDER BY resolves against the output names the SELECT list
        // introduces as well as the input columns.
        let order_scope = scope.with_output_names(super::select_output_aliases(&select.projection));
        (tail.sort_keys(&order_scope)?, limit, offset)
    } else {
        (Vec::new(), None, 0)
    };

    let rules = crate::engine_rules::resolve_engine_rules(table.info.engine);
    let mut plan = rules.plan_scan(crate::engine_rules::ScanParams {
        collection: table.name.clone(),
        alias: table.alias.clone(),
        filters,
        projection: scan_projection,
        sort_keys,
        limit,
        offset,
        distinct: select.distinct.is_some(),
        window_functions,
        indexes: table.info.indexes.clone(),
        temporal,
        bitemporal: table.info.bitemporal,
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
            on: sq.on,
            join_type: sq.join_type,
            condition: None,
            limit: None,
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

    Ok(PlannedSelect { plan, scope })
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
            && crate::aggregate_walk::contains_aggregate(expr, functions)
        {
            return true;
        }
    }
    false
}

/// Dispatch to the JOIN planner if the FROM contains joins.
fn try_plan_join(
    select: &Select,
    scope: &TableScope,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
) -> Result<Option<SqlPlan>> {
    if select.from.len() != 1 {
        return Ok(None);
    }
    let from = &select.from[0];
    if from.joins.is_empty() {
        return Ok(None);
    }
    crate::planner::join::plan_join_from_select(select, scope, catalog, functions, temporal)
}
