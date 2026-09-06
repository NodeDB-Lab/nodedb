// SPDX-License-Identifier: Apache-2.0

//! Single SELECT statement planning (no UNION, no CTE wrapper).

use nodedb_types::DatabaseId;
use sqlparser::ast::{self, Select};

use super::derived_from::try_plan_derived_from;
use super::helpers::{convert_projection, convert_where_to_filters};
use super::query_tail::QueryTail;
use super::where_search::try_extract_where_search;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::planner::ast_helpers::strip_single_table_qualifiers;
use crate::planner::lateral::plan::{
    LateralJoinArgs, is_lateral_derived, lateral_alias_from_factor, plan_lateral_join,
    subquery_from_factor,
};
use crate::resolver::columns::TableScope;
use crate::temporal::TemporalScope;
use crate::types::*;

use super::validate_columns;

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
) -> Result<SqlPlan> {
    // 0. Intercept array table-valued functions before catalog resolution
    //    so a name like `ARRAY_SLICE` is not looked up as a collection.
    if let Some(plan) =
        crate::planner::array_fn::try_plan_array_table_fn(&select.from, catalog, temporal)?
    {
        return Ok(plan);
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
    if let Some(plan) = try_plan_derived_from(select, catalog, functions, temporal, tail)? {
        return Ok(plan);
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
            return Ok(plan);
        }
        let projection = convert_projection(&select.projection)?;
        let mut columns = Vec::new();
        let mut values = Vec::new();
        for (i, proj) in projection.iter().enumerate() {
            match proj {
                Projection::Computed { expr, alias } => {
                    columns.push(alias.clone());
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
        return Ok(SqlPlan::ConstantResult { columns, values });
    }

    // 3. Check for JOINs (including LATERAL).
    if let Some(plan) = try_plan_join(select, &scope, catalog, functions, temporal)? {
        return Ok(plan);
    }

    // 3b. Comma-LATERAL syntax: `FROM t, LATERAL (SELECT ...) x`.
    // sqlparser represents this as two TableWithJoins elements in `select.from`,
    // where the second has an empty joins list and its relation is Derived{lateral:true}.
    if select.from.len() == 2 && is_lateral_derived(&select.from[1].relation) {
        let outer_twj = &select.from[0];
        let lateral_twj = &select.from[1];

        // Build outer scan plan.
        let outer_alias = extract_table_alias_from_twj(outer_twj)?;
        let outer_collection =
            crate::parser::normalize::table_name_from_factor(&outer_twj.relation)?
                .map(|(n, _)| n)
                .ok_or_else(|| SqlError::Unsupported {
                    detail: "LATERAL: outer side must be a plain table".into(),
                })?;
        let outer_info = catalog
            .resolve_relation(DatabaseId::DEFAULT, &outer_collection)?
            .ok_or_else(|| SqlError::UnknownTable {
                name: outer_collection.clone(),
            })?;
        let outer_scan = SqlPlan::Scan {
            collection: outer_collection,
            alias: outer_alias.clone(),
            engine: outer_info.engine,
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal,
        };

        let lateral_alias = lateral_alias_from_factor(&lateral_twj.relation)?.ok_or_else(|| {
            SqlError::Unsupported {
                detail: "LATERAL subquery requires an alias (e.g. LATERAL (...) AS x)".into(),
            }
        })?;
        let subquery = subquery_from_factor(&lateral_twj.relation)
            .expect("is_lateral_derived guarantees Derived variant");
        let projection = convert_projection(&select.projection)?;
        return plan_lateral_join(LateralJoinArgs {
            outer_plan: outer_scan,
            outer_alias,
            subquery,
            lateral_alias: &lateral_alias,
            left_join: false, // comma-LATERAL is INNER (no LEFT semantics)
            outer_projection: projection,
            catalog,
            temporal,
        })
        .map(Ok)?;
    }

    // 4. Single-table query.
    let table = scope.single_table().ok_or_else(|| SqlError::Unsupported {
        detail: "multi-table FROM without JOIN".into(),
    })?;

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

    // 4a. Closed-schema existence gate: a reference to a column the
    // collection does not declare raises 42703 (undefined_column) at plan
    // time instead of planning as a field lookup that folds to NULL per row
    // — silently wrong `WHERE` row sets, no-op `ORDER BY`, zero-row writes.
    // Schemaless collections without a declared column list stay open and
    // keep the fold. GROUP BY / HAVING may name projection aliases, so the
    // alias set is computed once here and reused by the ORDER BY check.
    let projection_aliases = validate_columns::projection_alias_set(select);
    validate_columns::validate_select(select, table, &projection_aliases)?;

    // 4. Extract subqueries from WHERE and rewrite as semi/anti joins.
    let (subquery_joins, effective_where) = if let Some(expr) = &select.selection {
        let extraction =
            crate::planner::subquery::extract_subqueries(expr, catalog, functions, temporal)?;
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
            let where_projection = convert_projection(&select.projection)?;
            if let Some(plan) = try_extract_where_search(expr, table, functions, &where_projection)?
            {
                return Ok(plan);
            }
            cached_projection = Some(where_projection);
            convert_where_to_filters(expr)?
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
                on: vec![(sq.outer_column, sq.inner_column)],
                join_type: sq.join_type,
                condition: None,
                limit: None,
                projection: Vec::new(),
                filters: Vec::new(),
            };
        }
        return Ok(plan);
    }

    // 7. Convert projection (reuse the WHERE-search conversion if we already
    // did it in step 5, to avoid converting the same projection twice).
    let projection = match cached_projection {
        Some(p) => p,
        None => convert_projection(&select.projection)?,
    };

    // 8. Convert window functions (SELECT with OVER).
    let window_functions = crate::planner::window::extract_window_functions(select, functions)?;

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
        let keys = tail.sort_keys()?;
        // ORDER BY may name projection aliases or output ordinals; the key
        // expressions are validated here against columns + output names so a
        // typo'd sort column raises 42703 instead of silently no-oping.
        validate_columns::validate_sort_keys(&keys, table, &projection_aliases)?;
        (keys, limit, offset)
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
            on: vec![(sq.outer_column, sq.inner_column)],
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

/// Extract the alias from the first table in a `TableWithJoins`.
fn extract_table_alias_from_twj(twj: &sqlparser::ast::TableWithJoins) -> Result<Option<String>> {
    crate::parser::normalize::table_name_from_factor(&twj.relation)
        .map(|relation| relation.map(|(name, alias)| alias.unwrap_or(name)))
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
