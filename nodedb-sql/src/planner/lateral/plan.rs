// SPDX-License-Identifier: Apache-2.0

//! LATERAL join planning: classify correlation shape and emit the appropriate
//! `SqlPlan::LateralTopK` or `SqlPlan::LateralLoop` variant.

use sqlparser::ast;

use super::correlation::analyse_lateral_where;
use super::subquery::{
    extract_inner_alias, extract_inner_collection, inner_non_correlated_filters, limit_from_query,
    reject_lateral_offset,
};
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::resolver::ColumnScope;
use crate::resolver::columns::{ResolvedTable, TableScope};
use crate::resolver::expr::convert_expr;
use crate::temporal::TemporalScope;
use crate::types::*;

/// The maximum outer-row count allowed for `LateralLoop` queries.
pub const LATERAL_LOOP_CAP: usize = 100_000;

/// Parameters for [`plan_lateral_join`].
pub struct LateralJoinArgs<'a> {
    /// Plan for the driving (outer) side.
    pub outer_plan: SqlPlan,
    /// Alias or name of the outer table for correlation detection.
    pub outer_alias: Option<String>,
    /// The LATERAL inner subquery.
    pub subquery: &'a ast::Query,
    /// Alias given to the LATERAL in the SQL (e.g. `x` in `LATERAL (...) x`).
    pub lateral_alias: &'a str,
    /// True when the enclosing join is LEFT JOIN LATERAL (outer rows
    /// preserved when inner produces no rows).
    pub left_join: bool,
    /// SELECT list projection to apply after the lateral.
    pub outer_projection: Vec<Projection>,
    /// The enclosing query's column namespace. The inner body nests inside it
    /// so a correlated reference to an outer relation resolves.
    pub outer_scope: &'a TableScope,
    pub catalog: &'a dyn SqlCatalog,
    pub functions: &'a FunctionRegistry,
    pub temporal: TemporalScope,
}

/// Plan a LATERAL subquery join.
///
/// Called when the right side of a JOIN (or a comma-separated FROM item) is a
/// `TableFactor::Derived { lateral: true, .. }`.
pub fn plan_lateral_join(args: LateralJoinArgs<'_>) -> Result<SqlPlan> {
    let LateralJoinArgs {
        outer_plan,
        outer_alias,
        subquery,
        lateral_alias,
        left_join,
        outer_projection,
        outer_scope,
        catalog,
        functions,
        temporal,
    } = args;
    let select = match subquery.body.as_ref() {
        sqlparser::ast::SetExpr::Select(s) => s,
        _ => {
            return Err(SqlError::Unsupported {
                detail: "LATERAL subquery body must be a SELECT".into(),
            });
        }
    };

    let outer_alias_str = outer_alias.as_deref().unwrap_or("").to_string();

    let analysis = analyse_lateral_where(subquery, &outer_alias_str);

    // The outer operand of a correlation predicate is stripped from the inner
    // WHERE before conversion, so it is checked here or nowhere.
    let outer_qualifier = outer_alias.as_deref();
    for key in &analysis.equi_keys {
        outer_scope.check_name(outer_qualifier, &key.outer_col)?;
    }
    for (_, outer_col) in &analysis.non_equi {
        outer_scope.check_name(outer_qualifier, outer_col)?;
    }

    // Determine if this is the equi-correlated + TopK shape:
    //   - At least one equi-key correlation.
    //   - A LIMIT k on the subquery.
    //   - No non-equi correlations (those require LateralLoop).
    let has_equi = !analysis.equi_keys.is_empty();
    let inner_limit = limit_from_query(subquery)?;
    reject_lateral_offset(subquery)?;
    let is_top_k = has_equi && inner_limit.is_some() && analysis.non_equi.is_empty();

    if is_top_k {
        plan_lateral_top_k(LateralTopKPlanArgs {
            outer_plan,
            outer_alias,
            select,
            subquery,
            equi_keys: analysis.equi_keys,
            inner_limit: inner_limit.expect("checked above"),
            lateral_alias,
            left_join,
            outer_projection,
            outer_scope,
            catalog,
            functions,
            temporal,
        })
    } else if has_equi && analysis.non_equi.is_empty() {
        // Equi-correlated, no LIMIT: rewrite as a regular hash join.
        //
        // The equi correlations become the join keys. The inner side is a bare
        // scan of the inner collection carrying only the residual (equi-stripped)
        // WHERE; the outer alias never leaks into inner name resolution. The join
        // executor scans the inner collection by name, so the join key columns
        // are available on the merged rows.
        let inner_plan = build_inner_scan(InnerScanArgs {
            select,
            residual_where: analysis.remaining,
            lateral_alias,
            catalog,
            temporal,
            outer_scope,
        })?;
        let equi_on: Vec<(String, String)> = analysis
            .equi_keys
            .into_iter()
            .map(|c| (c.outer_col, c.inner_col))
            .collect();
        Ok(SqlPlan::Join {
            left: Box::new(outer_plan),
            right: Box::new(inner_plan),
            on: equi_on,
            join_type: if left_join {
                JoinType::Left
            } else {
                JoinType::Inner
            },
            condition: None,
            limit: None,
            projection: outer_projection,
            filters: Vec::new(),
        })
    } else {
        // General correlation — LateralLoop.
        //
        // The inner side is a bare scan of the inner collection carrying the
        // residual WHERE (`analysis.remaining`): non-correlated predicates plus
        // any non-equi correlated predicates (e.g. `e.log_time > u.created_at`).
        // The latter lower to `*Column` scan filters (`GtColumn`, ...) whose
        // outer operand is bound per outer row by the Data Plane executor via
        // `bind_outer_values`. Equi correlations are applied separately through
        // `correlation_predicates`, so they are excluded from `remaining` and
        // not duplicated here. The subquery is not routed through `plan_query`
        // because its WHERE references the outer alias, which is not resolvable
        // in the inner FROM scope.
        let inner_plan = build_inner_scan(InnerScanArgs {
            select,
            residual_where: analysis.remaining,
            lateral_alias,
            catalog,
            temporal,
            outer_scope,
        })?;
        let correlation_predicates: Vec<(String, String)> = analysis
            .equi_keys
            .iter()
            .map(|c| (c.inner_col.clone(), c.outer_col.clone()))
            .collect();
        Ok(SqlPlan::LateralLoop {
            outer: Box::new(outer_plan),
            outer_alias,
            inner: Box::new(inner_plan),
            correlation_predicates,
            lateral_alias: lateral_alias.to_string(),
            projection: outer_projection,
            outer_row_cap: LATERAL_LOOP_CAP,
            left_join,
        })
    }
}

/// Parameters for [`build_inner_scan`].
struct InnerScanArgs<'a> {
    select: &'a sqlparser::ast::Select,
    residual_where: Option<ast::Expr>,
    lateral_alias: &'a str,
    catalog: &'a dyn SqlCatalog,
    temporal: TemporalScope,
    outer_scope: &'a TableScope,
}

/// Build the inner `SqlPlan::Scan` for a LATERAL join.
///
/// The scan carries the residual WHERE as filters. Correlated non-equi
/// predicates survive as column-vs-column comparisons and lower to
/// runtime-bound `*Column` filters downstream; the executor binds the outer
/// operand per outer row.
fn build_inner_scan(args: InnerScanArgs<'_>) -> Result<SqlPlan> {
    let InnerScanArgs {
        select,
        residual_where,
        lateral_alias,
        catalog,
        temporal,
        outer_scope,
    } = args;
    let inner_collection = extract_inner_collection(select)?;
    let inner_alias = extract_inner_alias(select)?;
    let inner_info = catalog
        .resolve_relation(nodedb_types::DatabaseId::DEFAULT, &inner_collection)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: inner_collection.clone(),
        })?;
    // A correlated residual predicate names the outer relation, so the inner
    // scope nests inside it.
    let inner_scope = TableScope::single(ResolvedTable {
        name: inner_collection.clone(),
        alias: inner_alias,
        info: inner_info.clone(),
    })?
    .nested_in(outer_scope.clone());
    let filters = match &residual_where {
        Some(expr) => crate::planner::select::convert_where_to_filters(expr, &inner_scope)?,
        None => Vec::new(),
    };
    // The lateral subquery's output relation is named by the LATERAL alias, and
    // the inner table alias is private to the subquery. Downstream the scan
    // alias qualifies the inner columns on a merged join row, so it carries the
    // LATERAL alias and `x.a` resolves the way `LateralLoop` and `LateralTopK`
    // already name their inner columns.
    Ok(SqlPlan::Scan {
        collection: inner_collection,
        alias: Some(lateral_alias.to_string()),
        engine: inner_info.engine,
        filters,
        projection: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
        window_functions: Vec::new(),
        temporal,
    })
}

/// Parameters for [`plan_lateral_top_k`].
struct LateralTopKPlanArgs<'a> {
    outer_plan: SqlPlan,
    outer_alias: Option<String>,
    select: &'a sqlparser::ast::Select,
    subquery: &'a ast::Query,
    equi_keys: Vec<super::correlation::CorrelationEq>,
    inner_limit: usize,
    lateral_alias: &'a str,
    left_join: bool,
    outer_projection: Vec<Projection>,
    outer_scope: &'a TableScope,
    catalog: &'a dyn SqlCatalog,
    functions: &'a FunctionRegistry,
    temporal: TemporalScope,
}

/// Plan the `LateralTopK` variant: equi-correlated + ORDER BY + LIMIT k.
fn plan_lateral_top_k(args: LateralTopKPlanArgs<'_>) -> Result<SqlPlan> {
    let LateralTopKPlanArgs {
        outer_plan,
        outer_alias,
        select,
        subquery,
        equi_keys,
        inner_limit,
        lateral_alias,
        left_join,
        outer_projection,
        outer_scope,
        catalog,
        functions,
        temporal,
    } = args;
    // Build a bare inner Scan without correlation filters (those are injected
    // at runtime per outer row).
    let inner_collection = extract_inner_collection(select)?;
    catalog
        .resolve_relation(nodedb_types::DatabaseId::DEFAULT, &inner_collection)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: inner_collection.clone(),
        })?;
    // The Top-K plan does not retain the inner alias, but it must still reject
    // malformed aliases before expressions referencing them are lowered.
    let _inner_alias = extract_inner_alias(select)?;
    let inner_scope = TableScope::resolve_from(catalog, functions, temporal, &select.from)?
        .nested_in(outer_scope.clone());
    let inner_filters =
        inner_non_correlated_filters(select, outer_alias.as_deref().unwrap_or(""), &inner_scope)?;

    // Extract ORDER BY from the inner subquery.
    // For LATERAL inner scans we only need simple column-expression sort keys;
    // the full search-trigger machinery (vector/hybrid search) is not applicable
    // here, so we convert expressions directly.
    let inner_order_by = if let Some(order_by) = &subquery.order_by {
        match &order_by.kind {
            ast::OrderByKind::Expressions(exprs) => exprs
                .iter()
                .map(|o| {
                    Ok(SortKey {
                        expr: convert_expr(&o.expr, &ColumnScope::Relations(&inner_scope))?,
                        ascending: o.options.asc.unwrap_or(true),
                        nulls_first: o
                            .options
                            .nulls_first
                            .unwrap_or(!o.options.asc.unwrap_or(true)),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            ast::OrderByKind::All(_) => Vec::new(),
        }
    } else {
        Vec::new()
    };

    let correlation_keys: Vec<(String, String)> = equi_keys
        .into_iter()
        .map(|c| (c.outer_col, c.inner_col))
        .collect();

    Ok(SqlPlan::LateralTopK {
        outer: Box::new(outer_plan),
        outer_alias,
        inner_collection,
        inner_filters,
        inner_order_by,
        inner_limit,
        correlation_keys,
        lateral_alias: lateral_alias.to_string(),
        projection: outer_projection,
        left_join,
    })
}
